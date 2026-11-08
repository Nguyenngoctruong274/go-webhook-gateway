package egress

import (
	"context"
	"errors"
	"net"
	"net/netip"
	"strings"
	"testing"
	"time"
)

func TestPolicyCheck(t *testing.T) {
	loopback := Policy{Allow: []netip.Prefix{netip.MustParsePrefix("127.0.0.0/8")}}
	tests := []struct {
		addr   string
		policy Policy
		deny   bool
	}{
		{"93.184.216.34", Policy{}, false},
		{"2606:2800:220:1:248:1893:25c8:1946", Policy{}, false},
		{"127.0.0.1", Policy{}, true},
		{"127.0.0.1", loopback, false},
		{"::ffff:127.0.0.1", Policy{}, true},
		{"::ffff:127.0.0.1", loopback, false}, // unmapped before the allow list too
		{"10.1.2.3", Policy{}, true},
		{"172.16.5.5", Policy{}, true},
		{"172.32.0.1", Policy{}, false}, // just outside 172.16/12
		{"192.168.1.1", Policy{}, true},
		{"169.254.169.254", Policy{}, true}, // cloud metadata
		{"169.254.169.254", loopback, true},
		{"100.64.0.1", Policy{}, true},
		{"0.0.0.0", Policy{}, true},
		{"255.255.255.255", Policy{}, true},
		{"224.0.0.1", Policy{}, true},
		{"::1", Policy{}, true},
		{"::", Policy{}, true},
		{"fd12::1", Policy{}, true},
		{"fe80::1", Policy{}, true},
		{"64:ff9b::7f00:1", Policy{}, true}, // NAT64 loopback
		{"2002:7f00:1::1", Policy{}, true},  // 6to4 loopback
		{"2001:db8::1", Policy{}, true},
		{"8.8.8.8", Policy{Ports: []int{443}}, true},
	}
	for _, tt := range tests {
		port := 443
		if len(tt.policy.Ports) > 0 {
			port = 80
		}
		err := tt.policy.Check(netip.MustParseAddr(tt.addr), port)
		if (err != nil) != tt.deny {
			t.Errorf("Check(%s, %d) with %+v = %v, want deny=%t", tt.addr, port, tt.policy, err, tt.deny)
		}
		if err != nil && !errors.Is(err, ErrDenied) {
			t.Errorf("Check(%s) error %v is not ErrDenied", tt.addr, err)
		}
	}
	if err := (Policy{Ports: []int{80, 443}}).Check(netip.MustParseAddr("8.8.8.8"), 443); err != nil {
		t.Fatalf("allowed port refused: %v", err)
	}
}

// fakeResolver answers from a table; hosts not in it do not resolve.
type fakeResolver struct {
	table map[string][]netip.Addr
	calls int
}

func (f *fakeResolver) LookupNetIP(_ context.Context, _, host string) ([]netip.Addr, error) {
	f.calls++
	addrs, ok := f.table[host]
	if !ok {
		return nil, &net.DNSError{Err: "no such host", Name: host, IsNotFound: true}
	}
	return addrs, nil
}

func listen(t *testing.T) (net.Listener, netip.Addr, string) {
	t.Helper()
	l, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	ap := netip.MustParseAddrPort(l.Addr().String())
	return l, ap.Addr(), ap.String()
}

func TestDialerConnectsToTheCheckedAddressOnly(t *testing.T) {
	l, ip, addr := listen(t)
	_, port, _ := net.SplitHostPort(addr)
	accepted := make(chan struct{}, 1)
	go func() {
		if c, err := l.Accept(); err == nil {
			c.Close()
			accepted <- struct{}{}
		}
	}()

	res := &fakeResolver{table: map[string][]netip.Addr{
		"receiver.example": {netip.MustParseAddr("10.0.0.9"), ip}, // one private, one allowed
		"evil.example":     {netip.MustParseAddr("169.254.169.254"), netip.MustParseAddr("127.0.0.1")},
	}}
	d := &Dialer{
		Policy:   Policy{Allow: []netip.Prefix{netip.MustParsePrefix("127.0.0.1/32")}},
		Resolver: res,
		Timeout:  time.Second,
	}

	conn, err := d.DialContext(context.Background(), "tcp", "receiver.example:"+port)
	if err != nil {
		t.Fatalf("dial through the allowed address: %v", err)
	}
	conn.Close()
	select {
	case <-accepted:
	case <-time.After(time.Second):
		t.Fatal("the listener never saw the connection")
	}
	if res.calls != 1 {
		t.Fatalf("%d lookups for one dial, want 1: the checked address must be the dialled one", res.calls)
	}

	// With the loopback allowed, evil.example's metadata address is
	// skipped and its loopback address is tried: the dial error, not a
	// denial, is what comes back (nobody listens on port 1).
	_, err = d.DialContext(context.Background(), "tcp", "evil.example:1")
	if err == nil || errors.Is(err, ErrDenied) || !strings.Contains(err.Error(), "refused") {
		t.Fatalf("evil.example with loopback allowed: %v, want a connection refused", err)
	}
	strict := &Dialer{Resolver: res, Timeout: time.Second}
	_, err = strict.DialContext(context.Background(), "tcp", "evil.example:"+port)
	if !errors.Is(err, ErrDenied) {
		t.Fatalf("strict policy let evil.example through: %v", err)
	}
	_, err = strict.DialContext(context.Background(), "tcp", "receiver.example:"+port)
	if !errors.Is(err, ErrDenied) {
		t.Fatalf("strict policy let the loopback through: %v", err)
	}

	// Literal IPs skip the resolver and are checked the same way.
	before := res.calls
	_, err = strict.DialContext(context.Background(), "tcp", "127.0.0.1:"+port)
	if !errors.Is(err, ErrDenied) || res.calls != before {
		t.Fatalf("literal loopback: %v (lookups %d)", err, res.calls-before)
	}
	_, err = strict.DialContext(context.Background(), "tcp", "[::ffff:127.0.0.1]:"+port)
	if !errors.Is(err, ErrDenied) {
		t.Fatalf("mapped loopback: %v", err)
	}
}

func TestDialerRefusesOddInputs(t *testing.T) {
	d := &Dialer{Resolver: &fakeResolver{table: map[string][]netip.Addr{}}}
	for _, tt := range []struct{ network, addr string }{
		{"udp", "8.8.8.8:53"},
		{"tcp", "8.8.8.8"},
		{"tcp", "8.8.8.8:0"},
		{"tcp", "8.8.8.8:70000"},
		{"tcp", "nowhere.example:443"},
	} {
		if _, err := d.DialContext(context.Background(), tt.network, tt.addr); err == nil {
			t.Errorf("%s %s accepted", tt.network, tt.addr)
		}
	}
}

func TestDialerHonoursCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	d := &Dialer{Timeout: 10 * time.Second, Policy: Policy{Allow: []netip.Prefix{netip.MustParsePrefix("198.51.100.0/24")}}}
	// A blackhole address: the dial would hang without the cancelled ctx.
	start := time.Now()
	_, err := d.DialContext(ctx, "tcp", "198.51.100.1:443")
	if err == nil || errors.Is(err, ErrDenied) || time.Since(start) > time.Second {
		t.Fatalf("dial with a cancelled context: %v after %v", err, time.Since(start))
	}
}
