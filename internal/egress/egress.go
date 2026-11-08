// Package egress decides which addresses the gateway may connect to.
// Customers register any URL they like; a URL whose host resolves to the
// gateway's own network (loopback, private ranges, link-local, the cloud
// metadata service) would turn the gateway into a signed proxy into that
// network. The Dialer resolves a host, checks every address against the
// policy and connects to the address it checked, so a DNS answer that
// changes between the check and the connection cannot be used to slip
// past it.
package egress

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"strconv"
	"time"
)

// ErrDenied marks a destination the policy refuses. It is the kind of
// failure a retry would not fix.
var ErrDenied = errors.New("destination not allowed")

// denied is what no public webhook receiver lives on. Addresses are
// checked after unmapping IPv4-in-IPv6, so ::ffff:127.0.0.1 is loopback.
var denied = []netip.Prefix{
	netip.MustParsePrefix("0.0.0.0/8"),       // "this" network
	netip.MustParsePrefix("10.0.0.0/8"),      // private
	netip.MustParsePrefix("100.64.0.0/10"),   // carrier-grade NAT
	netip.MustParsePrefix("127.0.0.0/8"),     // loopback
	netip.MustParsePrefix("169.254.0.0/16"),  // link-local, cloud metadata
	netip.MustParsePrefix("172.16.0.0/12"),   // private
	netip.MustParsePrefix("192.0.0.0/24"),    // IETF protocol assignments
	netip.MustParsePrefix("192.168.0.0/16"),  // private
	netip.MustParsePrefix("198.18.0.0/15"),   // benchmarking
	netip.MustParsePrefix("224.0.0.0/4"),     // multicast
	netip.MustParsePrefix("240.0.0.0/4"),     // reserved, broadcast
	netip.MustParsePrefix("::/128"),          // unspecified
	netip.MustParsePrefix("::1/128"),         // loopback
	netip.MustParsePrefix("64:ff9b::/96"),    // NAT64: an IPv4 in disguise
	netip.MustParsePrefix("fc00::/7"),        // unique local
	netip.MustParsePrefix("fe80::/10"),       // link-local
	netip.MustParsePrefix("ff00::/8"),        // multicast
	netip.MustParsePrefix("2001:db8::/32"),   // documentation
	netip.MustParsePrefix("2002::/16"),       // 6to4: carries an IPv4
	netip.MustParsePrefix("2001:0000::/32"),  // Teredo: carries an IPv4
	netip.MustParsePrefix("::ffff:0:0/96"),   // IPv4-mapped, if unmapping was skipped
	netip.MustParsePrefix("64:ff9b:1::/48"),  // local-use NAT64
	netip.MustParsePrefix("100::/64"),        // discard
	netip.MustParsePrefix("2001:2::/48"),     // benchmarking
	netip.MustParsePrefix("2001:10::/28"),    // ORCHID
	netip.MustParsePrefix("2001:20::/28"),    // ORCHIDv2
	netip.MustParsePrefix("3fff::/20"),       // documentation
	netip.MustParsePrefix("5f00::/16"),       // segment routing
	netip.MustParsePrefix("fec0::/10"),       // site-local, deprecated
	netip.MustParsePrefix("192.88.99.0/24"),  // 6to4 relay anycast
	netip.MustParsePrefix("198.51.100.0/24"), // documentation
	netip.MustParsePrefix("203.0.113.0/24"),  // documentation
	netip.MustParsePrefix("192.0.2.0/24"),    // documentation
}

// Policy says which destinations are allowed. The zero Policy allows
// every public address on any port and denies the ranges above.
type Policy struct {
	// Allow lists networks that are reachable even though they are in a
	// denied range: a private receiver in the same VPC, or loopback in
	// development and tests. Use it narrowly; each entry is a hole in
	// the guard.
	Allow []netip.Prefix
	// Ports, when set, is the only ports a receiver may listen on; empty
	// allows any port.
	Ports []int
}

// Check reports whether a connection to addr (an IP) on port is allowed.
func (p Policy) Check(addr netip.Addr, port int) error {
	if len(p.Ports) > 0 {
		ok := false
		for _, allowed := range p.Ports {
			if port == allowed {
				ok = true
				break
			}
		}
		if !ok {
			return fmt.Errorf("%w: port %d", ErrDenied, port)
		}
	}
	addr = addr.Unmap()
	if !addr.IsValid() {
		return fmt.Errorf("%w: invalid address", ErrDenied)
	}
	for _, a := range p.Allow {
		if a.Contains(addr) {
			return nil
		}
	}
	if !addr.IsGlobalUnicast() {
		return fmt.Errorf("%w: %s is not a global unicast address", ErrDenied, addr)
	}
	for _, d := range denied {
		if d.Contains(addr) {
			return fmt.Errorf("%w: %s is in %s", ErrDenied, addr, d)
		}
	}
	return nil
}

// Resolver looks hosts up; *net.Resolver is the real one.
type Resolver interface {
	LookupNetIP(ctx context.Context, network, host string) ([]netip.Addr, error)
}

// Dialer resolves, checks and connects. It is what an http.Transport's
// DialContext should be for requests to customer-controlled hosts.
type Dialer struct {
	Policy   Policy
	Resolver Resolver
	// Timeout bounds the connection to each candidate address.
	Timeout time.Duration
}

// DialContext resolves the host in addr ("host:port"), drops the
// addresses the policy refuses and connects to the first of the rest
// that answers. Nothing is connected to that was not checked: the
// connection is made to the checked IP, never to the host name again.
// A host with no allowed address fails with ErrDenied.
func (d *Dialer) DialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	if network != "tcp" && network != "tcp4" && network != "tcp6" {
		return nil, fmt.Errorf("%w: network %s", ErrDenied, network)
	}
	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, fmt.Errorf("egress: %w", err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil || port < 1 || port > 65535 {
		return nil, fmt.Errorf("%w: port %q", ErrDenied, portStr)
	}

	var addrs []netip.Addr
	if ip, err := netip.ParseAddr(host); err == nil {
		addrs = []netip.Addr{ip}
	} else {
		resolver := d.Resolver
		if resolver == nil {
			resolver = net.DefaultResolver
		}
		lookupNet := "ip"
		switch network {
		case "tcp4":
			lookupNet = "ip4"
		case "tcp6":
			lookupNet = "ip6"
		}
		addrs, err = resolver.LookupNetIP(ctx, lookupNet, host)
		if err != nil {
			return nil, fmt.Errorf("egress: resolve %s: %w", host, err)
		}
	}
	if len(addrs) == 0 {
		return nil, fmt.Errorf("egress: resolve %s: no addresses", host)
	}

	var (
		dialer  = net.Dialer{Timeout: d.Timeout}
		denial  error
		dialErr error
	)
	for _, ip := range addrs {
		if err := d.Policy.Check(ip, port); err != nil {
			denial = err
			continue
		}
		conn, err := dialer.DialContext(ctx, network, net.JoinHostPort(ip.Unmap().String(), portStr))
		if err == nil {
			return conn, nil
		}
		dialErr = err
		if ctx.Err() != nil {
			break
		}
	}
	if dialErr != nil {
		return nil, dialErr
	}
	return nil, fmt.Errorf("egress: %s: %w", host, denial)
}
