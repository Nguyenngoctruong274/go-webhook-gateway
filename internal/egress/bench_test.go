package egress

import (
	"net/netip"
	"testing"
)

// BenchmarkCheck is the cost the guard adds to every new connection
// before it dials: a linear walk of the denied ranges.
func BenchmarkCheck(b *testing.B) {
	p := Policy{}
	public := netip.MustParseAddr("93.184.216.34")
	b.ReportAllocs()
	for b.Loop() {
		if err := p.Check(public, 443); err != nil {
			b.Fatal(err)
		}
	}
}
