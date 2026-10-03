package orm

import (
	"net/netip"
	"strings"

	"github.com/jackc/pgx/v5/pgtype"
)

// Inet preserves both the address host bits and its prefix length. A nil *Inet
// represents SQL NULL; the zero value is invalid, never a default address.
type Inet struct{ prefix netip.Prefix }

// CIDR requires a network address with no host bits. Parsing never silently
// masks a supplied address. SQL NULL uses a nil *CIDR.
type CIDR struct{ prefix netip.Prefix }

func parseNetwork(text string) (netip.Prefix, error) {
	var prefix netip.Prefix
	var err error
	if strings.Contains(text, "/") {
		prefix, err = netip.ParsePrefix(text)
	} else {
		var address netip.Addr
		address, err = netip.ParseAddr(text)
		if err == nil {
			prefix = netip.PrefixFrom(address, address.BitLen())
		}
	}
	if err != nil || !prefix.IsValid() || prefix.Addr().Zone() != "" {
		return netip.Prefix{}, ErrScalarValue
	}
	return prefix, nil
}
func ParseInet(text string) (Inet, error) {
	prefix, err := parseNetwork(text)
	if err != nil {
		return Inet{}, err
	}
	return Inet{prefix}, nil
}
func ParseCIDR(text string) (CIDR, error) {
	prefix, err := parseNetwork(text)
	if err != nil || prefix != prefix.Masked() {
		return CIDR{}, ErrScalarValue
	}
	return CIDR{prefix}, nil
}
func (v Inet) Prefix() netip.Prefix { return v.prefix }
func (v CIDR) Prefix() netip.Prefix { return v.prefix }
func (v Inet) String() string {
	if !v.prefix.IsValid() {
		return ""
	}
	return v.prefix.String()
}
func (v CIDR) String() string {
	if !v.prefix.IsValid() {
		return ""
	}
	return v.prefix.String()
}
func (v Inet) ormScalarType() bool  { return true }
func (v CIDR) ormScalarType() bool  { return true }
func (v Inet) ormScalarValid() bool { return v.prefix.IsValid() && v.prefix.Addr().Zone() == "" }
func (v CIDR) ormScalarValid() bool {
	return v.prefix.IsValid() && v.prefix.Addr().Zone() == "" && v.prefix == v.prefix.Masked()
}
func (v Inet) NetipPrefixValue() (netip.Prefix, error) {
	if !v.ormScalarValid() {
		return netip.Prefix{}, ErrScalarValue
	}
	return v.prefix, nil
}
func (v CIDR) NetipPrefixValue() (netip.Prefix, error) {
	if !v.ormScalarValid() {
		return netip.Prefix{}, ErrScalarValue
	}
	return v.prefix, nil
}
func (v *Inet) ScanNetipPrefix(prefix netip.Prefix) error {
	next := Inet{prefix}
	if !next.ormScalarValid() {
		return ErrScalarValue
	}
	*v = next
	return nil
}
func (v *CIDR) ScanNetipPrefix(prefix netip.Prefix) error {
	next := CIDR{prefix}
	if !next.ormScalarValid() {
		return ErrScalarValue
	}
	*v = next
	return nil
}

var _ pgtype.NetipPrefixValuer = Inet{}
var _ pgtype.NetipPrefixValuer = CIDR{}
var _ pgtype.NetipPrefixScanner = (*Inet)(nil)
var _ pgtype.NetipPrefixScanner = (*CIDR)(nil)
