package orm

import (
	"errors"
	"reflect"
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
)

func TestNetworkNativeCodecHostBitsAndRefusal(t *testing.T) {
	registry := pgtype.NewMap()
	for _, text := range []string{"192.168.1.9/24", "2001:db8::1234/48", "::ffff:192.0.2.1/128", "0.0.0.0/0"} {
		inet, err := ParseInet(text)
		if err != nil {
			t.Fatal(err)
		}
		for _, format := range []int16{pgtype.BinaryFormatCode, pgtype.TextFormatCode} {
			bytes, err := registry.Encode(pgtype.InetOID, format, inet, nil)
			if err != nil {
				t.Fatal(err)
			}
			var output Inet
			if err := registry.Scan(pgtype.InetOID, format, bytes, &output); err != nil || output != inet {
				t.Fatal("inet host bits/prefix loss", err)
			}
			var nullable *Inet
			if err := registry.Scan(pgtype.InetOID, format, bytes, &nullable); err != nil || nullable == nil || *nullable != inet {
				t.Fatal("nullable inet", err)
			}
			if err := registry.Scan(pgtype.InetOID, format, nil, &nullable); err != nil || nullable != nil {
				t.Fatal("SQL NULL", err)
			}
		}
	}
	for _, text := range []string{"192.168.1.9/24", "2001:db8::1/48", "fe80::1%en0", "", "10.0.0.1/33"} {
		if _, err := ParseCIDR(text); !errors.Is(err, ErrScalarValue) {
			t.Fatal("invalid or unmasked CIDR", text, err)
		}
	}
	for _, text := range []string{"fe80::1%en0", "fe80::%en0", "fe80::1%en0/64"} {
		if _, err := ParseInet(text); !errors.Is(err, ErrScalarValue) {
			t.Fatal("inet zone normalized away", err)
		}
		if _, err := ParseCIDR(text); !errors.Is(err, ErrScalarValue) {
			t.Fatal("cidr zone normalized away", err)
		}
	}
	cidr, err := ParseCIDR("2001:db8::/48")
	if err != nil {
		t.Fatal(err)
	}
	bytes, err := registry.Encode(pgtype.CIDROID, pgtype.BinaryFormatCode, cidr, nil)
	if err != nil {
		t.Fatal(err)
	}
	var decoded CIDR
	if err := registry.Scan(pgtype.CIDROID, pgtype.BinaryFormatCode, bytes, &decoded); err != nil || decoded != cidr {
		t.Fatal("CIDR codec", err)
	}
	if err := registry.Scan(pgtype.CIDROID, pgtype.BinaryFormatCode, nil, &decoded); !errors.Is(err, ErrScalarValue) {
		t.Fatal("nonnullable NULL", err)
	}
	if qualifiedCatalogCodec(reflect.TypeOf(Inet{}), catalogCodec{oid: pgtype.CIDROID, kind: "b"}) || qualifiedCatalogCodec(reflect.TypeOf(CIDR{}), catalogCodec{oid: pgtype.InetOID, kind: "b"}) {
		t.Fatal("inet/cidr identities interchanged")
	}
	if !errors.Is(validateScalarValue(Inet{}), ErrScalarValue) || !errors.Is(validateScalarValue(CIDR{}), ErrScalarValue) {
		t.Fatal("zero network accepted")
	}
}
