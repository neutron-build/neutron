package orm

import (
	"errors"
	"net/netip"
	"reflect"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
)

func TestNetworkArrayAdmissionRoundTripsAndRefusal(t *testing.T) {
	ipv4Host, err := ParseInet("192.168.1.9/24")
	if err != nil {
		t.Fatal(err)
	}
	ipv6Host, err := ParseInet("2001:db8::1234/48")
	if err != nil {
		t.Fatal(err)
	}
	mapped, err := ParseInet("::ffff:192.0.2.1/128")
	if err != nil {
		t.Fatal(err)
	}
	dims := []pgtype.ArrayDimension{{Length: 2, LowerBound: -3}, {Length: 2, LowerBound: 7}}
	hosts, err := NewArray(dims, []*Inet{&ipv4Host, nil, &ipv6Host, &mapped})
	if err != nil {
		t.Fatal(err)
	}
	if hosts.Index(0) != ipv4Host || hosts.Index(1) != nil {
		t.Fatal("nullable network element not flattened")
	}
	registry := pgtype.NewMap()
	for _, format := range []int16{pgtype.BinaryFormatCode, pgtype.TextFormatCode} {
		encoded, err := registry.Encode(pgtype.InetArrayOID, format, hosts, nil)
		if err != nil {
			t.Fatal(err)
		}
		var decoded Array[*Inet]
		if err := registry.Scan(pgtype.InetArrayOID, format, encoded, scanDestination(reflect.ValueOf(&decoded).Elem())); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(decoded.Dimensions(), dims) || !reflect.DeepEqual(decoded.Elements(), hosts.Elements()) {
			t.Fatal("network array dimension/host-bit/NULL loss")
		}
		var strict Array[Inet]
		if err := registry.Scan(pgtype.InetArrayOID, format, encoded, scanDestination(reflect.ValueOf(&strict).Elem())); !errors.Is(err, ErrScalarValue) {
			t.Fatal("NULL element admitted to nonnullable network elements", err)
		}
		var nullable *Array[*Inet]
		if err := registry.Scan(pgtype.InetArrayOID, format, encoded, scanDestination(reflect.ValueOf(&nullable).Elem())); err != nil || nullable == nil {
			t.Fatal("nullable network array column", err)
		}
		if err := registry.Scan(pgtype.InetArrayOID, format, nil, scanDestination(reflect.ValueOf(&nullable).Elem())); err != nil || nullable != nil {
			t.Fatal("network array SQL NULL", err)
		}
	}
	empty, err := NewArray[*Inet](nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := registry.Encode(pgtype.InetArrayOID, pgtype.BinaryFormatCode, empty, nil)
	if err != nil {
		t.Fatal(err)
	}
	var decodedEmpty Array[*Inet]
	if err := registry.Scan(pgtype.InetArrayOID, pgtype.BinaryFormatCode, encoded, scanDestination(reflect.ValueOf(&decodedEmpty).Elem())); err != nil || len(decodedEmpty.Dimensions()) != 0 || len(decodedEmpty.Elements()) != 0 {
		t.Fatal("empty network array conflated SQL NULL", err)
	}
	ipv4Net, err := ParseCIDR("0.0.0.0/0")
	if err != nil {
		t.Fatal(err)
	}
	ipv6Net, err := ParseCIDR("2001:db8::/48")
	if err != nil {
		t.Fatal(err)
	}
	networks, err := NewArray([]pgtype.ArrayDimension{{Length: 2, LowerBound: 4}}, []CIDR{ipv4Net, ipv6Net})
	if err != nil {
		t.Fatal(err)
	}
	for _, format := range []int16{pgtype.BinaryFormatCode, pgtype.TextFormatCode} {
		encoded, err := registry.Encode(pgtype.CIDRArrayOID, format, networks, nil)
		if err != nil {
			t.Fatal(err)
		}
		var decoded Array[CIDR]
		if err := registry.Scan(pgtype.CIDRArrayOID, format, encoded, scanDestination(reflect.ValueOf(&decoded).Elem())); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(decoded.Dimensions(), networks.Dimensions()) || !reflect.DeepEqual(decoded.Elements(), networks.Elements()) {
			t.Fatal("cidr array dimension/prefix loss")
		}
	}
	hostBits, err := netip.ParsePrefix("192.168.1.9/24")
	if err != nil {
		t.Fatal(err)
	}
	var hostBitCIDR CIDR
	if err := hostBitCIDR.ScanNetipPrefix(hostBits); !errors.Is(err, ErrScalarValue) {
		t.Fatal("cidr element host bits masked", err)
	}
	if _, err := NewArray([]pgtype.ArrayDimension{{Length: 1}}, []Inet{{}}); !errors.Is(err, ErrScalarValue) {
		t.Fatal("zero inet element admitted", err)
	}
	if _, err := NewArray([]pgtype.ArrayDimension{{Length: 1}}, []CIDR{{}}); !errors.Is(err, ErrScalarValue) {
		t.Fatal("zero cidr element admitted", err)
	}
	if _, err := NewArray([]pgtype.ArrayDimension{{Length: 2}}, []*Inet{&ipv4Host}); err == nil {
		t.Fatal("network cardinality mismatch accepted")
	}
	if _, err := NewTable[struct {
		Hosts    Array[Inet]   `db:"hosts"`
		Networks Array[CIDR]   `db:"networks"`
		Optional *Array[*CIDR] `db:"optional,nullable"`
	}]("public", "network_array_shape"); err != nil {
		t.Fatal("network array mapping refused", err)
	}
	if !qualifiedCatalogCodec(reflect.TypeOf(Array[Inet]{}), catalogCodec{oid: pgtype.InetArrayOID, element: pgtype.InetOID, kind: "b"}) ||
		!qualifiedCatalogCodec(reflect.TypeOf(Array[CIDR]{}), catalogCodec{oid: pgtype.CIDRArrayOID, element: pgtype.CIDROID, kind: "b"}) {
		t.Fatal("native network array element OID refused")
	}
	if qualifiedCatalogCodec(reflect.TypeOf(Array[Inet]{}), catalogCodec{oid: pgtype.InetArrayOID, element: pgtype.CIDROID, kind: "b"}) ||
		qualifiedCatalogCodec(reflect.TypeOf(Array[CIDR]{}), catalogCodec{oid: pgtype.CIDRArrayOID, element: pgtype.InetOID, kind: "b"}) {
		t.Fatal("inet[]/cidr[] element identities interchanged")
	}
}

type arrayNetworkLiveModel struct {
	ID       int64         `db:"id"`
	Hosts    Array[*Inet]  `db:"hosts"`
	Networks Array[CIDR]   `db:"networks"`
	Optional *Array[*Inet] `db:"optional,nullable"`
}

func TestPostgresNetworkArraysDimensionsNullElementsAndCatalogIdentity(t *testing.T) {
	ctx, _, admin, records := liveTransactionSetup(t)
	schemaSQL := strings.TrimSuffix(records, ".records")
	schema := strings.ReplaceAll(strings.TrimSuffix(strings.TrimPrefix(schemaSQL, `"`), `"`), `""`, `"`)
	name := schemaSQL + ".network_arrays"
	if _, err := admin.Exec(ctx, "CREATE TABLE "+name+` (id bigint PRIMARY KEY,hosts inet[] NOT NULL,networks cidr[] NOT NULL,optional inet[]); INSERT INTO `+name+` VALUES (1,'[1:3]={"192.168.1.9/24",NULL,"2001:db8::1234/48"}','{2001:db8::/48,192.0.2.0/24}',NULL),(2,'{}','{}','{}')`); err != nil {
		t.Fatal(err)
	}
	table, err := NewPostgresTable[arrayNetworkLiveModel](ctx, admin, schema, "network_arrays")
	if err != nil {
		t.Fatal(err)
	}
	id, err := NewColumn[arrayNetworkLiveModel, int64](table, "ID")
	if err != nil {
		t.Fatal(err)
	}
	hosts, err := NewColumn[arrayNetworkLiveModel, Array[*Inet]](table, "Hosts")
	if err != nil {
		t.Fatal(err)
	}
	networks, err := NewColumn[arrayNetworkLiveModel, Array[CIDR]](table, "Networks")
	if err != nil {
		t.Fatal(err)
	}
	optional, err := NewColumn[arrayNetworkLiveModel, *Array[*Inet]](table, "Optional")
	if err != nil {
		t.Fatal(err)
	}
	loaded, err := Select(ctx, admin, table, Query[arrayNetworkLiveModel]{}.OrderBy(id.Asc()))
	if err != nil || len(loaded) != 2 {
		t.Fatal("native network array read", err)
	}
	first, second := loaded[0], loaded[1]
	firstHosts := first.Hosts.Elements()
	firstNetworks := first.Networks.Elements()
	if !reflect.DeepEqual(first.Hosts.Dimensions(), []pgtype.ArrayDimension{{Length: 3, LowerBound: 1}}) || len(firstHosts) != 3 || firstHosts[0].String() != "192.168.1.9/24" || firstHosts[1] != nil || firstHosts[2].String() != "2001:db8::1234/48" {
		t.Fatal("network array dimension/host bits/IPv6/NULL element loss")
	}
	if len(firstNetworks) != 2 || firstNetworks[0].String() != "2001:db8::/48" || firstNetworks[1].String() != "192.0.2.0/24" {
		t.Fatal("cidr array prefix loss")
	}
	if first.Optional != nil || second.Optional == nil || len(second.Optional.Elements()) != 0 || len(second.Hosts.Dimensions()) != 0 || len(second.Hosts.Elements()) != 0 || len(second.Networks.Elements()) != 0 {
		t.Fatal("empty network array vs SQL NULL loss")
	}
	// Independent native oracle does not call the ORM scanner or compiler.
	var nativeDims string
	var nullElement bool
	if err := admin.QueryRow(ctx, "SELECT array_dims(hosts),hosts[2] IS NULL FROM "+name+" WHERE id=1").Scan(&nativeDims, &nullElement); err != nil {
		t.Fatal(err)
	}
	if nativeDims != "[1:3]" || !nullElement {
		t.Fatal("independent native array oracle mismatch", nativeDims, nullElement)
	}
	host, err := ParseInet("192.0.2.1/24")
	if err != nil {
		t.Fatal(err)
	}
	six, err := ParseInet("2001:db8::1/64")
	if err != nil {
		t.Fatal(err)
	}
	writtenHosts, err := NewArray([]pgtype.ArrayDimension{{Length: 3, LowerBound: 0}}, []*Inet{&host, nil, &six})
	if err != nil {
		t.Fatal(err)
	}
	ten, err := ParseCIDR("10.0.0.0/8")
	if err != nil {
		t.Fatal(err)
	}
	v6Net, err := ParseCIDR("2001:db8::/32")
	if err != nil {
		t.Fatal(err)
	}
	writtenNetworks, err := NewArray([]pgtype.ArrayDimension{{Length: 2, LowerBound: 0}}, []CIDR{ten, v6Net})
	if err != nil {
		t.Fatal(err)
	}
	result, err := InsertOne(ctx, admin, table, Set(id, Some(int64(3))), Set(hosts, Some(writtenHosts)), Set(networks, Some(writtenNetworks)), Set(optional, Some(&writtenHosts)))
	if err != nil {
		t.Fatal("native network array write", err)
	}
	if !reflect.DeepEqual(result.Hosts.Dimensions(), writtenHosts.Dimensions()) || !reflect.DeepEqual(result.Hosts.Elements(), writtenHosts.Elements()) || !reflect.DeepEqual(result.Networks.Dimensions(), writtenNetworks.Dimensions()) || !reflect.DeepEqual(result.Networks.Elements(), writtenNetworks.Elements()) || result.Optional == nil || !reflect.DeepEqual(result.Optional.Elements(), writtenHosts.Elements()) {
		t.Fatal("network array RETURNING shape/NULL loss")
	}
	var hostsText, networksText string
	if err := admin.QueryRow(ctx, "SELECT hosts::text,networks::text FROM "+name+" WHERE id=3").Scan(&hostsText, &networksText); err != nil {
		t.Fatal(err)
	}
	if hostsText != `[0:2]={192.0.2.1/24,NULL,2001:db8::1/64}` || networksText != `[0:1]={10.0.0.0/8,2001:db8::/32}` {
		t.Fatal("independent native network write oracle", hostsText, networksText)
	}
	type wrongIdentity struct {
		ID       int64         `db:"id"`
		Hosts    Array[CIDR]   `db:"hosts"`
		Networks Array[CIDR]   `db:"networks"`
		Optional *Array[*CIDR] `db:"optional,nullable"`
	}
	if _, err := NewPostgresTable[wrongIdentity](ctx, admin, schema, "network_arrays"); !errors.Is(err, ErrCodecUnsupported) {
		t.Fatal("inet[] admitted as cidr[]", err)
	}
}
