package orm

import (
	"errors"
	"strings"
	"testing"
)

type networkLiveModel struct {
	ID       int64 `db:"id"`
	Address  Inet  `db:"address"`
	Network  CIDR  `db:"network"`
	Optional *Inet `db:"optional,nullable"`
}

func TestPostgresNetworkHostBitsPrefixesAndCatalogIdentity(t *testing.T) {
	ctx, _, admin, records := liveTransactionSetup(t)
	schemaSQL := strings.TrimSuffix(records, ".records")
	schema := strings.ReplaceAll(strings.TrimSuffix(strings.TrimPrefix(schemaSQL, `"`), `"`), `""`, `"`)
	name := schemaSQL + ".network_values"
	if _, err := admin.Exec(ctx, "CREATE TABLE "+name+" (id bigint PRIMARY KEY,address inet NOT NULL,network cidr NOT NULL,optional inet); INSERT INTO "+name+" VALUES (1,'192.168.1.9/24','2001:db8::/48',NULL)"); err != nil {
		t.Fatal(err)
	}
	table, err := NewPostgresTable[networkLiveModel](ctx, admin, schema, "network_values")
	if err != nil {
		t.Fatal(err)
	}
	loaded, err := SelectOne(ctx, admin, table, Query[networkLiveModel]{})
	if err != nil {
		t.Fatal(err)
	}
	var nativeAddress, nativeNetwork string
	if err := admin.QueryRow(ctx, "SELECT address::text,network::text FROM "+name+" WHERE id=1").Scan(&nativeAddress, &nativeNetwork); err != nil {
		t.Fatal(err)
	}
	if loaded.Address.String() != nativeAddress || loaded.Network.String() != nativeNetwork || loaded.Optional != nil {
		t.Fatal("native network oracle")
	}
	id, _ := NewColumn[networkLiveModel, int64](table, "ID")
	address, _ := NewColumn[networkLiveModel, Inet](table, "Address")
	network, _ := NewColumn[networkLiveModel, CIDR](table, "Network")
	optional, _ := NewColumn[networkLiveModel, *Inet](table, "Optional")
	inet, err := ParseInet("2001:db8::1234/48")
	if err != nil {
		t.Fatal(err)
	}
	cidr, err := ParseCIDR("192.0.2.0/24")
	if err != nil {
		t.Fatal(err)
	}
	written, err := InsertOne(ctx, admin, table, Set(id, Some(int64(2))), Set(address, Some(inet)), Set(network, Some(cidr)), Set(optional, Some(&inet)))
	if err != nil || written.Address != inet || written.Network != cidr || written.Optional == nil || *written.Optional != inet {
		t.Fatal("native round trip", err)
	}
	if err := admin.QueryRow(ctx, "SELECT address::text,network::text FROM "+name+" WHERE id=2").Scan(&nativeAddress, &nativeNetwork); err != nil {
		t.Fatal(err)
	}
	if nativeAddress != inet.String() || nativeNetwork != cidr.String() {
		t.Fatal("native write oracle")
	}
	type wrongIdentity struct {
		ID       int64 `db:"id"`
		Address  CIDR  `db:"address"`
		Network  CIDR  `db:"network"`
		Optional *Inet `db:"optional,nullable"`
	}
	if _, err := NewPostgresTable[wrongIdentity](ctx, admin, schema, "network_values"); !errors.Is(err, ErrCodecUnsupported) {
		t.Fatal("inet admitted as cidr", err)
	}
}
