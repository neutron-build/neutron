package model

import (
	"neutron.local/polyglot/go-codegen-spike/typed"
	"reflect"
	"testing"
)

func TestTypedColumnsAndProjection(t *testing.T) {
	if err := ValidateUserMetadata(); err != nil {
		t.Fatal(err)
	}
	columns := UserColumns()
	rows := []User{{ID: 1, Active: false, Score: 0, Name: ""}, {ID: 2, Active: true, Score: 3, Name: "name"}}
	var names []string = typed.Project(rows, columns.Name)
	var scores []int = typed.Project(rows, columns.Score)
	if !reflect.DeepEqual(names, []string{"", "name"}) || !reflect.DeepEqual(scores, []int{0, 3}) {
		t.Fatal(names, scores)
	}
	if b := columns.Active.Eq(false).Binding(); b.Name != "active" || b.Value != false {
		t.Fatal(b)
	}
	// Metadata is returned as a new slice: consumer mutation cannot poison the
	// next model validation. This is the bounded immutable-metadata direction.
	metadata := UserMetadata()
	metadata[0].DBName = "tampered"
	if err := ValidateUserMetadata(); err != nil {
		t.Fatal(err)
	}
	if err := typed.ValidateModel(reflect.TypeOf(User{}), metadata); err == nil {
		t.Fatal("tampered metadata accepted")
	}
}

func TestWriteOmissionZerosAndNull(t *testing.T) {
	if got := (UserWrite{}).Bindings(); len(got) != 0 {
		t.Fatal(got)
	}
	write := UserWrite{Active: typed.Some(false), Score: typed.Some(0), Name: typed.Some(""), Note: typed.Some((*string)(nil))}
	want := []typed.Binding{{Name: "active", Value: false}, {Name: "score", Value: 0}, {Name: "name", Value: ""}, {Name: "note", Value: nil}}
	if got := write.Bindings(); !reflect.DeepEqual(got, want) {
		t.Fatalf("got %#v want %#v", got, want)
	}
	value := ""
	got := (UserWrite{Note: typed.Some(&value)}).Bindings()
	if !reflect.DeepEqual(got, []typed.Binding{{Name: "note", Value: ""}}) {
		t.Fatal(got)
	}
	// Returned bind vectors snapshot pointer values; mutating the original
	// string variable after binding does not change the value sent to a driver.
	value = "later"
	if got[0].Value != "" {
		t.Fatal("pointer leaked into binding")
	}
}

func TestReflectionRejectsDuplicateTags(t *testing.T) {
	type bad struct {
		A string `db:"same"`
		B string `db:"same"`
	}
	err := typed.ValidateModel(reflect.TypeOf(bad{}), []typed.Field{})
	if err == nil {
		t.Fatal("duplicate tag accepted")
	}
}
