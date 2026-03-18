package ui

import (
	"testing"
)

func TestNewTable(t *testing.T) {
	tbl := NewTable("Name", "Age", "City")
	if tbl == nil {
		t.Fatal("NewTable returned nil")
	}
	if len(tbl.headers) != 3 {
		t.Errorf("headers len = %d, want 3", len(tbl.headers))
	}
	if tbl.RowCount() != 0 {
		t.Errorf("RowCount() = %d, want 0", tbl.RowCount())
	}
}

func TestTableAddRow(t *testing.T) {
	tbl := NewTable("A", "B")
	tbl.AddRow("1", "2")
	tbl.AddRow("3", "4")

	if tbl.RowCount() != 2 {
		t.Errorf("RowCount() = %d, want 2", tbl.RowCount())
	}
}

func TestTableAddRowFewerCells(t *testing.T) {
	tbl := NewTable("A", "B", "C")
	tbl.AddRow("1") // fewer cells than headers

	if tbl.RowCount() != 1 {
		t.Errorf("RowCount() = %d, want 1", tbl.RowCount())
	}

	// The row should be padded to match header count
	if len(tbl.rows[0]) != 3 {
		t.Errorf("row cells = %d, want 3 (padded)", len(tbl.rows[0]))
	}
}

func TestTableWidthTracking(t *testing.T) {
	tbl := NewTable("X", "Y")
	tbl.AddRow("hello", "w")

	// Width for column 0 should be max(1, 5) = 5
	if tbl.widths[0] != 5 {
		t.Errorf("width[0] = %d, want 5", tbl.widths[0])
	}
}

func TestTableEmptyHeaders(t *testing.T) {
	tbl := NewTable()
	if tbl == nil {
		t.Fatal("NewTable() with no headers returned nil")
	}
	if len(tbl.headers) != 0 {
		t.Errorf("headers len = %d, want 0", len(tbl.headers))
	}
}
