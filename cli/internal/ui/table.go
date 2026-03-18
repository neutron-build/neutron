package ui

import (
	"fmt"
	"strings"
)

// Table renders a simple ASCII table.
type Table struct {
	headers []string
	rows    [][]string
	widths  []int
}

// NewTable creates a table with the given column headers.
func NewTable(headers ...string) *Table {
	widths := make([]int, len(headers))
	for i, h := range headers {
		widths[i] = len(h)
	}
	return &Table{
		headers: headers,
		widths:  widths,
	}
}

// AddRow adds a row to the table.
func (t *Table) AddRow(cells ...string) {
	for i, c := range cells {
		if i < len(t.widths) && len(c) > t.widths[i] {
			t.widths[i] = len(c)
		}
	}
	// Pad if fewer cells than headers
	for len(cells) < len(t.headers) {
		cells = append(cells, "")
	}
	t.rows = append(t.rows, cells)
}

// Render prints the table to stdout.
func (t *Table) Render() {
	fmt.Println(t.border("┌", "┬", "┐"))
	t.printRow(t.headers, true)
	fmt.Println(t.border("├", "┼", "┤"))
	for _, row := range t.rows {
		t.printRow(row, false)
	}
	fmt.Println(t.border("└", "┴", "┘"))
}

func (t *Table) border(left, mid, right string) string {
	parts := make([]string, len(t.widths))
	for i, w := range t.widths {
		parts[i] = strings.Repeat("─", w+2)
	}
	return left + strings.Join(parts, mid) + right
}

func (t *Table) printRow(cells []string, bold bool) {
	parts := make([]string, len(t.widths))
	for i, w := range t.widths {
		cell := ""
		if i < len(cells) {
			cell = cells[i]
		}
		padded := cell + strings.Repeat(" ", w-len(cell))
		if bold {
			padded = BoldStyle.Render(padded)
		}
		parts[i] = " " + padded + " "
	}
	fmt.Println("│" + strings.Join(parts, "│") + "│")
}

// RowCount returns the number of data rows.
func (t *Table) RowCount() int {
	return len(t.rows)
}
