# ===----------------------------------------------------------------------=== #
# Neutron Mojo — CSV Reader
# ===----------------------------------------------------------------------=== #

"""Simple CSV parsing utilities."""


struct CSVRow(Copyable, Movable):
    """A single CSV row as a list of string fields."""
    var fields: List[String]

    fn __init__(out self):
        self.fields = List[String]()

    fn __init__(out self, var fields: List[String]):
        self.fields = fields^

    fn __copyinit__(out self, other: Self):
        self.fields = List[String]()
        for i in range(len(other.fields)):
            self.fields.append(other.fields[i])

    fn __moveinit__(out self, deinit other: Self):
        self.fields = other.fields^

    fn num_fields(self) -> Int:
        return len(self.fields)

    fn get(self, idx: Int) -> String:
        return self.fields[idx]


fn parse_csv_line(line: String, delimiter: String = ",") -> CSVRow:
    """Parse a single CSV line into fields.

    Simple parser: splits on delimiter, no quote handling.
    """
    var row = CSVRow()
    var current = String("")
    var delim_byte = ord(delimiter[byte=0])

    for i in range(len(line)):
        var ch = ord(line[byte=i])
        if ch == delim_byte:
            row.fields.append(current)
            current = String("")
        elif ch == 13:  # skip CR
            pass
        elif ch == 10:  # skip LF
            pass
        else:
            # Build character
            var buf = List[UInt8]()
            buf.append(UInt8(ch))
            buf.append(0)
            current += String(buf^)

    # Last field
    if len(current) > 0:
        row.fields.append(current)

    return row^
