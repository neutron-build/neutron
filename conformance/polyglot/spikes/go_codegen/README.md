# Go typed metadata API spike

This standalone module demonstrates a proposed Go API shape. It does not
execute SQL, implement an ORM, or establish parity with GORM.

Run `python3 verify.py` from this directory. The check runs `go generate`, checks
repeat generation produces identical bytes, runs runtime/model validation
tests, and requires four invalid consumers to fail compilation. Dependencies
are Go's standard library only. Generated output is ignored and recreated.

`model.UserColumns()` returns typed column accessors; `ID.Eq(int64(1))` accepts
an integer and `typed.Project(rows, columns.Name)` returns `[]string`. Projection
here runs over in-memory rows. Neither call constructs a SQL query.

`model.UserWrite` has `typed.Optional[T]` per mapped field. Its zero value omits
the field. `Some(false)`, `Some(0)` and `Some("")` remain explicit values.
`Note: Some((*string)(nil))` binds explicit NULL, while `Name` cannot accept
that pointer. Nullable strings bind a copied scalar value, not a live pointer.

The generator validates exported single-field declarations, unique DB tags,
identifier syntax, supported types, and pointer/nullable agreement. Runtime
reflection validates generated names/types/tags before future execution.
Metadata slices and column sets are returned afresh. The bounded prototype
supports `int`, `int64`, `bool`, `string` and nullable pointers only; other
types fail explicitly. SQL codec/type support is deliberately not inferred.

The production `go/orm` must still implement database-backed typed queries and
projection scanning, generated/default/write permissions, codecs, schema/table
identity and quoting, transactions, connection ownership, cancellation,
relationships/hooks and error semantics. It must snapshot mutation input at a
defined execution boundary, validate metadata before issuing SQL, and certify
those behaviors against native PostgreSQL state. Generation/import paths here
are private experimental module paths and must be replaced by actual public
package paths after API review.
