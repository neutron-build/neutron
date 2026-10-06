package orm

import "strings"

// Like binds a PostgreSQL LIKE pattern for a typed text column. % and _ retain
// their native wildcard meaning and backslash is the native default escape.
// Pattern text is never interpolated into SQL. Nullable predicates preserve
// native SQL NULL's unknown result rather than matching an empty substitute.
func Like[M any](column Column[M, string], pattern string) Predicate[M] {
	return patternPredicate[M](column.info, column.field, "LIKE", pattern)
}
func ILike[M any](column Column[M, string], pattern string) Predicate[M] {
	return patternPredicate[M](column.info, column.field, "ILIKE", pattern)
}
func LikeNullable[M any](column Column[M, *string], pattern string) Predicate[M] {
	return patternPredicate[M](column.info, column.field, "LIKE", pattern)
}
func ILikeNullable[M any](column Column[M, *string], pattern string) Predicate[M] {
	return patternPredicate[M](column.info, column.field, "ILIKE", pattern)
}
func patternPredicate[M any](info *modelInfo, field fieldInfo, operator, pattern string) Predicate[M] {
	return Predicate[M]{&expression{kind: operator, info: info, field: field, value: pattern}}
}

// ContainsText treats all supplied characters as literal text, escaping LIKE
// metacharacters. Use Like for application inputs that intentionally are patterns.
func ContainsText[M any](column Column[M, string], text string) Predicate[M] {
	escaped := strings.NewReplacer("\\", "\\\\", "%", "\\%", "_", "\\_").Replace(text)
	return Like(column, "%"+escaped+"%")
}
