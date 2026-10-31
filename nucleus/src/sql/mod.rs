//! SQL parsing layer — wraps sqlparser-rs and converts AST to Nucleus types.

use std::ops::ControlFlow;

use sqlparser::ast;
use sqlparser::ast::{Visit, Visitor};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;

use crate::catalog::{ColumnDef, ColumnGeneration, Deferrable, FkAction};
use crate::types::{DataType, NumericTypmod, NumericTypmodError};

/// Convert a sqlparser `ReferentialAction` to our internal `FkAction`.
///
/// Also available as `convert_fk_action` for use by ALTER TABLE ADD CONSTRAINT.
pub fn convert_fk_action(action: &Option<ast::ReferentialAction>) -> FkAction {
    match action {
        None => FkAction::NoAction,
        Some(ast::ReferentialAction::NoAction) => FkAction::NoAction,
        Some(ast::ReferentialAction::Restrict) => FkAction::Restrict,
        Some(ast::ReferentialAction::Cascade) => FkAction::Cascade,
        Some(ast::ReferentialAction::SetNull) => FkAction::SetNull,
        Some(ast::ReferentialAction::SetDefault) => FkAction::SetDefault,
    }
}

/// Maximum allowed parenthesis-nesting depth in a SQL statement before we reject
/// it outright as too complex.
///
/// This is a DoS guard, NOT a semantic limit. It backstops absurd parenthesis
/// nesting (plain `(((...)))`, arithmetic, etc.). Plain paren nesting does NOT
/// exponentially backtrack in sqlparser — it hits the parser's own recursion
/// limit cleanly — so this cap can be generous. Real-world SQL nests only a
/// handful of parens deep, so 100 rejects no legitimate query.
const MAX_PARSE_NESTING_DEPTH: usize = 100;

/// Maximum allowed nesting depth of `CAST` / `TRY_CAST` / `SAFE_CAST` / `CONVERT`
/// expressions before we reject the statement.
///
/// This is the load-bearing part of the DoS guard. sqlparser's recursive-descent
/// expression parser backtracks EXPONENTIALLY on deeply-nested CAST grammar: a
/// depth-48 chain explores on the order of 2^48 alternative parse paths and pins
/// a CPU core for minutes. Empirically the cliff is sharp — depth 47 parses in
/// ~1.5 ms, depth 48 never completes — and it sits UNDER sqlparser's own
/// `RecursionCounter` (DEFAULT_REMAINING_DEPTH = 50), so the built-in guard never
/// fires. We must therefore reject deep CAST nesting BEFORE handing the input to
/// the parser.
///
/// 32 is far below the ~48 cliff (so the exponential never gets going) yet far
/// above any realistic query (real queries nest at most a couple of casts).
/// Postgres behaves analogously: beyond `max_stack_depth` it returns
/// `54001 statement too complex` immediately rather than spinning. Note this caps
/// the *simultaneously-open* CAST depth, not the total CAST count — a query with
/// thousands of non-nested casts (`SELECT CAST(a AS INT), CAST(b AS INT), ...`)
/// parses in linear time and is unaffected.
const MAX_CAST_NESTING_DEPTH: usize = 32;

/// Returns true if the keyword token ending just before `paren_idx` (skipping
/// whitespace) is a CAST-family keyword — i.e. this `(` opens a cast expression.
/// `bytes[..paren_idx]` is the SQL up to (not including) the `(`.
fn paren_is_cast(bytes: &[u8], paren_idx: usize) -> bool {
    // Walk back over whitespace between the keyword and the '('.
    let mut end = paren_idx;
    while end > 0 && bytes[end - 1].is_ascii_whitespace() {
        end -= 1;
    }
    // Walk back over the identifier characters of the preceding token.
    let mut start = end;
    while start > 0 && (bytes[start - 1].is_ascii_alphanumeric() || bytes[start - 1] == b'_') {
        start -= 1;
    }
    if start == end {
        return false;
    }
    // The token must not be the tail of a longer identifier (e.g. `mycast(`):
    // the char before `start` must not be an identifier char.
    if start > 0 && (bytes[start - 1].is_ascii_alphanumeric() || bytes[start - 1] == b'_') {
        return false;
    }
    let word = &bytes[start..end];
    word.eq_ignore_ascii_case(b"CAST")
        || word.eq_ignore_ascii_case(b"TRY_CAST")
        || word.eq_ignore_ascii_case(b"SAFE_CAST")
        || word.eq_ignore_ascii_case(b"CONVERT")
}

/// Cheap O(n) pre-parse complexity guard.
///
/// Scans the raw SQL once, tracking (a) running parenthesis-nesting depth and
/// (b) running CAST-expression nesting depth, and returns an error if either
/// exceeds its cap. This MUST run before `Parser::parse_sql` so the
/// exponential-backtracking inputs never reach the recursive-descent parser.
///
/// String literals (`'...'`), quoted/escaped identifiers (`"..."`,
/// `` `...` ``), dollar-quoted strings (`$tag$...$tag$`), and SQL comments
/// (`-- ...`, `/* ... */`) are skipped so that parentheses appearing inside
/// string data or comments are not miscounted — e.g. `WHERE note = '(((('`
/// must not be rejected.
fn check_nesting_depth(sql: &str) -> Result<(), ParseError> {
    let bytes = sql.as_bytes();
    let mut i = 0;
    let len = bytes.len();
    let mut depth: usize = 0;
    // Per-open-paren stack of "did this paren open a CAST expression?" plus a
    // running count of currently-open CAST parens.
    let mut cast_stack: Vec<bool> = Vec::new();
    let mut cast_depth: usize = 0;

    while i < len {
        let b = bytes[i];
        match b {
            // ── Single-quoted string literal: '...' with '' escaping ──────────
            b'\'' => {
                i += 1;
                while i < len {
                    if bytes[i] == b'\'' {
                        // Doubled '' is an escaped quote inside the literal.
                        if i + 1 < len && bytes[i + 1] == b'\'' {
                            i += 2;
                            continue;
                        }
                        break; // closing quote
                    }
                    i += 1;
                }
            }
            // ── Double-quoted identifier: "..." with "" escaping ──────────────
            b'"' => {
                i += 1;
                while i < len {
                    if bytes[i] == b'"' {
                        if i + 1 < len && bytes[i + 1] == b'"' {
                            i += 2;
                            continue;
                        }
                        break;
                    }
                    i += 1;
                }
            }
            // ── Backtick-quoted identifier: `...` ─────────────────────────────
            b'`' => {
                i += 1;
                while i < len && bytes[i] != b'`' {
                    i += 1;
                }
            }
            // ── Line comment: -- to end of line ───────────────────────────────
            b'-' if i + 1 < len && bytes[i + 1] == b'-' => {
                i += 2;
                while i < len && bytes[i] != b'\n' {
                    i += 1;
                }
            }
            // ── Block comment: /* ... */ (non-nested, matching sqlparser) ──────
            b'/' if i + 1 < len && bytes[i + 1] == b'*' => {
                i += 2;
                while i + 1 < len && !(bytes[i] == b'*' && bytes[i + 1] == b'/') {
                    i += 1;
                }
                i += 1; // skip the closing '/'
            }
            // ── Dollar-quoted string: $tag$ ... $tag$ (Postgres) ──────────────
            b'$' => {
                // Find the closing '$' of the opening tag. A valid tag contains
                // only letters/digits/underscore; anything else means this '$'
                // is not a dollar-quote opener (e.g. a positional param $1).
                let tag_start = i;
                let mut j = i + 1;
                while j < len && (bytes[j].is_ascii_alphanumeric() || bytes[j] == b'_') {
                    j += 1;
                }
                if j < len && bytes[j] == b'$' {
                    // tag = sql[tag_start..=j]  (includes both '$' delimiters)
                    let tag = &bytes[tag_start..=j];
                    i = j + 1;
                    // Scan for the closing tag.
                    while i < len {
                        if bytes[i] == b'$'
                            && i + tag.len() <= len
                            && &bytes[i..i + tag.len()] == tag
                        {
                            i += tag.len();
                            break;
                        }
                        i += 1;
                    }
                    continue; // i already advanced past the closing tag
                }
                // Not a dollar-quote; fall through and treat '$' as an ordinary
                // character (i is advanced by the tail increment below).
                i += 1;
                continue;
            }
            b'(' => {
                depth += 1;
                if depth > MAX_PARSE_NESTING_DEPTH {
                    return Err(ParseError::StatementTooComplex(MAX_PARSE_NESTING_DEPTH));
                }
                let is_cast = paren_is_cast(bytes, i);
                cast_stack.push(is_cast);
                if is_cast {
                    cast_depth += 1;
                    if cast_depth > MAX_CAST_NESTING_DEPTH {
                        return Err(ParseError::StatementTooComplex(MAX_CAST_NESTING_DEPTH));
                    }
                }
            }
            b')' => {
                depth = depth.saturating_sub(1);
                if cast_stack.pop() == Some(true) {
                    cast_depth = cast_depth.saturating_sub(1);
                }
            }
            _ => {}
        }
        i += 1;
    }

    Ok(())
}

/// Maximum nesting depth of the *parsed* expression tree.
///
/// The pre-parse `check_nesting_depth` scan only sees parentheses, but a tree
/// can be arbitrarily deep with none: `a AND a AND a AND ...` parses into a
/// left-deep `BinaryOp` chain one level per term. sqlparser's own
/// `RecursionCounter` does not fire either, because equal-precedence infix
/// operators are consumed by a loop rather than by recursion.
///
/// Everything downstream of the parser walks that tree recursively — the
/// AST cache's deep `Clone`, literal substitution, planning, `Drop` — so an
/// unbounded tree is an unbounded stack walk, i.e. a process-killing stack
/// overflow rather than a query error. Reject it here, at the single choke
/// point every SQL string passes through, before anything recurses over it.
///
/// Kept equal to the executor's `MAX_EXPR_DEPTH` (256), which is the depth the
/// engine already commits to being able to recurse to during evaluation.
const MAX_AST_EXPR_DEPTH: usize = 256;

/// Depth-limited AST walker. Breaks as soon as the limit is passed, so the
/// visitor's own recursion never exceeds `MAX_AST_EXPR_DEPTH` levels either.
struct ExprDepthVisitor {
    depth: usize,
    limit: usize,
}

impl Visitor for ExprDepthVisitor {
    type Break = ();

    fn pre_visit_expr(&mut self, _expr: &ast::Expr) -> ControlFlow<Self::Break> {
        self.depth += 1;
        if self.depth > self.limit {
            ControlFlow::Break(())
        } else {
            ControlFlow::Continue(())
        }
    }

    fn post_visit_expr(&mut self, _expr: &ast::Expr) -> ControlFlow<Self::Break> {
        self.depth = self.depth.saturating_sub(1);
        ControlFlow::Continue(())
    }
}

/// Catches the ClickHouse `FROM <table> FINAL` modifier, which the PostgreSQL
/// dialect parses as a bare table ALIAS spelled `FINAL`.
struct FinalModifierVisitor {
    table: Option<String>,
}

impl Visitor for FinalModifierVisitor {
    type Break = ();

    fn pre_visit_table_factor(&mut self, factor: &ast::TableFactor) -> ControlFlow<Self::Break> {
        if let ast::TableFactor::Table {
            name,
            alias: Some(alias),
            ..
        } = factor
            // A QUOTED alias is an alias: `AS "FINAL"` names a relation and has
            // nothing to do with ClickHouse. Only the bare word is the modifier.
            && alias.name.quote_style.is_none()
            && alias.columns.is_empty()
            && alias.name.value.eq_ignore_ascii_case("final")
        {
            self.table = Some(object_name_key(name));
            return ControlFlow::Break(());
        }
        ControlFlow::Continue(())
    }
}

/// Reject `SELECT ... FROM <table> FINAL`.
///
/// The PostgreSQL dialect has no FINAL, so `FROM stats_hourly FINAL` parses as
/// a table aliased `FINAL` — the modifier was accepted and then completely
/// ignored. Against a live table holding superseded versions that returned
/// 158 where the raw events proved 72: byte-identical to the query without it.
/// Someone porting ClickHouse SQL has no way to notice.
///
/// Rejecting rather than implementing is deliberate. Nucleus collapses a
/// `replacing_mergetree` table on EVERY read, which is the thing ClickHouse's
/// FINAL exists to ask for — so there is no behaviour left for it to select,
/// and a keyword that means "do what already happens" is a keyword that will
/// silently mean nothing again the next time a read path is added. The error
/// says so.
fn check_final_modifier(stmts: &[ast::Statement]) -> Result<(), ParseError> {
    for stmt in stmts {
        let mut visitor = FinalModifierVisitor { table: None };
        let _ = stmt.visit(&mut visitor);
        if let Some(table) = visitor.table {
            return Err(ParseError::FinalNotSupported(table));
        }
    }
    Ok(())
}

/// Reject statements whose expression tree nests deeper than
/// `MAX_AST_EXPR_DEPTH`. See that constant for why the pre-parse scan is not
/// enough on its own.
fn check_ast_depth(stmts: &[ast::Statement]) -> Result<(), ParseError> {
    for stmt in stmts {
        let mut visitor = ExprDepthVisitor {
            depth: 0,
            limit: MAX_AST_EXPR_DEPTH,
        };
        if stmt.visit(&mut visitor).is_break() {
            return Err(ParseError::ExpressionTooDeep(MAX_AST_EXPR_DEPTH));
        }
    }
    Ok(())
}

/// Parse a SQL string into sqlparser AST statements.
pub fn parse(sql: &str) -> Result<Vec<ast::Statement>, ParseError> {
    // DoS guard: reject pathologically deep nesting BEFORE handing the input to
    // sqlparser, whose recursive-descent parser backtracks exponentially on deep
    // nested-CAST grammars (see `MAX_PARSE_NESTING_DEPTH`). This is the
    // load-bearing fix; it runs on every parse path (raw parse, the AST-cache
    // miss branch, and the wire fallback all funnel through here).
    check_nesting_depth(sql)?;

    // NOTE: we deliberately keep sqlparser's DEFAULT recursion limit (50). It is
    // well-tuned: deep single-path constructs (e.g. nested scalar subqueries
    // `SELECT (SELECT (...))`) error cleanly at 50 in well under a millisecond.
    // RAISING the limit re-introduces exponential blow-up for those forms, so the
    // pre-parse `check_nesting_depth` caps above (parens 100, CAST nesting 32) are
    // the load-bearing defense — the CAST cap sits below sqlparser's ~48 cliff,
    // which itself is below the default-50 recursion guard.
    let dialect = PostgreSqlDialect {};
    let normalized = normalize_unnest_table_syntax(&dialect, sql);
    let parsed = match normalized {
        Some(tokens) => Parser::new(&dialect)
            .with_tokens_with_locations(tokens)
            .parse_statements(),
        None => Parser::parse_sql(&dialect, sql),
    };
    let stmts = match parsed {
        Ok(stmts) => stmts,
        // sqlparser has no grammar for `OVERRIDING {SYSTEM | USER} VALUE` or
        // `SET CONSTRAINTS`; rewrite them into parseable statements that carry
        // the same information (see the two helpers).
        Err(error) => match parse_postgres_extensions(&dialect, sql) {
            Some(stmts) => stmts,
            None => return Err(error.into()),
        },
    };

    // Second DoS guard: paren-free constructs (long `AND`/`OR`/arithmetic
    // chains) build an arbitrarily deep tree that the scan above cannot see.
    // Reject before any recursive walk of the tree happens.
    check_ast_depth(&stmts)?;

    // `FROM t FINAL` parses as an alias here and used to be silently discarded.
    check_final_modifier(&stmts)?;

    Ok(stmts)
}

/// sqlparser's LATERAL function branch cannot carry WITH ORDINALITY.
/// PostgreSQL UNNEST is implicitly lateral, so its UNNEST table-factor branch
/// represents the same semantics. Normalize only unquoted builtin tokens;
/// quoted names, strings, comments and unrelated functions remain untouched.
fn normalize_unnest_table_syntax(
    dialect: &PostgreSqlDialect,
    sql: &str,
) -> Option<Vec<sqlparser::tokenizer::TokenWithSpan>> {
    use sqlparser::tokenizer::{Token, Tokenizer};
    let tokens = Tokenizer::new(dialect, sql).tokenize_with_location().ok()?;
    let significant: Vec<usize> = (0..tokens.len())
        .filter(|&i| !matches!(tokens[i].token, Token::Whitespace(_)))
        .collect();
    let word = |i: usize, value: &str| matches!(&tokens[i].token, Token::Word(w) if w.quote_style.is_none() && w.value.eq_ignore_ascii_case(value));
    let mut remove = std::collections::HashSet::new();
    for (pos, &index) in significant.iter().enumerate() {
        if !word(index, "unnest")
            || !significant
                .get(pos + 1)
                .is_some_and(|&i| matches!(tokens[i].token, Token::LParen))
        {
            continue;
        }
        let mut start = pos;
        if pos >= 2
            && matches!(tokens[significant[pos - 1]].token, Token::Period)
            && word(significant[pos - 2], "pg_catalog")
        {
            remove.insert(significant[pos - 2]);
            remove.insert(significant[pos - 1]);
            start -= 2;
        }
        if start > 0 && word(significant[start - 1], "lateral") {
            remove.insert(significant[start - 1]);
        }
    }
    if remove.is_empty() {
        return None;
    }
    Some(
        tokens
            .into_iter()
            .enumerate()
            .filter(|(i, _)| !remove.contains(i))
            .map(|(_, t)| t)
            .collect(),
    )
}

/// `OVERRIDING SYSTEM VALUE` / `OVERRIDING USER VALUE` on an INSERT.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Overriding {
    System,
    User,
}

/// The OVERRIDING clause of a parsed INSERT. sqlparser cannot represent it, so
/// [`parse`] strips it and records it in the INSERT's (otherwise unused, MySQL)
/// `priority` slot: `HIGH_PRIORITY` = SYSTEM VALUE, `LOW_PRIORITY` = USER VALUE.
pub fn insert_overriding(insert: &ast::Insert) -> Option<Overriding> {
    match insert.priority {
        Some(ast::MysqlInsertPriority::HighPriority) => Some(Overriding::System),
        Some(ast::MysqlInsertPriority::LowPriority) => Some(Overriding::User),
        _ => None,
    }
}

/// The `<ALL|name,...>|<DEFERRED|IMMEDIATE>` payload of a `SET CONSTRAINTS`
/// statement that [`parse`] rewrote into `SET nucleus.set_constraints = '...'`.
pub fn set_constraints_spec(set: &ast::Set) -> Option<String> {
    let ast::Set::SingleAssignment {
        variable, values, ..
    } = set
    else {
        return None;
    };
    if variable.to_string() != crate::executor::SET_CONSTRAINTS_SETTING {
        return None;
    }
    match values.first()? {
        ast::Expr::Value(v) => match &v.value {
            ast::Value::SingleQuotedString(text) => Some(text.clone()),
            _ => None,
        },
        _ => None,
    }
}

/// Parse SQL that uses PostgreSQL syntax sqlparser lacks: the OVERRIDING clause
/// of INSERT and `SET CONSTRAINTS`. `None` when the text uses neither, so the
/// caller reports the original parse error.
fn parse_postgres_extensions(
    dialect: &PostgreSqlDialect,
    sql: &str,
) -> Option<Vec<ast::Statement>> {
    use sqlparser::tokenizer::{Token, Tokenizer};
    let mut tokens = Tokenizer::new(dialect, sql).tokenize().ok()?;
    let word = |t: &Token, w: &str| matches!(t, Token::Word(x) if x.value.eq_ignore_ascii_case(w));
    let significant: Vec<usize> = (0..tokens.len())
        .filter(|&i| !matches!(tokens[i], Token::Whitespace(_)))
        .collect();

    // SET CONSTRAINTS { ALL | name [, ...] } { DEFERRED | IMMEDIATE }
    if significant.len() >= 4
        && word(&tokens[significant[0]], "SET")
        && word(&tokens[significant[1]], "CONSTRAINTS")
    {
        let mut rest: Vec<&Token> = significant[2..].iter().map(|&i| &tokens[i]).collect();
        if matches!(rest.last(), Some(Token::SemiColon)) {
            rest.pop();
        }
        let mode = match rest.pop()? {
            t if word(t, "DEFERRED") => "DEFERRED",
            t if word(t, "IMMEDIATE") => "IMMEDIATE",
            _ => return None,
        };
        let targets = if rest.len() == 1 && word(rest[0], "ALL") {
            "ALL".to_string()
        } else {
            let mut names = Vec::new();
            let mut expect_name = true;
            for t in rest {
                match (expect_name, t) {
                    (true, Token::Word(w)) => {
                        names.push(if w.quote_style.is_some() {
                            w.value.clone()
                        } else {
                            w.value.to_lowercase()
                        });
                        expect_name = false;
                    }
                    (false, Token::Comma) => expect_name = true,
                    _ => return None,
                }
            }
            if expect_name {
                return None;
            }
            names.join(",")
        };
        let rewritten = format!(
            "SET {} = '{}|{}'",
            crate::executor::SET_CONSTRAINTS_SETTING,
            targets.replace('\'', "''"),
            mode
        );
        return Parser::parse_sql(dialect, &rewritten).ok();
    }

    // OVERRIDING {SYSTEM | USER} VALUE, per `;`-separated statement.
    let mut kinds: Vec<Option<Overriding>> = Vec::new();
    let mut current: Option<Overriding> = None;
    let mut segment_has_tokens = false;
    let mut depth = 0i32;
    let mut remove = vec![false; tokens.len()];
    let mut found = false;
    let mut k = 0;
    while k < significant.len() {
        let t = &tokens[significant[k]];
        match t {
            Token::LParen => depth += 1,
            Token::RParen => depth -= 1,
            Token::SemiColon => {
                if segment_has_tokens {
                    kinds.push(current.take());
                }
                segment_has_tokens = false;
                depth = 0;
                k += 1;
                continue;
            }
            _ => {}
        }
        segment_has_tokens = true;
        if depth == 0
            && word(t, "OVERRIDING")
            && k + 2 < significant.len()
            && word(&tokens[significant[k + 2]], "VALUE")
        {
            let which = &tokens[significant[k + 1]];
            let kind = if word(which, "SYSTEM") {
                Some(Overriding::System)
            } else if word(which, "USER") {
                Some(Overriding::User)
            } else {
                None
            };
            if let Some(kind) = kind {
                current = Some(kind);
                found = true;
                for &i in &significant[k..k + 3] {
                    remove[i] = true;
                }
                k += 3;
                continue;
            }
        }
        k += 1;
    }
    if !found {
        return None;
    }
    if segment_has_tokens {
        kinds.push(current.take());
    }
    let mut idx = 0;
    tokens.retain(|_| {
        idx += 1;
        !remove[idx - 1]
    });
    let rewritten: String = tokens.iter().map(|t| t.to_string()).collect();
    let mut stmts = Parser::parse_sql(dialect, &rewritten).ok()?;
    if stmts.len() != kinds.len() {
        return None;
    }
    for (stmt, kind) in stmts.iter_mut().zip(kinds) {
        let Some(kind) = kind else { continue };
        let ast::Statement::Insert(insert) = stmt else {
            return None;
        };
        insert.priority = Some(match kind {
            Overriding::System => ast::MysqlInsertPriority::HighPriority,
            Overriding::User => ast::MysqlInsertPriority::LowPriority,
        });
    }
    Some(stmts)
}

/// Convert a sqlparser DataType to our internal DataType.
pub fn convert_data_type(dt: &ast::DataType) -> Result<DataType, ParseError> {
    match dt {
        // The short Postgres spellings (`bool`, `int2/4/8`, `float4/8`) are
        // SEPARATE sqlparser variants from the long ones, and only the long
        // ones were listed here. `eval_cast` already accepted both, so the
        // engine could evaluate `$1::int8` but this function — which is what
        // types a parameter for ParameterDescription and what types a CAST
        // result column — returned Err and the wire layer fell back to TEXT.
        // asyncpg then refused to bind a Python int to `SELECT $1::int8`
        // ("expected str, got int") for a cast that plainly says otherwise.
        ast::DataType::Boolean | ast::DataType::Bool => Ok(DataType::Bool),
        ast::DataType::Int(_) | ast::DataType::Integer(_) | ast::DataType::Int4(_) => {
            Ok(DataType::Int32)
        }
        ast::DataType::BigInt(_) | ast::DataType::Int8(_) => Ok(DataType::Int64),
        ast::DataType::Float(_)
        | ast::DataType::Double(_)
        | ast::DataType::DoublePrecision
        | ast::DataType::Float4
        | ast::DataType::Float8 => Ok(DataType::Float64),
        ast::DataType::Text
        | ast::DataType::Varchar(_)
        | ast::DataType::CharVarying(_)
        | ast::DataType::CharacterVarying(_) => Ok(DataType::Text),
        ast::DataType::Char(_) | ast::DataType::Character(_) => Ok(DataType::Text),
        ast::DataType::JSONB => Ok(DataType::Jsonb),
        ast::DataType::JSON => Ok(DataType::Jsonb),
        ast::DataType::Date => Ok(DataType::Date),
        ast::DataType::Timestamp(_, timezone) => {
            if matches!(
                timezone,
                ast::TimezoneInfo::WithTimeZone | ast::TimezoneInfo::Tz
            ) {
                Ok(DataType::TimestampTz)
            } else {
                Ok(DataType::Timestamp)
            }
        }
        ast::DataType::TimestampNtz(_) => Ok(DataType::Timestamp),
        ast::DataType::Interval { .. } => Ok(DataType::Interval),
        ast::DataType::Numeric(_) | ast::DataType::Decimal(_) | ast::DataType::Dec(_) => {
            Ok(DataType::Numeric)
        }
        ast::DataType::Uuid => Ok(DataType::Uuid),
        ast::DataType::Bytea => Ok(DataType::Bytea),
        ast::DataType::Blob(_) => Ok(DataType::Bytea),
        ast::DataType::SmallInt(_) | ast::DataType::TinyInt(_) | ast::DataType::Int2(_) => {
            Ok(DataType::Int32)
        }
        ast::DataType::Real => Ok(DataType::Float64),
        ast::DataType::Array(inner) => match inner {
            ast::ArrayElemTypeDef::AngleBracket(dt) => {
                Ok(DataType::Array(Box::new(convert_data_type(dt)?)))
            }
            ast::ArrayElemTypeDef::SquareBracket(dt, _) => {
                Ok(DataType::Array(Box::new(convert_data_type(dt)?)))
            }
            ast::ArrayElemTypeDef::Parenthesis(dt) => {
                Ok(DataType::Array(Box::new(convert_data_type(dt)?)))
            }
            ast::ArrayElemTypeDef::None => Ok(DataType::Array(Box::new(DataType::Text))),
        },
        ast::DataType::Custom(name, args) => {
            // Handle VECTOR(n) custom type
            if let Some(part) = name.0.first()
                && let Some(ident) = part.as_ident()
            {
                match ident.value.to_lowercase().as_str() {
                    "vector" => {
                        if args.is_empty() {
                            // VECTOR without dimension defaults to 0 (unknown dimension)
                            return Ok(DataType::Vector(0));
                        }
                        // Extract dimensionality from args (args are Strings in sqlparser 0.61)
                        if args.len() == 1
                            && let Ok(dim) = args[0].parse::<usize>()
                        {
                            return Ok(DataType::Vector(dim));
                        }
                        return Err(ParseError::UnsupportedDataType(
                            "VECTOR type requires a numeric dimension, e.g., VECTOR(384)".into(),
                        ));
                    }
                    // Serial types: stored as Int32/Int64; executor auto-creates sequences.
                    "serial" | "serial4" => return Ok(DataType::Int32),
                    "bigserial" | "serial8" => return Ok(DataType::Int64),
                    "smallserial" | "serial2" => return Ok(DataType::Int32),
                    _ => {}
                }
                // Fall through: treat as a user-defined type (e.g. an enum).
                return Ok(DataType::UserDefined(ident.value.clone()));
            }
            Err(ParseError::UnsupportedDataType(format!("{name}")))
        }
        other => Err(ParseError::UnsupportedDataType(format!("{other}"))),
    }
}

/// Extract column definitions from a CREATE TABLE statement's columns.
pub fn extract_columns(columns: &[ast::ColumnDef]) -> Result<Vec<ColumnDef>, ParseError> {
    columns
        .iter()
        .enumerate()
        .map(|(idx, col)| {
            let data_type = convert_data_type(&col.data_type)?;
            let nullable = !col.options.iter().any(|opt| {
                matches!(
                    opt.option,
                    ast::ColumnOption::NotNull | ast::ColumnOption::PrimaryKey(_)
                )
            });
            let default_expr = col.options.iter().find_map(|opt| match &opt.option {
                ast::ColumnOption::Default(expr) => Some(expr.to_string()),
                _ => None,
            });
            let generation = column_generation(&col.options);
            // A stored generated column is computed, never defaulted.
            let default_expr = if matches!(generation, Some(ColumnGeneration::Stored(_))) {
                None
            } else {
                default_expr
            };
            Ok(ColumnDef {
                name: col.name.value.clone(),
                data_type,
                nullable,
                default_expr,
                // 1-based so `0` stays available as "no id recorded" for
                // columns read from a pre-id snapshot.
                id: idx as u32 + 1,
                analyzer: None,
                generation,
                max_len: declared_max_len(&col.data_type),
                // The executor validates every declaration before it gets here
                // (`column_writes::declared_numeric_typmod`), so an invalid
                // one never reaches this lenient read.
                numeric_typmod: declared_numeric_typmod(&col.data_type).ok().flatten(),
            })
        })
        .collect()
}

/// `varchar(n)` / `char(n)` length in characters, when the declaration has one.
pub fn declared_max_len(dt: &ast::DataType) -> Option<u32> {
    let size = match dt {
        ast::DataType::Varchar(size)
        | ast::DataType::CharVarying(size)
        | ast::DataType::CharacterVarying(size)
        | ast::DataType::Char(size)
        | ast::DataType::Character(size) => size.as_ref()?,
        _ => return None,
    };
    match size {
        ast::CharacterLength::IntegerLength { length, .. } => u32::try_from(*length).ok(),
        ast::CharacterLength::Max => None,
    }
}

/// The `numeric(p, s)` / `decimal(p, s)` a declared SQL type carries (the
/// element's, for an array), validated. `Ok(None)` for an unconstrained
/// `numeric` and for every other type.
pub fn declared_numeric_typmod(
    dt: &ast::DataType,
) -> Result<Option<NumericTypmod>, NumericTypmodError> {
    match dt {
        ast::DataType::Numeric(info) | ast::DataType::Decimal(info) | ast::DataType::Dec(info) => {
            match info {
                ast::ExactNumberInfo::None => Ok(None),
                ast::ExactNumberInfo::Precision(precision) => {
                    NumericTypmod::new(*precision, 0).map(Some)
                }
                ast::ExactNumberInfo::PrecisionAndScale(precision, scale) => {
                    NumericTypmod::new(*precision, *scale).map(Some)
                }
            }
        }
        ast::DataType::Array(
            ast::ArrayElemTypeDef::AngleBracket(inner)
            | ast::ArrayElemTypeDef::SquareBracket(inner, _)
            | ast::ArrayElemTypeDef::Parenthesis(inner),
        ) => declared_numeric_typmod(inner),
        _ => Ok(None),
    }
}

/// The generation clause among a column's options, if any.
pub fn column_generation(options: &[ast::ColumnOptionDef]) -> Option<ColumnGeneration> {
    options.iter().find_map(|opt| match &opt.option {
        ast::ColumnOption::Generated {
            generated_as,
            generation_expr,
            ..
        } => Some(match (generation_expr, generated_as) {
            (Some(expr), _) => ColumnGeneration::Stored(expr.to_string()),
            (None, ast::GeneratedAs::ByDefault) => ColumnGeneration::IdentityByDefault,
            (None, _) => ColumnGeneration::IdentityAlways,
        }),
        _ => None,
    })
}

/// `DEFERRABLE` / `INITIALLY DEFERRED` of a constraint's characteristics.
pub fn deferrable_from(characteristics: Option<&ast::ConstraintCharacteristics>) -> Deferrable {
    let Some(c) = characteristics else {
        return Deferrable::NotDeferrable;
    };
    match (c.deferrable, c.initially) {
        (_, Some(ast::DeferrableInitial::Deferred)) => Deferrable::InitiallyDeferred,
        (Some(true), _) => Deferrable::InitiallyImmediate,
        _ => Deferrable::NotDeferrable,
    }
}

/// Return which column names require an auto-sequence (SERIAL / BIGSERIAL / SMALLSERIAL /
/// GENERATED ALWAYS AS IDENTITY / GENERATED BY DEFAULT AS IDENTITY).
/// The returned list contains `(column_name, is_bigserial)` pairs where `is_bigserial`
/// determines whether the sequence value should be cast to Int64.
pub fn extract_serial_columns(columns: &[ast::ColumnDef]) -> Vec<(String, bool)> {
    let mut serials = Vec::new();
    for col in columns {
        // Check the type name first.
        let is_serial = if let ast::DataType::Custom(name, _) = &col.data_type {
            if let Some(part) = name.0.first() {
                if let Some(ident) = part.as_ident() {
                    matches!(
                        ident.value.to_lowercase().as_str(),
                        "serial" | "serial4" | "serial2" | "smallserial" | "bigserial" | "serial8"
                    )
                } else {
                    false
                }
            } else {
                false
            }
        } else {
            false
        };

        let is_bigserial = if let ast::DataType::Custom(name, _) = &col.data_type {
            if let Some(part) = name.0.first() {
                if let Some(ident) = part.as_ident() {
                    matches!(ident.value.to_lowercase().as_str(), "bigserial" | "serial8")
                } else {
                    false
                }
            } else {
                false
            }
        } else {
            false
        };

        // Also check for GENERATED ALWAYS/BY DEFAULT AS IDENTITY.
        let has_identity = col.options.iter().any(|opt| {
            matches!(
                &opt.option,
                ast::ColumnOption::Generated {
                    generation_expr: None,
                    ..
                } | ast::ColumnOption::Identity(_)
            )
        });

        if is_serial || has_identity {
            serials.push((col.name.value.clone(), is_bigserial));
        }
    }
    serials
}

/// Catalog key for a (possibly quoted, possibly schema-qualified) object name.
/// Each part contributes its bare identifier value — `"users"` and `users` are
/// the same relation, matching Postgres, where quoting affects case folding
/// but not identity for names that need no folding. A leading `public.`
/// qualifier is dropped because unqualified DDL stores bare names;
/// `pg_catalog.` / `information_schema.` prefixes are preserved for
/// virtual-table dispatch.
pub fn object_name_key(name: &ast::ObjectName) -> String {
    let parts: Vec<String> = name
        .0
        .iter()
        .map(|p| match p.as_ident() {
            Some(id) => id.value.clone(),
            None => p.to_string(),
        })
        .collect();
    let skip = usize::from(parts.len() > 1 && parts[0] == "public");
    parts[skip..].join(".")
}

/// Bare column name for a single- or compound-identifier target (e.g. an
/// UPDATE assignment `"users"."age"` or `"age"`): the last part's value.
pub fn object_name_last(name: &ast::ObjectName) -> String {
    name.0
        .last()
        .and_then(|p| p.as_ident())
        .map(|id| id.value.clone())
        .unwrap_or_else(|| name.to_string())
}

/// Column name of an index/constraint column entry. Quoted identifiers
/// (`PRIMARY KEY("post_id")`) must resolve to the bare column name —
/// `expr.to_string()` would keep the quote characters and never match the
/// catalog.
pub fn index_column_name(col: &ast::IndexColumn) -> String {
    match &col.column.expr {
        ast::Expr::Identifier(ident) => ident.value.clone(),
        ast::Expr::CompoundIdentifier(parts) => parts
            .last()
            .map(|p| p.value.clone())
            .unwrap_or_else(|| col.column.expr.to_string()),
        other => other.to_string(),
    }
}

/// Extract table-level constraints and inline column constraints from a CREATE TABLE.
pub fn extract_constraints(
    columns: &[ast::ColumnDef],
    table_constraints: &[ast::TableConstraint],
) -> Vec<crate::catalog::TableConstraint> {
    use crate::catalog::TableConstraint;
    let mut constraints = Vec::new();

    // Inline column constraints (PRIMARY KEY, UNIQUE on single columns)
    for col in columns {
        for opt in &col.options {
            match &opt.option {
                ast::ColumnOption::PrimaryKey(_) => {
                    let has_pk = constraints
                        .iter()
                        .any(|c| matches!(c, TableConstraint::PrimaryKey { .. }));
                    if !has_pk {
                        constraints.push(TableConstraint::PrimaryKey {
                            name: opt.name.as_ref().map(|name| name.to_string()),
                            columns: vec![col.name.value.clone()],
                        });
                    }
                }
                ast::ColumnOption::Unique(_) => {
                    constraints.push(TableConstraint::Unique {
                        name: opt.name.as_ref().map(|name| name.to_string()),
                        columns: vec![col.name.value.clone()],
                    });
                }
                ast::ColumnOption::Check(expr) => {
                    constraints.push(TableConstraint::Check {
                        name: opt.name.as_ref().map(|name| name.to_string()),
                        expr: expr.to_string(),
                    });
                }
                ast::ColumnOption::ForeignKey(fk) => {
                    constraints.push(TableConstraint::ForeignKey {
                        name: opt.name.as_ref().map(|name| name.to_string()),
                        columns: vec![col.name.value.clone()],
                        ref_table: object_name_key(&fk.foreign_table),
                        ref_columns: fk
                            .referred_columns
                            .iter()
                            .map(|c| c.value.clone())
                            .collect(),
                        on_delete: convert_fk_action(&fk.on_delete),
                        on_update: convert_fk_action(&fk.on_update),
                        deferrable: deferrable_from(fk.characteristics.as_ref()),
                    });
                }
                _ => {}
            }
        }
    }

    // Table-level constraints
    for tc in table_constraints {
        match tc {
            ast::TableConstraint::PrimaryKey(pk) => {
                constraints.retain(|c| !matches!(c, TableConstraint::PrimaryKey { .. }));
                constraints.push(TableConstraint::PrimaryKey {
                    name: pk.name.as_ref().map(|name| name.to_string()),
                    columns: pk.columns.iter().map(index_column_name).collect(),
                });
            }
            ast::TableConstraint::Unique(u) => {
                constraints.push(TableConstraint::Unique {
                    name: u.name.as_ref().map(|n| n.to_string()),
                    columns: u.columns.iter().map(index_column_name).collect(),
                });
            }
            ast::TableConstraint::Check(ck) => {
                constraints.push(TableConstraint::Check {
                    name: ck.name.as_ref().map(|n| n.to_string()),
                    expr: ck.expr.to_string(),
                });
            }
            ast::TableConstraint::ForeignKey(fk) => {
                constraints.push(TableConstraint::ForeignKey {
                    name: fk.name.as_ref().map(|n| n.to_string()),
                    columns: fk.columns.iter().map(|c| c.value.clone()).collect(),
                    ref_table: object_name_key(&fk.foreign_table),
                    ref_columns: fk
                        .referred_columns
                        .iter()
                        .map(|c| c.value.clone())
                        .collect(),
                    on_delete: convert_fk_action(&fk.on_delete),
                    on_update: convert_fk_action(&fk.on_update),
                    deferrable: deferrable_from(fk.characteristics.as_ref()),
                });
            }
            _ => {}
        }
    }

    constraints
}

#[derive(Debug, thiserror::Error)]
pub enum ParseError {
    #[error("SQL parse error: {0}")]
    SqlParser(#[from] sqlparser::parser::ParserError),
    #[error("unsupported data type: {0}")]
    UnsupportedDataType(String),
    #[error("unexpected statement: expected {0}")]
    UnexpectedStatement(String),
    #[error("statement too complex: parenthesis nesting exceeds maximum of {0}")]
    StatementTooComplex(usize),
    #[error("statement too complex: expression nesting exceeds maximum of {0}")]
    ExpressionTooDeep(usize),
    #[error(
        "FINAL is not supported (table '{0}'). Nucleus collapses replacing_mergetree \
         tables on every read, so FINAL has nothing to select — remove it. If you \
         meant to alias the table, quote it: AS \"FINAL\"."
    )]
    FinalNotSupported(String),
}

// ============================================================================
// Tests
// ============================================================================

/// Substitute `$1`/`$2`... (positional) and `$name` (named) placeholders in
/// SQL text, honoring single/double quotes and line/block comments.
///
/// The one shared substitution scanner for procedure bodies and UDF bodies
/// (previously two hand-copied scanners that each grew the same bugs).
/// UTF-8-safe by construction: literal runs are copied as raw bytes and the
/// result is decoded once at the end — copying SQL bytes through
/// `out.push(bytes[i] as char)` mojibaked every multi-byte sequence into
/// Latin-1 (the WIR-4 family). All edits here touch ASCII delimiters only,
/// so the input's UTF-8 validity carries through.
pub(crate) fn substitute_sql_placeholders(
    sql: &str,
    positional: &[String],
    named: &std::collections::HashMap<String, String>,
) -> String {
    let mut out: Vec<u8> = Vec::with_capacity(sql.len() + 32);
    let bytes = sql.as_bytes();
    let mut i = 0usize;
    let mut in_single = false;
    let mut in_double = false;
    let mut in_line_comment = false;
    let mut in_block_comment = false;

    while i < bytes.len() {
        if in_line_comment {
            out.push(bytes[i]);
            if bytes[i] == b'\n' {
                in_line_comment = false;
            }
            i += 1;
            continue;
        }
        if in_block_comment {
            if i + 1 < bytes.len() && bytes[i] == b'*' && bytes[i + 1] == b'/' {
                out.push(b'*');
                out.push(b'/');
                in_block_comment = false;
                i += 2;
            } else {
                out.push(bytes[i]);
                i += 1;
            }
            continue;
        }
        if in_single {
            out.push(bytes[i]);
            if bytes[i] == b'\'' {
                if i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                    out.push(b'\'');
                    i += 2;
                } else {
                    in_single = false;
                    i += 1;
                }
            } else {
                i += 1;
            }
            continue;
        }
        if in_double {
            out.push(bytes[i]);
            if bytes[i] == b'"' {
                if i + 1 < bytes.len() && bytes[i + 1] == b'"' {
                    out.push(b'"');
                    i += 2;
                } else {
                    in_double = false;
                    i += 1;
                }
            } else {
                i += 1;
            }
            continue;
        }

        if i + 1 < bytes.len() && bytes[i] == b'-' && bytes[i + 1] == b'-' {
            out.push(b'-');
            out.push(b'-');
            in_line_comment = true;
            i += 2;
            continue;
        }
        if i + 1 < bytes.len() && bytes[i] == b'/' && bytes[i + 1] == b'*' {
            out.push(b'/');
            out.push(b'*');
            in_block_comment = true;
            i += 2;
            continue;
        }
        if bytes[i] == b'\'' {
            out.push(b'\'');
            in_single = true;
            i += 1;
            continue;
        }
        if bytes[i] == b'"' {
            out.push(b'"');
            in_double = true;
            i += 1;
            continue;
        }

        if bytes[i] == b'$' {
            let start = i;
            i += 1;
            if i < bytes.len() && bytes[i].is_ascii_digit() {
                let mut idx = 0usize;
                while i < bytes.len() && bytes[i].is_ascii_digit() {
                    idx = idx * 10 + (bytes[i] - b'0') as usize;
                    i += 1;
                }
                if idx > 0 && idx <= positional.len() {
                    out.extend_from_slice(positional[idx - 1].as_bytes());
                } else {
                    out.extend_from_slice(&bytes[start..i]);
                }
                continue;
            }
            if i < bytes.len() && (bytes[i].is_ascii_alphabetic() || bytes[i] == b'_') {
                let ident_start = i;
                i += 1;
                while i < bytes.len() && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_') {
                    i += 1;
                }
                let ident = &sql[ident_start..i];
                if let Some(repl) = named.get(ident) {
                    out.extend_from_slice(repl.as_bytes());
                } else {
                    out.extend_from_slice(&bytes[start..i]);
                }
                continue;
            }
            out.push(b'$');
            continue;
        }

        out.push(bytes[i]);
        i += 1;
    }

    String::from_utf8(out).unwrap_or_else(|_| sql.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_select() {
        let stmts = parse("SELECT 1").unwrap();
        assert_eq!(stmts.len(), 1);
        assert!(matches!(stmts[0], ast::Statement::Query(_)));
    }

    /// A paren-free `AND` chain builds one tree level per term, so it must be
    /// bounded by `check_ast_depth` — `check_nesting_depth` cannot see it, and
    /// neither can sqlparser's own recursion counter. Left unbounded it is a
    /// stack overflow (process abort), not a query error, because every walk of
    /// the tree downstream — the AST cache's `Clone` in particular — recurses.
    #[test]
    fn parse_rejects_paren_free_deep_expression_chain() {
        let terms = vec!["1=1"; MAX_AST_EXPR_DEPTH * 2];
        let sql = format!("SELECT id FROM t WHERE {}", terms.join(" AND "));
        assert!(matches!(
            parse(&sql),
            Err(ParseError::ExpressionTooDeep(MAX_AST_EXPR_DEPTH))
        ));
    }

    /// The cap must not reject chains a real query might contain.
    #[test]
    fn parse_accepts_moderately_deep_expression_chain() {
        let terms = vec!["1=1"; 100];
        let sql = format!("SELECT id FROM t WHERE {}", terms.join(" AND "));
        assert!(parse(&sql).is_ok());

        // Wide, not deep: many sibling expressions nest only one level each.
        let cols: Vec<String> = (0..500).map(|i| format!("c{i} + 1")).collect();
        assert!(parse(&format!("SELECT {} FROM t", cols.join(", "))).is_ok());
    }

    #[test]
    fn parse_create_table() {
        let stmts = parse("CREATE TABLE t (id INT PRIMARY KEY, name TEXT NOT NULL)").unwrap();
        assert_eq!(stmts.len(), 1);
        assert!(matches!(stmts[0], ast::Statement::CreateTable(_)));
    }

    #[test]
    fn parse_insert() {
        let stmts = parse("INSERT INTO t VALUES (1, 'hello')").unwrap();
        assert_eq!(stmts.len(), 1);
        assert!(matches!(stmts[0], ast::Statement::Insert(_)));
    }

    #[test]
    fn parse_update() {
        let stmts = parse("UPDATE t SET name = 'world' WHERE id = 1").unwrap();
        assert_eq!(stmts.len(), 1);
        assert!(matches!(stmts[0], ast::Statement::Update(_)));
    }

    #[test]
    fn parse_delete() {
        let stmts = parse("DELETE FROM t WHERE id = 1").unwrap();
        assert_eq!(stmts.len(), 1);
        assert!(matches!(stmts[0], ast::Statement::Delete(_)));
    }

    #[test]
    fn parse_multiple_statements() {
        let stmts = parse("SELECT 1; SELECT 2; SELECT 3").unwrap();
        assert_eq!(stmts.len(), 3);
    }

    #[test]
    fn parse_error_on_invalid_sql() {
        let result = parse("SELECTOID BLOOP FROM");
        assert!(result.is_err());
    }

    #[test]
    fn parse_empty_string() {
        let stmts = parse("").unwrap();
        assert!(stmts.is_empty());
    }

    #[test]
    fn convert_data_type_int() {
        let dt = convert_data_type(&ast::DataType::Integer(None)).unwrap();
        assert_eq!(dt, DataType::Int32);
    }

    #[test]
    fn convert_data_type_bigint() {
        let dt = convert_data_type(&ast::DataType::BigInt(None)).unwrap();
        assert_eq!(dt, DataType::Int64);
    }

    #[test]
    fn convert_data_type_float() {
        let dt = convert_data_type(&ast::DataType::Float(ast::ExactNumberInfo::None)).unwrap();
        assert_eq!(dt, DataType::Float64);
    }

    #[test]
    fn convert_data_type_text() {
        let dt = convert_data_type(&ast::DataType::Text).unwrap();
        assert_eq!(dt, DataType::Text);
    }

    #[test]
    fn convert_data_type_bool() {
        let dt = convert_data_type(&ast::DataType::Boolean).unwrap();
        assert_eq!(dt, DataType::Bool);
    }

    #[test]
    fn convert_data_type_jsonb() {
        let dt = convert_data_type(&ast::DataType::JSONB).unwrap();
        assert_eq!(dt, DataType::Jsonb);
    }

    #[test]
    fn convert_data_type_date() {
        let dt = convert_data_type(&ast::DataType::Date).unwrap();
        assert_eq!(dt, DataType::Date);
    }

    #[test]
    fn convert_data_type_uuid() {
        let dt = convert_data_type(&ast::DataType::Uuid).unwrap();
        assert_eq!(dt, DataType::Uuid);
    }

    #[test]
    fn convert_data_type_bytea() {
        let dt = convert_data_type(&ast::DataType::Bytea).unwrap();
        assert_eq!(dt, DataType::Bytea);
    }

    #[test]
    fn convert_data_type_numeric() {
        let dt = convert_data_type(&ast::DataType::Numeric(ast::ExactNumberInfo::None)).unwrap();
        assert_eq!(dt, DataType::Numeric);
    }

    #[test]
    fn convert_data_type_varchar() {
        let dt = convert_data_type(&ast::DataType::Varchar(None)).unwrap();
        assert_eq!(dt, DataType::Text);
    }

    #[test]
    fn extract_columns_basic() -> Result<(), ParseError> {
        let stmts = parse("CREATE TABLE t (id INT NOT NULL, name TEXT, age BIGINT)")?;
        if let ast::Statement::CreateTable(ct) = &stmts[0] {
            let cols = extract_columns(&ct.columns)?;
            assert_eq!(cols.len(), 3);
            assert_eq!(cols[0].name, "id");
            assert_eq!(cols[0].data_type, DataType::Int32);
            assert!(!cols[0].nullable);
            assert_eq!(cols[1].name, "name");
            assert_eq!(cols[1].data_type, DataType::Text);
            assert!(cols[1].nullable);
            assert_eq!(cols[2].name, "age");
            assert_eq!(cols[2].data_type, DataType::Int64);
        } else {
            return Err(ParseError::UnexpectedStatement("CREATE TABLE".into()));
        }
        Ok(())
    }

    #[test]
    fn extract_constraints_primary_key() -> Result<(), ParseError> {
        let stmts = parse("CREATE TABLE t (id INT PRIMARY KEY, name TEXT)")?;
        if let ast::Statement::CreateTable(ct) = &stmts[0] {
            let constraints = extract_constraints(&ct.columns, &ct.constraints);
            assert!(constraints.iter().any(|c| matches!(c, crate::catalog::TableConstraint::PrimaryKey { columns, .. } if columns == &["id"])));
        } else {
            return Err(ParseError::UnexpectedStatement("CREATE TABLE".into()));
        }
        Ok(())
    }

    #[test]
    fn parse_complex_query() {
        let sql = "SELECT u.id, u.name, COUNT(o.id) as order_count \
                   FROM users u \
                   JOIN orders o ON u.id = o.user_id \
                   WHERE u.active = true \
                   GROUP BY u.id, u.name \
                   HAVING COUNT(o.id) > 5 \
                   ORDER BY order_count DESC \
                   LIMIT 10";
        let stmts = parse(sql).unwrap();
        assert_eq!(stmts.len(), 1);
    }

    #[test]
    fn parse_cte() {
        let sql = "WITH active_users AS (SELECT * FROM users WHERE active = true) \
                   SELECT * FROM active_users";
        let stmts = parse(sql).unwrap();
        assert_eq!(stmts.len(), 1);
    }

    #[test]
    fn parse_window_function() {
        let sql = "SELECT name, salary, RANK() OVER (ORDER BY salary DESC) FROM employees";
        let stmts = parse(sql).unwrap();
        assert_eq!(stmts.len(), 1);
    }

    #[test]
    fn parse_create_index() {
        let stmts = parse("CREATE INDEX idx_name ON users (name)").unwrap();
        assert_eq!(stmts.len(), 1);
        assert!(matches!(stmts[0], ast::Statement::CreateIndex(_)));
    }

    #[test]
    fn parse_alter_table() {
        let stmts = parse("ALTER TABLE users ADD COLUMN email TEXT").unwrap();
        assert_eq!(stmts.len(), 1);
        assert!(matches!(stmts[0], ast::Statement::AlterTable(_)));
    }

    #[test]
    fn parse_transaction_statements() {
        assert!(parse("BEGIN").is_ok());
        assert!(parse("COMMIT").is_ok());
        assert!(parse("ROLLBACK").is_ok());
    }

    // ========================================================================
    // Property-based tests (proptest)
    // ========================================================================

    use proptest::prelude::*;

    /// Strategy for valid SQL identifiers: starts with a lowercase letter,
    /// followed by 0..20 lowercase alphanumeric or underscore characters, and
    /// is not a reserved word.
    ///
    /// The keyword filter is not decoration. Without it this generator produced
    /// `SELECT top FROM a`, which is not valid SQL in this dialect (`TOP` takes
    /// a count) — so the property failed on a query the parser is *right* to
    /// reject, and every one of the five round-trip properties below could fail
    /// the same way on any run. It never fired in CI because the seed is random
    /// per run; it fired locally on 2026-08-17, and proptest then persisted the
    /// case, which is the only reason a latent dice roll became visible at all.
    /// Filtering against sqlparser's own list keeps this correct as the dialect
    /// grows keywords, which a hand-written exclusion list would not.
    fn ident_strategy() -> impl Strategy<Value = String> {
        "[a-z][a-z0-9_]{0,20}".prop_filter("SQL keywords are not bare identifiers", |s| {
            !sqlparser::keywords::ALL_KEYWORDS.contains(&s.to_uppercase().as_str())
        })
    }

    /// Strategy for integer literal values.
    fn int_val_strategy() -> impl Strategy<Value = String> {
        (0i64..1_000_000i64).prop_map(|n| n.to_string())
    }

    proptest! {
        /// SELECT {col} FROM {table} roundtrips through parse -> to_string -> parse.
        #[test]
        fn prop_sql_select_roundtrip(
            col in ident_strategy(),
            table in ident_strategy(),
        ) {
            let sql = format!("SELECT {col} FROM {table}");
            let stmts1 = parse(&sql).expect("first parse should succeed");
            let formatted = stmts1[0].to_string();
            let stmts2 = parse(&formatted).expect("re-parse of formatted SQL should succeed");
            prop_assert_eq!(stmts1.len(), stmts2.len());
            prop_assert_eq!(stmts1[0].to_string(), stmts2[0].to_string());
        }

        /// INSERT INTO {table} ({col}) VALUES ({val}) roundtrips.
        #[test]
        fn prop_sql_insert_roundtrip(
            table in ident_strategy(),
            col in ident_strategy(),
            val in int_val_strategy(),
        ) {
            let sql = format!("INSERT INTO {table} ({col}) VALUES ({val})");
            let stmts1 = parse(&sql).expect("first parse should succeed");
            let formatted = stmts1[0].to_string();
            let stmts2 = parse(&formatted).expect("re-parse of formatted SQL should succeed");
            prop_assert_eq!(stmts1.len(), stmts2.len());
            prop_assert_eq!(stmts1[0].to_string(), stmts2[0].to_string());
        }

        /// CREATE TABLE {table} ({col1} INT, {col2} TEXT) roundtrips.
        #[test]
        fn prop_sql_create_table_roundtrip(
            table in ident_strategy(),
            col1 in ident_strategy(),
            col2 in ident_strategy(),
        ) {
            let sql = format!("CREATE TABLE {table} ({col1} INT, {col2} TEXT)");
            let stmts1 = parse(&sql).expect("first parse should succeed");
            let formatted = stmts1[0].to_string();
            let stmts2 = parse(&formatted).expect("re-parse of formatted SQL should succeed");
            prop_assert_eq!(stmts1.len(), stmts2.len());
            prop_assert_eq!(stmts1[0].to_string(), stmts2[0].to_string());
        }

        /// DELETE FROM {table} WHERE {col} = {val} roundtrips.
        #[test]
        fn prop_sql_delete_roundtrip(
            table in ident_strategy(),
            col in ident_strategy(),
            val in int_val_strategy(),
        ) {
            let sql = format!("DELETE FROM {table} WHERE {col} = {val}");
            let stmts1 = parse(&sql).expect("first parse should succeed");
            let formatted = stmts1[0].to_string();
            let stmts2 = parse(&formatted).expect("re-parse of formatted SQL should succeed");
            prop_assert_eq!(stmts1.len(), stmts2.len());
            prop_assert_eq!(stmts1[0].to_string(), stmts2[0].to_string());
        }

        /// UPDATE {table} SET {col} = {val} roundtrips.
        #[test]
        fn prop_sql_update_roundtrip(
            table in ident_strategy(),
            col in ident_strategy(),
            val in int_val_strategy(),
        ) {
            let sql = format!("UPDATE {table} SET {col} = {val}");
            let stmts1 = parse(&sql).expect("first parse should succeed");
            let formatted = stmts1[0].to_string();
            let stmts2 = parse(&formatted).expect("re-parse of formatted SQL should succeed");
            prop_assert_eq!(stmts1.len(), stmts2.len());
            prop_assert_eq!(stmts1[0].to_string(), stmts2[0].to_string());
        }

        /// Random garbage strings never cause panics in the parser.
        #[test]
        fn prop_sql_random_garbage_no_panic(s in "\\PC{0,200}") {
            // Should either parse or return an error, but never panic.
            let _ = parse(&s);
        }
    }
}
