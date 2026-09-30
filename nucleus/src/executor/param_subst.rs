//! AST-level parameter substitution for PREPARE/EXECUTE.
//!
//! Instead of string-replacing `$N` placeholders and re-parsing, this module
//! walks the already-parsed AST and replaces `Placeholder("$N")` nodes with
//! literal value nodes — skipping the SQL parser entirely on EXECUTE.

use std::ops::ControlFlow;

use sqlparser::ast::{self, Expr, Statement, VisitMut, VisitorMut};

use crate::types::Value;

/// Walk `stmt` in place, replacing every `$N` placeholder with the corresponding
/// literal value from `params` (1-indexed: `$1` → `params[0]`).
pub fn substitute_params_in_stmt(stmt: &mut Statement, params: &[Value]) {
    substitute_params_with_types(stmt, params, &[]);
}

/// Wire parameters may carry authoritative array types even without a SQL
/// cast. Keep their literal text until the executor applies session semantics.
pub fn substitute_params_with_types(
    stmt: &mut Statement,
    params: &[Value],
    types: &[Option<ast::DataType>],
) {
    let mut substitutor = ParamSubstitutor { params, types };
    // VisitorMut::visit walks all nested expressions automatically.
    let _ = stmt.visit(&mut substitutor);
}

struct ParamSubstitutor<'a> {
    params: &'a [Value],
    types: &'a [Option<ast::DataType>],
}

impl<'a> VisitorMut for ParamSubstitutor<'a> {
    type Break = ();

    fn post_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<Self::Break> {
        if let Expr::Value(vws) = expr
            && let ast::Value::Placeholder(ref placeholder) = vws.value
            && let Some(idx_str) = placeholder.strip_prefix('$')
            && let Ok(idx) = idx_str.parse::<usize>()
            && idx >= 1
            && idx <= self.params.len()
        {
            *expr = nucleus_value_to_ast(&self.params[idx - 1]);
            if let Some(Some(target)) = self.types.get(idx - 1) {
                *expr = Expr::Cast {
                    kind: ast::CastKind::DoubleColon,
                    expr: Box::new(expr.clone()),
                    data_type: target.clone(),
                    format: None,
                    array: false,
                };
            }
        }
        ControlFlow::Continue(())
    }
}

/// Convert a Nucleus `Value` into an sqlparser `Expr` literal.
fn nucleus_value_to_ast(val: &Value) -> Expr {
    use ast::Value as SqlVal;

    let sql_val = match val {
        Value::Int32(n) => SqlVal::Number(n.to_string(), false),
        Value::Int64(n) => SqlVal::Number(n.to_string(), false),
        Value::Float64(f) => SqlVal::Number(f.to_string(), false),
        Value::Text(s) => SqlVal::SingleQuotedString(s.clone()),
        Value::Bool(b) => SqlVal::Boolean(*b),
        Value::Null => SqlVal::Null,
        Value::Bytea(b) => {
            let hex_str: String = b.iter().map(|byte| format!("{byte:02x}")).collect();
            SqlVal::HexStringLiteral(hex_str)
        }
        Value::Jsonb(j) => SqlVal::SingleQuotedString(j.to_string()),
        // Date is stored as days-since-epoch i32
        Value::Date(d) => SqlVal::SingleQuotedString(d.to_string()),
        // Timestamp is stored as microseconds-since-epoch i64
        Value::Timestamp(ts) => SqlVal::SingleQuotedString(ts.to_string()),
        // UUID is stored as [u8; 16]
        Value::Uuid(u) => {
            let s = format!(
                "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
                u[0],
                u[1],
                u[2],
                u[3],
                u[4],
                u[5],
                u[6],
                u[7],
                u[8],
                u[9],
                u[10],
                u[11],
                u[12],
                u[13],
                u[14],
                u[15]
            );
            SqlVal::SingleQuotedString(s)
        }
        _ => SqlVal::SingleQuotedString(format!("{val:?}")),
    };
    Expr::value(sql_val)
}
