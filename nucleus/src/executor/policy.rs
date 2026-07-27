//! Row-level security policy DDL and predicate compilation.

use sqlparser::ast::{self, BinaryOperator, Expr};

use crate::security::{PolicyCommand, RlsPolicy, RlsPredicate};

use super::{ExecError, ExecResult, Executor};

impl Executor {
    pub(super) fn execute_create_policy(
        &self,
        policy: ast::CreatePolicy,
    ) -> Result<ExecResult, ExecError> {
        self.require_security_admin("create row security policies")?;
        let table = policy.table_name.to_string();
        let table_def = self
            .catalog
            .get_table_cached(&table)
            .ok_or_else(|| ExecError::TableNotFound(table.clone()))?;
        let command = match policy.command.unwrap_or(ast::CreatePolicyCommand::All) {
            ast::CreatePolicyCommand::All => PolicyCommand::All,
            ast::CreatePolicyCommand::Select => PolicyCommand::Select,
            ast::CreatePolicyCommand::Insert => PolicyCommand::Insert,
            ast::CreatePolicyCommand::Update => PolicyCommand::Update,
            ast::CreatePolicyCommand::Delete => PolicyCommand::Delete,
        };
        if command == PolicyCommand::Select && policy.with_check.is_some() {
            return Err(ExecError::Unsupported(
                "WITH CHECK is not valid for SELECT policies".into(),
            ));
        }
        if command == PolicyCommand::Insert && policy.using.is_some() {
            return Err(ExecError::Unsupported(
                "USING is not valid for INSERT policies".into(),
            ));
        }
        let mut using_predicate = policy
            .using
            .as_ref()
            .map(Self::compile_rls_predicate)
            .transpose()?
            .unwrap_or(RlsPredicate::AlwaysTrue);
        let mut check_predicate = policy
            .with_check
            .as_ref()
            .map(Self::compile_rls_predicate)
            .transpose()?;
        for predicate in std::iter::once(&using_predicate).chain(check_predicate.iter()) {
            Self::validate_rls_columns(predicate, &table_def)?;
        }
        // Resolve each referenced column to its stable id, once, here. From now
        // on the id is what the policy means and the name is a cache: a later
        // RENAME COLUMN refreshes the name through the id, so the policy keeps
        // guarding the same column instead of following the old name to
        // whatever is subsequently created under it.
        {
            let resolve = |name: &str| table_def.column_id(name);
            using_predicate.bind_column_ids(&resolve);
            if let Some(check) = check_predicate.as_mut() {
                check.bind_column_ids(&resolve);
            }
        }
        let target_roles = policy
            .to
            .unwrap_or_default()
            .into_iter()
            .map(|owner| match owner {
                ast::Owner::Ident(id) => id.value,
                ast::Owner::CurrentRole | ast::Owner::CurrentUser => {
                    self.current_session().session_context.read().user.clone()
                }
                ast::Owner::SessionUser => self
                    .current_session()
                    .authenticated_user
                    .read()
                    .clone()
                    .unwrap_or_default(),
            })
            .collect::<Vec<_>>();
        if let Ok(roles) = self.roles.try_read() {
            for role in &target_roles {
                if !role.eq_ignore_ascii_case("public") && !roles.contains_key(role) {
                    return Err(ExecError::Unsupported(format!(
                        "role '{role}' does not exist"
                    )));
                }
            }
        } else {
            return Err(ExecError::Runtime(
                "role catalog is busy; retry CREATE POLICY".into(),
            ));
        }
        let name = policy.name.value;
        self.with_mutable_security(|security| {
            if security.rls.policy(&table, &name).is_some() {
                return Err(ExecError::Unsupported(format!(
                    "policy '{name}' for table '{table}' already exists"
                )));
            }
            security.rls.add_policy(RlsPolicy {
                name,
                table,
                command,
                target_roles,
                predicate: using_predicate,
                check_predicate,
                permissive: !matches!(policy.policy_type, Some(ast::CreatePolicyType::Restrictive)),
            });
            Ok(())
        })??;
        self.bump_policy_gen();
        Ok(ExecResult::Command {
            tag: "CREATE POLICY".into(),
            rows_affected: 0,
        })
    }

    pub(super) fn execute_drop_policy(
        &self,
        policy: ast::DropPolicy,
    ) -> Result<ExecResult, ExecError> {
        self.require_security_admin("drop row security policies")?;
        let table = policy.table_name.to_string();
        let removed = self.with_mutable_security(|security| {
            security.rls.remove_policy(&table, &policy.name.value)
        })?;
        if !removed && !policy.if_exists {
            return Err(ExecError::Unsupported(format!(
                "policy '{}' for table '{table}' does not exist",
                policy.name.value
            )));
        }
        if removed {
            self.bump_policy_gen();
        }
        Ok(ExecResult::Command {
            tag: "DROP POLICY".into(),
            rows_affected: 0,
        })
    }

    fn compile_rls_predicate(expr: &Expr) -> Result<RlsPredicate, ExecError> {
        match expr {
            Expr::Nested(inner) => Self::compile_rls_predicate(inner),
            Expr::UnaryOp {
                op: ast::UnaryOperator::Not,
                expr,
            } => Ok(RlsPredicate::Not(Box::new(Self::compile_rls_predicate(
                expr,
            )?))),
            Expr::BinaryOp { left, op, right } if *op == BinaryOperator::And => {
                Ok(RlsPredicate::And(
                    Box::new(Self::compile_rls_predicate(left)?),
                    Box::new(Self::compile_rls_predicate(right)?),
                ))
            }
            Expr::BinaryOp { left, op, right } if *op == BinaryOperator::Or => {
                Ok(RlsPredicate::Or(
                    Box::new(Self::compile_rls_predicate(left)?),
                    Box::new(Self::compile_rls_predicate(right)?),
                ))
            }
            Expr::BinaryOp { left, op, right } if *op == BinaryOperator::Eq => {
                Self::compile_rls_equality(left, right)
                    .or_else(|_| Self::compile_rls_equality(right, left))
            }
            Expr::Value(v) => match &v.value {
                ast::Value::Boolean(true) => Ok(RlsPredicate::AlwaysTrue),
                ast::Value::Boolean(false) | ast::Value::Null => Ok(RlsPredicate::AlwaysFalse),
                _ => Err(Self::unsupported_policy_expr(expr)),
            },
            Expr::Function(function)
                if function.name.to_string().eq_ignore_ascii_case("has_role") =>
            {
                let role =
                    Self::single_quoted_argument(&function.to_string()).ok_or_else(|| {
                        ExecError::Unsupported("has_role() requires one string literal".into())
                    })?;
                Ok(RlsPredicate::HasRole { role })
            }
            _ => Err(Self::unsupported_policy_expr(expr)),
        }
    }

    fn compile_rls_equality(
        column_expr: &Expr,
        value_expr: &Expr,
    ) -> Result<RlsPredicate, ExecError> {
        let column = match column_expr {
            Expr::Identifier(id) => id.value.clone(),
            Expr::CompoundIdentifier(ids) => ids
                .last()
                .map(|id| id.value.clone())
                .ok_or_else(|| Self::unsupported_policy_expr(column_expr))?,
            _ => return Err(Self::unsupported_policy_expr(column_expr)),
        };
        let normalized = value_expr.to_string().to_ascii_lowercase().replace(' ', "");
        if matches!(
            normalized.as_str(),
            "current_user" | "session_user" | "current_user()"
        ) {
            return Ok(RlsPredicate::ColumnEqUser { column, column_id: 0 });
        }
        if normalized.starts_with("current_setting(") && normalized.contains("nucleus.tenant_id") {
            return Ok(RlsPredicate::ColumnEqTenant { column, column_id: 0 });
        }
        if let Expr::Value(v) = value_expr {
            let value = match &v.value {
                ast::Value::SingleQuotedString(s)
                | ast::Value::DoubleQuotedString(s)
                | ast::Value::NationalStringLiteral(s)
                | ast::Value::EscapedStringLiteral(s) => s.clone(),
                ast::Value::Number(n, _) => n.clone(),
                ast::Value::Boolean(v) => v.to_string(),
                _ => return Err(Self::unsupported_policy_expr(value_expr)),
            };
            return Ok(RlsPredicate::ColumnEqStr { column, value, column_id: 0 });
        }
        Err(Self::unsupported_policy_expr(value_expr))
    }

    fn single_quoted_argument(rendered: &str) -> Option<String> {
        let start = rendered.find('\'')? + 1;
        let end = rendered[start..].find('\'')? + start;
        Some(rendered[start..end].to_string())
    }

    fn unsupported_policy_expr(expr: &Expr) -> ExecError {
        ExecError::Unsupported(format!(
            "unsupported row-security predicate '{expr}'; supported forms are boolean constants, column equality to a literal/CURRENT_USER/current_setting('nucleus.tenant_id'), has_role(), NOT, AND, and OR"
        ))
    }

    fn validate_rls_columns(
        predicate: &RlsPredicate,
        table: &crate::catalog::TableDef,
    ) -> Result<(), ExecError> {
        match predicate {
            RlsPredicate::ColumnEqStr { column, .. }
            | RlsPredicate::ColumnEqTenant { column, .. }
            | RlsPredicate::ColumnEqUser { column, .. } => {
                if table.column_index(column).is_none() {
                    return Err(ExecError::ColumnNotFound(column.clone()));
                }
            }
            RlsPredicate::And(a, b) | RlsPredicate::Or(a, b) => {
                Self::validate_rls_columns(a, table)?;
                Self::validate_rls_columns(b, table)?;
            }
            RlsPredicate::Not(p) => Self::validate_rls_columns(p, table)?,
            RlsPredicate::HasRole { .. } | RlsPredicate::AlwaysTrue | RlsPredicate::AlwaysFalse => {
            }
        }
        Ok(())
    }
}
