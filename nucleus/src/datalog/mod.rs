//! Datalog logic programming engine for Nucleus.
//!
//! Provides a complete Datalog implementation with:
//! - Hand-written recursive descent parser for Datalog syntax
//! - Indexed fact store (EDB) with O(1) lookups
//! - Semi-naive bottom-up evaluator with stratified negation
//! - SQL integration functions for use from the executor
//! - Write-ahead log for crash recovery
//!
//! # Datalog Syntax
//! ```prolog
//! % Facts (ground terms only)
//! parent(alice, bob).
//! employee(alice, engineering, 150000).
//!
//! % Rules (head :- body)
//! ancestor(X, Y) :- parent(X, Y).
//! ancestor(X, Z) :- ancestor(X, Y), parent(Y, Z).
//!
//! % Stratified negation
//! not_ancestor(X, Y) :- person(X), person(Y), \+ ancestor(X, Y).
//!
//! % Queries
//! ?- ancestor(alice, Who).
//! ```

use std::collections::{HashMap, HashSet, VecDeque};
use std::fs::{File, OpenOptions};
use std::io::{self, BufWriter, Write};
use std::path::{Path, PathBuf};

use parking_lot::Mutex;

// ═══════════════════════════════════════════════════════════════════════════════
// Part A: Parser
// ═══════════════════════════════════════════════════════════════════════════════

/// A term in a Datalog literal — either a constant, a variable, or an aggregate.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Term {
    /// Lowercase atoms, quoted strings, or numbers.
    Const(String),
    /// Variables start with an uppercase letter.
    Var(String),
    /// Aggregate function: count(), sum(Var), min(Var), max(Var).
    Agg(AggFunc),
}

/// Aggregate function variants for Datalog rule heads.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum AggFunc {
    Count,
    Sum(String),
    Min(String),
    Max(String),
}

/// A literal (predicate applied to terms), possibly negated.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Literal {
    pub predicate: String,
    pub args: Vec<Term>,
    pub negated: bool,
}

/// A Datalog rule: `head :- body1, body2, ...`
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Rule {
    pub head: Literal,
    pub body: Vec<Literal>,
}

/// A ground fact (all arguments are constants).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Fact {
    pub predicate: String,
    pub args: Vec<String>,
}

/// A parsed Datalog statement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Statement {
    Fact(Fact),
    Rule(Rule),
    Query(Literal),
}

// ─── Token types ─────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq)]
enum Token {
    Atom(String),      // lowercase identifier
    Variable(String),  // uppercase identifier
    StringLit(String), // "quoted" or 'quoted'
    Number(String),    // integer or decimal
    LParen,            // (
    RParen,            // )
    Comma,             // ,
    Dot,               // .
    ColonDash,         // :-
    NegPrefix,         // \+
    QueryPrefix,       // ?-
}

/// Tokenize Datalog source text.
fn tokenize(input: &str) -> Result<Vec<Token>, String> {
    let mut tokens = Vec::new();
    let chars: Vec<char> = input.chars().collect();
    let len = chars.len();
    let mut i = 0;

    while i < len {
        let ch = chars[i];

        // Skip whitespace
        if ch.is_ascii_whitespace() {
            i += 1;
            continue;
        }

        // Comments: % to end of line
        if ch == '%' {
            while i < len && chars[i] != '\n' {
                i += 1;
            }
            continue;
        }

        // Two-character tokens
        if ch == ':' && i + 1 < len && chars[i + 1] == '-' {
            tokens.push(Token::ColonDash);
            i += 2;
            continue;
        }
        if ch == '?' && i + 1 < len && chars[i + 1] == '-' {
            tokens.push(Token::QueryPrefix);
            i += 2;
            continue;
        }
        if ch == '\\' && i + 1 < len && chars[i + 1] == '+' {
            tokens.push(Token::NegPrefix);
            i += 2;
            continue;
        }

        // Single-character tokens
        match ch {
            '(' => {
                tokens.push(Token::LParen);
                i += 1;
                continue;
            }
            ')' => {
                tokens.push(Token::RParen);
                i += 1;
                continue;
            }
            ',' => {
                tokens.push(Token::Comma);
                i += 1;
                continue;
            }
            '.' => {
                // Check if it's a decimal number (e.g., .5)
                if i + 1 < len && chars[i + 1].is_ascii_digit() {
                    let start = i;
                    i += 1;
                    while i < len && chars[i].is_ascii_digit() {
                        i += 1;
                    }
                    let num: String = chars[start..i].iter().collect();
                    tokens.push(Token::Number(num));
                    continue;
                }
                tokens.push(Token::Dot);
                i += 1;
                continue;
            }
            _ => {}
        }

        // Quoted strings (single or double quotes)
        if ch == '"' || ch == '\'' {
            let quote = ch;
            i += 1;
            let start = i;
            while i < len && chars[i] != quote {
                if chars[i] == '\\' && i + 1 < len {
                    i += 2; // skip escaped char
                } else {
                    i += 1;
                }
            }
            if i >= len {
                return Err(format!(
                    "Unterminated string literal starting at position {start}"
                ));
            }
            // Unescape only \\ \" \' — the exact sequences `format_fact_arg`
            // emits — so a checkpointed quoted constant re-parses to the same
            // string. Everything else (\n etc.) stays verbatim, as before.
            let mut s = String::with_capacity(i - start);
            let mut j = start;
            while j < i {
                if chars[j] == '\\' && j + 1 < i && matches!(chars[j + 1], '\\' | '"' | '\'') {
                    s.push(chars[j + 1]);
                    j += 2;
                } else {
                    s.push(chars[j]);
                    j += 1;
                }
            }
            tokens.push(Token::StringLit(s));
            i += 1; // skip closing quote
            continue;
        }

        // Numbers (integers and decimals, optionally negative)
        if ch.is_ascii_digit() || (ch == '-' && i + 1 < len && chars[i + 1].is_ascii_digit()) {
            let start = i;
            if ch == '-' {
                i += 1;
            }
            while i < len && chars[i].is_ascii_digit() {
                i += 1;
            }
            if i < len && chars[i] == '.' && i + 1 < len && chars[i + 1].is_ascii_digit() {
                i += 1;
                while i < len && chars[i].is_ascii_digit() {
                    i += 1;
                }
            }
            let num: String = chars[start..i].iter().collect();
            tokens.push(Token::Number(num));
            continue;
        }

        // Identifiers: atoms (lowercase start or _) and variables (uppercase start)
        if ch.is_ascii_alphabetic() || ch == '_' {
            let start = i;
            while i < len && (chars[i].is_ascii_alphanumeric() || chars[i] == '_') {
                i += 1;
            }
            let word: String = chars[start..i].iter().collect();
            if word.chars().next().unwrap().is_ascii_uppercase() {
                tokens.push(Token::Variable(word));
            } else {
                tokens.push(Token::Atom(word));
            }
            continue;
        }

        return Err(format!("Unexpected character '{ch}' at position {i}"));
    }

    Ok(tokens)
}

/// Recursive descent parser for Datalog programs.
struct Parser {
    tokens: Vec<Token>,
    pos: usize,
}

impl Parser {
    fn new(tokens: Vec<Token>) -> Self {
        Self { tokens, pos: 0 }
    }

    fn peek(&self) -> Option<&Token> {
        self.tokens.get(self.pos)
    }

    fn advance(&mut self) -> Option<&Token> {
        let tok = self.tokens.get(self.pos);
        if tok.is_some() {
            self.pos += 1;
        }
        tok
    }

    fn expect(&mut self, expected: &Token) -> Result<(), String> {
        match self.advance() {
            Some(tok) if tok == expected => Ok(()),
            Some(tok) => Err(format!("Expected {expected:?}, got {tok:?}")),
            None => Err(format!("Expected {expected:?}, got end of input")),
        }
    }

    /// Parse a complete Datalog program into a list of statements.
    fn parse_program(&mut self) -> Result<Vec<Statement>, String> {
        let mut stmts = Vec::new();
        while self.pos < self.tokens.len() {
            stmts.push(self.parse_statement()?);
        }
        Ok(stmts)
    }

    /// Parse a single statement: fact, rule, or query.
    fn parse_statement(&mut self) -> Result<Statement, String> {
        // Query: ?- literal .
        if self.peek() == Some(&Token::QueryPrefix) {
            self.advance(); // consume ?-
            let lit = self.parse_literal()?;
            self.expect(&Token::Dot)?;
            return Ok(Statement::Query(lit));
        }

        // Fact or rule: literal [ :- body ] .
        let head = self.parse_literal()?;

        match self.peek() {
            Some(Token::Dot) => {
                // It's a fact — all args must be constants
                self.advance(); // consume .
                let mut args = Vec::new();
                for (i, term) in head.args.iter().enumerate() {
                    match term {
                        Term::Const(c) => args.push(c.clone()),
                        Term::Var(v) => {
                            return Err(format!(
                                "Variable '{v}' in fact at argument position {i} \
                                 — facts must contain only constants"
                            ));
                        }
                        Term::Agg(_) => {
                            return Err(format!(
                                "Aggregate function in fact at argument position {i} \
                                 — facts must contain only constants"
                            ));
                        }
                    }
                }
                Ok(Statement::Fact(Fact {
                    predicate: head.predicate,
                    args,
                }))
            }
            Some(Token::ColonDash) => {
                // It's a rule: head :- body .
                self.advance(); // consume :-
                let body = self.parse_body()?;
                self.expect(&Token::Dot)?;
                Ok(Statement::Rule(Rule { head, body }))
            }
            other => Err(format!("Expected '.' or ':-' after literal, got {other:?}")),
        }
    }

    /// Parse a (possibly negated) literal: [\+] predicate(args...)
    fn parse_literal(&mut self) -> Result<Literal, String> {
        let negated = if self.peek() == Some(&Token::NegPrefix) {
            self.advance();
            true
        } else {
            false
        };

        let predicate = match self.advance() {
            Some(Token::Atom(a)) => a.clone(),
            Some(other) => return Err(format!("Expected predicate name, got {other:?}")),
            None => return Err("Expected predicate name, got end of input".into()),
        };

        self.expect(&Token::LParen)?;
        let args = self.parse_args()?;
        self.expect(&Token::RParen)?;

        Ok(Literal {
            predicate,
            args,
            negated,
        })
    }

    /// Parse comma-separated term list.
    fn parse_args(&mut self) -> Result<Vec<Term>, String> {
        let mut args = Vec::new();

        // Handle empty argument list
        if self.peek() == Some(&Token::RParen) {
            return Ok(args);
        }

        args.push(self.parse_term()?);
        while self.peek() == Some(&Token::Comma) {
            self.advance(); // consume ,
            args.push(self.parse_term()?);
        }
        Ok(args)
    }

    /// Parse a single term: variable, atom, string literal, number, or aggregate.
    ///
    /// Aggregates: `count()`, `sum(Var)`, `min(Var)`, `max(Var)`.
    fn parse_term(&mut self) -> Result<Term, String> {
        // Check for aggregate functions: atom immediately followed by '('
        if let Some(Token::Atom(a)) = self.peek() {
            let agg_name = a.clone();
            match agg_name.as_str() {
                "count" | "sum" | "min" | "max"
                    if self.tokens.get(self.pos + 1) == Some(&Token::LParen) =>
                {
                    self.advance(); // consume the atom
                    self.advance(); // consume '('
                    let agg = match agg_name.as_str() {
                        "count" => {
                            self.expect(&Token::RParen)?;
                            AggFunc::Count
                        }
                        "sum" => {
                            let var = self.parse_agg_var_arg()?;
                            self.expect(&Token::RParen)?;
                            AggFunc::Sum(var)
                        }
                        "min" => {
                            let var = self.parse_agg_var_arg()?;
                            self.expect(&Token::RParen)?;
                            AggFunc::Min(var)
                        }
                        "max" => {
                            let var = self.parse_agg_var_arg()?;
                            self.expect(&Token::RParen)?;
                            AggFunc::Max(var)
                        }
                        _ => unreachable!(),
                    };
                    return Ok(Term::Agg(agg));
                }
                _ => {}
            }
        }

        match self.advance() {
            Some(Token::Variable(v)) => Ok(Term::Var(v.clone())),
            Some(Token::Atom(a)) => Ok(Term::Const(a.clone())),
            Some(Token::StringLit(s)) => Ok(Term::Const(s.clone())),
            Some(Token::Number(n)) => Ok(Term::Const(n.clone())),
            Some(other) => Err(format!("Expected term, got {other:?}")),
            None => Err("Expected term, got end of input".into()),
        }
    }

    /// Parse the variable argument inside an aggregate function like `sum(Var)`.
    fn parse_agg_var_arg(&mut self) -> Result<String, String> {
        match self.advance() {
            Some(Token::Variable(v)) => Ok(v.clone()),
            Some(other) => Err(format!("Expected variable in aggregate, got {other:?}")),
            None => Err("Expected variable in aggregate, got end of input".into()),
        }
    }

    /// Parse rule body: literal, literal, ...
    fn parse_body(&mut self) -> Result<Vec<Literal>, String> {
        let mut lits = Vec::new();
        lits.push(self.parse_literal()?);
        while self.peek() == Some(&Token::Comma) {
            self.advance(); // consume ,
            lits.push(self.parse_literal()?);
        }
        Ok(lits)
    }
}

/// Parse a complete Datalog program.
pub fn parse(input: &str) -> Result<Vec<Statement>, String> {
    let tokens = tokenize(input)?;
    if tokens.is_empty() {
        return Ok(Vec::new());
    }
    let mut parser = Parser::new(tokens);
    parser.parse_program()
}

/// Parse a single literal from a string like `ancestor(alice, Who)`.
fn parse_literal_str(input: &str) -> Result<Literal, String> {
    let tokens = tokenize(input)?;
    if tokens.is_empty() {
        return Err("Empty input".into());
    }
    let mut parser = Parser::new(tokens);
    parser.parse_literal()
}

/// Parse a single statement from a string (fact, rule, or query).
fn parse_single_statement(input: &str) -> Result<Statement, String> {
    // Ensure input ends with a dot for facts/rules
    let trimmed = input.trim();
    let to_parse = if trimmed.ends_with('.') {
        trimmed.to_string()
    } else {
        format!("{trimmed}.")
    };
    let tokens = tokenize(&to_parse)?;
    if tokens.is_empty() {
        return Err("Empty input".into());
    }
    let mut parser = Parser::new(tokens);
    parser.parse_statement()
}

// ═══════════════════════════════════════════════════════════════════════════════
// Part B: Fact Store + Indexing
// ═══════════════════════════════════════════════════════════════════════════════

/// Empty set constant for when a predicate has no facts.
static EMPTY_SET: std::sync::LazyLock<HashSet<Vec<String>>> =
    std::sync::LazyLock::new(HashSet::new);

/// The core Datalog store, containing base facts (EDB), rules (IDB),
/// derived facts, and argument indexes.
#[derive(Default)]
#[allow(clippy::type_complexity)]
pub struct DatalogStore {
    /// Base facts (EDB — extensional database).
    /// Key: predicate name, Value: set of argument tuples.
    facts: HashMap<String, HashSet<Vec<String>>>,

    /// Index: (predicate, arg_position) -> value -> matching tuples.
    /// Provides O(1) lookup by predicate + argument position + value.
    indexes: HashMap<(String, usize), HashMap<String, Vec<Vec<String>>>>,

    /// Rules (IDB — intensional database).
    rules: Vec<Rule>,

    /// Derived facts (computed by evaluator, not stored in WAL).
    derived: HashMap<String, HashSet<Vec<String>>>,

    /// Predicates mutated since the last `clear_touched` — the transaction
    /// write-set the executor drains under the same write guard.
    txn_touched: HashSet<String>,
    /// Rules appended since the last `clear_touched`.
    txn_added_rules: Vec<Rule>,
}

impl DatalogStore {
    /// Create an empty Datalog store.
    pub fn new() -> Self {
        Self::default()
    }

    /// Forget any recorded mutations (called before a mutating operation).
    pub fn clear_touched(&mut self) {
        self.txn_touched.clear();
        self.txn_added_rules.clear();
    }

    /// Take the predicates and rules mutated since the last `clear_touched`.
    pub fn take_touched(&mut self) -> (HashSet<String>, Vec<Rule>) {
        (
            std::mem::take(&mut self.txn_touched),
            std::mem::take(&mut self.txn_added_rules),
        )
    }

    /// Assert a ground fact into the store and update indexes.
    pub fn assert_fact(&mut self, pred: &str, args: Vec<String>) {
        // Update indexes before inserting (so we index this new tuple)
        for (pos, val) in args.iter().enumerate() {
            self.indexes
                .entry((pred.to_string(), pos))
                .or_default()
                .entry(val.clone())
                .or_default()
                .push(args.clone());
        }
        self.facts.entry(pred.to_string()).or_default().insert(args);
        self.txn_touched.insert(pred.to_string());
        // Invalidate derived facts (rules may produce different results)
        self.derived.clear();
    }

    /// Retract a ground fact from the store and update indexes.
    pub fn retract_fact(&mut self, pred: &str, args: &[String]) {
        if let Some(fact_set) = self.facts.get_mut(pred) {
            fact_set.remove(args);
            if fact_set.is_empty() {
                self.facts.remove(pred);
            }
        }
        // Update indexes: remove matching tuples
        for (pos, val) in args.iter().enumerate() {
            let key = (pred.to_string(), pos);
            if let Some(val_map) = self.indexes.get_mut(&key) {
                if let Some(tuples) = val_map.get_mut(val) {
                    tuples.retain(|t| t != args);
                    if tuples.is_empty() {
                        val_map.remove(val);
                    }
                }
                if val_map.is_empty() {
                    self.indexes.remove(&key);
                }
            }
        }
        self.txn_touched.insert(pred.to_string());
        // Invalidate derived facts
        self.derived.clear();
    }

    /// Add a rule to the store. Rejects rules that would poison evaluation
    /// for every other predicate: unsafe negation here, and unstratifiable
    /// programs (negation/aggregation cycles) via `stratify` over the
    /// proposed ruleset — one bad rule pair used to silently empty every
    /// derived predicate store-wide.
    pub fn add_rule(&mut self, rule: Rule) -> Result<(), String> {
        check_rule_safety(&rule)?;
        let mut proposed = self.rules.clone();
        proposed.push(rule.clone());
        stratify(&proposed)?;
        self.txn_added_rules.push(rule.clone());
        self.rules.push(rule);
        // Invalidate derived facts
        self.derived.clear();
        Ok(())
    }

    /// Remove all facts for a given predicate.
    pub fn clear_predicate(&mut self, pred: &str) {
        self.facts.remove(pred);
        // Remove all indexes for this predicate
        let keys_to_remove: Vec<_> = self
            .indexes
            .keys()
            .filter(|(p, _)| p == pred)
            .cloned()
            .collect();
        for key in keys_to_remove {
            self.indexes.remove(&key);
        }
        self.txn_touched.insert(pred.to_string());
        // Invalidate derived facts
        self.derived.clear();
    }

    /// Get all facts for a predicate (returns empty set if none).
    pub fn get_facts(&self, pred: &str) -> &HashSet<Vec<String>> {
        self.facts.get(pred).unwrap_or(&EMPTY_SET)
    }

    /// O(1) indexed lookup: find all tuples for `pred` where argument at
    /// position `pos` equals `val`.
    pub fn lookup_index(&self, pred: &str, pos: usize, val: &str) -> Vec<Vec<String>> {
        self.indexes
            .get(&(pred.to_string(), pos))
            .and_then(|m| m.get(val))
            .cloned()
            .unwrap_or_default()
    }

    /// Get the combined set of base + derived facts for a predicate.
    fn all_facts(&self, pred: &str) -> HashSet<Vec<String>> {
        let mut result = self.get_facts(pred).clone();
        if let Some(d) = self.derived.get(pred) {
            result.extend(d.iter().cloned());
        }
        result
    }

    /// Get a combined view of all facts (base + derived) for all predicates.
    fn all_facts_map(&self) -> HashMap<String, HashSet<Vec<String>>> {
        let mut combined = self.facts.clone();
        for (pred, tuples) in &self.derived {
            combined
                .entry(pred.clone())
                .or_default()
                .extend(tuples.iter().cloned());
        }
        combined
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Part C: Semi-Naive Evaluator
// ═══════════════════════════════════════════════════════════════════════════════

/// Attempt to unify a pattern (with variables) against a ground tuple.
/// Returns a variable binding map if successful.
fn unify(pattern: &[Term], fact: &[String]) -> Option<HashMap<String, String>> {
    if pattern.len() != fact.len() {
        return None;
    }
    let mut bindings = HashMap::new();
    for (term, val) in pattern.iter().zip(fact.iter()) {
        match term {
            Term::Const(c) => {
                if c != val {
                    return None;
                }
            }
            Term::Var(v) => {
                if let Some(existing) = bindings.get(v) {
                    if existing != val {
                        return None; // Inconsistent binding
                    }
                } else {
                    bindings.insert(v.clone(), val.clone());
                }
            }
            Term::Agg(_) => {
                // Aggregates only appear in rule heads, not in match patterns.
                // If we encounter one during unification, it matches any value.
            }
        }
    }
    Some(bindings)
}

/// Apply variable bindings to a literal, producing a ground tuple.
/// Returns None if any variable is unbound or if aggregates are present.
fn apply_substitution(
    literal: &Literal,
    bindings: &HashMap<String, String>,
) -> Option<Vec<String>> {
    let mut result = Vec::with_capacity(literal.args.len());
    for term in &literal.args {
        match term {
            Term::Const(c) => result.push(c.clone()),
            Term::Var(v) => {
                result.push(bindings.get(v)?.clone());
            }
            Term::Agg(_) => {
                // Cannot produce a ground tuple for aggregate terms;
                // aggregate rules are handled separately in the evaluator.
                return None;
            }
        }
    }
    Some(result)
}

/// Check whether a rule head contains any aggregate functions.
fn rule_has_aggregates(rule: &Rule) -> bool {
    rule.head.args.iter().any(|t| matches!(t, Term::Agg(_)))
}

/// Datalog safety: every variable in a negated literal must occur in some
/// positive body literal of the same rule. An unbound variable cannot be
/// grounded for the negation check, and the evaluator's lenient fallback
/// treated the literal as satisfied for every candidate — over-deriving the
/// head — so the rule is rejected at registration instead.
fn check_rule_safety(rule: &Rule) -> Result<(), String> {
    let positive_vars: HashSet<&str> = rule
        .body
        .iter()
        .filter(|l| !l.negated)
        .flat_map(|l| l.args.iter())
        .filter_map(|t| match t {
            Term::Var(v) => Some(v.as_str()),
            _ => None,
        })
        .collect();
    for lit in &rule.body {
        if !lit.negated {
            continue;
        }
        for term in &lit.args {
            if let Term::Var(v) = term
                && !positive_vars.contains(v.as_str())
            {
                return Err(format!(
                    "unsafe negation: variable '{v}' in negated literal \
                     '\\+ {}(..)' does not occur in any positive body literal",
                    lit.predicate
                ));
            }
        }
    }
    Ok(())
}

/// Merge two binding maps. Returns None if there is a conflict.
fn merge_bindings(
    a: &HashMap<String, String>,
    b: &HashMap<String, String>,
) -> Option<HashMap<String, String>> {
    let mut merged = a.clone();
    for (k, v) in b {
        if let Some(existing) = merged.get(k) {
            if existing != v {
                return None;
            }
        } else {
            merged.insert(k.clone(), v.clone());
        }
    }
    Some(merged)
}

/// Evaluate the body of a rule using semi-naive optimization.
///
/// For each positive literal in the body, we try joining against the delta
/// set for at least one literal (to avoid recomputing known results).
/// Negated literals are checked against the full fact set.
fn join_body(
    body: &[Literal],
    all_facts: &HashMap<String, HashSet<Vec<String>>>,
    delta_pred: &str,
    delta: &HashSet<Vec<String>>,
) -> Vec<HashMap<String, String>> {
    if body.is_empty() {
        return vec![HashMap::new()];
    }

    // Find which positive body literals match the delta predicate
    let delta_positions: Vec<usize> = body
        .iter()
        .enumerate()
        .filter(|(_, lit)| !lit.negated && lit.predicate == delta_pred)
        .map(|(i, _)| i)
        .collect();

    let mut all_results = Vec::new();

    // For semi-naive: iterate over delta positions, and for each one,
    // use delta for that literal and full facts for the rest
    let positions_to_try = if delta_positions.is_empty() {
        // No body literal matches delta pred — use full facts for everything
        vec![usize::MAX] // sentinel meaning "no delta position"
    } else {
        delta_positions
    };

    for delta_pos in positions_to_try {
        let mut current_bindings: Vec<HashMap<String, String>> = vec![HashMap::new()];

        for (i, lit) in body.iter().enumerate() {
            if current_bindings.is_empty() {
                break;
            }

            if lit.negated {
                // Negation: keep bindings where the literal does NOT match
                current_bindings.retain(|bindings| {
                    if let Some(ground) = apply_substitution(lit, bindings) {
                        let pred_facts = all_facts.get(&lit.predicate);
                        !pred_facts.is_some_and(|fs| fs.contains(&ground))
                    } else {
                        // Unreachable through `add_rule`-validated rules
                        // (check_rule_safety rejects unbound negated
                        // variables); kept as a non-panicking fallback for
                        // directly constructed Rules.
                        true
                    }
                });
                continue;
            }

            // Positive literal: choose fact source
            let fact_source: Box<dyn Iterator<Item = &Vec<String>>> = if i == delta_pos {
                Box::new(delta.iter())
            } else {
                let pred_facts = all_facts.get(&lit.predicate);
                match pred_facts {
                    Some(fs) => Box::new(fs.iter()),
                    None => Box::new(std::iter::empty()),
                }
            };

            let facts_vec: Vec<&Vec<String>> = fact_source.collect();

            let mut next_bindings = Vec::new();
            for bindings in &current_bindings {
                for fact in &facts_vec {
                    if let Some(new_bindings) = unify(&lit.args, fact)
                        && let Some(merged) = merge_bindings(bindings, &new_bindings)
                    {
                        next_bindings.push(merged);
                    }
                }
            }
            current_bindings = next_bindings;
        }

        all_results.extend(current_bindings);
    }

    all_results
}

#[derive(Clone, Copy, PartialEq)]
enum EdgeKind {
    Positive,
    Negation,
    Aggregate,
}

/// Stratify rules by their full dependency graph.
///
/// Positive:    s(head) >= s(body)      (equality allowed — the semi-naive
///                                       fixpoint resolves mutual recursion
///                                       within a stratum)
/// Negation and aggregate consumers:
///              s(head) >= s(body) + 1  (strict; the body relation must be
///                                       complete before the rule fires)
/// A strict edge participating in a cycle is rejected up front.
///
/// The old DFS assigned strata from negation dependencies ONLY, so a rule
/// positively consuming a higher-stratum predicate landed in a lower stratum,
/// was evaluated before its input existed, and was never retried.
fn stratify(rules: &[Rule]) -> Result<Vec<Vec<usize>>, String> {
    if rules.is_empty() {
        return Ok(Vec::new());
    }

    let head_preds: HashSet<String> = rules.iter().map(|r| r.head.predicate.clone()).collect();
    let agg_heads: HashSet<String> = rules
        .iter()
        .filter(|r| rule_has_aggregates(r))
        .map(|r| r.head.predicate.clone())
        .collect();

    // One edge per rule body literal whose predicate is itself a rule head.
    // EDB predicates impose no ordering.
    let mut edges: Vec<(String, String, EdgeKind)> = Vec::new();
    let mut all_deps: HashMap<String, HashSet<String>> = HashMap::new();
    for rule in rules {
        for lit in &rule.body {
            if !head_preds.contains(&lit.predicate) {
                continue;
            }
            let kind = if lit.negated {
                EdgeKind::Negation
            } else if agg_heads.contains(&lit.predicate) {
                EdgeKind::Aggregate
            } else {
                EdgeKind::Positive
            };
            edges.push((rule.head.predicate.clone(), lit.predicate.clone(), kind));
            all_deps
                .entry(rule.head.predicate.clone())
                .or_default()
                .insert(lit.predicate.clone());
        }
    }

    // Reject non-stratifiable programs: any strict edge whose target can
    // reach its head through ANY dependency closes a cycle through negation
    // or aggregation. (Supersedes the old neg_deps-only check.)
    for (head, body, kind) in &edges {
        if *kind == EdgeKind::Positive {
            continue;
        }
        if body == head || can_reach(body, head, &all_deps) {
            let what = if *kind == EdgeKind::Negation {
                "Negation"
            } else {
                "Aggregation"
            };
            return Err(format!(
                "{what} cycle detected between '{head}' and '{body}' — \
                 program is not stratifiable"
            ));
        }
    }

    // Minimal stratum assignment by relaxation to fixpoint. Strict cycles
    // are excluded above, so values are bounded by the number of head
    // predicates and this terminates; predicates in the same positive SCC
    // converge to the same stratum.
    let mut stratum_of: HashMap<String, usize> =
        head_preds.iter().cloned().map(|p| (p, 0)).collect();
    loop {
        let mut changed = false;
        for (head, body, kind) in &edges {
            let need = stratum_of[body] + if *kind == EdgeKind::Positive { 0 } else { 1 };
            if need > stratum_of[head] {
                stratum_of.insert(head.clone(), need);
                changed = true;
            }
        }
        if !changed {
            break;
        }
    }

    let max_stratum = stratum_of.values().copied().max().unwrap_or(0);
    let mut strata = vec![Vec::new(); max_stratum + 1];
    for (i, rule) in rules.iter().enumerate() {
        strata[stratum_of[&rule.head.predicate]].push(i);
    }
    strata.retain(|s| !s.is_empty());
    Ok(strata)
}

/// Check if `from` can reach `to` in the dependency graph (BFS).
fn can_reach(from: &str, to: &str, deps: &HashMap<String, HashSet<String>>) -> bool {
    let mut visited = HashSet::new();
    let mut queue = VecDeque::new();
    queue.push_back(from.to_string());
    visited.insert(from.to_string());

    while let Some(current) = queue.pop_front() {
        if current == to {
            return true;
        }
        if let Some(neighbors) = deps.get(&current) {
            for n in neighbors {
                if visited.insert(n.clone()) {
                    queue.push_back(n.clone());
                }
            }
        }
    }
    false
}

/// Evaluate an aggregate rule against the current fact base.
///
/// 1. Compute all body bindings (like a normal rule).
/// 2. Identify which head positions are group-by keys vs aggregates.
/// 3. Group bindings by the key positions.
/// 4. Apply aggregate functions per group.
/// 5. Return the set of derived aggregate facts.
fn evaluate_aggregate_rule(
    rule: &Rule,
    all_facts: &HashMap<String, HashSet<Vec<String>>>,
) -> HashSet<Vec<String>> {
    // Collect all body bindings using every positive body predicate as delta
    let mut all_bindings: Vec<HashMap<String, String>> = Vec::new();

    let body_preds: HashSet<String> = rule
        .body
        .iter()
        .filter(|l| !l.negated)
        .map(|l| l.predicate.clone())
        .collect();

    let mut seen: HashSet<Vec<(String, String)>> = HashSet::new();
    for bp in &body_preds {
        let bp_facts = all_facts.get(bp).cloned().unwrap_or_default();
        let bindings_list = join_body(&rule.body, all_facts, bp, &bp_facts);
        for b in bindings_list {
            // Deduplicate by the full binding map to avoid double-counting
            // when multiple body predicates produce the same binding set
            let mut sig: Vec<(String, String)> =
                b.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
            sig.sort();
            if seen.insert(sig) {
                all_bindings.push(b);
            }
        }
    }

    // Handle rules with only negated body literals
    if body_preds.is_empty() && !rule.body.is_empty() {
        let bindings_list = join_body(&rule.body, all_facts, "", &HashSet::new());
        for b in bindings_list {
            all_bindings.push(b);
        }
    }

    // Determine key positions (Const/Var) and aggregate positions in the head
    let head_args = &rule.head.args;

    // Group by key values
    // Key = values of non-aggregate head terms
    let mut groups: HashMap<Vec<String>, Vec<HashMap<String, String>>> = HashMap::new();
    'bindings: for bindings in &all_bindings {
        let mut key = Vec::new();
        for term in head_args {
            match term {
                Term::Const(c) => key.push(c.clone()),
                Term::Var(v) => match bindings.get(v) {
                    Some(val) => key.push(val.clone()),
                    // An unbound head variable cannot form a ground fact. Skip the
                    // whole binding instead of substituting an empty-string
                    // placeholder (which silently corrupts the grouping key).
                    None => continue 'bindings,
                },
                Term::Agg(_) => {} // Skip aggregates for the key
            }
        }
        groups.entry(key).or_default().push(bindings.clone());
    }

    // Compute aggregate for each group
    let mut results = HashSet::new();
    for (key, group_bindings) in &groups {
        let mut result_tuple = Vec::new();
        let mut key_idx = 0;
        for term in head_args {
            match term {
                Term::Const(_) | Term::Var(_) => {
                    result_tuple.push(key[key_idx].clone());
                    key_idx += 1;
                }
                Term::Agg(agg) => {
                    let agg_val = match agg {
                        AggFunc::Count => group_bindings.len().to_string(),
                        AggFunc::Sum(var) => {
                            let sum: f64 = group_bindings
                                .iter()
                                .filter_map(|b| b.get(var))
                                .filter_map(|v| v.parse::<f64>().ok())
                                .sum();
                            // Format as integer if it has no fractional part
                            if sum == sum.trunc() && sum.abs() < i64::MAX as f64 {
                                (sum as i64).to_string()
                            } else {
                                sum.to_string()
                            }
                        }
                        AggFunc::Min(var) => {
                            let vals: Vec<f64> = group_bindings
                                .iter()
                                .filter_map(|b| b.get(var))
                                .filter_map(|v| v.parse::<f64>().ok())
                                .collect();
                            match vals.iter().copied().reduce(f64::min) {
                                Some(m) if m == m.trunc() && m.abs() < i64::MAX as f64 => {
                                    (m as i64).to_string()
                                }
                                Some(m) => m.to_string(),
                                None => "0".to_string(),
                            }
                        }
                        AggFunc::Max(var) => {
                            let vals: Vec<f64> = group_bindings
                                .iter()
                                .filter_map(|b| b.get(var))
                                .filter_map(|v| v.parse::<f64>().ok())
                                .collect();
                            match vals.iter().copied().reduce(f64::max) {
                                Some(m) if m == m.trunc() && m.abs() < i64::MAX as f64 => {
                                    (m as i64).to_string()
                                }
                                Some(m) => m.to_string(),
                                None => "0".to_string(),
                            }
                        }
                    };
                    result_tuple.push(agg_val);
                }
            }
        }
        results.insert(result_tuple);
    }

    results
}

impl DatalogStore {
    /// Run the semi-naive evaluation algorithm to compute all derived facts.
    ///
    /// 1. Stratify rules by the full dependency graph
    /// 2. For each stratum, run semi-naive fixed-point iteration
    /// 3. Aggregate rules are evaluated after fixpoint reaches convergence
    /// 4. Derived facts are stored in `self.derived`
    ///
    /// Errors on unstratifiable programs instead of silently returning an
    /// empty derived set — a poisoned store used to answer base facts as if
    /// nothing was wrong.
    pub fn evaluate(&mut self) -> Result<(), String> {
        self.derived.clear();

        let rules = self.rules.clone();
        let strata = stratify(&rules)?;

        // Count total facts for parallelism threshold check
        let total_facts: usize = self.facts.values().map(|s| s.len()).sum();
        let parallelism_threshold = 100;

        for stratum in &strata {
            // Separate aggregate rules from normal rules
            let (agg_rule_indices, normal_rule_indices): (Vec<usize>, Vec<usize>) = stratum
                .iter()
                .copied()
                .partition(|&idx| rule_has_aggregates(&rules[idx]));

            // ── Phase 1: Normal (non-aggregate) rules via semi-naive ──

            let mut delta: HashMap<String, HashSet<Vec<String>>> = HashMap::new();
            let all_facts = self.all_facts_map();

            // Determine if we should parallelize this stratum
            let use_parallel = normal_rule_indices.len() >= 2
                && total_facts + self.derived.values().map(|s| s.len()).sum::<usize>()
                    >= parallelism_threshold;

            if use_parallel {
                // Parallel initial pass
                let thread_deltas: Vec<HashMap<String, HashSet<Vec<String>>>> = std::thread::scope(
                    |s| {
                        let handles: Vec<_> = normal_rule_indices
                            .iter()
                            .map(|&rule_idx| {
                                let rule = &rules[rule_idx];
                                let all_facts_ref = &all_facts;
                                s.spawn(move || {
                                    let mut local_delta: HashMap<String, HashSet<Vec<String>>> =
                                        HashMap::new();
                                    let head_pred = &rule.head.predicate;
                                    let body_preds: HashSet<String> = rule
                                        .body
                                        .iter()
                                        .filter(|l| !l.negated)
                                        .map(|l| l.predicate.clone())
                                        .collect();

                                    for bp in &body_preds {
                                        let bp_facts =
                                            all_facts_ref.get(bp).cloned().unwrap_or_default();
                                        let bindings_list =
                                            join_body(&rule.body, all_facts_ref, bp, &bp_facts);
                                        for bindings in bindings_list {
                                            if let Some(result) =
                                                apply_substitution(&rule.head, &bindings)
                                            {
                                                local_delta
                                                    .entry(head_pred.clone())
                                                    .or_default()
                                                    .insert(result);
                                            }
                                        }
                                    }

                                    if body_preds.is_empty() && !rule.body.is_empty() {
                                        let bindings_list = join_body(
                                            &rule.body,
                                            all_facts_ref,
                                            "",
                                            &HashSet::new(),
                                        );
                                        for bindings in bindings_list {
                                            if let Some(result) =
                                                apply_substitution(&rule.head, &bindings)
                                            {
                                                local_delta
                                                    .entry(head_pred.clone())
                                                    .or_default()
                                                    .insert(result);
                                            }
                                        }
                                    }

                                    local_delta
                                })
                            })
                            .collect();

                        handles
                            .into_iter()
                            .map(|h| match h.join() {
                                Ok(delta) => delta,
                                Err(_) => {
                                    // A worker panic must not bring down the server.
                                    tracing::error!(
                                        target: "nucleus::datalog",
                                        "datalog worker thread panicked; its partition contributed no facts this round"
                                    );
                                    Default::default()
                                }
                            })
                            .collect()
                    },
                );

                // Merge thread results, filtering already-known facts
                for thread_delta in thread_deltas {
                    for (pred, tuples) in thread_delta {
                        let existing = self.all_facts(&pred);
                        for tuple in tuples {
                            if !existing.contains(&tuple) {
                                delta.entry(pred.clone()).or_default().insert(tuple);
                            }
                        }
                    }
                }
            } else {
                // Sequential initial pass (original logic)
                for &rule_idx in &normal_rule_indices {
                    let rule = &rules[rule_idx];
                    let head_pred = &rule.head.predicate;

                    let body_preds: HashSet<String> = rule
                        .body
                        .iter()
                        .filter(|l| !l.negated)
                        .map(|l| l.predicate.clone())
                        .collect();

                    for bp in &body_preds {
                        let bp_facts = all_facts.get(bp).cloned().unwrap_or_default();
                        let bindings_list = join_body(&rule.body, &all_facts, bp, &bp_facts);

                        for bindings in bindings_list {
                            if let Some(result) = apply_substitution(&rule.head, &bindings) {
                                let existing = self.all_facts(head_pred);
                                if !existing.contains(&result) {
                                    delta.entry(head_pred.clone()).or_default().insert(result);
                                }
                            }
                        }
                    }

                    if body_preds.is_empty() && !rule.body.is_empty() {
                        let bindings_list = join_body(&rule.body, &all_facts, "", &HashSet::new());
                        for bindings in bindings_list {
                            if let Some(result) = apply_substitution(&rule.head, &bindings) {
                                let existing = self.all_facts(head_pred);
                                if !existing.contains(&result) {
                                    delta.entry(head_pred.clone()).or_default().insert(result);
                                }
                            }
                        }
                    }
                }
            }

            // Merge initial delta into derived
            for (pred, tuples) in &delta {
                self.derived
                    .entry(pred.clone())
                    .or_default()
                    .extend(tuples.iter().cloned());
            }

            // Fixed-point loop: keep applying rules until no new facts are derived
            let max_iterations = 10_000; // Safety limit
            for _ in 0..max_iterations {
                if delta.values().all(|s| s.is_empty()) {
                    break;
                }

                let mut new_delta: HashMap<String, HashSet<Vec<String>>> = HashMap::new();
                let all_facts = self.all_facts_map();

                if use_parallel && normal_rule_indices.len() >= 2 {
                    // Parallel fixpoint iteration
                    let thread_deltas: Vec<HashMap<String, HashSet<Vec<String>>>> =
                        std::thread::scope(|s| {
                            let handles: Vec<_> = normal_rule_indices
                                .iter()
                                .map(|&rule_idx| {
                                    let rule = &rules[rule_idx];
                                    let all_facts_ref = &all_facts;
                                    let delta_ref = &delta;
                                    s.spawn(move || {
                                        let mut local_delta: HashMap<String, HashSet<Vec<String>>> =
                                            HashMap::new();
                                        let head_pred = &rule.head.predicate;

                                        for (delta_pred, delta_facts) in delta_ref {
                                            if delta_facts.is_empty() {
                                                continue;
                                            }
                                            let bindings_list = join_body(
                                                &rule.body,
                                                all_facts_ref,
                                                delta_pred,
                                                delta_facts,
                                            );
                                            for bindings in bindings_list {
                                                if let Some(result) =
                                                    apply_substitution(&rule.head, &bindings)
                                                {
                                                    local_delta
                                                        .entry(head_pred.clone())
                                                        .or_default()
                                                        .insert(result);
                                                }
                                            }
                                        }

                                        local_delta
                                    })
                                })
                                .collect();

                            handles
                                .into_iter()
                                .map(|h| match h.join() {
                                    Ok(delta) => delta,
                                    Err(_) => {
                                        tracing::error!(
                                            target: "nucleus::datalog",
                                            "datalog worker thread panicked; its partition contributed no facts this round"
                                        );
                                        Default::default()
                                    }
                                })
                                .collect()
                        });

                    // Merge thread results, filtering already-known facts
                    for thread_delta in thread_deltas {
                        for (pred, tuples) in thread_delta {
                            let existing = self.all_facts(&pred);
                            for tuple in tuples {
                                if !existing.contains(&tuple) {
                                    new_delta.entry(pred.clone()).or_default().insert(tuple);
                                }
                            }
                        }
                    }
                } else {
                    // Sequential fixpoint iteration (original logic)
                    for &rule_idx in &normal_rule_indices {
                        let rule = &rules[rule_idx];
                        let head_pred = &rule.head.predicate;

                        for (delta_pred, delta_facts) in &delta {
                            if delta_facts.is_empty() {
                                continue;
                            }

                            let bindings_list =
                                join_body(&rule.body, &all_facts, delta_pred, delta_facts);

                            for bindings in bindings_list {
                                if let Some(result) = apply_substitution(&rule.head, &bindings) {
                                    let existing = self.all_facts(head_pred);
                                    if !existing.contains(&result) {
                                        new_delta
                                            .entry(head_pred.clone())
                                            .or_default()
                                            .insert(result);
                                    }
                                }
                            }
                        }
                    }
                }

                if new_delta.values().all(|s| s.is_empty()) {
                    break;
                }

                // Merge new_delta into derived and prepare for next iteration
                for (pred, tuples) in &new_delta {
                    self.derived
                        .entry(pred.clone())
                        .or_default()
                        .extend(tuples.iter().cloned());
                }
                delta = new_delta;
            }

            // ── Phase 2: Aggregate rules ──
            // Evaluated after fixpoint so they see all derived facts from this stratum.
            if !agg_rule_indices.is_empty() {
                let all_facts = self.all_facts_map();
                for &rule_idx in &agg_rule_indices {
                    let rule = &rules[rule_idx];
                    let agg_results = evaluate_aggregate_rule(rule, &all_facts);
                    self.derived
                        .entry(rule.head.predicate.clone())
                        .or_default()
                        .extend(agg_results);
                }
            }
        }
        Ok(())
    }

    /// Query the store: evaluate all rules, then return tuples matching the
    /// given literal pattern.
    pub fn query(&mut self, literal: &Literal) -> Result<Vec<Vec<String>>, String> {
        self.evaluate()?;

        let all = self.all_facts(&literal.predicate);
        let mut results = Vec::new();

        for tuple in &all {
            if unify(&literal.args, tuple).is_some() {
                results.push(tuple.clone());
            }
        }

        // Sort for deterministic output
        results.sort();
        Ok(results)
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Part D: SQL Integration Functions
// ═══════════════════════════════════════════════════════════════════════════════

impl DatalogStore {
    /// Parse and assert a fact from a string like `"parent(alice, bob)"`.
    pub fn sql_assert(&mut self, input: &str) -> Result<String, String> {
        let stmt = parse_single_statement(input)?;
        match stmt {
            Statement::Fact(fact) => {
                let pred = fact.predicate.clone();
                let arity = fact.args.len();
                self.assert_fact(&fact.predicate, fact.args);
                Ok(format!("ASSERT {pred}/{arity}"))
            }
            Statement::Rule(_) => Err("Expected a fact, got a rule".into()),
            Statement::Query(_) => Err("Expected a fact, got a query".into()),
        }
    }

    /// Parse and add a rule from a string like `"ancestor(X,Y) :- parent(X,Y)"`.
    pub fn sql_rule(&mut self, input: &str) -> Result<String, String> {
        let stmt = parse_single_statement(input)?;
        match stmt {
            Statement::Rule(rule) => {
                let pred = rule.head.predicate.clone();
                let arity = rule.head.args.len();
                self.add_rule(rule)?;
                Ok(format!("RULE {pred}/{arity}"))
            }
            Statement::Fact(_) => Err("Expected a rule, got a fact".into()),
            Statement::Query(_) => Err("Expected a rule, got a query".into()),
        }
    }

    /// Parse and evaluate a query from a string like `"ancestor(alice, Who)"`.
    /// Returns a JSON array of result tuples.
    pub fn sql_query(&mut self, input: &str) -> Result<String, String> {
        let literal = parse_literal_str(input)?;
        // The negation flag used to be parsed and then ignored — the query
        // returned exactly the tuples the user asked to EXCLUDE. Evaluating a
        // complement needs a domain declaration, so reject instead.
        if literal.negated {
            return Err(
                "negated literals are not supported in queries — query the positive \
                 predicate instead (evaluating a complement requires a domain \
                 declaration)"
                    .into(),
            );
        }
        let results = self.query(&literal)?;

        // Build JSON output
        let json_rows: Vec<String> = results
            .iter()
            .map(|tuple| {
                let vals: Vec<String> = tuple
                    .iter()
                    .map(|v| format!("\"{}\"", v.replace('\\', "\\\\").replace('"', "\\\"")))
                    .collect();
                format!("[{}]", vals.join(", "))
            })
            .collect();

        Ok(format!("[{}]", json_rows.join(", ")))
    }

    /// Retract a fact from a string like `"parent(alice, bob)"`.
    pub fn sql_retract(&mut self, input: &str) -> Result<String, String> {
        let stmt = parse_single_statement(input)?;
        match stmt {
            Statement::Fact(fact) => {
                let pred = fact.predicate.clone();
                let arity = fact.args.len();
                self.retract_fact(&fact.predicate, &fact.args);
                Ok(format!("RETRACT {pred}/{arity}"))
            }
            Statement::Rule(_) => Err("Expected a fact, got a rule".into()),
            Statement::Query(_) => Err("Expected a fact, got a query".into()),
        }
    }

    /// Clear all facts for a predicate.
    pub fn sql_clear(&mut self, predicate: &str) -> Result<String, String> {
        self.clear_predicate(predicate);
        Ok(format!("CLEAR {predicate}"))
    }

    /// Import rows from tabular data (e.g., from a relational table).
    pub fn import_rows(&mut self, predicate: &str, rows: Vec<Vec<String>>) {
        for row in rows {
            self.assert_fact(predicate, row);
        }
    }

    /// Capture a snapshot of all state for transaction rollback.
    pub fn txn_snapshot(&self) -> DatalogTxnSnapshot {
        DatalogTxnSnapshot {
            facts: self.facts.clone(),
            indexes: self.indexes.clone(),
            rules: self.rules.clone(),
            derived: self.derived.clone(),
        }
    }

    /// Revert only the predicates in `touched` (and the rules in `added_rules`)
    /// using `snap` as the before-image. Predicates this transaction never
    /// wrote are left alone, so a ROLLBACK cannot destroy facts another session
    /// asserted since BEGIN.
    ///
    /// No compensating WAL records: `datalog_wal` is opened by the executor but
    /// never appended to, so datalog holds no durable state to resurrect.
    pub fn txn_restore_scoped(
        &mut self,
        snap: &DatalogTxnSnapshot,
        touched: &HashSet<String>,
        added_rules: &[Rule],
    ) {
        for pred in touched {
            match snap.facts.get(pred) {
                Some(facts) => {
                    self.facts.insert(pred.clone(), facts.clone());
                }
                None => {
                    self.facts.remove(pred);
                }
            }
            let index_keys: Vec<(String, usize)> = self
                .indexes
                .keys()
                .filter(|(p, _)| p == pred)
                .cloned()
                .collect();
            for key in index_keys {
                self.indexes.remove(&key);
            }
            for (key, value) in &snap.indexes {
                if &key.0 == pred {
                    self.indexes.insert(key.clone(), value.clone());
                }
            }
        }
        // `add_rule` only ever appends, so undo is removal by value — position
        // is not stable when another session appended concurrently.
        for rule in added_rules {
            if let Some(pos) = self.rules.iter().rposition(|r| r == rule) {
                self.rules.remove(pos);
            }
        }
        // `derived` is a memoized evaluation cache, invalidated by every
        // mutation; clearing it is always safe and never loses committed state.
        self.derived.clear();
    }

    /// Restore state from a transaction snapshot (for ROLLBACK).
    #[cfg_attr(not(test), allow(dead_code))]
    pub fn txn_restore(&mut self, snap: DatalogTxnSnapshot) {
        self.facts = snap.facts;
        self.indexes = snap.indexes;
        self.rules = snap.rules;
        self.derived = snap.derived;
    }
}

/// Snapshot of datalog state for transaction rollback.
#[allow(clippy::type_complexity)]
pub struct DatalogTxnSnapshot {
    facts: HashMap<String, HashSet<Vec<String>>>,
    indexes: HashMap<(String, usize), HashMap<String, Vec<Vec<String>>>>,
    rules: Vec<Rule>,
    derived: HashMap<String, HashSet<Vec<String>>>,
}

// ═══════════════════════════════════════════════════════════════════════════════
// Part E: Write-Ahead Log
// ═══════════════════════════════════════════════════════════════════════════════

// ─── WAL entry type tags ─────────────────────────────────────────────────────

const WAL_ASSERT: u8 = 0x01;
const WAL_RETRACT: u8 = 0x02;
const WAL_ADD_RULE: u8 = 0x03;
const WAL_CLEAR_PRED: u8 = 0x04;
const WAL_SNAPSHOT: u8 = 0x10;
/// S63: ASSERT carrying the coordinating transaction id (`u64 LE` between
/// the tag and the length prefix). Body otherwise identical to
/// [`WAL_ASSERT`]'s record.
const WAL_ASSERT_XACT: u8 = 0x05;
/// S63: RETRACT carrying the coordinating transaction id.
const WAL_RETRACT_XACT: u8 = 0x06;
/// S63: ADD_RULE carrying the coordinating transaction id.
const WAL_ADD_RULE_XACT: u8 = 0x07;
/// S63: CLEAR_PRED carrying the coordinating transaction id.
const WAL_CLEAR_PRED_XACT: u8 = 0x08;

/// The reserved id carried by records written OUTSIDE any explicit
/// transaction; from `executor::enlistment`. Declared locally so the WAL
/// layer stays testable without importing the executor.
const XACT_AUTOCOMMIT: u64 = 0;

/// Recovered state from a Datalog WAL replay.
pub struct DatalogWalState {
    pub facts: Vec<(String, Vec<String>)>, // (predicate, args)
    pub rules: Vec<String>,                // rule text
    /// The highest coordinating transaction id seen on a tagged record,
    /// whether that record was kept or discarded. Seeds the XactId
    /// high-water mark at executor construction (S63).
    pub max_xact_id: u64,
}

/// Append-only WAL for the Datalog engine.
pub struct DatalogWal {
    path: PathBuf,
    writer: Mutex<BufWriter<File>>,
    /// The writer holds an inode a checkpoint's rename displaced: it is
    /// unlinked, so appends to it "succeed" into a file no recovery reads.
    /// Set when a checkpoint replaced the log but its reopen failed; cleared
    /// by the next successful reattach. See `reattach_if_stranded`.
    stranded: std::sync::atomic::AtomicBool,
    /// The highest coordinating transaction id recovered at open (S63).
    max_xact_id: u64,
    /// Test-only one-shot checkpoint-reopen fault; see `checkpoint`.
    #[cfg(test)]
    fail_reopen_once: std::sync::atomic::AtomicBool,
}

impl DatalogWal {
    /// Open or create the WAL file in `dir`, replaying with an EMPTY
    /// committed set so every tagged record keeps — the pre-S63 contract.
    /// The executor opens through [`DatalogWal::open_with_committed`]
    /// instead, passing the coordinating transaction ids the SQL side
    /// durably committed so the S63 replay filter can discard the rest.
    ///
    /// Returns `(wal, recovered_state)`. If no WAL file exists the recovered
    /// state is empty. Corrupt trailing bytes are silently ignored
    /// (best-effort recovery).
    pub fn open(dir: &Path) -> io::Result<(Self, DatalogWalState)> {
        Self::open_with_committed(dir, &std::collections::HashSet::new())
    }

    /// Open or create the WAL file in `dir` whose replay is filtered by the
    /// S63 committed set: a tagged record whose coordinating transaction id
    /// is neither `XACT_AUTOCOMMIT` nor in `committed` was written inside a
    /// transaction that never committed, and is discarded.
    pub fn open_with_committed(
        dir: &Path,
        committed: &std::collections::HashSet<u64>,
    ) -> io::Result<(Self, DatalogWalState)> {
        std::fs::create_dir_all(dir)?;
        let path = dir.join("datalog.wal");
        let state = if path.exists() {
            let data = std::fs::read(&path)?;
            replay_wal(&data, committed)
        } else {
            DatalogWalState {
                facts: Vec::new(),
                rules: Vec::new(),
                max_xact_id: 0,
            }
        };
        let max_xact_id = state.max_xact_id;
        let file = OpenOptions::new().create(true).append(true).open(&path)?;
        Ok((
            Self {
                path,
                writer: Mutex::new(BufWriter::new(file)),
                stranded: std::sync::atomic::AtomicBool::new(false),
                max_xact_id,
                #[cfg(test)]
                fail_reopen_once: std::sync::atomic::AtomicBool::new(false),
            },
            state,
        ))
    }

    /// The highest coordinating transaction id this log recovered (S63), 0
    /// when it holds none. Seeds the executor's XactId counter so a reopened
    /// process never mints an id a surviving tagged record already carries.
    pub fn max_xact_id(&self) -> u64 {
        self.max_xact_id
    }

    /// Log an ASSERT operation. The text should be parseable as a Datalog fact
    /// (e.g., `"parent(alice, bob)."`).
    ///
    /// `xact` is the coordinating transaction id the record is tagged with:
    /// `Some(XACT_AUTOCOMMIT)` for a write outside any explicit transaction,
    /// `Some(id)` inside one, `None` to write the legacy untagged record
    /// (kept unconditionally on replay — the pre-S63 compatibility rule).
    pub fn log_assert(&self, xact: Option<u64>, text: &str) -> io::Result<()> {
        self.write_entry(xact, WAL_ASSERT, WAL_ASSERT_XACT, text.as_bytes())
    }

    /// Log a RETRACT operation. `xact` mirrors [`DatalogWal::log_assert`].
    pub fn log_retract(&self, xact: Option<u64>, text: &str) -> io::Result<()> {
        self.write_entry(xact, WAL_RETRACT, WAL_RETRACT_XACT, text.as_bytes())
    }

    /// Log an ADD_RULE operation. The text should be parseable as a Datalog rule
    /// (e.g., `"ancestor(X,Y) :- parent(X,Y)."`). `xact` mirrors
    /// [`DatalogWal::log_assert`].
    pub fn log_rule(&self, xact: Option<u64>, text: &str) -> io::Result<()> {
        self.write_entry(xact, WAL_ADD_RULE, WAL_ADD_RULE_XACT, text.as_bytes())
    }

    /// Log a CLEAR_PRED operation. `xact` mirrors [`DatalogWal::log_assert`].
    pub fn log_clear(&self, xact: Option<u64>, predicate: &str) -> io::Result<()> {
        self.write_entry(
            xact,
            WAL_CLEAR_PRED,
            WAL_CLEAR_PRED_XACT,
            predicate.as_bytes(),
        )
    }

    /// Write a snapshot of all current facts and rules, then truncate the log
    /// to just this snapshot entry.
    pub fn checkpoint(&self, store: &DatalogStore) -> io::Result<()> {
        let mut payload = Vec::new();

        // Encode facts: n_facts, then each as text
        let mut fact_texts = Vec::new();
        for (pred, tuples) in &store.facts {
            for tuple in tuples {
                let text = format!(
                    "{}({}).",
                    pred,
                    tuple
                        .iter()
                        .map(|a| format_fact_arg(a))
                        .collect::<Vec<_>>()
                        .join(", ")
                );
                fact_texts.push(text);
            }
        }
        payload.extend_from_slice(&(fact_texts.len() as u32).to_le_bytes());
        for text in &fact_texts {
            let bytes = text.as_bytes();
            payload.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
            payload.extend_from_slice(bytes);
        }

        // Encode rules: n_rules, then each as text
        let rule_texts: Vec<String> = store.rules.iter().map(format_rule).collect();
        payload.extend_from_slice(&(rule_texts.len() as u32).to_le_bytes());
        for text in &rule_texts {
            let bytes = text.as_bytes();
            payload.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
            payload.extend_from_slice(bytes);
        }

        // Flush existing writer, truncate, write snapshot
        {
            self.writer.lock().flush()?;
        }

        // Guard the length prefix: a payload over 4 GiB would silently truncate
        // when cast to u32 and corrupt the snapshot on replay. Fail loudly instead.
        let plen = u32::try_from(payload.len()).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "datalog checkpoint payload exceeds 4 GiB",
            )
        })?;

        // CRASH SAFETY: stage + fsync + atomic rename, rather than truncating
        // the live log and rewriting it in place (a crash in that window lost
        // every fact and rule).
        let tmp_path = self.path.with_extension("wal.compacting");
        {
            let file = OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&tmp_path)?;
            let mut w = BufWriter::new(file);
            w.write_all(&[WAL_SNAPSHOT])?;
            w.write_all(&plen.to_le_bytes())?;
            w.write_all(&payload)?;
            w.flush()?;
            w.get_ref().sync_all()?;
        }
        let mut guard = self.writer.lock();
        std::fs::rename(&tmp_path, &self.path)?;
        if let Some(dir) = self.path.parent()
            && let Ok(d) = std::fs::File::open(dir)
        {
            let _ = d.sync_all();
        }
        // Re-open in append mode for future writes. The rename above already
        // unlinked the inode `guard` wraps, so a failure here must mark the
        // writer stranded: appends reattach (or fail loudly), never write
        // through a handle no future recovery reads.
        #[cfg(test)]
        let injected: Option<io::Error> = self
            .fail_reopen_once
            .swap(false, std::sync::atomic::Ordering::AcqRel)
            .then(|| io::Error::other("injected datalog WAL reopen failure"));
        #[cfg(not(test))]
        let injected: Option<io::Error> = None;
        let file = match injected {
            Some(e) => {
                self.stranded
                    .store(true, std::sync::atomic::Ordering::Release);
                return Err(e);
            }
            None => match OpenOptions::new().append(true).open(&self.path) {
                Ok(f) => f,
                Err(e) => {
                    self.stranded
                        .store(true, std::sync::atomic::Ordering::Release);
                    return Err(e);
                }
            },
        };
        *guard = BufWriter::new(file);
        self.stranded
            .store(false, std::sync::atomic::Ordering::Release);
        Ok(())
    }

    /// Re-point the writer at the live log file after a checkpoint replaced
    /// the file but could not reopen it. While stranded, `writer` holds an
    /// UNLINKED inode — appends to it succeed into a file no future recovery
    /// reads — so this runs before every append: a successful reopen recovers
    /// the writer, and a failed one fails the append loudly instead of
    /// acknowledging a write to a dead inode.
    fn reattach_if_stranded(&self, w: &mut BufWriter<File>) -> io::Result<()> {
        if !self.stranded.load(std::sync::atomic::Ordering::Acquire) {
            return Ok(());
        }
        let file = OpenOptions::new()
            .append(true)
            .open(&self.path)
            .map_err(|e| {
                io::Error::new(
                    e.kind(),
                    format!(
                        "datalog WAL writer is stranded: a checkpoint replaced {} but its \
                         reopen failed; refusing to append to the unlinked old file ({e})",
                        self.path.display()
                    ),
                )
            })?;
        *w = BufWriter::new(file);
        self.stranded
            .store(false, std::sync::atomic::Ordering::Release);
        Ok(())
    }

    /// Write a single WAL entry, tagged or legacy per `xact`.
    ///
    /// The `datalog.wal_append` fault point exists so a probe can fail exactly
    /// this append: every NU-013 mutation path now treats a failed WAL append
    /// as a failed statement, and `probe_io_faults` proves it stays that way.
    fn write_entry(
        &self,
        xact: Option<u64>,
        plain: u8,
        xact_tagged: u8,
        data: &[u8],
    ) -> io::Result<()> {
        if let Some(e) = crate::storage::crashpoint::io_fault("datalog.wal_append") {
            return Err(e);
        }
        let mut buf = Vec::with_capacity(1 + 8 + 4 + data.len());
        match xact {
            Some(x) => {
                buf.push(xact_tagged);
                buf.extend_from_slice(&x.to_le_bytes());
            }
            None => buf.push(plain),
        }
        buf.extend_from_slice(&(data.len() as u32).to_le_bytes());
        buf.extend_from_slice(data);

        let mut w = self.writer.lock();
        self.reattach_if_stranded(&mut w)?;
        w.write_all(&buf)?;
        w.flush()
    }
}

/// True when `s` re-parses as a bare Atom token (lowercase or `_` start).
fn is_bare_atom(s: &str) -> bool {
    let mut cs = s.chars();
    matches!(cs.next(), Some(c) if c.is_ascii_lowercase() || c == '_')
        && cs.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// True when `s` re-parses as a Number token. The leading-`.` form (`.5`)
/// is not matched — the quoted fallback round-trips it identically.
fn is_bare_number(s: &str) -> bool {
    let body = s.strip_prefix('-').unwrap_or(s);
    let parts: Vec<&str> = body.splitn(2, '.').collect();
    !parts.is_empty()
        && parts
            .iter()
            .all(|p| !p.is_empty() && p.bytes().all(|b| b.is_ascii_digit()))
}

/// Serialize one fact/rule constant so `parse_single_statement` reproduces it
/// exactly. The checkpoint REGENERATES Datalog text from parsed args, so any
/// argument that is not a bare atom or number must be quoted — or it either
/// fails to re-parse (fact silently dropped by replay's `if let Ok`) or
/// re-parses with a different arity (fact silently corrupted). Order matters:
/// bare, then either-quote wrap, then escape.
fn format_fact_arg(s: &str) -> String {
    if is_bare_atom(s) || is_bare_number(s) {
        s.to_string()
    } else if !s.contains('"') && !s.contains('\\') {
        format!("\"{s}\"")
    } else if !s.contains('\'') && !s.contains('\\') {
        format!("'{s}'")
    } else {
        format!("\"{}\"", s.replace('\\', "\\\\").replace('"', "\\\""))
    }
}

/// Format a Rule back to Datalog text.
fn format_rule(rule: &Rule) -> String {
    let head = format_literal(&rule.head);
    let body: Vec<String> = rule.body.iter().map(format_literal).collect();
    format!("{head} :- {}.", body.join(", "))
}

/// Format a Literal to Datalog text.
fn format_literal(lit: &Literal) -> String {
    let prefix = if lit.negated { "\\+ " } else { "" };
    let args: Vec<String> = lit
        .args
        .iter()
        .map(|t| match t {
            Term::Const(c) => format_fact_arg(c),
            Term::Var(v) => v.clone(),
            Term::Agg(agg) => match agg {
                AggFunc::Count => "count()".to_string(),
                AggFunc::Sum(v) => format!("sum({v})"),
                AggFunc::Min(v) => format!("min({v})"),
                AggFunc::Max(v) => format!("max({v})"),
            },
        })
        .collect();
    format!("{prefix}{}({})", lit.predicate, args.join(", "))
}

/// Replay a Datalog WAL file and reconstruct the state, filtered by the S63
/// committed set.
///
/// SNAPSHOT entries reset all state; a snapshot is committed by construction
/// (the S7 checkpoint gate keeps one from folding an open transaction's
/// writes) and always replays.
///
/// `committed` is the set of coordinating transaction ids that durably
/// committed on the SQL side. A tagged record whose id is neither
/// `XACT_AUTOCOMMIT` nor in it was written inside a transaction that never
/// committed, and is discarded — its body is still parsed past, and ids feed
/// `max_xact_id` whether kept or discarded, so the caller can seed the XactId
/// high-water mark.
fn replay_wal(data: &[u8], committed: &std::collections::HashSet<u64>) -> DatalogWalState {
    let mut facts: Vec<(String, Vec<String>)> = Vec::new();
    let mut rules: Vec<String> = Vec::new();
    // Track facts as a set for retraction
    let mut fact_set: HashMap<String, HashSet<Vec<String>>> = HashMap::new();
    let mut max_xact_id: u64 = 0;

    let mut pos = 0usize;

    while pos < data.len() {
        let Some(&entry_type) = data.get(pos) else {
            break;
        };
        pos += 1;

        let mut keep_tagged = true;
        if matches!(
            entry_type,
            WAL_ASSERT_XACT | WAL_RETRACT_XACT | WAL_ADD_RULE_XACT | WAL_CLEAR_PRED_XACT
        ) {
            let Some(xact) = wal_read_u64(data, &mut pos) else {
                break;
            };
            max_xact_id = max_xact_id.max(xact);
            keep_tagged = xact == XACT_AUTOCOMMIT || committed.contains(&xact);
        }

        let Some(text_len) = wal_read_u32(data, &mut pos) else {
            break;
        };
        let text_len = text_len as usize;
        if pos + text_len > data.len() {
            break;
        }
        let text_bytes = &data[pos..pos + text_len];
        pos += text_len;

        let text = match std::str::from_utf8(text_bytes) {
            Ok(s) => s.to_string(),
            Err(_) => break, // Corrupt entry
        };

        match entry_type {
            WAL_ASSERT | WAL_ASSERT_XACT => {
                if keep_tagged && let Ok(Statement::Fact(fact)) = parse_single_statement(&text) {
                    fact_set
                        .entry(fact.predicate.clone())
                        .or_default()
                        .insert(fact.args.clone());
                }
            }
            WAL_RETRACT | WAL_RETRACT_XACT => {
                if keep_tagged
                    && let Ok(Statement::Fact(fact)) = parse_single_statement(&text)
                    && let Some(set) = fact_set.get_mut(&fact.predicate)
                {
                    set.remove(&fact.args);
                    if set.is_empty() {
                        fact_set.remove(&fact.predicate);
                    }
                }
            }
            WAL_ADD_RULE | WAL_ADD_RULE_XACT => {
                // Verify it parses as a rule
                if keep_tagged && let Ok(Statement::Rule(_)) = parse_single_statement(&text) {
                    rules.push(text);
                }
            }
            WAL_CLEAR_PRED | WAL_CLEAR_PRED_XACT => {
                if keep_tagged {
                    fact_set.remove(&text);
                }
            }
            WAL_SNAPSHOT => {
                // Reset state and parse snapshot payload
                fact_set.clear();
                rules.clear();

                // The "text" here is actually the binary payload
                let payload = text_bytes;
                let mut spos = 0usize;

                // Read facts
                let n_facts = match wal_read_u32(payload, &mut spos) {
                    Some(n) => n as usize,
                    None => continue,
                };
                for _ in 0..n_facts {
                    let flen = match wal_read_u32(payload, &mut spos) {
                        Some(n) => n as usize,
                        None => break,
                    };
                    if spos + flen > payload.len() {
                        break;
                    }
                    let ftext = match std::str::from_utf8(&payload[spos..spos + flen]) {
                        Ok(s) => s.to_string(),
                        Err(_) => break,
                    };
                    spos += flen;

                    if let Ok(Statement::Fact(fact)) = parse_single_statement(&ftext) {
                        fact_set
                            .entry(fact.predicate.clone())
                            .or_default()
                            .insert(fact.args.clone());
                    }
                }

                // Read rules
                let n_rules = match wal_read_u32(payload, &mut spos) {
                    Some(n) => n as usize,
                    None => continue,
                };
                for _ in 0..n_rules {
                    let rlen = match wal_read_u32(payload, &mut spos) {
                        Some(n) => n as usize,
                        None => break,
                    };
                    if spos + rlen > payload.len() {
                        break;
                    }
                    let rtext = match std::str::from_utf8(&payload[spos..spos + rlen]) {
                        Ok(s) => s.to_string(),
                        Err(_) => break,
                    };
                    spos += rlen;

                    if let Ok(Statement::Rule(_)) = parse_single_statement(&rtext) {
                        rules.push(rtext);
                    }
                }
            }
            _ => {
                // Unknown entry type — stop replay
                break;
            }
        }
    }

    // Convert fact_set to flat list
    facts.clear();
    for (pred, tuples) in &fact_set {
        for tuple in tuples {
            facts.push((pred.clone(), tuple.clone()));
        }
    }

    DatalogWalState {
        facts,
        rules,
        max_xact_id,
    }
}

/// Read a u32 from WAL data.
fn wal_read_u32(data: &[u8], pos: &mut usize) -> Option<u32> {
    let b = data.get(*pos..*pos + 4)?;
    *pos += 4;
    Some(u32::from_le_bytes([b[0], b[1], b[2], b[3]]))
}

/// Read a u64 from WAL data.
fn wal_read_u64(data: &[u8], pos: &mut usize) -> Option<u64> {
    let b = data.get(*pos..*pos + 8)?;
    *pos += 8;
    Some(u64::from_le_bytes([
        b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7],
    ]))
}

/// Rebuild a DatalogStore from WAL state.
pub fn restore_from_wal(state: DatalogWalState) -> DatalogStore {
    let mut store = DatalogStore::new();

    // Restore facts
    for (pred, args) in state.facts {
        store.assert_fact(&pred, args);
    }

    // Restore rules. A legacy WAL may carry a rule the registration checks
    // now reject (unsafe negation, unstratifiable pair): warn-skip it rather
    // than abort recovery over one entry.
    for rule_text in state.rules {
        if let Ok(Statement::Rule(rule)) = parse_single_statement(&rule_text)
            && let Err(e) = store.add_rule(rule)
        {
            tracing::warn!("restored rule rejected by validation, skipping: {e}");
        }
    }

    store
}

// ═══════════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;

    // ─── S63: the recovery filter ────────────────────────────────────────────

    /// One log exercising every filter decision at once: legacy and
    /// autocommit records keep, committed ids keep, unknown ids discard —
    /// and a discarded record in the MIDDLE does not stop the records after
    /// it (they are length-framed, so parsing past is exact). Also asserts
    /// the max surviving tagged id (kept or not) is reported for the XactId
    /// floor.
    #[test]
    fn tagged_records_filter_on_the_committed_set() {
        let dir = tempfile::tempdir().unwrap();
        {
            let (wal, _) = DatalogWal::open(dir.path()).unwrap();
            wal.log_assert(None, "base(legacy).").unwrap();
            wal.log_assert(Some(XACT_AUTOCOMMIT), "base(auto).")
                .unwrap();
            wal.log_assert(Some(7), "kept(committed).").unwrap();
            wal.log_assert(Some(8), "dropped(never).").unwrap();
            // An abandoned transaction's RETRACT, RULE and CLEAR must not
            // reach the state that survived it either.
            wal.log_retract(Some(9), "base(auto).").unwrap();
            wal.log_rule(Some(9), "ghost(X) :- base(X).").unwrap();
            wal.log_clear(Some(9), "kept").unwrap();
            wal.log_assert(Some(10), "kept(late).").unwrap();
        }

        let committed: std::collections::HashSet<u64> = [7u64, 10].into_iter().collect();
        let (_wal, state) = DatalogWal::open_with_committed(dir.path(), &committed).unwrap();
        assert_eq!(
            state.max_xact_id, 10,
            "discarded records still feed the floor"
        );
        let preds: Vec<&str> = state.facts.iter().map(|(p, _)| p.as_str()).collect();
        assert!(preds.contains(&"base"), "legacy + autocommit keep");
        assert_eq!(
            state.facts.iter().filter(|(p, _)| p == "base").count(),
            2,
            "the abandoned RETRACT (id 9) must not remove the autocommit fact"
        );
        assert!(preds.contains(&"kept"), "committed ids keep: {preds:?}");
        assert!(
            !preds.contains(&"dropped"),
            "id 8 never committed; its fact must be discarded, not replayed"
        );
        assert!(
            state.rules.iter().all(|r| !r.contains("ghost")),
            "the abandoned RULE (id 9) must not reach recovery"
        );
    }

    // ─── Parser tests ────────────────────────────────────────────────────────

    #[test]
    fn test_parse_simple_facts() {
        let input = "parent(alice, bob). parent(bob, charlie).";
        let stmts = parse(input).unwrap();
        assert_eq!(stmts.len(), 2);

        match &stmts[0] {
            Statement::Fact(f) => {
                assert_eq!(f.predicate, "parent");
                assert_eq!(f.args, vec!["alice", "bob"]);
            }
            _ => panic!("Expected Fact"),
        }

        match &stmts[1] {
            Statement::Fact(f) => {
                assert_eq!(f.predicate, "parent");
                assert_eq!(f.args, vec!["bob", "charlie"]);
            }
            _ => panic!("Expected Fact"),
        }
    }

    #[test]
    fn test_parse_rules_with_multiple_body_literals() {
        let input = "ancestor(X, Z) :- ancestor(X, Y), parent(Y, Z).";
        let stmts = parse(input).unwrap();
        assert_eq!(stmts.len(), 1);

        match &stmts[0] {
            Statement::Rule(r) => {
                assert_eq!(r.head.predicate, "ancestor");
                assert_eq!(r.head.args.len(), 2);
                assert_eq!(r.body.len(), 2);
                assert_eq!(r.body[0].predicate, "ancestor");
                assert_eq!(r.body[1].predicate, "parent");
            }
            _ => panic!("Expected Rule"),
        }
    }

    #[test]
    fn test_parse_queries() {
        let input = "?- ancestor(alice, Who).";
        let stmts = parse(input).unwrap();
        assert_eq!(stmts.len(), 1);

        match &stmts[0] {
            Statement::Query(lit) => {
                assert_eq!(lit.predicate, "ancestor");
                assert_eq!(lit.args[0], Term::Const("alice".into()));
                assert_eq!(lit.args[1], Term::Var("Who".into()));
            }
            _ => panic!("Expected Query"),
        }
    }

    #[test]
    fn test_parse_negation() {
        let input = "not_ancestor(X, Y) :- person(X), person(Y), \\+ ancestor(X, Y).";
        let stmts = parse(input).unwrap();
        assert_eq!(stmts.len(), 1);

        match &stmts[0] {
            Statement::Rule(r) => {
                assert!(!r.body[0].negated);
                assert!(!r.body[1].negated);
                assert!(r.body[2].negated);
                assert_eq!(r.body[2].predicate, "ancestor");
            }
            _ => panic!("Expected Rule"),
        }
    }

    #[test]
    fn test_parse_malformed_input() {
        assert!(parse("parent(alice, ).").is_err());
        assert!(parse("parent alice bob.").is_err());
        assert!(parse("parent(X, Y) :- .").is_err());
    }

    #[test]
    fn test_comments_ignored() {
        let input = "% This is a comment\nparent(alice, bob).\n% Another comment\n";
        let stmts = parse(input).unwrap();
        assert_eq!(stmts.len(), 1);
        match &stmts[0] {
            Statement::Fact(f) => assert_eq!(f.predicate, "parent"),
            _ => panic!("Expected Fact"),
        }
    }

    // ─── Fact store tests ────────────────────────────────────────────────────

    #[test]
    fn test_assert_and_retrieve_facts() {
        let mut store = DatalogStore::new();
        store.assert_fact("parent", vec!["alice".into(), "bob".into()]);
        store.assert_fact("parent", vec!["bob".into(), "charlie".into()]);

        let facts = store.get_facts("parent");
        assert_eq!(facts.len(), 2);
        assert!(facts.contains(&vec!["alice".to_string(), "bob".to_string()]));
        assert!(facts.contains(&vec!["bob".to_string(), "charlie".to_string()]));
    }

    #[test]
    fn test_retract_removes_facts() {
        let mut store = DatalogStore::new();
        store.assert_fact("parent", vec!["alice".into(), "bob".into()]);
        store.assert_fact("parent", vec!["bob".into(), "charlie".into()]);

        store.retract_fact("parent", &["alice".to_string(), "bob".to_string()]);

        let facts = store.get_facts("parent");
        assert_eq!(facts.len(), 1);
        assert!(!facts.contains(&vec!["alice".to_string(), "bob".to_string()]));
    }

    #[test]
    fn test_index_lookup() {
        let mut store = DatalogStore::new();
        store.assert_fact("parent", vec!["alice".into(), "bob".into()]);
        store.assert_fact("parent", vec!["alice".into(), "charlie".into()]);
        store.assert_fact("parent", vec!["bob".into(), "dave".into()]);

        // Lookup by first argument
        let results = store.lookup_index("parent", 0, "alice");
        assert_eq!(results.len(), 2);

        let results = store.lookup_index("parent", 0, "bob");
        assert_eq!(results.len(), 1);

        // Lookup by second argument
        let results = store.lookup_index("parent", 1, "bob");
        assert_eq!(results.len(), 1);
    }

    #[test]
    fn test_pattern_matching_with_variables() {
        let pattern = vec![Term::Const("alice".into()), Term::Var("X".into())];
        let fact = vec!["alice".to_string(), "bob".to_string()];
        let bindings = unify(&pattern, &fact).unwrap();
        assert_eq!(bindings.get("X").unwrap(), "bob");

        // Non-matching
        let fact2 = vec!["eve".to_string(), "bob".to_string()];
        assert!(unify(&pattern, &fact2).is_none());
    }

    #[test]
    fn test_duplicate_fact_set_semantics() {
        let mut store = DatalogStore::new();
        store.assert_fact("parent", vec!["alice".into(), "bob".into()]);
        store.assert_fact("parent", vec!["alice".into(), "bob".into()]);

        // Set semantics: no duplicates
        let facts = store.get_facts("parent");
        assert_eq!(facts.len(), 1);
    }

    #[test]
    fn test_clear_predicate() {
        let mut store = DatalogStore::new();
        store.assert_fact("parent", vec!["alice".into(), "bob".into()]);
        store.assert_fact("parent", vec!["bob".into(), "charlie".into()]);
        store.assert_fact("friend", vec!["alice".into(), "eve".into()]);

        store.clear_predicate("parent");

        assert!(store.get_facts("parent").is_empty());
        assert_eq!(store.get_facts("friend").len(), 1);
    }

    // ─── Evaluator tests ─────────────────────────────────────────────────────

    #[test]
    fn test_transitive_closure_ancestor() {
        let mut store = DatalogStore::new();
        store.assert_fact("parent", vec!["alice".into(), "bob".into()]);
        store.assert_fact("parent", vec!["bob".into(), "charlie".into()]);
        store.assert_fact("parent", vec!["charlie".into(), "dave".into()]);

        // ancestor(X, Y) :- parent(X, Y).
        store
            .add_rule(Rule {
                head: Literal {
                    predicate: "ancestor".into(),
                    args: vec![Term::Var("X".into()), Term::Var("Y".into())],
                    negated: false,
                },
                body: vec![Literal {
                    predicate: "parent".into(),
                    args: vec![Term::Var("X".into()), Term::Var("Y".into())],
                    negated: false,
                }],
            })
            .unwrap();

        // ancestor(X, Z) :- ancestor(X, Y), parent(Y, Z).
        store
            .add_rule(Rule {
                head: Literal {
                    predicate: "ancestor".into(),
                    args: vec![Term::Var("X".into()), Term::Var("Z".into())],
                    negated: false,
                },
                body: vec![
                    Literal {
                        predicate: "ancestor".into(),
                        args: vec![Term::Var("X".into()), Term::Var("Y".into())],
                        negated: false,
                    },
                    Literal {
                        predicate: "parent".into(),
                        args: vec![Term::Var("Y".into()), Term::Var("Z".into())],
                        negated: false,
                    },
                ],
            })
            .unwrap();

        let query = Literal {
            predicate: "ancestor".into(),
            args: vec![Term::Const("alice".into()), Term::Var("Who".into())],
            negated: false,
        };
        let results = store.query(&query).unwrap();

        // alice -> bob, alice -> charlie, alice -> dave
        assert_eq!(results.len(), 3);
        let names: HashSet<&str> = results.iter().map(|r| r[1].as_str()).collect();
        assert!(names.contains("bob"));
        assert!(names.contains("charlie"));
        assert!(names.contains("dave"));
    }

    #[test]
    fn test_linear_recursion_path() {
        let mut store = DatalogStore::new();
        // A simple directed graph: a->b->c->d->e
        store.assert_fact("edge", vec!["a".into(), "b".into()]);
        store.assert_fact("edge", vec!["b".into(), "c".into()]);
        store.assert_fact("edge", vec!["c".into(), "d".into()]);
        store.assert_fact("edge", vec!["d".into(), "e".into()]);

        // path(X, Y) :- edge(X, Y).
        store
            .add_rule(Rule {
                head: Literal {
                    predicate: "path".into(),
                    args: vec![Term::Var("X".into()), Term::Var("Y".into())],
                    negated: false,
                },
                body: vec![Literal {
                    predicate: "edge".into(),
                    args: vec![Term::Var("X".into()), Term::Var("Y".into())],
                    negated: false,
                }],
            })
            .unwrap();

        // path(X, Z) :- edge(X, Y), path(Y, Z).
        store
            .add_rule(Rule {
                head: Literal {
                    predicate: "path".into(),
                    args: vec![Term::Var("X".into()), Term::Var("Z".into())],
                    negated: false,
                },
                body: vec![
                    Literal {
                        predicate: "edge".into(),
                        args: vec![Term::Var("X".into()), Term::Var("Y".into())],
                        negated: false,
                    },
                    Literal {
                        predicate: "path".into(),
                        args: vec![Term::Var("Y".into()), Term::Var("Z".into())],
                        negated: false,
                    },
                ],
            })
            .unwrap();

        let query = Literal {
            predicate: "path".into(),
            args: vec![Term::Const("a".into()), Term::Var("X".into())],
            negated: false,
        };
        let results = store.query(&query).unwrap();
        assert_eq!(results.len(), 4); // b, c, d, e
    }

    #[test]
    fn test_mutual_recursion() {
        let mut store = DatalogStore::new();
        store.assert_fact("base_even", vec!["0".into()]);
        store.assert_fact("succ", vec!["0".into(), "1".into()]);
        store.assert_fact("succ", vec!["1".into(), "2".into()]);
        store.assert_fact("succ", vec!["2".into(), "3".into()]);
        store.assert_fact("succ", vec!["3".into(), "4".into()]);

        // even(X) :- base_even(X).
        store
            .add_rule(Rule {
                head: Literal {
                    predicate: "even".into(),
                    args: vec![Term::Var("X".into())],
                    negated: false,
                },
                body: vec![Literal {
                    predicate: "base_even".into(),
                    args: vec![Term::Var("X".into())],
                    negated: false,
                }],
            })
            .unwrap();

        // odd(Y) :- even(X), succ(X, Y).
        store
            .add_rule(Rule {
                head: Literal {
                    predicate: "odd".into(),
                    args: vec![Term::Var("Y".into())],
                    negated: false,
                },
                body: vec![
                    Literal {
                        predicate: "even".into(),
                        args: vec![Term::Var("X".into())],
                        negated: false,
                    },
                    Literal {
                        predicate: "succ".into(),
                        args: vec![Term::Var("X".into()), Term::Var("Y".into())],
                        negated: false,
                    },
                ],
            })
            .unwrap();

        // even(Y) :- odd(X), succ(X, Y).
        store
            .add_rule(Rule {
                head: Literal {
                    predicate: "even".into(),
                    args: vec![Term::Var("Y".into())],
                    negated: false,
                },
                body: vec![
                    Literal {
                        predicate: "odd".into(),
                        args: vec![Term::Var("X".into())],
                        negated: false,
                    },
                    Literal {
                        predicate: "succ".into(),
                        args: vec![Term::Var("X".into()), Term::Var("Y".into())],
                        negated: false,
                    },
                ],
            })
            .unwrap();

        let query_even = Literal {
            predicate: "even".into(),
            args: vec![Term::Var("X".into())],
            negated: false,
        };
        let results = store.query(&query_even).unwrap();
        let evens: HashSet<&str> = results.iter().map(|r| r[0].as_str()).collect();
        assert!(evens.contains("0"));
        assert!(evens.contains("2"));
        assert!(evens.contains("4"));
        assert!(!evens.contains("1"));
        assert!(!evens.contains("3"));
    }

    #[test]
    fn test_stratified_negation() {
        let mut store = DatalogStore::new();
        store.assert_fact("person", vec!["alice".into()]);
        store.assert_fact("person", vec!["bob".into()]);
        store.assert_fact("person", vec!["charlie".into()]);
        store.assert_fact("likes", vec!["alice".into(), "bob".into()]);
        store.assert_fact("likes", vec!["bob".into(), "charlie".into()]);

        // dislikes(X, Y) :- person(X), person(Y), \+ likes(X, Y).
        store
            .add_rule(Rule {
                head: Literal {
                    predicate: "dislikes".into(),
                    args: vec![Term::Var("X".into()), Term::Var("Y".into())],
                    negated: false,
                },
                body: vec![
                    Literal {
                        predicate: "person".into(),
                        args: vec![Term::Var("X".into())],
                        negated: false,
                    },
                    Literal {
                        predicate: "person".into(),
                        args: vec![Term::Var("Y".into())],
                        negated: false,
                    },
                    Literal {
                        predicate: "likes".into(),
                        args: vec![Term::Var("X".into()), Term::Var("Y".into())],
                        negated: true,
                    },
                ],
            })
            .unwrap();

        let query = Literal {
            predicate: "dislikes".into(),
            args: vec![Term::Const("alice".into()), Term::Var("Y".into())],
            negated: false,
        };
        let results = store.query(&query).unwrap();
        // alice likes bob, so dislikes alice/charlie and alice/alice
        let targets: HashSet<&str> = results.iter().map(|r| r[1].as_str()).collect();
        assert!(targets.contains("alice")); // self-dislike (alice doesn't like alice)
        assert!(targets.contains("charlie"));
        assert!(!targets.contains("bob")); // alice likes bob
    }

    #[test]
    fn test_fixed_point_convergence() {
        let mut store = DatalogStore::new();
        store.assert_fact("edge", vec!["a".into(), "b".into()]);
        store.assert_fact("edge", vec!["b".into(), "c".into()]);

        // reach(X, Y) :- edge(X, Y).
        store
            .add_rule(Rule {
                head: Literal {
                    predicate: "reach".into(),
                    args: vec![Term::Var("X".into()), Term::Var("Y".into())],
                    negated: false,
                },
                body: vec![Literal {
                    predicate: "edge".into(),
                    args: vec![Term::Var("X".into()), Term::Var("Y".into())],
                    negated: false,
                }],
            })
            .unwrap();

        // reach(X, Z) :- reach(X, Y), reach(Y, Z).
        store
            .add_rule(Rule {
                head: Literal {
                    predicate: "reach".into(),
                    args: vec![Term::Var("X".into()), Term::Var("Z".into())],
                    negated: false,
                },
                body: vec![
                    Literal {
                        predicate: "reach".into(),
                        args: vec![Term::Var("X".into()), Term::Var("Y".into())],
                        negated: false,
                    },
                    Literal {
                        predicate: "reach".into(),
                        args: vec![Term::Var("Y".into()), Term::Var("Z".into())],
                        negated: false,
                    },
                ],
            })
            .unwrap();

        let query = Literal {
            predicate: "reach".into(),
            args: vec![Term::Var("X".into()), Term::Var("Y".into())],
            negated: false,
        };
        let results = store.query(&query).unwrap();
        // edges: a->b, b->c; transitive: a->c
        assert_eq!(results.len(), 3); // (a,b), (b,c), (a,c)
    }

    #[test]
    fn test_cyclic_data_no_infinite_loop() {
        let mut store = DatalogStore::new();
        // Cycle: a -> b -> c -> a
        store.assert_fact("edge", vec!["a".into(), "b".into()]);
        store.assert_fact("edge", vec!["b".into(), "c".into()]);
        store.assert_fact("edge", vec!["c".into(), "a".into()]);

        // reach(X, Y) :- edge(X, Y).
        store
            .add_rule(Rule {
                head: Literal {
                    predicate: "reach".into(),
                    args: vec![Term::Var("X".into()), Term::Var("Y".into())],
                    negated: false,
                },
                body: vec![Literal {
                    predicate: "edge".into(),
                    args: vec![Term::Var("X".into()), Term::Var("Y".into())],
                    negated: false,
                }],
            })
            .unwrap();

        // reach(X, Z) :- reach(X, Y), edge(Y, Z).
        store
            .add_rule(Rule {
                head: Literal {
                    predicate: "reach".into(),
                    args: vec![Term::Var("X".into()), Term::Var("Z".into())],
                    negated: false,
                },
                body: vec![
                    Literal {
                        predicate: "reach".into(),
                        args: vec![Term::Var("X".into()), Term::Var("Y".into())],
                        negated: false,
                    },
                    Literal {
                        predicate: "edge".into(),
                        args: vec![Term::Var("Y".into()), Term::Var("Z".into())],
                        negated: false,
                    },
                ],
            })
            .unwrap();

        let query = Literal {
            predicate: "reach".into(),
            args: vec![Term::Var("X".into()), Term::Var("Y".into())],
            negated: false,
        };
        let results = store.query(&query).unwrap();
        // 3 nodes, each can reach all 3 (including self via cycle) = 9
        // But a->a, b->b, c->c only via going around, which is valid
        assert_eq!(results.len(), 9);
    }

    #[test]
    fn test_multi_argument_rules() {
        let mut store = DatalogStore::new();
        store.assert_fact(
            "employee",
            vec!["alice".into(), "engineering".into(), "150000".into()],
        );
        store.assert_fact(
            "employee",
            vec!["bob".into(), "engineering".into(), "120000".into()],
        );
        store.assert_fact(
            "employee",
            vec!["charlie".into(), "marketing".into(), "100000".into()],
        );

        // in_dept(Name, Dept) :- employee(Name, Dept, Salary).
        store
            .add_rule(Rule {
                head: Literal {
                    predicate: "in_dept".into(),
                    args: vec![Term::Var("Name".into()), Term::Var("Dept".into())],
                    negated: false,
                },
                body: vec![Literal {
                    predicate: "employee".into(),
                    args: vec![
                        Term::Var("Name".into()),
                        Term::Var("Dept".into()),
                        Term::Var("Salary".into()),
                    ],
                    negated: false,
                }],
            })
            .unwrap();

        let query = Literal {
            predicate: "in_dept".into(),
            args: vec![Term::Var("N".into()), Term::Const("engineering".into())],
            negated: false,
        };
        let results = store.query(&query).unwrap();
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_large_fact_base() {
        let mut store = DatalogStore::new();

        // Insert 100 edges forming a chain (produces ~5K derived path facts)
        for i in 0..100 {
            store.assert_fact("edge", vec![format!("n{i}"), format!("n{}", i + 1)]);
        }

        // path(X, Y) :- edge(X, Y).
        store
            .add_rule(Rule {
                head: Literal {
                    predicate: "path".into(),
                    args: vec![Term::Var("X".into()), Term::Var("Y".into())],
                    negated: false,
                },
                body: vec![Literal {
                    predicate: "edge".into(),
                    args: vec![Term::Var("X".into()), Term::Var("Y".into())],
                    negated: false,
                }],
            })
            .unwrap();

        // path(X, Z) :- edge(X, Y), path(Y, Z).
        store
            .add_rule(Rule {
                head: Literal {
                    predicate: "path".into(),
                    args: vec![Term::Var("X".into()), Term::Var("Z".into())],
                    negated: false,
                },
                body: vec![
                    Literal {
                        predicate: "edge".into(),
                        args: vec![Term::Var("X".into()), Term::Var("Y".into())],
                        negated: false,
                    },
                    Literal {
                        predicate: "path".into(),
                        args: vec![Term::Var("Y".into()), Term::Var("Z".into())],
                        negated: false,
                    },
                ],
            })
            .unwrap();

        // Query just from n0 — should reach all 100 nodes
        let query = Literal {
            predicate: "path".into(),
            args: vec![Term::Const("n0".into()), Term::Var("X".into())],
            negated: false,
        };
        let results = store.query(&query).unwrap();
        assert_eq!(results.len(), 100); // n0 -> n1..n100
    }

    #[test]
    fn test_empty_result_for_unmatched_query() {
        let mut store = DatalogStore::new();
        store.assert_fact("parent", vec!["alice".into(), "bob".into()]);

        let query = Literal {
            predicate: "parent".into(),
            args: vec![Term::Const("eve".into()), Term::Var("X".into())],
            negated: false,
        };
        let results = store.query(&query).unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn test_semi_naive_same_as_naive() {
        // Verify semi-naive produces the same results by checking a known case
        let mut store = DatalogStore::new();
        store.assert_fact("edge", vec!["a".into(), "b".into()]);
        store.assert_fact("edge", vec!["b".into(), "c".into()]);
        store.assert_fact("edge", vec!["c".into(), "d".into()]);
        store.assert_fact("edge", vec!["a".into(), "c".into()]); // shortcut

        // tc(X, Y) :- edge(X, Y).
        store
            .add_rule(Rule {
                head: Literal {
                    predicate: "tc".into(),
                    args: vec![Term::Var("X".into()), Term::Var("Y".into())],
                    negated: false,
                },
                body: vec![Literal {
                    predicate: "edge".into(),
                    args: vec![Term::Var("X".into()), Term::Var("Y".into())],
                    negated: false,
                }],
            })
            .unwrap();

        // tc(X, Z) :- tc(X, Y), tc(Y, Z).
        store
            .add_rule(Rule {
                head: Literal {
                    predicate: "tc".into(),
                    args: vec![Term::Var("X".into()), Term::Var("Z".into())],
                    negated: false,
                },
                body: vec![
                    Literal {
                        predicate: "tc".into(),
                        args: vec![Term::Var("X".into()), Term::Var("Y".into())],
                        negated: false,
                    },
                    Literal {
                        predicate: "tc".into(),
                        args: vec![Term::Var("Y".into()), Term::Var("Z".into())],
                        negated: false,
                    },
                ],
            })
            .unwrap();

        let query = Literal {
            predicate: "tc".into(),
            args: vec![Term::Var("X".into()), Term::Var("Y".into())],
            negated: false,
        };
        let results = store.query(&query).unwrap();

        // Expected TC: (a,b), (b,c), (c,d), (a,c), (a,d), (b,d)
        assert_eq!(results.len(), 6);
        let pairs: HashSet<(String, String)> = results
            .iter()
            .map(|r| (r[0].clone(), r[1].clone()))
            .collect();
        assert!(pairs.contains(&("a".into(), "b".into())));
        assert!(pairs.contains(&("b".into(), "c".into())));
        assert!(pairs.contains(&("c".into(), "d".into())));
        assert!(pairs.contains(&("a".into(), "c".into())));
        assert!(pairs.contains(&("a".into(), "d".into())));
        assert!(pairs.contains(&("b".into(), "d".into())));
    }

    // ─── SQL integration tests ───────────────────────────────────────────────

    #[test]
    fn test_sql_assert_and_query() {
        let mut store = DatalogStore::new();
        store.sql_assert("parent(alice, bob)").unwrap();
        store.sql_assert("parent(bob, charlie)").unwrap();

        let result = store.sql_query("parent(alice, X)").unwrap();
        assert!(result.contains("bob"));
    }

    #[test]
    fn test_sql_rule_and_recursive_query() {
        let mut store = DatalogStore::new();
        store.sql_assert("parent(alice, bob)").unwrap();
        store.sql_assert("parent(bob, charlie)").unwrap();
        store.sql_assert("parent(charlie, dave)").unwrap();

        store.sql_rule("ancestor(X, Y) :- parent(X, Y)").unwrap();
        store
            .sql_rule("ancestor(X, Z) :- ancestor(X, Y), parent(Y, Z)")
            .unwrap();

        let result = store.sql_query("ancestor(alice, Who)").unwrap();
        assert!(result.contains("bob"));
        assert!(result.contains("charlie"));
        assert!(result.contains("dave"));
    }

    #[test]
    fn test_sql_retract() {
        let mut store = DatalogStore::new();
        store.sql_assert("parent(alice, bob)").unwrap();
        store.sql_assert("parent(bob, charlie)").unwrap();

        store.sql_retract("parent(alice, bob)").unwrap();

        let result = store.sql_query("parent(alice, X)").unwrap();
        assert_eq!(result, "[]");
    }

    #[test]
    fn test_sql_clear() {
        let mut store = DatalogStore::new();
        store.sql_assert("parent(alice, bob)").unwrap();
        store.sql_assert("parent(bob, charlie)").unwrap();

        store.sql_clear("parent").unwrap();

        let result = store.sql_query("parent(X, Y)").unwrap();
        assert_eq!(result, "[]");
    }

    #[test]
    fn test_import_rows() {
        let mut store = DatalogStore::new();
        store.import_rows(
            "employee",
            vec![
                vec!["alice".into(), "engineering".into()],
                vec!["bob".into(), "marketing".into()],
                vec!["charlie".into(), "engineering".into()],
            ],
        );

        let result = store.sql_query("employee(X, engineering)").unwrap();
        assert!(result.contains("alice"));
        assert!(result.contains("charlie"));
        assert!(!result.contains("bob"));
    }

    #[test]
    fn test_sql_error_handling() {
        let mut store = DatalogStore::new();
        assert!(store.sql_assert("not a valid fact(").is_err());
        assert!(store.sql_rule("parent(alice, bob)").is_err()); // fact, not rule
        assert!(store.sql_assert("ancestor(X, Y) :- parent(X, Y)").is_err()); // rule, not fact
    }

    #[test]
    fn test_sql_json_output_format() {
        let mut store = DatalogStore::new();
        store.sql_assert("parent(alice, bob)").unwrap();

        let result = store.sql_query("parent(X, Y)").unwrap();
        // Should be valid JSON array
        assert!(result.starts_with('['));
        assert!(result.ends_with(']'));
        assert!(result.contains("\"alice\""));
        assert!(result.contains("\"bob\""));
    }

    #[test]
    fn test_sql_cross_predicate_rules() {
        let mut store = DatalogStore::new();
        store.sql_assert("teaches(smith, math)").unwrap();
        store.sql_assert("teaches(jones, physics)").unwrap();
        store.sql_assert("enrolled(alice, math)").unwrap();
        store.sql_assert("enrolled(bob, physics)").unwrap();
        store.sql_assert("enrolled(charlie, math)").unwrap();

        store.sql_rule("student_of(Student, Teacher) :- enrolled(Student, Course), teaches(Teacher, Course)").unwrap();

        let result = store.sql_query("student_of(alice, T)").unwrap();
        assert!(result.contains("smith"));
        assert!(!result.contains("jones"));
    }

    // ─── WAL tests ───────────────────────────────────────────────────────────

    #[test]
    fn test_wal_assert_facts_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let (wal, state) = DatalogWal::open(dir.path()).unwrap();
        assert!(state.facts.is_empty());

        wal.log_assert(None, "parent(alice, bob).").unwrap();
        wal.log_assert(None, "parent(bob, charlie).").unwrap();
        drop(wal);

        let (_wal2, state2) = DatalogWal::open(dir.path()).unwrap();
        assert_eq!(state2.facts.len(), 2);

        // Verify we can rebuild a store from WAL state
        let mut store = restore_from_wal(state2);
        let result = store.sql_query("parent(alice, X)").unwrap();
        assert!(result.contains("bob"));
    }

    #[test]
    fn test_wal_rules_survive_restart() {
        let dir = tempfile::tempdir().unwrap();
        let (wal, _) = DatalogWal::open(dir.path()).unwrap();

        wal.log_assert(None, "parent(alice, bob).").unwrap();
        wal.log_assert(None, "parent(bob, charlie).").unwrap();
        wal.log_rule(None, "ancestor(X, Y) :- parent(X, Y).")
            .unwrap();
        wal.log_rule(None, "ancestor(X, Z) :- ancestor(X, Y), parent(Y, Z).")
            .unwrap();
        drop(wal);

        let (_wal2, state2) = DatalogWal::open(dir.path()).unwrap();
        let mut store = restore_from_wal(state2);

        let result = store.sql_query("ancestor(alice, Who)").unwrap();
        assert!(result.contains("bob"));
        assert!(result.contains("charlie"));
    }

    #[test]
    fn test_wal_derived_facts_recomputed() {
        // Derived facts are not stored in WAL — they must be recomputed
        let dir = tempfile::tempdir().unwrap();
        let (wal, _) = DatalogWal::open(dir.path()).unwrap();

        wal.log_assert(None, "edge(a, b).").unwrap();
        wal.log_assert(None, "edge(b, c).").unwrap();
        wal.log_rule(None, "path(X, Y) :- edge(X, Y).").unwrap();
        wal.log_rule(None, "path(X, Z) :- edge(X, Y), path(Y, Z).")
            .unwrap();
        drop(wal);

        let (_wal2, state2) = DatalogWal::open(dir.path()).unwrap();
        // WAL state should only have base facts, not derived path facts
        assert_eq!(state2.facts.len(), 2);

        // But after evaluation, derived facts should be available
        let mut store = restore_from_wal(state2);
        let result = store.sql_query("path(a, c)").unwrap();
        assert!(result.contains("\"a\""));
        assert!(result.contains("\"c\""));
    }

    #[test]
    fn test_wal_retracted_facts_gone() {
        let dir = tempfile::tempdir().unwrap();
        let (wal, _) = DatalogWal::open(dir.path()).unwrap();

        wal.log_assert(None, "parent(alice, bob).").unwrap();
        wal.log_assert(None, "parent(bob, charlie).").unwrap();
        wal.log_retract(None, "parent(alice, bob).").unwrap();
        drop(wal);

        let (_wal2, state2) = DatalogWal::open(dir.path()).unwrap();
        assert_eq!(state2.facts.len(), 1);

        let store = restore_from_wal(state2);
        assert!(
            store
                .get_facts("parent")
                .contains(&vec!["bob".to_string(), "charlie".to_string()])
        );
        assert!(
            !store
                .get_facts("parent")
                .contains(&vec!["alice".to_string(), "bob".to_string()])
        );
    }

    #[test]
    fn test_wal_corrupt_recovery() {
        let dir = tempfile::tempdir().unwrap();
        let (wal, _) = DatalogWal::open(dir.path()).unwrap();

        wal.log_assert(None, "parent(alice, bob).").unwrap();
        drop(wal);

        // Append garbage bytes to the WAL file
        let wal_path = dir.path().join("datalog.wal");
        let mut file = OpenOptions::new().append(true).open(&wal_path).unwrap();
        file.write_all(&[0xFF, 0xFE, 0xFD, 0x00, 0x01]).unwrap();
        file.flush().unwrap();
        drop(file);

        // Should recover the good entry and skip the corrupt tail
        let (_wal2, state2) = DatalogWal::open(dir.path()).unwrap();
        assert_eq!(state2.facts.len(), 1);
        let (pred, args) = &state2.facts[0];
        assert_eq!(pred, "parent");
        assert_eq!(args, &vec!["alice".to_string(), "bob".to_string()]);
    }

    #[test]
    fn test_wal_checkpoint_and_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let (wal, _) = DatalogWal::open(dir.path()).unwrap();

        // Write some facts and a rule
        wal.log_assert(None, "parent(alice, bob).").unwrap();
        wal.log_assert(None, "parent(bob, charlie).").unwrap();
        wal.log_rule(None, "ancestor(X, Y) :- parent(X, Y).")
            .unwrap();

        // Build a store from current state
        let mut store = DatalogStore::new();
        store.assert_fact("parent", vec!["alice".into(), "bob".into()]);
        store.assert_fact("parent", vec!["bob".into(), "charlie".into()]);
        store
            .add_rule(Rule {
                head: Literal {
                    predicate: "ancestor".into(),
                    args: vec![Term::Var("X".into()), Term::Var("Y".into())],
                    negated: false,
                },
                body: vec![Literal {
                    predicate: "parent".into(),
                    args: vec![Term::Var("X".into()), Term::Var("Y".into())],
                    negated: false,
                }],
            })
            .unwrap();

        // Checkpoint
        wal.checkpoint(&store).unwrap();

        // Write more facts after checkpoint
        wal.log_assert(None, "parent(charlie, dave).").unwrap();
        drop(wal);

        // Reopen and verify
        let (_wal2, state2) = DatalogWal::open(dir.path()).unwrap();
        assert_eq!(state2.facts.len(), 3); // alice/bob, bob/charlie, charlie/dave
        assert_eq!(state2.rules.len(), 1);

        let mut store2 = restore_from_wal(state2);
        let result = store2.sql_query("ancestor(alice, Who)").unwrap();
        assert!(result.contains("bob"));
    }

    // ─── Aggregation tests ──────────────────────────────────────────────────

    #[test]
    fn test_agg_count_per_group() {
        let mut store = DatalogStore::new();
        store.assert_fact(
            "employee",
            vec!["alice".into(), "engineering".into(), "150000".into()],
        );
        store.assert_fact(
            "employee",
            vec!["bob".into(), "engineering".into(), "120000".into()],
        );
        store.assert_fact(
            "employee",
            vec!["charlie".into(), "marketing".into(), "100000".into()],
        );

        // dept_count(Dept, count()) :- employee(_, Dept, _).
        store
            .sql_rule("dept_count(Dept, count()) :- employee(Name, Dept, Sal)")
            .unwrap();

        let results = store
            .query(&Literal {
                predicate: "dept_count".into(),
                args: vec![Term::Var("D".into()), Term::Var("C".into())],
                negated: false,
            })
            .unwrap();
        assert_eq!(results.len(), 2);
        let map: HashMap<&str, &str> = results
            .iter()
            .map(|r| (r[0].as_str(), r[1].as_str()))
            .collect();
        assert_eq!(map["engineering"], "2");
        assert_eq!(map["marketing"], "1");
    }

    #[test]
    fn test_agg_sum_per_group() {
        let mut store = DatalogStore::new();
        store.assert_fact(
            "employee",
            vec!["alice".into(), "engineering".into(), "150000".into()],
        );
        store.assert_fact(
            "employee",
            vec!["bob".into(), "engineering".into(), "120000".into()],
        );
        store.assert_fact(
            "employee",
            vec!["charlie".into(), "marketing".into(), "100000".into()],
        );

        // dept_salary(Dept, sum(Sal)) :- employee(_, Dept, Sal).
        store
            .sql_rule("dept_salary(Dept, sum(Sal)) :- employee(Name, Dept, Sal)")
            .unwrap();

        let results = store
            .query(&Literal {
                predicate: "dept_salary".into(),
                args: vec![Term::Var("D".into()), Term::Var("S".into())],
                negated: false,
            })
            .unwrap();
        assert_eq!(results.len(), 2);
        let map: HashMap<&str, &str> = results
            .iter()
            .map(|r| (r[0].as_str(), r[1].as_str()))
            .collect();
        assert_eq!(map["engineering"], "270000");
        assert_eq!(map["marketing"], "100000");
    }

    #[test]
    fn test_agg_min_per_group() {
        let mut store = DatalogStore::new();
        store.assert_fact(
            "employee",
            vec!["alice".into(), "engineering".into(), "150000".into()],
        );
        store.assert_fact(
            "employee",
            vec!["bob".into(), "engineering".into(), "120000".into()],
        );
        store.assert_fact(
            "employee",
            vec!["charlie".into(), "marketing".into(), "100000".into()],
        );

        store
            .sql_rule("dept_min(Dept, min(Sal)) :- employee(Name, Dept, Sal)")
            .unwrap();

        let results = store
            .query(&Literal {
                predicate: "dept_min".into(),
                args: vec![Term::Var("D".into()), Term::Var("M".into())],
                negated: false,
            })
            .unwrap();
        let map: HashMap<&str, &str> = results
            .iter()
            .map(|r| (r[0].as_str(), r[1].as_str()))
            .collect();
        assert_eq!(map["engineering"], "120000");
        assert_eq!(map["marketing"], "100000");
    }

    #[test]
    fn test_agg_max_per_group() {
        let mut store = DatalogStore::new();
        store.assert_fact(
            "employee",
            vec!["alice".into(), "engineering".into(), "150000".into()],
        );
        store.assert_fact(
            "employee",
            vec!["bob".into(), "engineering".into(), "120000".into()],
        );
        store.assert_fact(
            "employee",
            vec!["charlie".into(), "marketing".into(), "100000".into()],
        );

        store
            .sql_rule("dept_max(Dept, max(Sal)) :- employee(Name, Dept, Sal)")
            .unwrap();

        let results = store
            .query(&Literal {
                predicate: "dept_max".into(),
                args: vec![Term::Var("D".into()), Term::Var("M".into())],
                negated: false,
            })
            .unwrap();
        let map: HashMap<&str, &str> = results
            .iter()
            .map(|r| (r[0].as_str(), r[1].as_str()))
            .collect();
        assert_eq!(map["engineering"], "150000");
        assert_eq!(map["marketing"], "100000");
    }

    #[test]
    fn test_agg_count_no_grouping() {
        // count() with no group-by key — total count
        let mut store = DatalogStore::new();
        store.assert_fact("item", vec!["a".into()]);
        store.assert_fact("item", vec!["b".into()]);
        store.assert_fact("item", vec!["c".into()]);

        // total(count()) :- item(_).
        store.sql_rule("total(count()) :- item(X)").unwrap();

        let results = store
            .query(&Literal {
                predicate: "total".into(),
                args: vec![Term::Var("N".into())],
                negated: false,
            })
            .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0][0], "3");
    }

    #[test]
    fn test_agg_sum_with_negatives() {
        let mut store = DatalogStore::new();
        store.assert_fact("val", vec!["a".into(), "10".into()]);
        store.assert_fact("val", vec!["b".into(), "-5".into()]);
        store.assert_fact("val", vec!["c".into(), "3".into()]);

        store.sql_rule("total_val(sum(V)) :- val(K, V)").unwrap();

        let results = store
            .query(&Literal {
                predicate: "total_val".into(),
                args: vec![Term::Var("S".into())],
                negated: false,
            })
            .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0][0], "8");
    }

    #[test]
    fn test_agg_parse_roundtrip() {
        // Verify that aggregate rules parse and format correctly for WAL
        let input = "dept_count(Dept, count()) :- employee(Name, Dept, Sal).";
        let stmts = parse(input).unwrap();
        assert_eq!(stmts.len(), 1);
        match &stmts[0] {
            Statement::Rule(r) => {
                assert_eq!(r.head.args.len(), 2);
                assert_eq!(r.head.args[0], Term::Var("Dept".into()));
                assert_eq!(r.head.args[1], Term::Agg(AggFunc::Count));
                // Round-trip through format
                let text = format_rule(r);
                assert!(text.contains("count()"));
            }
            _ => panic!("Expected Rule"),
        }

        // sum
        let stmts2 = parse("ds(Dept, sum(Sal)) :- employee(N, Dept, Sal).").unwrap();
        match &stmts2[0] {
            Statement::Rule(r) => {
                assert_eq!(r.head.args[1], Term::Agg(AggFunc::Sum("Sal".into())));
                let text = format_rule(r);
                assert!(text.contains("sum(Sal)"));
            }
            _ => panic!("Expected Rule"),
        }
    }

    #[test]
    fn test_agg_with_derived_body() {
        // Aggregate over derived (not base) facts
        let mut store = DatalogStore::new();
        store.assert_fact("parent", vec!["alice".into(), "bob".into()]);
        store.assert_fact("parent", vec!["alice".into(), "charlie".into()]);
        store.assert_fact("parent", vec!["bob".into(), "dave".into()]);
        store.assert_fact("parent", vec!["bob".into(), "eve".into()]);

        // child_of(Parent, Child) :- parent(Parent, Child).
        store
            .sql_rule("child_of(Parent, Child) :- parent(Parent, Child)")
            .unwrap();
        // child_count(Parent, count()) :- child_of(Parent, Child).
        store
            .sql_rule("child_count(Parent, count()) :- child_of(Parent, Child)")
            .unwrap();

        let results = store
            .query(&Literal {
                predicate: "child_count".into(),
                args: vec![Term::Var("P".into()), Term::Var("N".into())],
                negated: false,
            })
            .unwrap();
        let map: HashMap<&str, &str> = results
            .iter()
            .map(|r| (r[0].as_str(), r[1].as_str()))
            .collect();
        assert_eq!(map["alice"], "2");
        assert_eq!(map["bob"], "2");
    }

    #[test]
    fn test_agg_multiple_aggregates_same_rule() {
        // Test rule with multiple aggregate functions (count + sum in same rule)
        let mut store = DatalogStore::new();
        store.assert_fact("sale", vec!["electronics".into(), "100".into()]);
        store.assert_fact("sale", vec!["electronics".into(), "200".into()]);
        store.assert_fact("sale", vec!["clothing".into(), "50".into()]);

        // We test count and sum separately since a single head can have both
        store
            .sql_rule("sale_count(Cat, count()) :- sale(Cat, Amt)")
            .unwrap();
        store
            .sql_rule("sale_total(Cat, sum(Amt)) :- sale(Cat, Amt)")
            .unwrap();

        let count_results = store
            .query(&Literal {
                predicate: "sale_count".into(),
                args: vec![Term::Var("C".into()), Term::Var("N".into())],
                negated: false,
            })
            .unwrap();
        let count_map: HashMap<&str, &str> = count_results
            .iter()
            .map(|r| (r[0].as_str(), r[1].as_str()))
            .collect();
        assert_eq!(count_map["electronics"], "2");
        assert_eq!(count_map["clothing"], "1");

        let total_results = store
            .query(&Literal {
                predicate: "sale_total".into(),
                args: vec![Term::Var("C".into()), Term::Var("T".into())],
                negated: false,
            })
            .unwrap();
        let total_map: HashMap<&str, &str> = total_results
            .iter()
            .map(|r| (r[0].as_str(), r[1].as_str()))
            .collect();
        assert_eq!(total_map["electronics"], "300");
        assert_eq!(total_map["clothing"], "50");
    }

    #[test]
    fn test_agg_empty_body_match() {
        // Aggregate with no matching body facts should produce no results
        let mut store = DatalogStore::new();
        store.sql_rule("total(count()) :- item(X)").unwrap();

        let results = store
            .query(&Literal {
                predicate: "total".into(),
                args: vec![Term::Var("N".into())],
                negated: false,
            })
            .unwrap();
        assert!(results.is_empty());
    }

    // ─── Parallel evaluation tests ──────────────────────────────────────────

    #[test]
    fn test_parallel_evaluation_large_fact_base() {
        // Create 200 facts to trigger parallel evaluation (threshold = 100)
        let mut store = DatalogStore::new();
        for i in 0..200 {
            store.assert_fact("edge", vec![format!("n{i}"), format!("n{}", i + 1)]);
        }

        // Two rules in same stratum = eligible for parallelism
        // path(X, Y) :- edge(X, Y).
        store
            .add_rule(Rule {
                head: Literal {
                    predicate: "path".into(),
                    args: vec![Term::Var("X".into()), Term::Var("Y".into())],
                    negated: false,
                },
                body: vec![Literal {
                    predicate: "edge".into(),
                    args: vec![Term::Var("X".into()), Term::Var("Y".into())],
                    negated: false,
                }],
            })
            .unwrap();
        // path(X, Z) :- edge(X, Y), path(Y, Z).
        store
            .add_rule(Rule {
                head: Literal {
                    predicate: "path".into(),
                    args: vec![Term::Var("X".into()), Term::Var("Z".into())],
                    negated: false,
                },
                body: vec![
                    Literal {
                        predicate: "edge".into(),
                        args: vec![Term::Var("X".into()), Term::Var("Y".into())],
                        negated: false,
                    },
                    Literal {
                        predicate: "path".into(),
                        args: vec![Term::Var("Y".into()), Term::Var("Z".into())],
                        negated: false,
                    },
                ],
            })
            .unwrap();

        let results = store
            .query(&Literal {
                predicate: "path".into(),
                args: vec![Term::Const("n0".into()), Term::Var("X".into())],
                negated: false,
            })
            .unwrap();
        // n0 can reach n1..n200
        assert_eq!(results.len(), 200);
    }

    #[test]
    fn test_parallel_multiple_predicates_same_stratum() {
        // Multiple rules defining different predicates, same stratum, large fact base
        let mut store = DatalogStore::new();
        for i in 0..150 {
            store.assert_fact("data", vec![format!("k{i}"), format!("{}", i * 10)]);
        }

        // doubled(K, V) :- data(K, V).
        store
            .add_rule(Rule {
                head: Literal {
                    predicate: "doubled".into(),
                    args: vec![Term::Var("K".into()), Term::Var("V".into())],
                    negated: false,
                },
                body: vec![Literal {
                    predicate: "data".into(),
                    args: vec![Term::Var("K".into()), Term::Var("V".into())],
                    negated: false,
                }],
            })
            .unwrap();
        // mirrored(V, K) :- data(K, V).
        store
            .add_rule(Rule {
                head: Literal {
                    predicate: "mirrored".into(),
                    args: vec![Term::Var("V".into()), Term::Var("K".into())],
                    negated: false,
                },
                body: vec![Literal {
                    predicate: "data".into(),
                    args: vec![Term::Var("K".into()), Term::Var("V".into())],
                    negated: false,
                }],
            })
            .unwrap();

        let r1 = store
            .query(&Literal {
                predicate: "doubled".into(),
                args: vec![Term::Var("K".into()), Term::Var("V".into())],
                negated: false,
            })
            .unwrap();
        assert_eq!(r1.len(), 150);

        let r2 = store
            .query(&Literal {
                predicate: "mirrored".into(),
                args: vec![Term::Var("V".into()), Term::Var("K".into())],
                negated: false,
            })
            .unwrap();
        assert_eq!(r2.len(), 150);
    }

    #[test]
    fn test_parallel_deterministic_results() {
        // Verify parallel produces identical results across multiple runs
        for _ in 0..5 {
            let mut store = DatalogStore::new();
            for i in 0..120 {
                store.assert_fact("node", vec![format!("n{i}")]);
            }
            for i in 0..119 {
                store.assert_fact("link", vec![format!("n{i}"), format!("n{}", i + 1)]);
            }

            // connected(X, Y) :- link(X, Y).
            store
                .add_rule(Rule {
                    head: Literal {
                        predicate: "connected".into(),
                        args: vec![Term::Var("X".into()), Term::Var("Y".into())],
                        negated: false,
                    },
                    body: vec![Literal {
                        predicate: "link".into(),
                        args: vec![Term::Var("X".into()), Term::Var("Y".into())],
                        negated: false,
                    }],
                })
                .unwrap();
            // connected(X, Z) :- connected(X, Y), link(Y, Z).
            store
                .add_rule(Rule {
                    head: Literal {
                        predicate: "connected".into(),
                        args: vec![Term::Var("X".into()), Term::Var("Z".into())],
                        negated: false,
                    },
                    body: vec![
                        Literal {
                            predicate: "connected".into(),
                            args: vec![Term::Var("X".into()), Term::Var("Y".into())],
                            negated: false,
                        },
                        Literal {
                            predicate: "link".into(),
                            args: vec![Term::Var("Y".into()), Term::Var("Z".into())],
                            negated: false,
                        },
                    ],
                })
                .unwrap();

            let results = store
                .query(&Literal {
                    predicate: "connected".into(),
                    args: vec![Term::Const("n0".into()), Term::Var("X".into())],
                    negated: false,
                })
                .unwrap();
            assert_eq!(results.len(), 119); // n0 -> n1..n119
        }
    }

    #[test]
    fn test_parallel_below_threshold_still_correct() {
        // Below the parallelism threshold (< 100 facts) — should still work (sequential)
        let mut store = DatalogStore::new();
        for i in 0..10 {
            store.assert_fact("edge", vec![format!("n{i}"), format!("n{}", i + 1)]);
        }

        store
            .add_rule(Rule {
                head: Literal {
                    predicate: "path".into(),
                    args: vec![Term::Var("X".into()), Term::Var("Y".into())],
                    negated: false,
                },
                body: vec![Literal {
                    predicate: "edge".into(),
                    args: vec![Term::Var("X".into()), Term::Var("Y".into())],
                    negated: false,
                }],
            })
            .unwrap();
        store
            .add_rule(Rule {
                head: Literal {
                    predicate: "path".into(),
                    args: vec![Term::Var("X".into()), Term::Var("Z".into())],
                    negated: false,
                },
                body: vec![
                    Literal {
                        predicate: "edge".into(),
                        args: vec![Term::Var("X".into()), Term::Var("Y".into())],
                        negated: false,
                    },
                    Literal {
                        predicate: "path".into(),
                        args: vec![Term::Var("Y".into()), Term::Var("Z".into())],
                        negated: false,
                    },
                ],
            })
            .unwrap();

        let results = store
            .query(&Literal {
                predicate: "path".into(),
                args: vec![Term::Const("n0".into()), Term::Var("X".into())],
                negated: false,
            })
            .unwrap();
        assert_eq!(results.len(), 10); // n0 -> n1..n10
    }

    // ─── GDL regression tests ───────────────────────────────────────────────

    /// GDL-1: strata must be assigned over the FULL dependency graph. A rule
    /// positively consuming a higher-stratum (negation-defined) predicate used
    /// to land in a LOWER stratum, evaluated before its input existed, and was
    /// never retried — `uses` stayed empty forever while `unreach` answered fine.
    #[test]
    fn test_positive_consumer_sees_higher_stratum_results() {
        let mut store = DatalogStore::new();
        store.sql_assert("edge(a, b).").unwrap();
        store.sql_assert("edge(b, c).").unwrap();
        store.sql_rule("path(X, Y) :- edge(X, Y).").unwrap();
        store
            .sql_rule("unreach(X, Y) :- edge(X, Y), \\+ path(Y, X).")
            .unwrap();
        store.sql_rule("uses(X) :- unreach(X, Y).").unwrap();

        // Control: the negation-defined predicate itself derives.
        let unreach = store.sql_query("unreach(A, B)").unwrap();
        assert_eq!(unreach, "[[\"a\", \"b\"], [\"b\", \"c\"]]");

        // The positive consumer of a higher-stratum predicate must see them.
        let uses = store.sql_query("uses(X)").unwrap();
        assert_eq!(uses, "[[\"a\"], [\"b\"]]", "consumer ran before its input");
    }

    /// GDL-2: a normal rule consuming an aggregate predicate must run in a
    /// LATER stratum. Aggregates used to run once after the stratum fixpoint
    /// with consumers already evaluated — `big` was forever empty while
    /// `cnt` answered fine.
    #[test]
    fn test_aggregate_consumer_rule() {
        let mut store = DatalogStore::new();
        store.sql_assert("emp(d1, 10).").unwrap();
        store.sql_assert("emp(d1, 20).").unwrap();
        store.sql_assert("emp(d2, 30).").unwrap();
        store.sql_rule("cnt(D, count()) :- emp(D, S).").unwrap();
        store.sql_rule("big(D) :- cnt(D, N).").unwrap();

        let cnt = store.sql_query("cnt(D, N)").unwrap();
        assert_eq!(cnt, "[[\"d1\", \"2\"], [\"d2\", \"1\"]]");

        let big = store.sql_query("big(D)").unwrap();
        assert_eq!(
            big, "[[\"d1\"], [\"d2\"]]",
            "aggregate consumer saw nothing"
        );
    }

    /// GDL-2: aggregate-of-aggregate. The single pre-aggregate `all_facts`
    /// snapshot meant aggregate rule 2 evaluated against a world without
    /// aggregate rule 1's output.
    #[test]
    fn test_aggregate_of_aggregate() {
        let mut store = DatalogStore::new();
        store.sql_assert("emp(d1, 10).").unwrap();
        store.sql_assert("emp(d1, 20).").unwrap();
        store.sql_assert("emp(d2, 30).").unwrap();
        store.sql_rule("cnt(D, count()) :- emp(D, S).").unwrap();
        store.sql_rule("total(sum(N)) :- cnt(D, N).").unwrap();

        let total = store.sql_query("total(T)").unwrap();
        assert_eq!(total, "[[\"3\"]]", "the stale snapshot summed an empty set");
    }

    /// GDL-5: an unstratifiable pair must be rejected at registration — one
    /// bad rule pair used to silently disable ALL rule evaluation store-wide
    /// (every derived predicate answered empty, no error anywhere).
    #[test]
    fn test_unstratifiable_program_rejected_not_swallowed() {
        let mut store = DatalogStore::new();
        store.sql_assert("node(a).").unwrap();
        store.sql_assert("node(b).").unwrap();
        store
            .sql_rule("reach(X, Y) :- edge(X, Y), \\+ blocked(Y).")
            .unwrap();

        // Completes the negation cycle: edge ← ¬reach ← edge.
        let err = store
            .sql_rule("edge(X, Y) :- node(X), node(Y), \\+ reach(X, Y).")
            .expect_err("negation cycle must be rejected at registration");
        assert!(err.contains("stratifiable"), "unexpected error: {err}");

        // Blast radius: a fresh, safe rule still evaluates afterwards.
        store.sql_assert("leaf(l).").unwrap();
        store.sql_rule("isleaf(X) :- leaf(X).").unwrap();
        let out = store.sql_query("isleaf(X)").unwrap();
        assert_eq!(out, "[[\"l\"]]");

        // Hand-poisoned store (a legacy WAL could write exactly this): query
        // must surface the error, not return base facts as if fine.
        store.rules.push(Rule {
            head: Literal {
                predicate: "p".into(),
                args: vec![Term::Var("X".into())],
                negated: false,
            },
            body: vec![Literal {
                predicate: "p".into(),
                args: vec![Term::Var("X".into())],
                negated: true,
            }],
        });
        let err = store
            .sql_query("node(A)")
            .expect_err("poisoned program must error, not answer");
        assert!(err.contains("stratifiable"), "unexpected error: {err}");
    }

    /// GDL-6: unsafe negation (a variable appearing ONLY in a negated
    /// literal) used to be silently treated as satisfied for every candidate,
    /// over-deriving the head.
    #[test]
    fn test_unsafe_negation_rejected() {
        let mut store = DatalogStore::new();
        store.sql_assert("q(a).").unwrap();
        store.sql_assert("r(a, z).").unwrap();

        let err = store
            .sql_rule("p(X) :- q(X), \\+ r(X, Y).")
            .expect_err("unsafe negation must be rejected at registration");
        assert!(err.contains("unsafe negation"), "unexpected error: {err}");

        // The store was not poisoned: p is simply absent.
        let out = store.sql_query("p(X)").unwrap();
        assert_eq!(out, "[]");
        // And a safe variant still works.
        store.sql_rule("safe(X) :- q(X), \\+ r(X, z).").unwrap();
        let out = store.sql_query("safe(X)").unwrap();
        assert_eq!(out, "[]", "r(a,z) exists, so no q row passes");
    }

    /// GDL-9: a negated literal in a QUERY returned exactly the tuples the
    /// user asked to EXCLUDE — the negation flag was parsed and then ignored.
    #[test]
    fn test_query_rejects_negated_literal() {
        let mut store = DatalogStore::new();
        store.sql_assert("likes(alice, bob).").unwrap();

        let err = store
            .sql_query("\\+ likes(alice, bob)")
            .expect_err("negated query literal must be rejected");
        assert!(err.contains("negated"), "unexpected error: {err}");

        let ok = store.sql_query("likes(alice, bob)").unwrap();
        assert_eq!(ok, "[[\"alice\", \"bob\"]]");
    }

    /// GDL-3: checkpoint re-serializes facts from parsed args. Any argument
    /// that is not a bare lowercase atom or number either failed to re-parse
    /// (fact silently DROPPED on replay) or re-parsed with a different arity
    /// (fact silently corrupted).
    #[test]
    fn test_wal_checkpoint_quoted_facts_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        {
            let (wal, _) = DatalogWal::open(dir.path()).unwrap();
            let mut store = DatalogStore::new();
            store.sql_assert("city(\"New York\", 8.4).").unwrap();
            store.sql_assert("tag(\"a, b\").").unwrap();
            store.sql_assert("note(\"it's here\").").unwrap();
            store
                .sql_assert("edge_case(\"both ' and \\\" quotes\").")
                .unwrap();
            store.sql_assert("plain(bare_atom_2, 42, 8.4).").unwrap();
            store.sql_rule("ny(P) :- city(\"New York\", P).").unwrap();
            wal.checkpoint(&store).unwrap();
        }

        let (_wal2, state2) = DatalogWal::open(dir.path()).unwrap();
        let mut store2 = restore_from_wal(state2);

        let city = store2.sql_query("city(N, P)").unwrap();
        assert_eq!(
            city, "[[\"New York\", \"8.4\"]]",
            "quoted fact was dropped/corrupted"
        );

        let tag = store2.sql_query("tag(X)").unwrap();
        assert_eq!(
            tag, "[[\"a, b\"]]",
            "comma inside a quoted arg re-parsed as 2 args"
        );

        let note = store2.sql_query("note(X)").unwrap();
        assert_eq!(note, "[[\"it's here\"]]");

        let both = store2.sql_query("edge_case(X)").unwrap();
        assert_eq!(both, "[[\"both ' and \\\" quotes\"]]");

        let plain = store2.sql_query("plain(A, B, C)").unwrap();
        assert_eq!(plain, "[[\"bare_atom_2\", \"42\", \"8.4\"]]");

        // A rule with a quoted const round-trips and still evaluates.
        let ny = store2.sql_query("ny(P)").unwrap();
        assert_eq!(ny, "[[\"8.4\"]]", "rule with a quoted const was dropped");
    }

    /// PART 0 (datalog stranded writer): a checkpoint whose reopen fails must
    /// not leave the writer appending into the unlinked inode the rename
    /// displaced — those appends report success while no recovery can read
    /// them. Same shape as the wave-3 fixes in the other WALs.
    #[test]
    fn a_failed_checkpoint_reopen_does_not_strand_the_writer() {
        let dir = tempfile::tempdir().unwrap();
        {
            let (wal, _) = DatalogWal::open(dir.path()).unwrap();
            wal.log_assert(None, "parent(alice, bob).").unwrap();
            let mut store = DatalogStore::new();
            store.assert_fact("parent", vec!["alice".into(), "bob".into()]);
            wal.fail_reopen_once
                .store(true, std::sync::atomic::Ordering::SeqCst);
            wal.checkpoint(&store)
                .expect_err("the injected reopen failure must fail the checkpoint");
            wal.log_assert(None, "parent(bob, charlie).")
                .expect("a later append must reattach, not strand");
        }
        let (_, state) = DatalogWal::open(dir.path()).unwrap();
        let mut sorted: Vec<_> = state.facts.into_iter().collect();
        sorted.sort();
        assert_eq!(
            sorted,
            vec![
                (
                    "parent".to_string(),
                    vec!["alice".to_string(), "bob".to_string()]
                ),
                (
                    "parent".to_string(),
                    vec!["bob".to_string(), "charlie".to_string()]
                ),
            ],
            "the post-checkpoint-failure append went to the unlinked inode: it \
             returned Ok and no recovery can ever read it"
        );
    }
}
