//! Cypher query language parser for the graph engine (Phase 8).
//!
//! Parses basic Cypher queries:
//!   MATCH (n:Label)-[r:TYPE]->(m) WHERE n.prop = value RETURN n, m
//!   CREATE (n:Label {prop: value})
//!
//! Converts to AST that can be executed against GraphStore.

use std::collections::BTreeMap;
use std::fmt;

use super::{Direction, PropValue};

type NodeInternals = (Option<String>, Vec<String>, BTreeMap<String, PropValue>);
type EdgeInternals = (Option<String>, Option<String>, BTreeMap<String, PropValue>);

// ============================================================================
// AST types
// ============================================================================

/// A parsed Cypher statement.
#[derive(Debug, Clone, PartialEq)]
pub enum CypherStatement {
    Match {
        pattern: Pattern,
        where_clause: Option<WhereClause>,
        return_clause: ReturnClause,
        optional: bool,
        /// Optional WITH clause — projects intermediate columns between MATCH and RETURN.
        with_clause: Option<WithClause>,
        /// Optional WHERE after WITH (filters projected results).
        with_where: Option<WhereClause>,
    },
    Create {
        items: Vec<CreateItem>,
    },
    Delete {
        variables: Vec<String>,
    },
}

/// A WITH clause that projects intermediate results.
///
/// Syntax: `WITH n.name AS name, count(*) AS cnt`
#[derive(Debug, Clone, PartialEq)]
pub struct WithClause {
    pub items: Vec<WithItem>,
}

/// A single item in a WITH clause.
#[derive(Debug, Clone, PartialEq)]
pub struct WithItem {
    /// The expression being projected (reuses ReturnItem for simplicity).
    pub expr: ReturnItem,
    /// Optional alias (AS name).
    pub alias: Option<String>,
}

/// A graph pattern — sequence of nodes connected by edges.
#[derive(Debug, Clone, PartialEq)]
pub struct Pattern {
    pub nodes: Vec<NodePattern>,
    pub edges: Vec<EdgePattern>,
}

/// A node pattern like `(n:Person {name: 'Alice'})`.
#[derive(Debug, Clone, PartialEq)]
pub struct NodePattern {
    pub variable: Option<String>,
    pub labels: Vec<String>,
    pub properties: BTreeMap<String, PropValue>,
}

/// An edge pattern like `-[r:KNOWS]->`  or `-[*1..3]->`.
#[derive(Debug, Clone, PartialEq)]
pub struct EdgePattern {
    pub variable: Option<String>,
    pub edge_type: Option<String>,
    pub direction: Direction,
    pub from_idx: usize,
    pub to_idx: usize,
    pub min_hops: Option<usize>,
    pub max_hops: Option<usize>,
}

/// A WHERE clause containing conditions.
#[derive(Debug, Clone, PartialEq)]
pub struct WhereClause {
    pub conditions: Vec<Condition>,
}

/// A single condition in a WHERE clause.
#[derive(Debug, Clone, PartialEq)]
pub enum Condition {
    PropertyEquals {
        variable: String,
        property: String,
        value: PropValue,
    },
    And(Box<Condition>, Box<Condition>),
}

/// A RETURN clause specifying what to project.
#[derive(Debug, Clone, PartialEq)]
pub struct ReturnClause {
    pub items: Vec<ReturnItem>,
}

/// A single item in a RETURN clause.
#[derive(Debug, Clone, PartialEq)]
pub enum ReturnItem {
    Variable(String),
    Property(String, String),
    Count,
    All,
}

/// An item to create.
#[derive(Debug, Clone, PartialEq)]
pub enum CreateItem {
    Node {
        variable: Option<String>,
        labels: Vec<String>,
        properties: BTreeMap<String, PropValue>,
    },
    Edge {
        from_var: String,
        to_var: String,
        edge_type: String,
        properties: BTreeMap<String, PropValue>,
    },
}

// ============================================================================
// Error type
// ============================================================================

/// Errors from Cypher parsing.
#[derive(Debug, Clone, PartialEq)]
pub enum CypherError {
    UnexpectedToken { expected: String, found: String },
    UnexpectedEnd,
    InvalidSyntax(String),
    UnknownKeyword(String),
}

impl fmt::Display for CypherError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CypherError::UnexpectedToken { expected, found } => {
                write!(f, "expected {expected}, found '{found}'")
            }
            CypherError::UnexpectedEnd => write!(f, "unexpected end of input"),
            CypherError::InvalidSyntax(msg) => write!(f, "invalid syntax: {msg}"),
            CypherError::UnknownKeyword(kw) => write!(f, "unknown keyword: {kw}"),
        }
    }
}

impl std::error::Error for CypherError {}

// ============================================================================
// Tokenizer
// ============================================================================

#[derive(Debug, Clone, PartialEq)]
enum Token {
    // Punctuation
    LParen,
    RParen,
    LBracket,
    RBracket,
    LBrace,
    RBrace,
    Colon,
    Comma,
    Dot,
    Star,
    Dash,
    Gt,
    Lt,
    Eq,
    // Literals
    Ident(String),
    StringLit(String),
    IntLit(i64),
    FloatLit(f64),
    // Keywords (case-insensitive, stored uppercase)
    Keyword(String),
}

impl fmt::Display for Token {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Token::LParen => write!(f, "("),
            Token::RParen => write!(f, ")"),
            Token::LBracket => write!(f, "["),
            Token::RBracket => write!(f, "]"),
            Token::LBrace => write!(f, "{{"),
            Token::RBrace => write!(f, "}}"),
            Token::Colon => write!(f, ":"),
            Token::Comma => write!(f, ","),
            Token::Dot => write!(f, "."),
            Token::Star => write!(f, "*"),
            Token::Dash => write!(f, "-"),
            Token::Gt => write!(f, ">"),
            Token::Lt => write!(f, "<"),
            Token::Eq => write!(f, "="),
            Token::Ident(s) => write!(f, "{s}"),
            Token::StringLit(s) => write!(f, "'{s}'"),
            Token::IntLit(n) => write!(f, "{n}"),
            Token::FloatLit(n) => write!(f, "{n}"),
            Token::Keyword(k) => write!(f, "{k}"),
        }
    }
}

const KEYWORDS: &[&str] = &[
    "MATCH", "CREATE", "DELETE", "RETURN", "WHERE", "AND", "OR", "NOT", "TRUE",
    "FALSE", "NULL", "COUNT", "AS", "SET", "REMOVE", "DETACH", "OPTIONAL",
    "WITH", "ORDER", "BY", "SKIP", "LIMIT", "DISTINCT",
];

fn is_keyword(word: &str) -> bool {
    let upper = word.to_uppercase();
    KEYWORDS.contains(&upper.as_str())
}

fn tokenize(input: &str) -> Result<Vec<Token>, CypherError> {
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
            '[' => {
                tokens.push(Token::LBracket);
                i += 1;
                continue;
            }
            ']' => {
                tokens.push(Token::RBracket);
                i += 1;
                continue;
            }
            '{' => {
                tokens.push(Token::LBrace);
                i += 1;
                continue;
            }
            '}' => {
                tokens.push(Token::RBrace);
                i += 1;
                continue;
            }
            ':' => {
                tokens.push(Token::Colon);
                i += 1;
                continue;
            }
            ',' => {
                tokens.push(Token::Comma);
                i += 1;
                continue;
            }
            '.' => {
                tokens.push(Token::Dot);
                i += 1;
                continue;
            }
            '*' => {
                tokens.push(Token::Star);
                i += 1;
                continue;
            }
            '>' => {
                tokens.push(Token::Gt);
                i += 1;
                continue;
            }
            '<' => {
                tokens.push(Token::Lt);
                i += 1;
                continue;
            }
            '=' => {
                tokens.push(Token::Eq);
                i += 1;
                continue;
            }
            _ => {}
        }

        // Dash: could be negative number or relationship arrow
        if ch == '-' {
            // Check if next char is a digit (negative number) and previous token
            // allows a numeric literal (after =, comma, colon, or at start)
            let could_be_negative = i + 1 < len && chars[i + 1].is_ascii_digit() && {
                matches!(
                    tokens.last(),
                    None | Some(Token::Colon)
                        | Some(Token::Comma)
                        | Some(Token::Eq)
                        | Some(Token::LBrace)
                        | Some(Token::Gt)
                        | Some(Token::Lt)
                )
            };
            if could_be_negative {
                // Parse negative number
                let start = i;
                i += 1; // skip '-'
                while i < len && (chars[i].is_ascii_digit() || chars[i] == '.') {
                    i += 1;
                }
                let num_str: String = chars[start..i].iter().collect();
                if num_str.contains('.') {
                    let val: f64 = num_str.parse().map_err(|_| {
                        CypherError::InvalidSyntax(format!("invalid float: {num_str}"))
                    })?;
                    tokens.push(Token::FloatLit(val));
                } else {
                    let val: i64 = num_str.parse().map_err(|_| {
                        CypherError::InvalidSyntax(format!("invalid integer: {num_str}"))
                    })?;
                    tokens.push(Token::IntLit(val));
                }
                continue;
            }
            tokens.push(Token::Dash);
            i += 1;
            continue;
        }

        // String literal (single or double quotes)
        if ch == '\'' || ch == '"' {
            let quote = ch;
            i += 1;
            let start = i;
            while i < len && chars[i] != quote {
                if chars[i] == '\\' {
                    i += 1; // skip escaped char
                }
                i += 1;
            }
            if i >= len {
                return Err(CypherError::InvalidSyntax("unterminated string".into()));
            }
            let s: String = chars[start..i].iter().collect();
            tokens.push(Token::StringLit(s));
            i += 1; // skip closing quote
            continue;
        }

        // Numbers
        if ch.is_ascii_digit() {
            let start = i;
            while i < len && (chars[i].is_ascii_digit() || chars[i] == '.') {
                i += 1;
            }
            let num_str: String = chars[start..i].iter().collect();
            if num_str.contains('.') {
                let val: f64 = num_str.parse().map_err(|_| {
                    CypherError::InvalidSyntax(format!("invalid float: {num_str}"))
                })?;
                tokens.push(Token::FloatLit(val));
            } else {
                let val: i64 = num_str.parse().map_err(|_| {
                    CypherError::InvalidSyntax(format!("invalid integer: {num_str}"))
                })?;
                tokens.push(Token::IntLit(val));
            }
            continue;
        }

        // Identifiers and keywords
        if ch.is_ascii_alphabetic() || ch == '_' {
            let start = i;
            while i < len && (chars[i].is_ascii_alphanumeric() || chars[i] == '_') {
                i += 1;
            }
            let word: String = chars[start..i].iter().collect();

            // Check for boolean literals
            if word.eq_ignore_ascii_case("true") {
                tokens.push(Token::Keyword("TRUE".into()));
                continue;
            }
            if word.eq_ignore_ascii_case("false") {
                tokens.push(Token::Keyword("FALSE".into()));
                continue;
            }
            if word.eq_ignore_ascii_case("null") {
                tokens.push(Token::Keyword("NULL".into()));
                continue;
            }

            if is_keyword(&word) {
                tokens.push(Token::Keyword(word.to_uppercase()));
            } else {
                tokens.push(Token::Ident(word));
            }
            continue;
        }

        return Err(CypherError::InvalidSyntax(format!(
            "unexpected character: '{ch}'"
        )));
    }

    Ok(tokens)
}

// ============================================================================
// Parser
// ============================================================================

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

    fn advance(&mut self) -> Result<&Token, CypherError> {
        if self.pos >= self.tokens.len() {
            return Err(CypherError::UnexpectedEnd);
        }
        let tok = &self.tokens[self.pos];
        self.pos += 1;
        Ok(tok)
    }

    fn expect_keyword(&mut self, kw: &str) -> Result<(), CypherError> {
        let tok = self.advance()?.clone();
        match &tok {
            Token::Keyword(k) if k == kw => Ok(()),
            other => Err(CypherError::UnexpectedToken {
                expected: kw.to_string(),
                found: other.to_string(),
            }),
        }
    }

    fn expect_token(&mut self, expected: &Token) -> Result<(), CypherError> {
        let tok = self.advance()?.clone();
        if &tok == expected {
            Ok(())
        } else {
            Err(CypherError::UnexpectedToken {
                expected: expected.to_string(),
                found: tok.to_string(),
            })
        }
    }

    fn at_end(&self) -> bool {
        self.pos >= self.tokens.len()
    }

    fn check_keyword(&self, kw: &str) -> bool {
        matches!(self.peek(), Some(Token::Keyword(k)) if k == kw)
    }

    fn check_token(&self, expected: &Token) -> bool {
        self.peek() == Some(expected)
    }

    // ---- Top-level parse ----

    fn parse_statement(&mut self) -> Result<CypherStatement, CypherError> {
        match self.peek() {
            Some(Token::Keyword(k)) if k == "MATCH" || k == "OPTIONAL" => self.parse_match(),
            Some(Token::Keyword(k)) if k == "CREATE" => self.parse_create(),
            Some(Token::Keyword(k)) if k == "DELETE" => self.parse_delete(),
            Some(Token::Keyword(k)) => {
                let k = k.clone();
                Err(CypherError::UnknownKeyword(k))
            }
            // Treat unrecognized identifiers at statement position as unknown keywords
            Some(Token::Ident(name)) => {
                let name = name.clone();
                Err(CypherError::UnknownKeyword(name))
            }
            Some(other) => Err(CypherError::InvalidSyntax(format!(
                "expected statement keyword, found '{other}'"
            ))),
            None => Err(CypherError::UnexpectedEnd),
        }
    }

    // ---- MATCH ----

    fn parse_match(&mut self) -> Result<CypherStatement, CypherError> {
        // Check for OPTIONAL prefix
        let optional = if self.check_keyword("OPTIONAL") {
            self.advance()?;
            true
        } else {
            false
        };

        self.expect_keyword("MATCH")?;
        let pattern = self.parse_pattern()?;

        let where_clause = if self.check_keyword("WHERE") {
            Some(self.parse_where()?)
        } else {
            None
        };

        // Check for WITH clause (pipe intermediate results)
        let (with_clause, with_where) = if self.check_keyword("WITH") {
            let wc = self.parse_with_clause()?;
            let ww = if self.check_keyword("WHERE") {
                Some(self.parse_where()?)
            } else {
                None
            };
            (Some(wc), ww)
        } else {
            (None, None)
        };

        self.expect_keyword("RETURN")?;
        let return_clause = self.parse_return_clause()?;

        Ok(CypherStatement::Match {
            pattern,
            where_clause,
            return_clause,
            optional,
            with_clause,
            with_where,
        })
    }

    // ---- CREATE ----

    fn parse_create(&mut self) -> Result<CypherStatement, CypherError> {
        self.expect_keyword("CREATE")?;
        let mut items = Vec::new();

        // Parse first node
        let node = self.parse_create_node()?;
        let first_var = match &node {
            CreateItem::Node { variable, .. } => variable.clone(),
            _ => None,
        };
        items.push(node);

        // Check for relationship chain: -[:TYPE]->(...) or <-[:TYPE]-(...)
        while self.check_token(&Token::Dash) || self.check_token(&Token::Lt) {
            let (edge, next_node) = self.parse_create_edge_and_node(first_var.clone(), &items)?;
            items.push(edge);
            items.push(next_node);
        }

        // Check for comma-separated additional items
        while self.check_token(&Token::Comma) {
            self.advance()?; // consume comma

            if self.check_token(&Token::LParen) {
                // Could be a standalone node or start of a pattern
                let node = self.parse_create_node()?;
                let _var = match &node {
                    CreateItem::Node { variable, .. } => variable.clone(),
                    _ => None,
                };
                items.push(node);

                while self.check_token(&Token::Dash) || self.check_token(&Token::Lt) {
                    let (edge, next_node) =
                        self.parse_create_edge_and_node(None, &items)?;
                    items.push(edge);
                    items.push(next_node);
                }
            }
        }

        Ok(CypherStatement::Create { items })
    }

    fn parse_create_node(&mut self) -> Result<CreateItem, CypherError> {
        self.expect_token(&Token::LParen)?;
        let (variable, labels, properties) = self.parse_node_internals()?;
        self.expect_token(&Token::RParen)?;

        Ok(CreateItem::Node {
            variable,
            labels,
            properties,
        })
    }

    fn parse_create_edge_and_node(
        &mut self,
        _prev_var: Option<String>,
        existing_items: &[CreateItem],
    ) -> Result<(CreateItem, CreateItem), CypherError> {
        // Determine direction: -[...]-> or <-[...]-
        let direction_start = self.advance()?.clone();
        let incoming = direction_start == Token::Lt;

        if incoming {
            // <-[...]-
            self.expect_token(&Token::Dash)?;
        }

        // Parse edge bracket content
        self.expect_token(&Token::LBracket)?;
        let (edge_var, edge_type, edge_props) = self.parse_edge_internals()?;
        let _ = edge_var; // edge variable not used in CreateItem::Edge
        self.expect_token(&Token::RBracket)?;

        // Consume arrow tail
        self.expect_token(&Token::Dash)?;
        if !incoming {
            self.expect_token(&Token::Gt)?;
        }

        // Parse the target node
        let next_node = self.parse_create_node()?;

        // Determine from/to variable names
        let from_var_name: String;
        let to_var_name: String;

        // Find the variable of the most recent node in existing_items
        let prev_node_var = existing_items
            .iter()
            .rev()
            .find_map(|item| match item {
                CreateItem::Node { variable, .. } => variable.clone(),
                _ => None,
            })
            .unwrap_or_default();

        let next_node_var = match &next_node {
            CreateItem::Node { variable, .. } => variable.clone().unwrap_or_default(),
            _ => String::new(),
        };

        if incoming {
            from_var_name = next_node_var;
            to_var_name = prev_node_var;
        } else {
            from_var_name = prev_node_var;
            to_var_name = next_node_var;
        }

        let et = edge_type.unwrap_or_default();

        Ok((
            CreateItem::Edge {
                from_var: from_var_name,
                to_var: to_var_name,
                edge_type: et,
                properties: edge_props,
            },
            next_node,
        ))
    }

    // ---- DELETE ----

    fn parse_delete(&mut self) -> Result<CypherStatement, CypherError> {
        self.expect_keyword("DELETE")?;
        let mut variables = Vec::new();

        let tok = self.advance()?.clone();
        match tok {
            Token::Ident(name) => variables.push(name),
            Token::IntLit(id) => variables.push(id.to_string()),
            other => {
                return Err(CypherError::UnexpectedToken {
                    expected: "identifier or node ID".into(),
                    found: other.to_string(),
                });
            }
        }

        while self.check_token(&Token::Comma) {
            self.advance()?;
            let tok = self.advance()?.clone();
            match tok {
                Token::Ident(name) => variables.push(name),
                Token::IntLit(id) => variables.push(id.to_string()),
                other => {
                    return Err(CypherError::UnexpectedToken {
                        expected: "identifier or node ID".into(),
                        found: other.to_string(),
                    });
                }
            }
        }

        Ok(CypherStatement::Delete { variables })
    }

    // ---- Pattern ----

    fn parse_pattern(&mut self) -> Result<Pattern, CypherError> {
        let mut nodes = Vec::new();
        let mut edges = Vec::new();

        // Parse first node
        nodes.push(self.parse_node_pattern()?);

        // Parse chain of -[edge]->(node) or <-[edge]-(node)
        while !self.at_end() && (self.check_token(&Token::Dash) || self.check_token(&Token::Lt)) {
            // Peek ahead: if it's DASH followed by something that isn't '[' or
            // could be an arrow, we parse the edge
            let edge_start = self.peek().cloned();

            let from_idx = nodes.len() - 1;

            let incoming = edge_start == Some(Token::Lt);

            if incoming {
                // <-[...]-
                self.advance()?; // consume '<'
                self.expect_token(&Token::Dash)?;
            } else {
                // -[...]->(...)
                self.advance()?; // consume '-'
            }

            // Parse edge bracket
            self.expect_token(&Token::LBracket)?;
            let (variable, edge_type, _props) = self.parse_edge_internals()?;

            // Check for variable-length pattern *min..max
            let mut min_hops = None;
            let mut max_hops = None;
            if self.check_token(&Token::Star) {
                self.advance()?; // consume '*'
                if let Some(Token::IntLit(_)) = self.peek() {
                    let tok = self.advance()?.clone();
                    if let Token::IntLit(n) = tok {
                        min_hops = Some(n as usize);
                    }
                }
                if self.check_token(&Token::Dot) {
                    self.advance()?; // consume first '.'
                    self.expect_token(&Token::Dot)?; // consume second '.'
                    if let Some(Token::IntLit(_)) = self.peek() {
                        let tok = self.advance()?.clone();
                        if let Token::IntLit(n) = tok {
                            max_hops = Some(n as usize);
                        }
                    }
                }
            }

            self.expect_token(&Token::RBracket)?;

            // Consume arrow end
            self.expect_token(&Token::Dash)?;
            let direction = if !incoming {
                self.expect_token(&Token::Gt)?;
                Direction::Outgoing
            } else {
                Direction::Incoming
            };

            // Parse target node
            nodes.push(self.parse_node_pattern()?);
            let to_idx = nodes.len() - 1;

            edges.push(EdgePattern {
                variable,
                edge_type,
                direction,
                from_idx,
                to_idx,
                min_hops,
                max_hops,
            });
        }

        Ok(Pattern { nodes, edges })
    }

    fn parse_node_pattern(&mut self) -> Result<NodePattern, CypherError> {
        self.expect_token(&Token::LParen)?;
        let (variable, labels, properties) = self.parse_node_internals()?;
        self.expect_token(&Token::RParen)?;

        Ok(NodePattern {
            variable,
            labels,
            properties,
        })
    }

    /// Parse the internals of a node: variable, labels, properties.
    /// Called after consuming `(` and before consuming `)`.
    fn parse_node_internals(
        &mut self,
    ) -> Result<NodeInternals, CypherError> {
        let mut variable = None;
        let mut labels = Vec::new();
        let mut properties = BTreeMap::new();

        // Check for variable name (identifier not followed by nothing special,
        // or followed by colon for labels)
        if let Some(Token::Ident(_)) = self.peek() {
            let tok = self.advance()?.clone();
            if let Token::Ident(name) = tok {
                variable = Some(name);
            }
        }

        // Check for labels (:Label1:Label2)
        while self.check_token(&Token::Colon) {
            self.advance()?; // consume ':'
            let tok = self.advance()?.clone();
            match tok {
                Token::Ident(label) => labels.push(label),
                other => {
                    return Err(CypherError::UnexpectedToken {
                        expected: "label name".into(),
                        found: other.to_string(),
                    });
                }
            }
        }

        // Check for properties {key: value, ...}
        if self.check_token(&Token::LBrace) {
            properties = self.parse_property_map()?;
        }

        Ok((variable, labels, properties))
    }

    /// Parse the internals of an edge bracket: variable, type, properties.
    /// Called after consuming `[` and before consuming `]`.
    fn parse_edge_internals(
        &mut self,
    ) -> Result<EdgeInternals, CypherError> {
        let mut variable = None;
        let mut edge_type = None;
        let mut properties = BTreeMap::new();

        // Check for variable name
        if let Some(Token::Ident(_)) = self.peek() {
            let tok = self.advance()?.clone();
            if let Token::Ident(name) = tok {
                variable = Some(name);
            }
        }

        // Check for type (:TYPE)
        if self.check_token(&Token::Colon) {
            self.advance()?; // consume ':'
            let tok = self.advance()?.clone();
            match tok {
                Token::Ident(t) => edge_type = Some(t),
                Token::Keyword(t) => edge_type = Some(t),
                other => {
                    return Err(CypherError::UnexpectedToken {
                        expected: "edge type".into(),
                        found: other.to_string(),
                    });
                }
            }
        }

        // Check for properties
        if self.check_token(&Token::LBrace) {
            properties = self.parse_property_map()?;
        }

        Ok((variable, edge_type, properties))
    }

    /// Parse `{key: value, key2: value2, ...}`.
    fn parse_property_map(&mut self) -> Result<BTreeMap<String, PropValue>, CypherError> {
        self.expect_token(&Token::LBrace)?;
        let mut map = BTreeMap::new();

        if self.check_token(&Token::RBrace) {
            self.advance()?;
            return Ok(map);
        }

        loop {
            // Key
            let key = match self.advance()?.clone() {
                Token::Ident(k) => k,
                other => {
                    return Err(CypherError::UnexpectedToken {
                        expected: "property name".into(),
                        found: other.to_string(),
                    });
                }
            };

            self.expect_token(&Token::Colon)?;

            // Value
            let value = self.parse_prop_value()?;
            map.insert(key, value);

            if self.check_token(&Token::Comma) {
                self.advance()?;
            } else {
                break;
            }
        }

        self.expect_token(&Token::RBrace)?;
        Ok(map)
    }

    /// Parse a property value: string, int, float, bool, null.
    fn parse_prop_value(&mut self) -> Result<PropValue, CypherError> {
        let tok = self.advance()?.clone();
        match tok {
            Token::StringLit(s) => Ok(PropValue::Text(s)),
            Token::IntLit(n) => Ok(PropValue::Int(n)),
            Token::FloatLit(f) => Ok(PropValue::Float(f)),
            Token::Keyword(ref k) if k == "TRUE" => Ok(PropValue::Bool(true)),
            Token::Keyword(ref k) if k == "FALSE" => Ok(PropValue::Bool(false)),
            Token::Keyword(ref k) if k == "NULL" => Ok(PropValue::Null),
            other => Err(CypherError::UnexpectedToken {
                expected: "value".into(),
                found: other.to_string(),
            }),
        }
    }

    // ---- WHERE ----

    fn parse_where(&mut self) -> Result<WhereClause, CypherError> {
        self.expect_keyword("WHERE")?;
        let mut conditions = Vec::new();

        conditions.push(self.parse_condition()?);

        while self.check_keyword("AND") {
            self.advance()?; // consume AND
            conditions.push(self.parse_condition()?);
        }

        Ok(WhereClause { conditions })
    }

    fn parse_condition(&mut self) -> Result<Condition, CypherError> {
        // Parse: variable.property = value
        // or:    variable.property > value  (treat as equality for this basic parser — extend later)
        let var_tok = self.advance()?.clone();
        let variable = match var_tok {
            Token::Ident(name) => name,
            other => {
                return Err(CypherError::UnexpectedToken {
                    expected: "variable name".into(),
                    found: other.to_string(),
                });
            }
        };

        self.expect_token(&Token::Dot)?;

        let prop_tok = self.advance()?.clone();
        let property = match prop_tok {
            Token::Ident(name) => name,
            other => {
                return Err(CypherError::UnexpectedToken {
                    expected: "property name".into(),
                    found: other.to_string(),
                });
            }
        };

        // Accept = or > or < (all mapped to PropertyEquals for now)
        let op = self.advance()?.clone();
        match op {
            Token::Eq | Token::Gt | Token::Lt => {}
            other => {
                return Err(CypherError::UnexpectedToken {
                    expected: "operator (=, >, <)".into(),
                    found: other.to_string(),
                });
            }
        }

        let value = self.parse_prop_value()?;

        Ok(Condition::PropertyEquals {
            variable,
            property,
            value,
        })
    }

    // ---- RETURN ----

    fn parse_with_clause(&mut self) -> Result<WithClause, CypherError> {
        self.expect_keyword("WITH")?;
        let mut items = Vec::new();

        items.push(self.parse_with_item()?);
        while self.check_token(&Token::Comma) {
            self.advance()?;
            items.push(self.parse_with_item()?);
        }

        Ok(WithClause { items })
    }

    fn parse_with_item(&mut self) -> Result<WithItem, CypherError> {
        // Parse the expression (reuse return item logic)
        let expr = self.parse_return_item()?;

        // Check for optional AS alias
        let alias = if self.check_keyword("AS") {
            self.advance()?; // consume AS
            let tok = self.advance()?.clone();
            match tok {
                Token::Ident(name) => Some(name),
                other => {
                    return Err(CypherError::UnexpectedToken {
                        expected: "alias name".into(),
                        found: other.to_string(),
                    });
                }
            }
        } else {
            None
        };

        Ok(WithItem { expr, alias })
    }

    fn parse_return_clause(&mut self) -> Result<ReturnClause, CypherError> {
        let mut items = Vec::new();

        // First item
        items.push(self.parse_return_item()?);

        // Additional comma-separated items
        while self.check_token(&Token::Comma) {
            self.advance()?;
            items.push(self.parse_return_item()?);
        }

        Ok(ReturnClause { items })
    }

    fn parse_return_item(&mut self) -> Result<ReturnItem, CypherError> {
        // Check for *
        if self.check_token(&Token::Star) {
            self.advance()?;
            return Ok(ReturnItem::All);
        }

        // Check for COUNT(*)
        if self.check_keyword("COUNT") {
            self.advance()?; // consume COUNT
            self.expect_token(&Token::LParen)?;
            self.expect_token(&Token::Star)?;
            self.expect_token(&Token::RParen)?;
            return Ok(ReturnItem::Count);
        }

        // Identifier: could be variable or variable.property
        let tok = self.advance()?.clone();
        let name = match tok {
            Token::Ident(n) => n,
            other => {
                return Err(CypherError::UnexpectedToken {
                    expected: "return item".into(),
                    found: other.to_string(),
                });
            }
        };

        // Check for .property
        if self.check_token(&Token::Dot) {
            self.advance()?; // consume '.'
            let prop_tok = self.advance()?.clone();
            let prop = match prop_tok {
                Token::Ident(p) => p,
                other => {
                    return Err(CypherError::UnexpectedToken {
                        expected: "property name".into(),
                        found: other.to_string(),
                    });
                }
            };
            return Ok(ReturnItem::Property(name, prop));
        }

        Ok(ReturnItem::Variable(name))
    }
}

// ============================================================================
// Public API
// ============================================================================

/// Parse a Cypher query string into a `CypherStatement` AST.
///
/// Supports:
///   - `MATCH (n:Label) RETURN n`
///   - `MATCH (n:Label)-[:TYPE]->(m) RETURN n, m`
///   - `MATCH (n:Label {prop: value}) RETURN n`
///   - `CREATE (n:Label {prop: value})`
///   - `MATCH (n) WHERE n.prop = value RETURN n`
///   - `DELETE n, m`
pub fn parse_cypher(input: &str) -> Result<CypherStatement, CypherError> {
    let tokens = tokenize(input)?;
    if tokens.is_empty() {
        return Err(CypherError::UnexpectedEnd);
    }
    let mut parser = Parser::new(tokens);
    let stmt = parser.parse_statement()?;
    Ok(stmt)
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_match() {
        let stmt = parse_cypher("MATCH (n) RETURN n").unwrap();
        match stmt {
            CypherStatement::Match {
                pattern,
                where_clause,
                return_clause,
                ..
            } => {
                assert_eq!(pattern.nodes.len(), 1);
                assert_eq!(pattern.edges.len(), 0);
                assert_eq!(
                    pattern.nodes[0].variable,
                    Some("n".into())
                );
                assert!(pattern.nodes[0].labels.is_empty());
                assert!(where_clause.is_none());
                assert_eq!(return_clause.items.len(), 1);
                assert_eq!(
                    return_clause.items[0],
                    ReturnItem::Variable("n".into())
                );
            }
            _ => panic!("expected Match statement"),
        }
    }

    #[test]
    fn test_parse_match_with_label() {
        let stmt = parse_cypher("MATCH (n:Person) RETURN n").unwrap();
        match stmt {
            CypherStatement::Match { pattern, .. } => {
                assert_eq!(pattern.nodes.len(), 1);
                assert_eq!(
                    pattern.nodes[0].variable,
                    Some("n".into())
                );
                assert_eq!(pattern.nodes[0].labels, vec!["Person".to_string()]);
            }
            _ => panic!("expected Match statement"),
        }
    }

    #[test]
    fn test_parse_match_with_relationship() {
        let stmt =
            parse_cypher("MATCH (n:Person)-[:KNOWS]->(m:Person) RETURN n, m").unwrap();
        match stmt {
            CypherStatement::Match {
                pattern,
                return_clause,
                ..
            } => {
                assert_eq!(pattern.nodes.len(), 2);
                assert_eq!(pattern.edges.len(), 1);

                let edge = &pattern.edges[0];
                assert_eq!(edge.edge_type, Some("KNOWS".into()));
                assert_eq!(edge.direction, Direction::Outgoing);
                assert_eq!(edge.from_idx, 0);
                assert_eq!(edge.to_idx, 1);

                assert_eq!(
                    pattern.nodes[0].labels,
                    vec!["Person".to_string()]
                );
                assert_eq!(
                    pattern.nodes[1].labels,
                    vec!["Person".to_string()]
                );

                assert_eq!(return_clause.items.len(), 2);
                assert_eq!(
                    return_clause.items[0],
                    ReturnItem::Variable("n".into())
                );
                assert_eq!(
                    return_clause.items[1],
                    ReturnItem::Variable("m".into())
                );
            }
            _ => panic!("expected Match statement"),
        }
    }

    #[test]
    fn test_parse_match_with_properties() {
        let stmt =
            parse_cypher("MATCH (n:Person {name: 'Alice'}) RETURN n").unwrap();
        match stmt {
            CypherStatement::Match { pattern, .. } => {
                assert_eq!(pattern.nodes.len(), 1);
                let node = &pattern.nodes[0];
                assert_eq!(node.variable, Some("n".into()));
                assert_eq!(node.labels, vec!["Person".to_string()]);
                assert_eq!(
                    node.properties.get("name"),
                    Some(&PropValue::Text("Alice".into()))
                );
            }
            _ => panic!("expected Match statement"),
        }
    }

    #[test]
    fn test_parse_create_node() {
        let stmt =
            parse_cypher("CREATE (n:Person {name: 'Bob', age: 25})").unwrap();
        match stmt {
            CypherStatement::Create { items } => {
                assert_eq!(items.len(), 1);
                match &items[0] {
                    CreateItem::Node {
                        variable,
                        labels,
                        properties,
                    } => {
                        assert_eq!(variable, &Some("n".into()));
                        assert_eq!(labels, &vec!["Person".to_string()]);
                        assert_eq!(
                            properties.get("name"),
                            Some(&PropValue::Text("Bob".into()))
                        );
                        assert_eq!(
                            properties.get("age"),
                            Some(&PropValue::Int(25))
                        );
                    }
                    _ => panic!("expected Node create item"),
                }
            }
            _ => panic!("expected Create statement"),
        }
    }

    #[test]
    fn test_parse_create_with_edge() {
        let stmt = parse_cypher(
            "CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'})",
        )
        .unwrap();
        match stmt {
            CypherStatement::Create { items } => {
                // Should have: Node(a), Edge(a->b), Node(b)
                assert_eq!(items.len(), 3);

                match &items[0] {
                    CreateItem::Node {
                        variable, labels, ..
                    } => {
                        assert_eq!(variable, &Some("a".into()));
                        assert_eq!(labels, &vec!["Person".to_string()]);
                    }
                    _ => panic!("expected first item to be Node"),
                }

                match &items[1] {
                    CreateItem::Edge {
                        from_var,
                        to_var,
                        edge_type,
                        ..
                    } => {
                        assert_eq!(from_var, "a");
                        assert_eq!(to_var, "b");
                        assert_eq!(edge_type, "KNOWS");
                    }
                    _ => panic!("expected second item to be Edge"),
                }

                match &items[2] {
                    CreateItem::Node {
                        variable, labels, ..
                    } => {
                        assert_eq!(variable, &Some("b".into()));
                        assert_eq!(labels, &vec!["Person".to_string()]);
                    }
                    _ => panic!("expected third item to be Node"),
                }
            }
            _ => panic!("expected Create statement"),
        }
    }

    #[test]
    fn test_parse_return_clause() {
        // Test multiple return items including property access, *, and COUNT(*)
        let stmt = parse_cypher("MATCH (n:Person) RETURN n.name").unwrap();
        match stmt {
            CypherStatement::Match { return_clause, .. } => {
                assert_eq!(return_clause.items.len(), 1);
                assert_eq!(
                    return_clause.items[0],
                    ReturnItem::Property("n".into(), "name".into())
                );
            }
            _ => panic!("expected Match"),
        }

        let stmt2 = parse_cypher("MATCH (n) RETURN *").unwrap();
        match stmt2 {
            CypherStatement::Match { return_clause, .. } => {
                assert_eq!(return_clause.items.len(), 1);
                assert_eq!(return_clause.items[0], ReturnItem::All);
            }
            _ => panic!("expected Match"),
        }

        let stmt3 = parse_cypher("MATCH (n) RETURN COUNT(*)").unwrap();
        match stmt3 {
            CypherStatement::Match { return_clause, .. } => {
                assert_eq!(return_clause.items.len(), 1);
                assert_eq!(return_clause.items[0], ReturnItem::Count);
            }
            _ => panic!("expected Match"),
        }
    }

    #[test]
    fn test_parse_error_invalid() {
        // Empty input
        let err = parse_cypher("");
        assert!(err.is_err());

        // Missing RETURN after MATCH
        let err = parse_cypher("MATCH (n)");
        assert!(err.is_err());

        // Invalid token
        let err = parse_cypher("MATCH (n) RETURN @invalid");
        assert!(err.is_err());

        // Unclosed parenthesis
        let err = parse_cypher("MATCH (n RETURN n");
        assert!(err.is_err());

        // Unknown top-level keyword
        let err = parse_cypher("MERGE (n:Foo)");
        assert!(err.is_err());
        match err {
            Err(CypherError::UnknownKeyword(_)) => {}
            _ => panic!("expected UnknownKeyword error"),
        }
    }

    #[test]
    fn test_parse_where_clause() {
        let stmt =
            parse_cypher("MATCH (n:Person) WHERE n.age = 30 RETURN n").unwrap();
        match stmt {
            CypherStatement::Match {
                where_clause: Some(wc),
                ..
            } => {
                assert_eq!(wc.conditions.len(), 1);
                match &wc.conditions[0] {
                    Condition::PropertyEquals {
                        variable,
                        property,
                        value,
                    } => {
                        assert_eq!(variable, "n");
                        assert_eq!(property, "age");
                        assert_eq!(value, &PropValue::Int(30));
                    }
                    _ => panic!("expected PropertyEquals"),
                }
            }
            _ => panic!("expected Match with WHERE"),
        }
    }

    #[test]
    fn test_parse_where_with_and() {
        let stmt = parse_cypher(
            "MATCH (n:Person) WHERE n.age = 30 AND n.name = 'Alice' RETURN n",
        )
        .unwrap();
        match stmt {
            CypherStatement::Match {
                where_clause: Some(wc),
                ..
            } => {
                assert_eq!(wc.conditions.len(), 2);
            }
            _ => panic!("expected Match with WHERE"),
        }
    }

    #[test]
    fn test_parse_delete() {
        let stmt = parse_cypher("DELETE n").unwrap();
        match stmt {
            CypherStatement::Delete { variables } => {
                assert_eq!(variables, vec!["n".to_string()]);
            }
            _ => panic!("expected Delete"),
        }

        let stmt2 = parse_cypher("DELETE n, m").unwrap();
        match stmt2 {
            CypherStatement::Delete { variables } => {
                assert_eq!(
                    variables,
                    vec!["n".to_string(), "m".to_string()]
                );
            }
            _ => panic!("expected Delete"),
        }
    }

    #[test]
    fn test_parse_multiple_labels() {
        let stmt = parse_cypher("MATCH (n:Person:Employee) RETURN n").unwrap();
        match stmt {
            CypherStatement::Match { pattern, .. } => {
                assert_eq!(
                    pattern.nodes[0].labels,
                    vec!["Person".to_string(), "Employee".to_string()]
                );
            }
            _ => panic!("expected Match"),
        }
    }

    #[test]
    fn test_tokenizer_string_values() {
        let stmt = parse_cypher(
            "CREATE (n:Movie {title: 'The Matrix', year: 1999, rating: 8.7})",
        )
        .unwrap();
        match stmt {
            CypherStatement::Create { items } => {
                match &items[0] {
                    CreateItem::Node { properties, .. } => {
                        assert_eq!(
                            properties.get("title"),
                            Some(&PropValue::Text("The Matrix".into()))
                        );
                        assert_eq!(
                            properties.get("year"),
                            Some(&PropValue::Int(1999))
                        );
                        assert_eq!(
                            properties.get("rating"),
                            Some(&PropValue::Float(8.7))
                        );
                    }
                    _ => panic!("expected Node"),
                }
            }
            _ => panic!("expected Create"),
        }
    }

    // ========================================================================
    // Property-based tests (proptest)
    // ========================================================================

    use proptest::prelude::*;

    proptest! {
        #[test]
        fn prop_parse_cypher_never_panics(input in ".{1,200}") {
            // parse_cypher on arbitrary non-empty strings should never panic.
            // It may return Ok or Err, but must not crash.
            let _ = parse_cypher(&input);
        }

        #[test]
        fn prop_parse_cypher_ascii_never_panics(input in "[[:ascii:]]{1,200}") {
            let _ = parse_cypher(&input);
        }

        #[test]
        fn prop_valid_match_always_parses(
            var in "[a-z]{1,5}",
            label in "[A-Z][a-z]{3,8}"
        ) {
            // Filter out labels that happen to be Cypher keywords
            prop_assume!(!is_keyword(&label));
            prop_assume!(!is_keyword(&var));
            let query = format!("MATCH ({var}:{label}) RETURN {var}");
            let result = parse_cypher(&query);
            prop_assert!(result.is_ok(), "valid MATCH query should parse: {:?}", result);
        }

        #[test]
        fn prop_valid_create_always_parses(
            var in "[a-z]{1,5}",
            label in "[A-Z][a-z]{3,8}",
            prop_val in any::<i64>()
        ) {
            prop_assume!(!is_keyword(&label));
            prop_assume!(!is_keyword(&var));
            let query = format!("CREATE ({var}:{label} {{age: {prop_val}}})");
            let result = parse_cypher(&query);
            prop_assert!(result.is_ok(), "valid CREATE query should parse: {:?}", result);
        }
    }
}
