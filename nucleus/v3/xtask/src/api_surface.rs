// Provenance (card A-1a): nucleus/v2/xtask/src/api_surface.rs at 187cb226a9bf57ab2a7ba6b2b90fb28cdaf27c6b.
//! API-surface lint (PLAN-v3 §6.1): standard-tier code may use only allowlisted
//! public items of the core crates nucleus-store, nucleus-env, nucleus-am and nucleus-txn.
//!
//! Syntactic: every `nucleus_store::…` / `nucleus_txn::…` path and `use` tree is
//! expanded and must fall under an allowlist entry (an entry allows itself and
//! everything below it). Aliasing a crate root is rejected because the alias
//! would hide later paths. Comments and literals are skipped. Which files are
//! in scope is decided by `scripts/check-api-surface.sh`.

use std::process::ExitCode;

const CRATES: [&str; 4] = ["nucleus_store", "nucleus_env", "nucleus_am", "nucleus_txn"];

pub fn run(args: &[String]) -> ExitCode {
    let Some((allow_path, files)) = args.split_first() else {
        eprintln!("usage: xtask api-surface ALLOWLIST FILE...");
        return ExitCode::from(2);
    };
    let allow = match std::fs::read_to_string(allow_path) {
        Ok(s) => parse_allowlist(&s),
        Err(e) => {
            eprintln!("api-surface: {allow_path}: {e}");
            return ExitCode::from(2);
        }
    };
    let mut bad = 0usize;
    for f in files {
        let src = match std::fs::read_to_string(f) {
            Ok(s) => s,
            Err(e) => {
                eprintln!("api-surface: {f}: {e}");
                return ExitCode::from(2);
            }
        };
        for v in check(&src, &allow) {
            println!("{f}:{}: {}", v.line, v.msg);
            bad += 1;
        }
    }
    if bad > 0 {
        eprintln!(
            "api-surface: {bad} violation(s). Use an allowlisted item or move the work to a premium card ({allow_path})."
        );
        return ExitCode::FAILURE;
    }
    println!("api-surface: {} file(s) clean", files.len());
    ExitCode::SUCCESS
}

/// One path per line; `#` comments; a trailing `::*` is the same as the bare path.
pub fn parse_allowlist(s: &str) -> Vec<String> {
    s.lines()
        .map(|l| l.split('#').next().unwrap_or_default().trim())
        .filter(|l| !l.is_empty())
        .map(|l| l.strip_suffix("::*").unwrap_or(l).to_string())
        .collect()
}

fn allowed(path: &str, allow: &[String]) -> bool {
    allow.iter().any(|a| {
        path.strip_prefix(a.as_str())
            .is_some_and(|r| r.is_empty() || r.starts_with("::"))
    })
}

#[derive(Debug, PartialEq, Eq)]
pub struct Violation {
    pub line: usize,
    pub msg: String,
}

pub fn check(src: &str, allow: &[String]) -> Vec<Violation> {
    let t = lex(src);
    let mut out = Vec::new();
    for (i, tok) in t.iter().enumerate() {
        let Tok::Ident(name) = &tok.tok else { continue };
        if !CRATES.contains(&name.as_str()) {
            continue;
        }
        // `m::nucleus_store` is checked like the crate: `use nucleus_store;` makes
        // `crate::nucleus_store::…` resolve, so a prefix must not exempt a path.
        match t.get(i + 1).map(|x| &x.tok) {
            Some(Tok::Ident(a)) if a == "as" => out.push(Violation {
                line: tok.line,
                msg: format!("aliasing `{name}` is not allowed (it hides paths from this lint)"),
            }),
            Some(Tok::Colons) => {
                let mut paths = Vec::new();
                tree(&t, i + 2, name, &mut paths);
                for (p, line) in paths {
                    if !allowed(&p, allow) {
                        out.push(Violation {
                            line,
                            msg: format!("`{p}` is not in the API allowlist"),
                        });
                    }
                }
            }
            _ => {}
        }
    }
    out
}

/// Expands the path or use tree at `i` under `prefix`; returns the next index.
fn tree(t: &[Token], i: usize, prefix: &str, out: &mut Vec<(String, usize)>) -> usize {
    let Some(tok) = t.get(i) else { return i };
    match &tok.tok {
        Tok::Star => {
            out.push((format!("{prefix}::*"), tok.line));
            i + 1
        }
        Tok::Open => {
            let mut j = i + 1;
            loop {
                match t.get(j).map(|x| &x.tok) {
                    None => return j,
                    Some(Tok::Close) => return j + 1,
                    Some(Tok::Ident(s)) if s == "self" => {
                        out.push((prefix.to_string(), t[j].line));
                        j = skip_alias(t, j + 1);
                    }
                    Some(_) => {
                        let k = tree(t, j, prefix, out);
                        j = if k == j { j + 1 } else { k };
                    }
                }
            }
        }
        Tok::Ident(s) => {
            let path = format!("{prefix}::{s}");
            let more = t.get(i + 1).map(|x| &x.tok) == Some(&Tok::Colons)
                && matches!(
                    t.get(i + 2).map(|x| &x.tok),
                    Some(Tok::Ident(_) | Tok::Open | Tok::Star)
                );
            if more {
                tree(t, i + 2, &path, out)
            } else {
                out.push((path, tok.line));
                skip_alias(t, i + 1)
            }
        }
        _ => i,
    }
}

fn skip_alias(t: &[Token], i: usize) -> usize {
    match t.get(i).map(|x| &x.tok) {
        Some(Tok::Ident(a)) if a == "as" => i + 2,
        _ => i,
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Tok {
    Ident(String),
    Colons,
    Open,
    Close,
    Star,
    Other,
}

#[derive(Debug)]
struct Token {
    tok: Tok,
    line: usize,
}

/// Just enough of a Rust lexer to find paths: comments, string/char literals
/// (incl. raw and byte forms), lifetimes and raw identifiers are handled.
fn lex(src: &str) -> Vec<Token> {
    let c: Vec<char> = src.chars().collect();
    let mut out = Vec::new();
    let mut line = 1;
    let mut i = 0;
    while i < c.len() {
        let ch = c[i];
        let next = c.get(i + 1).copied();
        let tok = match ch {
            '\n' => {
                line += 1;
                i += 1;
                continue;
            }
            _ if ch.is_whitespace() => {
                i += 1;
                continue;
            }
            '/' if next == Some('/') => {
                while i < c.len() && c[i] != '\n' {
                    i += 1;
                }
                continue;
            }
            '/' if next == Some('*') => {
                i = block_comment(&c, i + 2, &mut line);
                continue;
            }
            '"' => {
                i = string(&c, i + 1, &mut line);
                Tok::Other
            }
            '\'' => {
                i = quote(&c, i);
                Tok::Other
            }
            ':' if next == Some(':') => {
                i += 2;
                Tok::Colons
            }
            '{' | '}' | '*' => {
                i += 1;
                match ch {
                    '{' => Tok::Open,
                    '}' => Tok::Close,
                    _ => Tok::Star,
                }
            }
            _ if ch.is_ascii_digit() => {
                while i < c.len() && (c[i] == '_' || c[i].is_alphanumeric()) {
                    i += 1;
                }
                Tok::Other
            }
            _ if ch == '_' || ch.is_alphabetic() => {
                let start = i;
                while i < c.len() && (c[i] == '_' || c[i].is_alphanumeric()) {
                    i += 1;
                }
                let word: String = c[start..i].iter().collect();
                match (word.as_str(), c.get(i)) {
                    ("b" | "c", Some('"')) => {
                        i = string(&c, i + 1, &mut line);
                        Tok::Other
                    }
                    ("r" | "br" | "cr", Some('"' | '#')) => {
                        let mut j = i;
                        while c.get(j) == Some(&'#') {
                            j += 1;
                        }
                        if c.get(j) == Some(&'"') {
                            i = raw_string(&c, j + 1, j - i, &mut line);
                            Tok::Other
                        } else if word == "r" && j - i == 1 {
                            // Raw identifier `r#name`: lex `name` next.
                            i = j;
                            continue;
                        } else {
                            Tok::Ident(word)
                        }
                    }
                    _ => Tok::Ident(word),
                }
            }
            _ => {
                i += 1;
                Tok::Other
            }
        };
        out.push(Token { tok, line });
    }
    out
}

fn block_comment(c: &[char], mut i: usize, line: &mut usize) -> usize {
    let mut depth = 1;
    while i < c.len() && depth > 0 {
        match (c[i], c.get(i + 1)) {
            ('/', Some('*')) => {
                depth += 1;
                i += 2;
            }
            ('*', Some('/')) => {
                depth -= 1;
                i += 2;
            }
            ('\n', _) => {
                *line += 1;
                i += 1;
            }
            _ => i += 1,
        }
    }
    i
}

/// From just after the opening quote; returns the index after the closing one.
fn string(c: &[char], mut i: usize, line: &mut usize) -> usize {
    while i < c.len() {
        match c[i] {
            '\\' => {
                if c.get(i + 1) == Some(&'\n') {
                    *line += 1;
                }
                i += 2;
            }
            '"' => return i + 1,
            '\n' => {
                *line += 1;
                i += 1;
            }
            _ => i += 1,
        }
    }
    i
}

fn raw_string(c: &[char], mut i: usize, hashes: usize, line: &mut usize) -> usize {
    while i < c.len() {
        if c[i] == '"' && (1..=hashes).all(|k| c.get(i + k) == Some(&'#')) {
            return i + 1 + hashes;
        }
        if c[i] == '\n' {
            *line += 1;
        }
        i += 1;
    }
    i
}

/// At a `'`: skips a char literal, or just the quote of a lifetime or label.
fn quote(c: &[char], i: usize) -> usize {
    if c.get(i + 1) == Some(&'\\') {
        let mut j = i + 3;
        while j < c.len() && c[j] != '\'' {
            j += 1;
        }
        return j + 1;
    }
    if c.get(i + 2) == Some(&'\'') {
        return i + 3;
    }
    i + 1
}

#[cfg(test)]
mod tests {
    use super::*;

    fn allow() -> Vec<String> {
        parse_allowlist(
            "# c\nnucleus_txn::Ts\nnucleus_store::KvError  # trailing\nnucleus_txn::visibility::Read::*\n",
        )
    }

    fn paths(src: &str) -> Vec<String> {
        check(src, &allow())
            .into_iter()
            .map(|v| v.msg.split('`').nth(1).unwrap_or_default().to_string())
            .collect()
    }

    #[test]
    fn allowlist_parsing_and_prefix_rule() {
        let a = allow();
        assert_eq!(
            a,
            [
                "nucleus_txn::Ts",
                "nucleus_store::KvError",
                "nucleus_txn::visibility::Read"
            ]
        );
        assert!(allowed("nucleus_txn::Ts", &a));
        assert!(allowed("nucleus_txn::Ts::ZERO", &a));
        assert!(!allowed("nucleus_txn::TsX", &a));
        assert!(!allowed("nucleus_txn::visibility", &a));
        assert!(!allowed("nucleus_txn::*", &a));
    }

    #[test]
    fn qualified_paths() {
        let src = "fn f() -> nucleus_store::Result<()> { let t = nucleus_txn::Ts::ZERO; nucleus_store::Batch::default(); }";
        assert_eq!(
            paths(src),
            ["nucleus_store::Result", "nucleus_store::Batch::default"]
        );
    }

    #[test]
    fn use_trees_expand() {
        let src = "use nucleus_txn::{Ts, visibility::{Read, read as r, self}, Intent as I};\n\
                   use nucleus_store::{KvError, Op::*};";
        assert_eq!(
            paths(src),
            [
                "nucleus_txn::visibility::read",
                "nucleus_txn::visibility",
                "nucleus_txn::Intent",
                "nucleus_store::Op::*"
            ]
        );
    }

    #[test]
    fn globs_need_the_parent_allowlisted() {
        assert_eq!(paths("use nucleus_txn::*;"), ["nucleus_txn::*"]);
        assert!(paths("use nucleus_txn::visibility::Read::*;").is_empty());
    }

    #[test]
    fn crate_aliases_rejected() {
        let src = "use nucleus_store as kv;\nextern crate nucleus_txn as t;\nuse nucleus_txn::{self as tx};";
        let v = check(src, &allow());
        assert_eq!(v.len(), 3);
        assert_eq!(v.iter().map(|v| v.line).collect::<Vec<_>>(), [1, 2, 3]);
        assert!(v[0].msg.contains("aliasing"));
        assert_eq!(
            paths("use nucleus_store;\nextern crate nucleus_txn;"),
            Vec::<String>::new()
        );
    }

    #[test]
    fn leading_colons_and_reexported_roots() {
        assert_eq!(
            paths("let x = ::nucleus_store::Op::Delete(k);"),
            ["nucleus_store::Op::Delete"]
        );
        assert_eq!(
            paths("use ::nucleus_store::Batch;"),
            ["nucleus_store::Batch"]
        );
        // `pub use nucleus_store;` in `shim` must not launder later paths.
        assert_eq!(
            paths(
                "mod shim { pub use nucleus_store; }\nuse crate::shim::nucleus_store::Batch;\nself::nucleus_store::Op; super::nucleus_txn::Ts;"
            ),
            ["nucleus_store::Batch", "nucleus_store::Op"]
        );
        assert_eq!(
            paths("use crate::{nucleus_store::Snapshot};"),
            ["nucleus_store::Snapshot"]
        );
        assert_eq!(check("use crate::nucleus_store as k;", &allow()).len(), 1);
    }

    #[test]
    fn comments_strings_lifetimes_skipped() {
        let src = r####"
            // nucleus_store::Batch
            /* nucleus_store::Op /* nested */ nucleus_store::Op */
            const S: &str = "nucleus_store::Op \" nucleus_store::Op";
            const R: &str = r#"nucleus_store::Op " nucleus_store::Op"#;
            const B: &[u8] = br##"nucleus_store::Op"##;
            fn f<'a>(x: &'a str) -> char { let q = '"'; let e = '\''; let b = b'{'; 'l: loop { break 'l; } }
            fn r#match() { nucleus_store::Snapshot; }
        "####;
        let v = check(src, &allow());
        assert_eq!(v.len(), 1);
        assert_eq!(v[0].line, 8);
        assert!(v[0].msg.contains("nucleus_store::Snapshot"));
    }

    #[test]
    fn turbofish_stops_the_path() {
        assert_eq!(
            paths("nucleus_store::Op::<u8>::new()"),
            ["nucleus_store::Op"]
        );
        assert!(paths("nucleus_txn::Ts::<>::default").is_empty());
    }
}
