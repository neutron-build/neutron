# Why this (empty) directory exists

`tests/g2x/mod.rs` declares the G2 checker as an **inline** module:

```rust
pub mod g2 {
    #[path = "../../../../nucleus-txn/tests/g2/history.rs"]
    pub mod history;
    #[path = "../../../../nucleus-txn/tests/g2/check.rs"]
    pub mod check;
}
```

For a `mod` item nested inside an inline module, rustc resolves a
`#[path]` attribute relative to the inline module's own directory — this
directory, `tests/g2x/g2/` — not relative to the file that declares it.
The relative paths then climb four levels (`g2x` → `tests` →
`nucleus-catalog` → `crates`) to reach `nucleus-txn`'s test tree, where
the checker lives and is compiled from verbatim (never copied, never
edited — card C-T6g's "No copy of the checker").

The catch that motivates this file: path resolution walks the **base
directory first**, and `..` only traverses directories that exist. A
base directory that is itself purely notional (`g2/` is an inline
module, so nothing would ever create it) makes *every* relative `#[path]`
— climbing ones included — fail with "couldn't find file", long before
the target is reached. So the notional directory is real: this README is
its only content, and the git checkout recreates it for every build.
