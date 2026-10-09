//! Scratch space for the bake-off (C-S1: "write scratch data under
//! /tmp/nv2-bakeoff and delete it in the test"). Every test and binary puts
//! its stores under [`root`], which is `/tmp/nv2-bakeoff` (a real directory on
//! macOS; `/tmp` is a symlink to `/private/tmp`).

use std::path::PathBuf;

/// The bake-off scratch root, created on demand.
pub fn root() -> PathBuf {
    let root = PathBuf::from("/tmp/nv2-bakeoff");
    let _ = std::fs::create_dir_all(&root);
    root
}
