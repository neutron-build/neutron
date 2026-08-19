# Mojo 1.0 Migration Gap List

Date: 2026-08-19
Scope: everything under `mojo/` (primarily `neutron-mojo/`).
From: `max` **26.2.0.dev2026021605** (nightly, pinned in `neutron-mojo/pixi.lock`; stdlib
vendored at `neutron-mojo/.pixi/envs/default/.../mojo/stdlib/std/`).
To: **Mojo 1.0.0** (released 2026-08-11; GitHub tag `mojo/v1.0.0` == `max/v26.5.0`, commit
`b4497b7c`). The "waiting for 1.0" gate has fired — 1.0 shipped while this package was
parked.

## Method and evidence legend

Every entry below is marked:

- **VERIFIED** — checked against both the pinned stdlib source (on disk) and the actual
  Mojo 1.0.0 stdlib source / official changelogs (`mojo/docs/releases/v0.26.2.md`,
  `v1.0.0b1.md`, `v1.0.0b2.md`, `v1.0.0.md` at tag `mojo/v1.0.0`). The pin predates the
  26.2.0 release, so all four changelogs are in scope.
- **UNVERIFIED** — could not be confirmed without compiling against the 1.0 toolchain.
  No entry in this section should be treated as fact.

The 1.0.0 changelog states most breaking changes ship with a deprecated alias and a
compiler fix-it, so the ordering below is by *volume of code touched*, not per-item
difficulty. Nothing here was compiled: the vendored toolchain is Linux x86-64 (the Feb
validation ran under WSL) and cannot execute on this macOS host.

## Inventory (what was audited)

| Item | Count |
|---|---|
| Source files (`src/neutron_mojo/`) | 112 files, 33,798 lines, 161 structs |
| Tests (`test/`) | 125 files, 38,919 lines |
| Benchmarks (`benchmarks/neutron/`) | 3 files, 208 lines |
| `neutron-mojo-infer/`, `neutron-mojo-python/` | docstring-only stubs, zero code |
| External (stdlib) imports | 173 lines across 12 modules: `math`, `time`, `testing`, `collections`, `memory`, `sys`, `python`, `random`, `pathlib`, `algorithm`, `os`, `ffi` |
| Imports from `max`/GPU packages | none (CPU-first; confirmed by grep) |

All 12 external stdlib modules still exist at 1.0, and every imported *symbol* survives
(`math.abs/sqrt/exp/log/tanh/sin/cos/isnan/isinf`, `time.perf_counter_ns`,
`testing.assert_true/false/equal`, `collections.List/Dict/Optional/Set`,
`memory.UnsafePointer/alloc/memcpy/memset_zero`, `python.Python/PythonObject`,
`random.random_float64`, `pathlib.Path` incl. `read_bytes`, `os.stat`,
`ffi.external_call/c_int`, `algorithm.vectorize`, `sys.size_of/simd_width_of/
bit_width_of/num_physical_cores/argv`). The gaps are in *language constructs* and a
handful of moved/removed APIs, not in the imported symbol set — with the two exceptions
in the "Already broken" section.

## Gap list, ordered by work required

### G1 — `fn` is a hard error: rename every `fn` to `def` — VERIFIED

- **Scale:** 3,375 declarations (1,575 in `src`, 1,800 in `test`) + 4 inline `fn(`
  closures. Touches every file in the package.
- **What changed:** 26.2 deprecated `fn` ("def/fn unification"); b1 made it a warning;
  b2 made it a **compilation error**. `def` now has the old `fn` semantics (non-raising
  by default, explicit `raises`), so a direct rename is semantically neutral — existing
  `raises` annotations carry over unchanged. The 1.0 stdlib contains zero `fn`
  declarations and 556 `def __init__`.
- **Evidence:** v0.26.2 changelog "def/fn unification"; v1.0.0b2 "fn is now an error";
  1.0.0 stdlib mirror: 0 `fn __init__` vs 556 `def __init__`.
- **Migration:** mechanical, sed-able (`^\s*fn ` → `def `). Do it first; nothing compiles
  before this lands.

### G2 — stdlib imports must be `std.`-qualified — VERIFIED

- **Scale:** 173 import lines (170 in `neutron-mojo`, 3 in `benchmarks/neutron/`).
- **What changed:** b2: "Implicit `std` imports are now an error... Imports from the
  standard library must now be fully qualified."
- **Evidence:** v1.0.0b2 changelog; 1.0 stdlib itself imports as
  `from std.utils.numerics import ...` (1.0 `math/__init__.mojo:29`); b1 changelog
  example `from std.os.atomic import Atomic, Consistency, fence`.
- **Migration:** `from math import abs` → `from std.math import abs`, for all 12 modules.
  Mechanical; combine with G1 in the same pass.

### G3 — init unification: `__copyinit__`/`__moveinit__` removed — VERIFIED

- **Scale:** 74 of 112 src files carry copy/move boilerplate (82 `__copyinit__` lines in
  53 files, 123 `__moveinit__` lines in 65 files).
- **What changed:** 26.2 renamed both to `__init__` overloads with keyword-only
  arguments (legacy names temporarily accepted); b1 stopped auto-rewriting the legacy
  names — they now fail to compile. At 1.0 the trait requirements are:
  - `Copyable`: `def __init__(out self, *, copy: Self)` (`std/traits/copyable.mojo:57`)
  - `Movable`: `def __init__(out self, *, deinit move: Self)`
    (`std/traits/movable.mojo:51`; 26.2 docs called the keyword `take`, 1.0 trait
    spells it `deinit move` — implement to the 1.0 trait)
- **Evidence:** v0.26.2 "Init unification"; v1.0.0b1 "Removed" section; 1.0 stdlib has
  zero occurrences of either legacy name (pin had 63 `__copyinit__`).
- **Migration:** mostly *deletion*, not rewriting — 1.0 synthesizes field-wise copy/move
  constructors when the user provides no definition (see `ImplicitlyCopyable` docstring,
  `std/traits/copyable.mojo:118-134`, using `@fieldwise_init`). Most of the 74 files
  hand-copy fields one by one (e.g. `quant/types.mojo:120-125`, `serve/http.mojo:33-43`)
  and can drop the methods entirely. Only types with genuinely custom ownership
  (reference-counted `tensor/storage.mojo`, mmap-holding `io/binary_reader.mojo`) need
  explicit constructors.

### G4 — `Writable.write_to` signature is now `def write_to(self, Some[Writer])` — VERIFIED

- **Scale:** 20 `write_to` definitions across 15 files (tensor, quant, fusion, dlpack,
  io, model).
- **What changed / was already wrong:** the package overrides
  `fn write_to[W: Writer](self, mut writer: W)` (e.g. `tensor/shape.mojo:145`,
  `quant/types.mojo:76`). The trait has required
  `write_to(self, mut writer: Some[Writer])` since *before* the pin
  (`format/__init__.mojo:182` at pin) — the generic-parameter form does not override
  the trait requirement, so these structs were relying on the reflection-based default,
  not their hand-written methods. At 1.0 the required spelling is
  `def write_to(self, mut writer: Some[Writer])` (`std/format/__init__.mojo:173`).
- **Migration:** rewrite each to the concrete signature (or delete them: 1.0's
  reflection-based default prints `TypeName(field=value, ...)` with zero code).
- **Evidence:** 1.0 `std/format/__init__.mojo:143-197`; pin
  `format/__init__.mojo:152,182`.

### G5 — `UnsafePointer` is non-null by design — VERIFIED

- **Scale:** 8 null-construction sites in 3 files; `UnsafePointer[...]` appears 49 times
  total.
- **What changed:** b1 removed the default null constructor, `__bool__`, `Defaultable`
  and `Boolable` conformance. Nullability must be expressed as
  `Optional[UnsafePointer[...]]` (niche-optimized, zero-overhead, FFI-safe). At 1.0
  `Pointer`/`UnsafePointer` were additionally unified; `UnsafePointer` survives as a
  `comptime` type alias (`std/memory/unsafe_pointer.mojo:152`), origin spellings like
  `MutExternalOrigin` still exist (`std/origin/__init__.mojo:118`).
- **Sites:** `dlpack/dlpack.mojo:172,176,177,243,244`; `dlpack/exchange.mojo:46`;
  `io/binary_reader.mojo:75,88`.
- **Evidence:** v1.0.0b1 highlights "UnsafePointer is non-null by design"; v1.0.0
  "Pointer and UnsafePointer are unified".

### G6 — `algorithm.parallelize` moved to the `max` package — VERIFIED

- **Scale:** 1 import + 2 call sites.
- **What changed:** `std.algorithm` at 1.0 contains only `map`, `tile*`, `unswitch`,
  `vectorize` (checked exhaustively; `algorithm/functional.mojo` is a 42-line re-export
  shim). `parallelize`, `parallelize_over_rows`, `sync_parallelize` now live in
  **`max.algorithm`** (`max/mojo/max/algorithm/__init__.mojo:29`; confirmed by
  `std/runtime/asyncrt.mojo:142` importing it from there).
- **Sites:** `tensor/simd_math.mojo:23` (import), `:401`, `:769` (calls, with
  `num_physical_cores()` — which still exists).
- **Migration:** `from algorithm import vectorize` (std) +
  `from max.algorithm import parallelize` (max). Adds an explicit dependency on the max
  package, which the pixi workspace already provides.

### G7 — `alias` → `comptime`: 13 leftover declarations — VERIFIED

- **Scale:** 13 lines in 2 files.
- **What changed:** the `alias`→`comptime` rename completed; the 1.0 stdlib contains
  zero `alias` declarations (506 `comptime` at the pin already, `comptime` only at 1.0).
- **Sites:** `autograd/backward.mojo:29,150,176,214,277,311,347,436`;
  `autograd/ops.mojo:31,48,102,160,279`.
- **Migration:** rename. The other 132 `comptime` uses in the package are already in
  the modern spelling.

### G8 — `__del__` → `__deinit__` — VERIFIED

- **Scale:** 2 sites.
- **What changed:** 1.0: "`ImplicitlyDestructible` becomes `Deinitable` with the
  destructor spelled `__deinit__()`".
- **Sites:** `io/binary_reader.mojo:119` (munmap), `tensor/storage.mojo:85` (dealloc).
- **Evidence:** v1.0.0 highlights "One name, and one type, per concept".

### G9 — `@parameter if` → `comptime if` — VERIFIED

- **Scale:** 2 sites.
- **Sites:** `tensor/simd_math.mojo:385,753` (`@parameter` blocks; a third match at
  `tensor/ops.mojo:378` is inside a comment).
- **Evidence:** v0.26.2 "`comptime if` and `comptime for`... The `@parameter` forms will
  be deprecated soon"; replaced idiom is `comptime if`.

### G10 — `constrained[...]` no longer public — VERIFIED

- **Scale:** 1 site.
- **Site:** `tensor/dim.mojo:32` — `constrained[S > 0, "Static dimension size must be
  positive"]()`.
- **What changed:** at 1.0 `builtin/constrained.mojo` (114 lines) contains only the
  private `_constrained_conforms_to` helper — no public `constrained`. 26.2 finalized
  `comptime assert` as the replacement.
- **Migration:** `comptime assert S > 0, "Static dimension size must be positive"`.

### G11 — negative indexing removed from all collections — VERIFIED

- **Scale:** 3 sites in 1 test file.
- **What changed:** b1 removed negative indexing; `x[-1]` is a compile-time error.
- **Sites:** `test/test_shape.mojo:46-48` (`s[-1]`, `s[-2]`, `s[-3]` — these call
  neutron's own `Shape.__getitem__`; if it forwards to `List` indexing it breaks, if it
  handles negatives itself it can keep doing so).
- **Evidence:** v1.0.0b1 "Negative indexing has been removed from all stdlib
  collections".

### G12 — build config: nightly `max` → stable `mojo` — VERIFIED (channel/package), UNVERIFIED (this workspace's pixi behavior)

- **Sites:** `neutron-mojo/mojoproject.toml` (`max = ">=25.1"`, channel
  `conda.modular.com/max-nightly/`), `pixi.lock`, `scripts/validate-core.sh` (resolves
  `mojo` binary from `.pixi` or `PATH`), README claims "tracks the `max` nightly channel
  (`max >= 25.1`)".
- **What changed:** 1.0 ships as conda package **`mojo`** (version `1.0.0`) from the
  stable channel — Modular's own 1.0 workspace uses `channels = ["conda-forge",
  "https://conda.modular.com/max/"]` with `mojo = "*"` (`mojo/pixi.toml` at tag
  `mojo/v1.0.0`). Also: `mojo package` is now `mojo precompile` and `.mojopkg` is
  deprecated in favor of `.mojoc` (b2) — affects packaging steps if any are added.
- **Migration:** switch dependency to `mojo = ">=1.0"` on the stable channel, re-solve
  the lock (on Linux or macOS-arm64 — a macOS toolchain is finally possible since 1.0
  lists `osx-arm64`), re-run `scripts/validate-core.sh`.

## Already broken — at the pin *and* at 1.0 (not migration gaps, but they block it)

These are the most important findings of the audit: the code as committed does not fully
compile even against its own pinned toolchain, so the Feb 20 "125/125 passed" report
cannot describe HEAD. Both failures sit in code paths the validation harness never
compiled.

### P1 — `from sys import bitwidthof` never existed — VERIFIED

- **Site:** `quant/types.mojo:16`. The import is unused in the file, but imports must
  resolve regardless.
- **Evidence:** the pinned `sys/__init__.mojo` exports `bit_width_of` (line 33) and
  never `bitwidthof`; grep of the entire pinned and 1.0 stdlib sources finds no
  `bitwidthof` symbol anywhere. (`tensor/dtype.mojo:12` correctly imports
  `bit_width_of`.)
- **Blast radius:** everything importing `quant/types.mojo` fails: `quant/__init__.mojo`,
  the four quant codecs, `nn/mixed_quant.mojo`, `nn/__init__.mojo`, and at least 7 tests
  (`test_quant_types`, `test_quant_integration`, `test_mixed_pipeline`,
  `test_multi_arch`, `test_mixed_quant`, and transitively anything importing
  `neutron_mojo.nn.*` if the package `__init__` pulls `mixed_quant`).
- **Fix:** delete the line.

### P2 — `from time import now` never existed — VERIFIED

- **Sites:** `benchmarks/neutron/bench_matmul.mojo:14`, `bench_elementwise.mojo:14`,
  `bench_softmax.mojo:16` (8 `now()` calls total).
- **Evidence:** pinned `time/time.mojo` exports `perf_counter`, `perf_counter_ns`,
  `monotonic`, `sleep` — no `now`. Same at 1.0.
- **Blast radius:** all three benchmark harnesses. The validation harness only runs
  `test/test_*.mojo`, so this was never caught.
- **Fix:** `from std.time import perf_counter_ns` (note: returns `Int` at 1.0, was
  `UInt` at the pin — `nn/profiler.mojo` already wraps it in `Int(...)`, which stays
  valid).

## Semantic / behavioral changes needing an audit pass (compile-and-see)

- **B1 — implicit `Int` → SIMD-scalar conversions deprecated (26.2).** Tensor kernels
  passing integer literals where `Float32`/`Int8` scalars are expected will warn, then
  break. Modular ships `mojo build --experimental-fixit` to assist. VERIFIED that the
  deprecation exists; UNVERIFIED how many sites in this package trip it.
- **B2 — bounds checking on by default for all CPU collections (b1).** Correctness win,
  potential perf regression in hot loops (`-D ASSERT=none` to disable). VERIFIED change;
  UNVERIFIED impact.
- **B3 — collections accept move-only elements (b2); `List`/`Dict` gained owned
  iteration (`IterableOwned`), `Optional` gained `map`/`and_then`.** Relaxations — no
  action needed, listed because they may let G3 delete more boilerplate than expected.
- **B4 — `range()` overloads with mixed/`Intable` args removed (b1).** The package's
  `range(` calls pass `Int` — expected fine, spot-check after G1.
- **B5 — var-less declarations deprecated, `self` must be typed `Self`, list literals
  build `Array` not `List` (1.0).** Grep found no var-less declarations and no list
  literals in src; expected clean.

## UNVERIFIED items (could not check without a 1.0 toolchain)

- **U1 — the 4 inline `fn(` closure literals** (in `tensor/`, `autograd/`): closure
  syntax was reworked across 26.2–1.0 (unified capture lists, `lambda` expressions).
  Expected to need `def(` or `lambda`, not verified.
- **U2 — `python/` bridge files (5 files: `bridge`, `hf`, `hf_pipeline`, `torch_bridge`,
  `__init__`).** `Python`, `PythonObject`, and `Python.import_module` all survive at 1.0
  (verified), and Python interop got *faster* in b2, but the fine-grained
  `PythonObject` method surface those files use was not itemized.
- **U3 — `[tool.mojo]` section in `mojoproject.toml`** (`src`/`test` keys): the 1.0-era
  mojo-pixi integration was not verified to still read it.
- **U4 — `UnsafePointer` as a deprecated alias of unified `Pointer`:** exists at 1.0,
  expected to compile possibly with warnings; whether it warns was not verified.
- **U5 — `serve/http.mojo`'s embedded Python `http.server` transport string** — Python
  side, out of Mojo-migration scope, noted only because it embeds Python source as a
  Mojo string literal.

## Reference-material drift (no code impact today)

`study/max_*.md` document MAX kernel APIs (`Layout`, `TensorCore`, `LayoutTensor`,
`NDBuffer`) from the pre-1.0 era. b1 **removed `NDBuffer`** (migrate to `TileTensor`)
and b2 added `TileTensor` guides; the study notes predate this and will mislead a future
GPU effort if used as-is. None of these APIs are imported by current code.

## Suggested migration order

1. Fix P1 and P2 (delete/rename dead imports) — unblocks everything, valid at pin and 1.0.
2. G1 + G2 in one mechanical pass (`fn`→`def`, `std.`-qualify 173 imports).
3. G3 (delete/rewrite copy-move boilerplate), G4 (`write_to`), G5 (nullable pointers),
   G7, G8, G9, G10, G11 — all small and local.
4. G6 (`parallelize` from `max.algorithm`) + G12 (toolchain switch), then compile and
   run `scripts/validate-core.sh` against 1.0.
5. Work the B1 fix-it pass; re-run benchmarks.
6. Re-verify UNVERIFIED items against compiler diagnostics.

With this list, the migration is plausibly 1–2 focused days: ~90% of the line count is
two mechanical renames (G1, G2), and the copy-move boilerplate (G3) is mostly deletion.
