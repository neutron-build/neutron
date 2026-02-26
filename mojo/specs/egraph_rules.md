# E-Graph Rewrite Rule Catalog

Pre-1.0 reference document. Complete catalog of e-graph rewrite rules for tensor/arithmetic expression optimization.

E-graphs (equality graphs) compactly represent equivalence classes of expressions. Rewrite rules are applied to discover equivalent forms, and a cost function extracts the optimal expression. This catalog defines the full rule set organized by category, with priorities based on empirical frequency in tensor computation workloads.

---

## Rule Index

| # | Name | Category | Phase | Priority |
|---|------|----------|-------|----------|
| 1 | Add Identity | Identity | 1 | High |
| 2 | Mul Identity | Identity | 1 | High |
| 3 | Mul Zero | Collapse | 1 | High |
| 4 | Double Negation | Cancellation | 1 | High |
| 5 | Sub Self | Cancellation | 1 | High |
| 6 | Div Self | Cancellation | 1 | Medium |
| 7 | Add Commutativity | Commutativity | 2 | High |
| 8 | Mul Commutativity | Commutativity | 2 | High |
| 9 | Add Associativity | Associativity | 2 | Medium |
| 10 | Mul Associativity | Associativity | 2 | Medium |
| 11 | Distribute Mul over Add | Distribution | 2 | Medium |
| 12 | Factor Mul over Add | Distribution | 2 | Medium |
| 13 | Neg as Mul | Strength Reduction | 1 | High |
| 14 | Sub as Add Neg | Strength Reduction | 1 | High |
| 15 | Div as Mul Recip | Strength Reduction | 1 | Medium |
| 16 | Mul by Power of 2 | Strength Reduction | 1 | Medium |
| 17 | Div by Power of 2 | Strength Reduction | 1 | Medium |
| 18 | Square via Mul | Strength Reduction | 1 | Low |
| 19 | Idempotent And | Idempotence | 1 | Medium |
| 20 | Idempotent Or | Idempotence | 1 | Medium |
| 21 | Idempotent Min | Idempotence | 1 | Medium |
| 22 | Idempotent Max | Idempotence | 1 | Medium |
| 23 | Exp-Log Inverse | Inverse | 1 | Medium |
| 24 | Log-Exp Inverse | Inverse | 1 | Medium |
| 25 | Sqrt-Square Inverse | Inverse | 1 | Low |
| 26 | Transpose Involution | Cancellation | 1 | High |
| 27 | MatMul Associativity | Associativity | 2 | Medium |
| 28 | Transpose of MatMul | Distribution | 2 | Medium |
| 29 | Add Broadcast Fusion | Fusion | 2 | Medium |
| 30 | Consecutive Reshape | Fusion | 2 | High |
| 31 | Reshape-Transpose Swap | Fusion | 2 | Low |
| 32 | Dead Code Elimination | Dead Code | 1 | High |
| 33 | Constant Folding | Collapse | 1 | High |
| 34 | Fused Multiply-Add | Fusion | 2 | High |
| 35 | ReLU Simplification | Collapse | 1 | Medium |
| 36 | Exp Product Fusion | Fusion | 2 | Low |
| 37 | Log Quotient Fission | Distribution | 2 | Low |
| 38 | Sum Factorization | Distribution | 2 | Low |

---

## Rule Definitions

### Rule 1: Add Identity

- **Pattern**: `(add ?x 0)` --> `?x`
- **Category**: Identity
- **Phase**: 1 (algebraic simplification)
- **Priority**: High -- triggers on nearly every model graph due to bias initialization
- **Conditions**: None
- **Proof**: Additive identity axiom: `a + 0 = a` for all `a` in any ring.

---

### Rule 2: Mul Identity

- **Pattern**: `(mul ?x 1)` --> `?x`
- **Category**: Identity
- **Phase**: 1 (algebraic simplification)
- **Priority**: High -- common after constant propagation and normalization
- **Conditions**: None
- **Proof**: Multiplicative identity axiom: `a * 1 = a` for all `a` in any ring.

---

### Rule 3: Mul Zero

- **Pattern**: `(mul ?x 0)` --> `0`
- **Category**: Collapse
- **Phase**: 1 (algebraic simplification)
- **Priority**: High -- eliminates entire subgraphs
- **Conditions**: `?x` must not produce side effects (no NaN propagation required)
- **Proof**: Zero property of multiplication: `a * 0 = 0`. Note: IEEE 754 caveat -- `NaN * 0 = NaN`, so this rule is only valid under fast-math semantics or when `?x` is known finite.

---

### Rule 4: Double Negation

- **Pattern**: `(neg (neg ?x))` --> `?x`
- **Category**: Cancellation
- **Phase**: 1 (algebraic simplification)
- **Priority**: High -- frequently produced by differentiation passes
- **Conditions**: None
- **Proof**: Involution of additive inverse: `-(-a) = a`.

---

### Rule 5: Sub Self

- **Pattern**: `(sub ?x ?x)` --> `0`
- **Category**: Cancellation
- **Phase**: 1 (algebraic simplification)
- **Priority**: High -- appears in residual connection gradients
- **Conditions**: `?x` must be deterministic (same value both occurrences)
- **Proof**: Additive inverse: `a - a = 0`.

---

### Rule 6: Div Self

- **Pattern**: `(div ?x ?x)` --> `1`
- **Category**: Cancellation
- **Phase**: 1 (algebraic simplification)
- **Priority**: Medium -- appears in normalization layers
- **Conditions**: `?x != 0` and `?x` is deterministic
- **Proof**: Multiplicative inverse: `a / a = 1` for `a != 0`.

---

### Rule 7: Add Commutativity

- **Pattern**: `(add ?x ?y)` --> `(add ?y ?x)`
- **Category**: Commutativity
- **Phase**: 2 (equality saturation)
- **Priority**: High -- enables other rules by canonicalizing operand order
- **Conditions**: None
- **Proof**: Commutativity of addition: `a + b = b + a`.
- **Note**: In practice, a canonical ordering (e.g., by e-class id) prevents infinite loops.

---

### Rule 8: Mul Commutativity

- **Pattern**: `(mul ?x ?y)` --> `(mul ?y ?x)`
- **Category**: Commutativity
- **Phase**: 2 (equality saturation)
- **Priority**: High -- enables constant hoisting and other matches
- **Conditions**: None
- **Proof**: Commutativity of multiplication over commutative rings: `a * b = b * a`.

---

### Rule 9: Add Associativity

- **Pattern**: `(add (add ?x ?y) ?z)` --> `(add ?x (add ?y ?z))`
- **Category**: Associativity
- **Phase**: 2 (equality saturation)
- **Priority**: Medium -- enables constant folding across expression trees
- **Conditions**: None
- **Proof**: Associativity of addition: `(a + b) + c = a + (b + c)`.
- **Note**: Floating-point addition is not truly associative; this rule is valid under fast-math or when exact reassociation is acceptable.

---

### Rule 10: Mul Associativity

- **Pattern**: `(mul (mul ?x ?y) ?z)` --> `(mul ?x (mul ?y ?z))`
- **Category**: Associativity
- **Phase**: 2 (equality saturation)
- **Priority**: Medium -- enables strength reduction when constants group together
- **Conditions**: None
- **Proof**: Associativity of multiplication: `(a * b) * c = a * (b * c)`.

---

### Rule 11: Distribute Mul over Add

- **Pattern**: `(mul ?a (add ?b ?c))` --> `(add (mul ?a ?b) (mul ?a ?c))`
- **Category**: Distribution
- **Phase**: 2 (equality saturation)
- **Priority**: Medium -- opens opportunities for common subexpression elimination
- **Conditions**: None
- **Proof**: Left distributive law: `a * (b + c) = a*b + a*c`.

---

### Rule 12: Factor Mul over Add

- **Pattern**: `(add (mul ?a ?b) (mul ?a ?c))` --> `(mul ?a (add ?b ?c))`
- **Category**: Distribution (reverse)
- **Phase**: 2 (equality saturation)
- **Priority**: Medium -- reduces operation count by factoring common terms
- **Conditions**: `?a` must be the same e-class in both operands
- **Proof**: Reverse distributive law (factoring): `a*b + a*c = a*(b + c)`.

---

### Rule 13: Neg as Mul

- **Pattern**: `(neg ?x)` --> `(mul -1 ?x)`
- **Category**: Strength Reduction
- **Phase**: 1 (algebraic simplification)
- **Priority**: High -- normalizes negation for downstream pattern matching
- **Conditions**: None
- **Proof**: Definition of additive inverse in terms of multiplication: `-a = (-1) * a`.

---

### Rule 14: Sub as Add Neg

- **Pattern**: `(sub ?x ?y)` --> `(add ?x (neg ?y))`
- **Category**: Strength Reduction
- **Phase**: 1 (algebraic simplification)
- **Priority**: High -- canonicalizes subtraction so add-rules apply
- **Conditions**: None
- **Proof**: Definition: `a - b = a + (-b)`.

---

### Rule 15: Div as Mul Recip

- **Pattern**: `(div ?x ?y)` --> `(mul ?x (recip ?y))`
- **Category**: Strength Reduction
- **Phase**: 1 (algebraic simplification)
- **Priority**: Medium -- enables mul-chain optimizations
- **Conditions**: `?y != 0`
- **Proof**: `a / b = a * (1/b)` for `b != 0`.

---

### Rule 16: Mul by Power of 2

- **Pattern**: `(mul ?x 2^k)` --> `(shl ?x k)`
- **Category**: Strength Reduction
- **Phase**: 1 (algebraic simplification)
- **Priority**: Medium -- applies to integer types only
- **Conditions**: `2^k` is a compile-time constant power of 2; `?x` is an integer type
- **Proof**: Binary representation: `a * 2^k = a << k`.

---

### Rule 17: Div by Power of 2

- **Pattern**: `(div ?x 2^k)` --> `(shr ?x k)`
- **Category**: Strength Reduction
- **Phase**: 1 (algebraic simplification)
- **Priority**: Medium -- applies to unsigned integer types
- **Conditions**: `2^k` is a compile-time constant; `?x` is unsigned integer
- **Proof**: Integer division by power of 2 equals logical right shift for unsigned values.
- **Note**: For signed integers, arithmetic right shift requires adjustment for negative values.

---

### Rule 18: Square via Mul

- **Pattern**: `(pow ?x 2)` --> `(mul ?x ?x)`
- **Category**: Strength Reduction
- **Phase**: 1 (algebraic simplification)
- **Priority**: Low -- pow is uncommon in optimized graphs
- **Conditions**: None
- **Proof**: Definition of exponentiation: `x^2 = x * x`.

---

### Rule 19: Idempotent And

- **Pattern**: `(bitand ?x ?x)` --> `?x`
- **Category**: Idempotence
- **Phase**: 1 (algebraic simplification)
- **Priority**: Medium -- appears in mask operations
- **Conditions**: None
- **Proof**: Idempotence of bitwise AND: `a & a = a`.

---

### Rule 20: Idempotent Or

- **Pattern**: `(bitor ?x ?x)` --> `?x`
- **Category**: Idempotence
- **Phase**: 1 (algebraic simplification)
- **Priority**: Medium -- appears in flag merging
- **Conditions**: None
- **Proof**: Idempotence of bitwise OR: `a | a = a`.

---

### Rule 21: Idempotent Min

- **Pattern**: `(min ?x ?x)` --> `?x`
- **Category**: Idempotence
- **Phase**: 1 (algebraic simplification)
- **Priority**: Medium -- appears after clamp expansion
- **Conditions**: None
- **Proof**: `min(a, a) = a` by definition.

---

### Rule 22: Idempotent Max

- **Pattern**: `(max ?x ?x)` --> `?x`
- **Category**: Idempotence
- **Phase**: 1 (algebraic simplification)
- **Priority**: Medium -- appears after clamp expansion
- **Conditions**: None
- **Proof**: `max(a, a) = a` by definition.

---

### Rule 23: Exp-Log Inverse

- **Pattern**: `(exp (log ?x))` --> `?x`
- **Category**: Inverse
- **Phase**: 1 (algebraic simplification)
- **Priority**: Medium -- appears in log-space computation round-trips
- **Conditions**: `?x > 0`
- **Proof**: `exp` and `ln` are inverse functions on `(0, +inf)`: `e^(ln(x)) = x`.

---

### Rule 24: Log-Exp Inverse

- **Pattern**: `(log (exp ?x))` --> `?x`
- **Category**: Inverse
- **Phase**: 1 (algebraic simplification)
- **Priority**: Medium -- appears in log-softmax simplification
- **Conditions**: None (defined for all real `?x`)
- **Proof**: `ln(e^x) = x` for all real `x`.

---

### Rule 25: Sqrt-Square Inverse

- **Pattern**: `(sqrt (mul ?x ?x))` --> `(abs ?x)`
- **Category**: Inverse
- **Phase**: 1 (algebraic simplification)
- **Priority**: Low -- uncommon pattern
- **Conditions**: `?x` is real-valued
- **Proof**: `sqrt(x^2) = |x|`. Note: not `x`, because `sqrt` returns the non-negative root.
- **Citation**: Standard real analysis identity.

---

### Rule 26: Transpose Involution

- **Pattern**: `(transpose (transpose ?x))` --> `?x`
- **Category**: Cancellation
- **Phase**: 1 (algebraic simplification)
- **Priority**: High -- very common in automatic differentiation outputs
- **Conditions**: None (for 2D tensors with default axes)
- **Proof**: Transposition is an involution: `(A^T)^T = A`.
- **Note**: For higher-rank tensors, axes must match: `transpose(transpose(x, perm), inv_perm) = x`.

---

### Rule 27: MatMul Associativity

- **Pattern**: `(matmul (matmul ?A ?B) ?C)` --> `(matmul ?A (matmul ?B ?C))`
- **Category**: Associativity
- **Phase**: 2 (equality saturation)
- **Priority**: Medium -- critical for optimizing chain matrix multiplication
- **Conditions**: Inner dimensions must be compatible
- **Proof**: Matrix multiplication is associative: `(AB)C = A(BC)`.
- **Note**: The cost function should prefer the association that minimizes total FLOPs (cf. matrix chain multiplication problem).

---

### Rule 28: Transpose of MatMul

- **Pattern**: `(transpose (matmul ?A ?B))` --> `(matmul (transpose ?B) (transpose ?A))`
- **Category**: Distribution
- **Phase**: 2 (equality saturation)
- **Priority**: Medium -- used to push transposes toward leaves for fusion
- **Conditions**: None
- **Proof**: `(AB)^T = B^T A^T` (reversal law for transpose).
- **Citation**: Standard linear algebra identity.

---

### Rule 29: Add Broadcast Fusion

- **Pattern**: `(add (broadcast ?x ?s) (broadcast ?y ?s))` --> `(broadcast (add ?x ?y) ?s)`
- **Category**: Fusion
- **Phase**: 2 (equality saturation)
- **Priority**: Medium -- reduces memory traffic by fusing before broadcast
- **Conditions**: `?x` and `?y` have the same pre-broadcast shape; `?s` is the same target shape
- **Proof**: Elementwise addition distributes over broadcast: broadcasting then adding equals adding then broadcasting (when shapes are compatible).

---

### Rule 30: Consecutive Reshape

- **Pattern**: `(reshape (reshape ?x ?s1) ?s2)` --> `(reshape ?x ?s2)`
- **Category**: Fusion
- **Phase**: 2 (equality saturation)
- **Priority**: High -- very common; intermediate reshapes are redundant
- **Conditions**: `product(?s1) == product(?s2) == numel(?x)` (element count preserved)
- **Proof**: Reshape is a view operation on contiguous data; composing two reshapes yields a single reshape to the final shape.

---

### Rule 31: Reshape-Transpose Swap

- **Pattern**: `(reshape (transpose ?x ?perm) ?s)` --> `(transpose (reshape ?x ?s') ?perm')`
- **Category**: Fusion
- **Phase**: 2 (equality saturation)
- **Priority**: Low -- requires careful shape/permutation analysis
- **Conditions**: Shapes and permutation must be compatible; `?s'` and `?perm'` are derived from `?s`, `?perm`, and `shape(?x)`
- **Proof**: Under specific shape constraints, the operations can be reordered. This is a conditional rewrite requiring shape inference.

---

### Rule 32: Dead Code Elimination

- **Pattern**: `(let ?x ?expr ?body)` --> `?body` (when `?x` is not free in `?body`)
- **Category**: Dead Code
- **Phase**: 1 (algebraic simplification)
- **Priority**: High -- removes unused computations
- **Conditions**: `?x` does not appear free in `?body`; `?expr` is pure (no side effects)
- **Proof**: If a bound variable is never referenced, the binding and its expression are dead code.
- **Note**: In graph IR, this manifests as nodes with zero consumers.

---

### Rule 33: Constant Folding

- **Pattern**: `(op c1 c2)` --> `eval(op, c1, c2)`
- **Category**: Collapse
- **Phase**: 1 (algebraic simplification)
- **Priority**: High -- always profitable; reduces graph size
- **Conditions**: Both operands are compile-time constants
- **Proof**: Any pure operation on known constants can be evaluated at compile time.
- **Note**: Must respect IEEE 754 semantics (rounding, special values) unless fast-math is enabled.

---

### Rule 34: Fused Multiply-Add

- **Pattern**: `(add (mul ?a ?b) ?c)` --> `(fma ?a ?b ?c)`
- **Category**: Fusion
- **Phase**: 2 (equality saturation)
- **Priority**: High -- maps to hardware FMA instructions; reduces latency and improves precision
- **Conditions**: Target hardware supports FMA (virtually all modern CPUs and GPUs)
- **Proof**: `fma(a, b, c) = a*b + c` computed with a single rounding step (IEEE 754-2008 fusedMultiplyAdd).
- **Citation**: IEEE 754-2008, Section 5.4.1.
- **Note**: FMA has higher precision than separate mul+add because it uses a single rounding.

---

### Rule 35: ReLU Simplification

- **Pattern**: `(max ?x 0)` --> `(relu ?x)`
- **Category**: Collapse
- **Phase**: 1 (algebraic simplification)
- **Priority**: Medium -- canonicalizes ReLU for pattern matching in later fusion passes
- **Conditions**: None
- **Proof**: Definition of ReLU: `relu(x) = max(x, 0)`.

---

### Rule 36: Exp Product Fusion

- **Pattern**: `(mul (exp ?a) (exp ?b))` --> `(exp (add ?a ?b))`
- **Category**: Fusion
- **Phase**: 2 (equality saturation)
- **Priority**: Low -- reduces transcendental function calls
- **Conditions**: None
- **Proof**: Exponential product rule: `e^a * e^b = e^(a+b)`.
- **Citation**: Standard calculus identity.

---

### Rule 37: Log Quotient Fission

- **Pattern**: `(log (div ?a ?b))` --> `(sub (log ?a) (log ?b))`
- **Category**: Distribution
- **Phase**: 2 (equality saturation)
- **Priority**: Low -- useful for numerical stability in log-space
- **Conditions**: `?a > 0`, `?b > 0`
- **Proof**: Logarithm quotient rule: `ln(a/b) = ln(a) - ln(b)`.
- **Citation**: Standard calculus identity.

---

### Rule 38: Sum Factorization

- **Pattern**: `(add (mul ?a ?x) (mul ?b ?x))` --> `(mul (add ?a ?b) ?x)`
- **Category**: Distribution (factoring)
- **Phase**: 2 (equality saturation)
- **Priority**: Low -- variant of Rule 12 with right-factor
- **Conditions**: `?x` is the same e-class in both terms
- **Proof**: Right distributive law: `a*x + b*x = (a+b)*x`.

---

## Phase Execution Strategy

### Phase 1: Algebraic Simplification

Run rules with Phase = 1 to convergence. These are directed rewrites (the RHS is always preferred over the LHS by the cost function). They simplify the graph without expanding the e-graph significantly.

**Rules**: 1, 2, 3, 4, 5, 6, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 32, 33, 35

### Phase 2: Equality Saturation

Run rules with Phase = 2 for a bounded number of iterations (or until saturation). These are bidirectional or structure-changing rewrites that grow the e-graph. The cost function extracts the optimal expression.

**Rules**: 7, 8, 9, 10, 11, 12, 27, 28, 29, 30, 31, 34, 36, 37, 38

---

## Priority Distribution

| Priority | Count | Description |
|----------|-------|-------------|
| High | 15 | Rules that fire frequently and are always profitable |
| Medium | 15 | Rules that fire moderately; profit depends on context |
| Low | 8 | Rules that fire rarely or require specific conditions |

---

## Cost Model Considerations

When extracting the optimal expression from the e-graph, the cost function should consider:

1. **Operation cost**: Transcendental functions (exp, log, sqrt) cost more than arithmetic (add, mul). FMA is cheaper than separate mul+add.
2. **Memory traffic**: Fewer intermediate tensors = less memory bandwidth. Fusion rules (29, 30, 34) generally reduce cost.
3. **Parallelism**: Some rewrites (e.g., reassociation) can improve instruction-level parallelism.
4. **Precision**: Under strict IEEE 754 mode, reassociation rules (9, 10) are not applicable. FMA (34) is always valid.
5. **Hardware mapping**: Strength reduction rules (16, 17) only benefit integer paths; FMA (34) requires hardware support.

---

## IEEE 754 Compatibility Notes

Several rules require **fast-math** semantics or explicit opt-in:

| Rule | Issue | Safe Under |
|------|-------|------------|
| 3 (Mul Zero) | `NaN * 0 = NaN`, not `0` | fast-math, or `?x` known finite |
| 9 (Add Assoc) | FP addition is not associative | fast-math, or exact mode off |
| 10 (Mul Assoc) | FP multiplication is not associative | fast-math, or exact mode off |
| 11, 12 (Distrib) | Distribution changes rounding | fast-math |
| 34 (FMA) | Single vs double rounding | Always valid (FMA is more precise) |

All other rules are exact and safe under strict IEEE 754 semantics.
