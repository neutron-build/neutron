# ===----------------------------------------------------------------------=== #
# Neutron Mojo — Rewrite Application Engine
# ===----------------------------------------------------------------------=== #

"""Equality saturation engine for e-graph rewrites.

Applies rewrite rules to an e-graph until saturation (no more rewrites possible)
or iteration limit is reached. Implements the core equality saturation algorithm:
1. Match all rule patterns against e-graph nodes
2. For each match, apply the rewrite (add RHS, merge with LHS)
3. Rebuild e-graph to propagate equivalences
4. Repeat until saturation

Reference: "egg: Fast and Extensible Equality Saturation" (Willsey et al., 2021)
"""

from collections import List
from .egraph import EGraph, CanonicalNode
from .eclass import ClassId
from .rules import RewriteRule, RuleSet
from .pattern import Pattern, PatternKind, Bindings, match_pattern, match_pattern_egraph
from .graph import OpKind


# ===----------------------------------------------------------------------=== #
# Match — A pattern match result
# ===----------------------------------------------------------------------=== #

struct Match(Copyable, Movable):
    """A successful pattern match.

    Records which rule matched, which e-class it matched in, the variable
    bindings extracted from the match, and the RHS pattern to instantiate.
    """
    var rule_name: String
    var matched_class: ClassId  # The e-class where pattern matched
    var bindings: Bindings
    var rhs: Pattern  # RHS pattern for instantiation

    fn __init__(out self, rule_name: String, matched_class: ClassId,
                var bindings: Bindings, var rhs: Pattern):
        self.rule_name = rule_name
        self.matched_class = matched_class
        self.bindings = bindings^
        self.rhs = rhs^

    fn __copyinit__(out self, existing: Self):
        self.rule_name = existing.rule_name
        self.matched_class = existing.matched_class
        self.bindings = existing.bindings.copy()
        self.rhs = existing.rhs.copy()

    fn copy(self) -> Match:
        """Return a copy of this match."""
        return Match(self.rule_name, self.matched_class,
                     self.bindings.copy(), self.rhs.copy())


# ===----------------------------------------------------------------------=== #
# RewriteStats — Statistics from equality saturation
# ===----------------------------------------------------------------------=== #

struct RewriteStats(Movable):
    """Statistics from running equality saturation."""
    var iterations: Int
    var total_matches: Int
    var rules_applied: Int
    var nodes_added: Int

    fn __init__(out self):
        self.iterations = 0
        self.total_matches = 0
        self.rules_applied = 0
        self.nodes_added = 0


# ===----------------------------------------------------------------------=== #
# RewriteEngine — Equality saturation engine
# ===----------------------------------------------------------------------=== #

struct RewriteEngine:
    """Equality saturation engine.

    Applies rewrite rules to an e-graph until saturation. Supports two-phase
    execution:
    - Phase 1: Directed simplifications (always profitable)
    - Phase 2: Bidirectional equality saturation (explore equivalent forms)
    """
    var max_iterations: Int
    var max_nodes: Int  # Stop if e-graph grows too large

    fn __init__(out self, max_iterations: Int = 10, max_nodes: Int = 10000):
        self.max_iterations = max_iterations
        self.max_nodes = max_nodes

    fn run_phase1(self, mut egraph: EGraph, ruleset: RuleSet) raises -> RewriteStats:
        """Run Phase 1: directed simplifications.

        Apply Phase 1 rules (identity, cancellation, collapse) until no more
        matches. These rules always reduce complexity, so termination is guaranteed.

        Args:
            egraph: The e-graph to rewrite (modified in-place).
            ruleset: The rule set containing Phase 1 rules.

        Returns:
            Statistics from the rewrite process.
        """
        var stats = RewriteStats()
        var phase1_rules = ruleset.get_phase1_rules()

        for iter in range(self.max_iterations):
            var matches = self._find_matches(egraph, phase1_rules)
            if len(matches) == 0:
                break  # Saturation reached

            stats.total_matches += len(matches)
            var applied = self._apply_matches(egraph, matches)
            stats.rules_applied += applied
            stats.iterations += 1

            if egraph.num_nodes() > self.max_nodes:
                break  # E-graph too large, stop

        stats.nodes_added = egraph.num_nodes()
        return stats^

    fn run_phase2(self, mut egraph: EGraph, ruleset: RuleSet) raises -> RewriteStats:
        """Run Phase 2: equality saturation.

        Apply Phase 2 rules (commutativity, associativity, distribution) for a
        bounded number of iterations. These rules explore equivalent forms
        without necessarily reducing complexity.

        Args:
            egraph: The e-graph to rewrite (modified in-place).
            ruleset: The rule set containing Phase 2 rules.

        Returns:
            Statistics from the rewrite process.
        """
        var stats = RewriteStats()
        var phase2_rules = ruleset.get_phase2_rules()

        for iter in range(self.max_iterations):
            var matches = self._find_matches(egraph, phase2_rules)
            if len(matches) == 0:
                break  # No more matches

            stats.total_matches += len(matches)
            var applied = self._apply_matches(egraph, matches)
            stats.rules_applied += applied
            stats.iterations += 1

            if egraph.num_nodes() > self.max_nodes:
                break  # E-graph too large, stop

        stats.nodes_added = egraph.num_nodes()
        return stats^

    fn _find_matches(self, mut egraph: EGraph, rules: List[RewriteRule]) raises -> List[Match]:
        """Find all pattern matches in the e-graph.

        For each rule, try to match its LHS pattern against all canonical nodes
        in the e-graph.

        Args:
            egraph: The e-graph to search.
            rules: The rules to match.

        Returns:
            List of successful matches.
        """
        var matches = List[Match]()

        for rule_idx in range(len(rules)):
            var rule = rules[rule_idx].copy()

            # Count max variable IDs in pattern for bindings allocation
            var num_vars = _count_vars(rule.lhs)

            # Try to match against each e-class
            for node_idx in range(egraph.num_nodes()):
                if node_idx >= len(egraph.classes):
                    continue
                var class_id = egraph.find(egraph.classes[node_idx].id)

                var bindings = Bindings(num_vars)
                if match_pattern_egraph(rule.lhs, class_id, bindings, egraph):
                    matches.append(Match(rule.name, class_id, bindings^, rule.rhs.copy())^)

        return matches^

    fn _apply_matches(self, mut egraph: EGraph, matches: List[Match]) raises -> Int:
        """Apply all matches by adding RHS patterns and merging.

        For each match:
        1. Instantiate the RHS pattern with bindings from the match
        2. Add the RHS to the e-graph
        3. Merge the RHS e-class with the LHS e-class (they're equivalent)

        Args:
            egraph: The e-graph to modify.
            matches: The matches to apply.

        Returns:
            Number of rewrites successfully applied.
        """
        var applied = 0

        for i in range(len(matches)):
            var m = matches[i].copy()
            var rhs_class = _instantiate_pattern(m.rhs, m.bindings, egraph)
            if rhs_class.id() >= 0:
                _ = egraph.merge(m.matched_class, rhs_class)
                applied += 1

        return applied


# ===----------------------------------------------------------------------=== #
# Simplified Runner — For testing basic rewrite scenarios
# ===----------------------------------------------------------------------=== #

fn apply_simple_rewrite(
    mut egraph: EGraph,
    lhs_class: ClassId,
    rhs_class: ClassId
) -> Bool:
    """Apply a simple rewrite: merge lhs_class with rhs_class.

    This is a simplified rewrite for testing. In a full implementation,
    the RHS would be instantiated from a pattern.

    Args:
        egraph: The e-graph to modify.
        lhs_class: The LHS e-class (from pattern match).
        rhs_class: The RHS e-class (rewrite target).

    Returns:
        True if rewrite was applied.
    """
    _ = egraph.merge(lhs_class, rhs_class)
    return True


fn count_rewrites_applied(
    mut egraph: EGraph,
    ruleset: RuleSet,
    max_iterations: Int = 5
) -> Int:
    """Count how many rewrites would be applied.

    Args:
        egraph: The e-graph to analyze.
        ruleset: The rule set.
        max_iterations: Maximum iterations.

    Returns:
        Number of rewrites applied.
    """
    var engine = RewriteEngine(max_iterations)
    try:
        var stats = engine.run_phase1(egraph, ruleset)
        return stats.rules_applied
    except:
        return 0


fn _count_vars(pattern: Pattern) -> Int:
    """Count the max variable index + 1 in a pattern tree."""
    var max_var = -1
    if pattern.kind == PatternKind.Var:
        if pattern.var_id > max_var:
            max_var = pattern.var_id
    for i in range(len(pattern.children)):
        var child_max = _count_vars(pattern.children[i])
        if child_max > max_var:
            max_var = child_max
    return max_var + 1


fn _instantiate_pattern(pattern: Pattern, bindings: Bindings, mut egraph: EGraph) raises -> ClassId:
    """Instantiate a pattern RHS using bindings.

    Recursively creates e-graph nodes for the RHS pattern:
    - Var: returns the bound class from bindings
    - Const: returns the constant ClassId
    - Op: recursively instantiates children, creates a new CanonicalNode,
      and adds it to the e-graph (hash-consing deduplicates)

    Args:
        pattern: The RHS pattern to instantiate.
        bindings: Variable bindings from the LHS match.
        egraph: The e-graph to add new nodes to.

    Returns:
        ClassId of the instantiated expression.
    """
    if pattern.kind == PatternKind.Var:
        return bindings.get(pattern.var_id)

    elif pattern.kind == PatternKind.Const:
        return ClassId(pattern.class_id)

    elif pattern.kind == PatternKind.Op:
        # Recursively instantiate children
        var node = CanonicalNode(pattern.op)
        for i in range(len(pattern.children)):
            var child_class = _instantiate_pattern(
                pattern.children[i], bindings, egraph
            )
            node.inputs.append(child_class)
        # Add to e-graph (hash-consing handles dedup)
        return egraph.add(node^)

    return ClassId(-1)
