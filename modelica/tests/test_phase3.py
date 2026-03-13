"""
Phase 3 tests: Nucleus TimeSeries, MCP tools, Julia bridge, surrogate models.

Database tests are skipped unless NUCLEUS_TEST_URL is set.
Julia bridge tests are skipped unless juliacall is installed.
Surrogate model tests run if scikit-learn is available.
"""

import os
import math
import numpy as np
import pytest

from neutron_sim import Variable, System, simulate
from neutron_sim.domains.mechanical import Mass, Spring, Damper, Fixed
from neutron_sim import connect


# ── Helpers ───────────────────────────────────────────────────────────────────

TEST_URL = os.environ.get("NUCLEUS_TEST_URL", "")


def spring_mass_result():
    mass = Mass(m=1.0)
    spring = Spring(k=10.0)
    wall = Fixed()
    system = System(
        components=[mass, spring, wall],
        connections=[connect(spring.flange_a, wall.flange), connect(spring.flange_b, mass.flange)],
        initial_conditions={mass.x: 1.0, mass.v: 0.0},
    )
    result = simulate(system, t_span=(0, 5), dt=0.1)
    return result, mass


# ── Nucleus store/load (unit-level — uses mock connection) ────────────────────

class MockConn:
    """In-memory mock for a psycopg3 connection."""

    def __init__(self):
        self._calls: list[tuple[str, tuple]] = []

    def execute(self, sql: str, params: tuple = ()):
        self._calls.append((sql, params))
        return self  # chainable

    def fetchall(self):
        return []

    def fetchone(self):
        return None


class TestNucleusStore:
    def test_store_results_calls_ts_insert(self):
        from neutron_sim.nucleus import store_results

        result, mass = spring_mass_result()
        conn = MockConn()
        count = store_results(conn, run_id="test-run-001", result=result,
                               variables=[mass.x])

        ts_calls = [c for c in conn._calls if "TS_INSERT" in c[0]]
        assert len(ts_calls) == count
        assert count == len(result.t)

    def test_store_all_variables_by_default(self):
        from neutron_sim.nucleus import store_results

        result, mass = spring_mass_result()
        conn = MockConn()
        count = store_results(conn, run_id="test-run-002", result=result)

        # Should write x and v
        n_vars = len(list(result._by_name.keys()))
        assert count == n_vars * len(result.t)

    def test_series_naming_convention(self):
        from neutron_sim.nucleus import store_results

        result, mass = spring_mass_result()
        conn = MockConn()
        store_results(conn, run_id="my-run", result=result, variables=[mass.x])

        # Series should be "sim:my-run:{var_name}"
        expected_series = f"sim:my-run:{mass.x.name}"
        assert any(expected_series in str(c) for c in conn._calls)

    def test_store_with_variable_names_as_strings(self):
        from neutron_sim.nucleus import store_results

        result, mass = spring_mass_result()
        conn = MockConn()
        count = store_results(conn, run_id="str-run", result=result,
                               variables=[mass.x.name])
        assert count == len(result.t)

    def test_store_skips_unknown_variables(self):
        from neutron_sim.nucleus import store_results

        result, _ = spring_mass_result()
        conn = MockConn()
        count = store_results(conn, run_id="skip-run", result=result,
                               variables=["does_not_exist"])
        assert count == 0

    def test_timestamp_offset(self):
        from neutron_sim.nucleus import store_results

        result, mass = spring_mass_result()
        conn = MockConn()
        t_offset = 1_700_000_000_000  # fixed ms
        store_results(conn, run_id="ts-run", result=result,
                      variables=[mass.x], t_offset_ms=t_offset)

        # First call should use t_offset_ms as timestamp (t=0 → ts_ms=t_offset+0)
        first_ts = conn._calls[0][1][1]
        assert first_ts == t_offset


# ── MCP Tool Tests ────────────────────────────────────────────────────────────

class TestMCPTool:
    def setup_method(self):
        # Clear registry before each test to avoid pollution
        from neutron_sim.ai.mcp_tool import _TOOLS
        _TOOLS.clear()

    def test_decorator_registers_tool(self):
        from neutron_sim.ai import mcp_tool, list_tools

        @mcp_tool("sim_test")
        def my_sim(description: str) -> dict:
            """Simulate something."""
            return {}

        tools = list_tools()
        names = [t["name"] for t in tools]
        assert "sim_test" in names

    def test_decorator_preserves_function(self):
        from neutron_sim.ai import mcp_tool

        @mcp_tool("test_fn")
        def add(x: int, y: int) -> int:
            return x + y

        assert add(2, 3) == 5

    def test_decorator_uses_function_name_as_default(self):
        from neutron_sim.ai import mcp_tool, list_tools

        @mcp_tool()
        def my_default_name():
            pass

        tools = list_tools()
        names = [t["name"] for t in tools]
        assert "my_default_name" in names

    def test_get_tool(self):
        from neutron_sim.ai import mcp_tool, get_tool

        @mcp_tool("get_me")
        def handler():
            return 42

        fn = get_tool("get_me")
        assert fn is not None
        assert fn() == 42

    def test_get_tool_missing_returns_none(self):
        from neutron_sim.ai import get_tool
        assert get_tool("nonexistent_tool_xyz") is None

    def test_call_tool(self):
        from neutron_sim.ai import mcp_tool, call_tool

        @mcp_tool("adder")
        def add_numbers(a: int, b: int) -> int:
            return a + b

        result = call_tool("adder", a=10, b=5)
        assert result == 15

    def test_call_tool_missing_raises(self):
        from neutron_sim.ai import call_tool

        with pytest.raises(KeyError, match="not registered"):
            call_tool("not_a_tool_xyz")

    def test_tool_description_from_docstring(self):
        from neutron_sim.ai import mcp_tool, list_tools

        @mcp_tool("described")
        def fn_with_docs():
            """This is the description."""
            pass

        tools = {t["name"]: t for t in list_tools()}
        assert "This is the description." in tools["described"]["description"]

    def test_tool_schema_has_parameters(self):
        from neutron_sim.ai import mcp_tool, list_tools

        @mcp_tool("typed_fn")
        def fn(name: str, count: int) -> dict:
            pass

        tools = {t["name"]: t for t in list_tools()}
        params = tools["typed_fn"]["parameters"]
        assert "name" in params
        assert "count" in params
        assert params["name"]["type"] == "string"
        assert params["count"]["type"] == "integer"

    def test_multiple_tools_registered(self):
        from neutron_sim.ai import mcp_tool, list_tools

        @mcp_tool("tool_a")
        def ta(): pass

        @mcp_tool("tool_b")
        def tb(): pass

        names = {t["name"] for t in list_tools()}
        assert "tool_a" in names
        assert "tool_b" in names


# ── Surrogate Model Tests ─────────────────────────────────────────────────────

try:
    import sklearn
    HAS_SKLEARN = True
except ImportError:
    HAS_SKLEARN = False


@pytest.mark.skipif(not HAS_SKLEARN, reason="scikit-learn not installed")
class TestSurrogateModel:
    def _make_sweep_data(self, n=30):
        """Simulate spring-mass for varying k values, return X, y."""
        k_values = np.linspace(5.0, 50.0, n)
        X = k_values.reshape(-1, 1)
        max_x = []
        for k in k_values:
            mass = Mass(m=1.0)
            spring = Spring(k=float(k))
            wall = Fixed()
            system = System(
                components=[mass, spring, wall],
                connections=[
                    connect(spring.flange_a, wall.flange),
                    connect(spring.flange_b, mass.flange),
                ],
                initial_conditions={mass.x: 1.0, mass.v: 0.0},
            )
            result = simulate(system, t_span=(0, 5), dt=0.05)
            max_x.append(float(result[mass.x.name].max()))
        y = {"max_x": np.array(max_x)}
        return X, y

    def test_train_surrogate_ridge(self):
        from neutron_sim.ai import train_surrogate

        X, y = self._make_sweep_data()
        surrogate = train_surrogate(X, y, input_params=["k"], output_vars=["max_x"])
        assert surrogate is not None
        assert "max_x" in surrogate._models

    def test_surrogate_predict(self):
        from neutron_sim.ai import train_surrogate

        X, y = self._make_sweep_data()
        surrogate = train_surrogate(X, y, input_params=["k"], output_vars=["max_x"])
        pred = surrogate.predict(k=25.0)
        assert "max_x" in pred
        # Spring max displacement for undamped system starting at x=1 should be 1.0
        # (energy conservation: max_x = x0 = 1.0 regardless of k)
        assert pred["max_x"] == pytest.approx(1.0, abs=0.05)

    def test_surrogate_score(self):
        from neutron_sim.ai import train_surrogate

        X, y = self._make_sweep_data(n=40)
        n_train = 30
        surrogate = train_surrogate(
            X[:n_train], {k: v[:n_train] for k, v in y.items()},
            input_params=["k"], output_vars=["max_x"]
        )
        scores = surrogate.score(X[n_train:], {k: v[n_train:] for k, v in y.items()})
        # R² should be > 0.5 for this simple problem
        assert scores["max_x"] > 0.5

    def test_train_surrogate_random_forest(self):
        from neutron_sim.ai import train_surrogate

        X, y = self._make_sweep_data(n=20)
        surrogate = train_surrogate(
            X, y, input_params=["k"], output_vars=["max_x"], model_type="rf"
        )
        pred = surrogate.predict(k=10.0)
        assert "max_x" in pred

    def test_train_surrogate_invalid_type(self):
        from neutron_sim.ai import train_surrogate

        X, y = self._make_sweep_data(n=5)
        with pytest.raises(ValueError, match="Unknown model_type"):
            train_surrogate(X, y, input_params=["k"], output_vars=["max_x"], model_type="svm")

    def test_surrogate_model_fields(self):
        from neutron_sim.ai import train_surrogate, SurrogateModel

        X, y = self._make_sweep_data(n=10)
        surrogate = train_surrogate(X, y, input_params=["k"], output_vars=["max_x"])
        assert isinstance(surrogate, SurrogateModel)
        assert surrogate.input_params == ["k"]
        assert surrogate.output_vars == ["max_x"]


# ── Julia Bridge Tests ────────────────────────────────────────────────────────

class TestJuliaBridge:
    def test_bridge_instantiates(self):
        from neutron_sim.julia import JuliaBridge
        bridge = JuliaBridge()
        assert bridge is not None

    def test_bridge_is_available_property(self):
        from neutron_sim.julia import JuliaBridge
        bridge = JuliaBridge()
        # Just check the property doesn't raise; value depends on environment
        available = bridge.is_available
        assert isinstance(available, bool)

    @pytest.mark.skipif(
        not os.environ.get("JULIACALL_AVAILABLE"),
        reason="juliacall not available; set JULIACALL_AVAILABLE=1 to run",
    )
    def test_bridge_simulate(self):
        from neutron_sim.julia import JuliaBridge

        bridge = JuliaBridge()
        result = bridge.simulate(
            equations="""
            @variables t x(t) v(t)
            @parameters m=1.0 k=10.0 c=0.0
            D = Differential(t)
            eqs = [D(x) ~ v, m*D(v) ~ -k*x - c*v]
            """,
            t_span=(0.0, 5.0),
            initial_conditions={"x": 1.0, "v": 0.0},
            saveat=0.1,
        )
        assert "x" in result.variables
        assert len(result.t) > 1
        # Undamped spring-mass: amplitude stays ≈ 1.0
        assert abs(result["x"]).max() == pytest.approx(1.0, abs=0.05)


# ── Integration Tests (requires NUCLEUS_TEST_URL) ─────────────────────────────

@pytest.mark.skipif(not TEST_URL, reason="NUCLEUS_TEST_URL not set")
class TestNucleusIntegration:
    def test_store_and_load_round_trip(self):
        import psycopg
        from neutron_sim.nucleus import store_results, load_results

        with psycopg.connect(TEST_URL) as conn:
            result, mass = spring_mass_result()
            run_id = "test-round-trip-001"

            store_results(conn, run_id=run_id, result=result,
                          variables=[mass.x], t_offset_ms=0)
            conn.commit()

            t_ms, arrays = load_results(conn, run_id, [mass.x.name])
            arr = arrays.get(mass.x.name, np.array([]))

            np.testing.assert_allclose(arr, result[mass.x.name], rtol=1e-6)
