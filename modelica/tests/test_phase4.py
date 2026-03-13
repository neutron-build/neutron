"""
Phase 4 tests: Visualization and surrogate models.

Matplotlib is used in non-interactive mode; no display required.
Surrogate model tests are conditionally skipped if scikit-learn is absent.
"""

import os
import numpy as np
import pytest
import matplotlib
matplotlib.use("Agg")  # non-interactive backend — no display needed

from neutron_sim import Variable, System, simulate, connect
from neutron_sim.domains.mechanical import Mass, Spring, Damper, Fixed


# ── Helpers ───────────────────────────────────────────────────────────────────

def spring_mass_result(m=1.0, k=10.0, c=0.0, x0=1.0, t_end=5.0, dt=0.05):
    mass = Mass(m=m)
    spring = Spring(k=k)
    wall = Fixed()
    comps = [mass, spring, wall]
    conns = [
        connect(spring.flange_a, wall.flange),
        connect(spring.flange_b, mass.flange),
    ]
    if c > 0:
        damper = Damper(c=c)
        comps.append(damper)
        conns = [
            connect(spring.flange_a, wall.flange),
            connect(spring.flange_b, damper.flange_b, mass.flange),
            connect(damper.flange_a, wall.flange),
        ]
    system = System(
        components=comps,
        connections=conns,
        initial_conditions={mass.x: x0, mass.v: 0.0},
    )
    result = simulate(system, t_span=(0, t_end), dt=dt)
    return result, mass


# ── Time-Series Plot Tests ────────────────────────────────────────────────────

class TestTimeSeriesPlot:
    def test_plot_returns_figure(self):
        from neutron_sim.viz import plot_timeseries
        import matplotlib.pyplot as plt

        result, mass = spring_mass_result()
        fig = plot_timeseries(result, show=False)
        assert fig is not None
        assert hasattr(fig, "savefig")
        plt.close("all")

    def test_plot_with_variable_list(self):
        from neutron_sim.viz import plot_timeseries
        import matplotlib.pyplot as plt

        result, mass = spring_mass_result()
        fig = plot_timeseries(result, variables=[mass.x], show=False)
        ax = fig.axes[0]
        assert len(ax.lines) == 1
        plt.close("all")

    def test_plot_with_variable_names(self):
        from neutron_sim.viz import plot_timeseries
        import matplotlib.pyplot as plt

        result, mass = spring_mass_result()
        fig = plot_timeseries(result, variables=[mass.x.name, mass.v.name], show=False)
        ax = fig.axes[0]
        assert len(ax.lines) == 2
        plt.close("all")

    def test_plot_title(self):
        from neutron_sim.viz import plot_timeseries
        import matplotlib.pyplot as plt

        result, _ = spring_mass_result()
        fig = plot_timeseries(result, title="My Title", show=False)
        ax = fig.axes[0]
        assert ax.get_title() == "My Title"
        plt.close("all")

    def test_plot_all_variables_by_default(self):
        from neutron_sim.viz import plot_timeseries
        import matplotlib.pyplot as plt

        result, _ = spring_mass_result()
        n_vars = len(list(result._by_name.keys()))
        fig = plot_timeseries(result, show=False)
        ax = fig.axes[0]
        assert len(ax.lines) == n_vars
        plt.close("all")

    def test_plot_on_existing_axes(self):
        from neutron_sim.viz import plot_timeseries
        import matplotlib.pyplot as plt

        result, mass = spring_mass_result()
        fig, ax = plt.subplots()
        returned_fig = plot_timeseries(result, variables=[mass.x], ax=ax, show=False)
        assert returned_fig is fig
        plt.close("all")

    def test_plot_comparison(self):
        from neutron_sim.viz import plot_comparison
        from neutron_sim.solvers.ode import SimulationResult
        import matplotlib.pyplot as plt
        import numpy as np

        # Build two results with the same variable name "x" for comparison
        t = np.linspace(0, 5, 100)
        r1 = SimulationResult(t=t, state_names=["x", "v"],
                              y=np.vstack([np.cos(t), -np.sin(t)]))
        r2 = SimulationResult(t=t, state_names=["x", "v"],
                              y=np.vstack([np.cos(2 * t), -2 * np.sin(2 * t)]))

        results = {"k=10": r1, "k=20": r2}
        fig = plot_comparison(results, variable="x", show=False)
        ax = fig.axes[0]
        assert len(ax.lines) == 2
        plt.close("all")

    def test_plot_parameter_sweep(self):
        from neutron_sim.viz import plot_parameter_sweep
        import matplotlib.pyplot as plt

        sweep_data = [(k, 1.0) for k in np.linspace(5, 50, 10)]
        fig = plot_parameter_sweep(
            sweep_data, param_name="k", output_name="max_x", show=False
        )
        assert fig is not None
        plt.close("all")


# ── Phase Portrait Tests ──────────────────────────────────────────────────────

class TestPhasePortrait:
    def test_phase_portrait_returns_figure(self):
        from neutron_sim.viz import phase_portrait
        import matplotlib.pyplot as plt

        result, mass = spring_mass_result()
        fig = phase_portrait(result, mass.x, mass.v, show=False, color_by_time=False)
        assert fig is not None
        plt.close("all")

    def test_phase_portrait_with_time_color(self):
        from neutron_sim.viz import phase_portrait
        import matplotlib.pyplot as plt

        result, mass = spring_mass_result()
        fig = phase_portrait(result, mass.x, mass.v, show=False, color_by_time=True)
        assert fig is not None
        plt.close("all")

    def test_phase_portrait_with_string_names(self):
        from neutron_sim.viz import phase_portrait
        import matplotlib.pyplot as plt

        result, mass = spring_mass_result()
        fig = phase_portrait(
            result, mass.x.name, mass.v.name,
            title="Phase Portrait Test",
            show=False, color_by_time=False,
        )
        ax = fig.axes[0]
        assert ax.get_title() == "Phase Portrait Test"
        plt.close("all")

    def test_phase_portrait_on_existing_axes(self):
        from neutron_sim.viz import phase_portrait
        import matplotlib.pyplot as plt

        result, mass = spring_mass_result()
        fig, ax = plt.subplots()
        returned_fig = phase_portrait(result, mass.x, mass.v, ax=ax, show=False, color_by_time=False)
        assert returned_fig is fig
        plt.close("all")

    def test_poincare_section(self):
        from neutron_sim.viz import poincare_section
        import matplotlib.pyplot as plt

        result, mass = spring_mass_result(t_end=20.0, dt=0.01)
        fig = poincare_section(
            result,
            var_x=mass.x,
            var_y=mass.v,
            section_var=mass.x,
            section_value=0.0,
            direction="positive",
            show=False,
        )
        assert fig is not None
        plt.close("all")

    def test_poincare_finds_crossings(self):
        from neutron_sim.viz import poincare_section
        import matplotlib.pyplot as plt

        # Undamped oscillator crosses x=0 twice per period
        result, mass = spring_mass_result(t_end=30.0, dt=0.005)
        fig = poincare_section(
            result, mass.x, mass.v, mass.x,
            section_value=0.0, direction="positive", show=False,
        )
        # Should have multiple crossing points on the plot
        ax = fig.axes[0]
        lines = ax.lines
        total_points = sum(len(line.get_xdata()) for line in lines)
        assert total_points >= 2  # at least 2 crossings in 30s
        plt.close("all")


# ── SimulationResult.plot / phase_plot (existing API) ─────────────────────────

class TestBuiltinPlotMethods:
    """The SimulationResult.plot and phase_plot methods (Phase 1) still work."""

    def test_result_plot_method(self):
        import matplotlib.pyplot as plt

        result, mass = spring_mass_result()
        fig = result.plot([mass.x], show=False)
        assert fig is not None
        plt.close("all")

    def test_result_phase_plot_method(self):
        import matplotlib.pyplot as plt

        result, mass = spring_mass_result()
        fig = result.phase_plot(mass.x, mass.v, show=False)
        assert fig is not None
        plt.close("all")


# ── SimulationResult.summary ──────────────────────────────────────────────────

class TestSummary:
    def test_summary_contains_all_variables(self):
        result, mass = spring_mass_result()
        s = result.summary()
        for vname in result._by_name:
            assert vname in s

    def test_summary_stats_correct(self):
        result, mass = spring_mass_result(x0=1.0, c=0.0)
        s = result.summary()
        xname = mass.x.name
        # Max displacement ≈ 1.0 for undamped spring
        assert s[xname]["max"] == pytest.approx(1.0, abs=0.02)
        assert s[xname]["min"] < 0   # oscillates below zero


# ── End-to-end example: parameter sweep + surrogate prediction ─────────────────

try:
    import sklearn
    HAS_SKLEARN = True
except ImportError:
    HAS_SKLEARN = False


@pytest.mark.skipif(not HAS_SKLEARN, reason="scikit-learn not installed")
class TestEndToEndSweep:
    def test_sweep_and_surrogate(self):
        from neutron_sim.ai import train_surrogate
        from neutron_sim.viz import plot_parameter_sweep, plot_timeseries
        import matplotlib.pyplot as plt

        k_values = np.linspace(5.0, 40.0, 20)
        X = k_values.reshape(-1, 1)
        max_x_values = []

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
            r = simulate(system, t_span=(0, 5), dt=0.05)
            max_x_values.append(float(r[mass.x.name].max()))

        y = {"max_x": np.array(max_x_values)}

        # Train surrogate
        surrogate = train_surrogate(X, y, input_params=["k"], output_vars=["max_x"])
        pred_25 = surrogate.predict(k=25.0)
        assert pred_25["max_x"] == pytest.approx(1.0, abs=0.05)

        # Plot sweep
        sweep_data = list(zip(k_values, max_x_values))
        fig = plot_parameter_sweep(sweep_data, "k [N/m]", "max_x [m]", show=False)
        assert fig is not None
        plt.close("all")
