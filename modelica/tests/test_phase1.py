"""
Phase 1 tests: Core modeling API + SciPy solver + Mechanical components.

Validates numerical results against known analytical solutions.
"""

import math
import numpy as np
import pytest

from neutron_sim import Variable, Parameter, Equation, der, System, simulate, connect
from neutron_sim.domains.mechanical import Mass, Spring, Damper, Fixed, Force
from neutron_sim.solvers.auto_select import estimate_stiffness, select_method


# ────────────────────────────────────────────────
# 1. Core expression evaluation
# ────────────────────────────────────────────────

class TestExpressions:
    def test_variable_eval(self):
        x = Variable("x")
        assert x.eval({"x": 3.0}) == 3.0

    def test_parameter_eval(self):
        p = Parameter("p", value=5.0)
        assert p.eval({}) == 5.0

    def test_binop_add(self):
        x = Variable("x")
        expr = x + 2.0
        assert expr.eval({"x": 3.0}) == 5.0

    def test_binop_mul(self):
        x = Variable("x")
        expr = 3.0 * x
        assert expr.eval({"x": 4.0}) == 12.0

    def test_unary_neg(self):
        x = Variable("x")
        expr = -x
        assert expr.eval({"x": 7.0}) == -7.0

    def test_equation_residual(self):
        x = Variable("x")
        eq = x == 5.0
        assert eq.residual({"x": 5.0}) == pytest.approx(0.0)
        assert eq.residual({"x": 3.0}) == pytest.approx(-2.0)

    def test_der_expression(self):
        x = Variable("x")
        dx = der(x)
        assert dx.eval({"dx": 2.0}) == 2.0

    def test_der_requires_variable(self):
        with pytest.raises(TypeError):
            der(Parameter("p", 1.0))  # type: ignore


# ────────────────────────────────────────────────
# 2. Simple ODE: harmonic oscillator (exact solution)
# ────────────────────────────────────────────────

class TestHarmonicOscillator:
    """
    x'' + ω² x = 0
    Exact: x(t) = x0 * cos(ω*t) + (v0/ω) * sin(ω*t)
    """

    def setup_method(self):
        self.x = Variable("x")
        self.v = Variable("v")
        self.omega = 2.0  # rad/s
        self.x0 = 1.0
        self.v0 = 0.0

    def _make_system(self):
        x, v, omega = self.x, self.v, self.omega
        eqs = [
            der(x) == v,
            der(v) == -(omega ** 2) * x,
        ]
        # Manual system without components
        from neutron_sim.core.system import System as _Sys
        s = _Sys.__new__(_Sys)
        s.components = []
        s.connections = []
        s._user_ics = {x: self.x0, v: self.v0}
        s._equations = eqs
        return s

    def test_frequency(self):
        system = self._make_system()
        result = simulate(system, t_span=(0, 10), dt=0.01)
        t = result.t
        x_num = result[self.x]

        omega = self.omega
        x_exact = self.x0 * np.cos(omega * t)

        # Allow 0.1% relative error
        np.testing.assert_allclose(x_num, x_exact, rtol=1e-3, atol=1e-4)

    def test_energy_conservation(self):
        """Total energy should stay constant (no damping)."""
        system = self._make_system()
        result = simulate(system, t_span=(0, 20), dt=0.01)
        x = result[self.x]
        v = result[self.v]
        omega = self.omega
        E = 0.5 * v**2 + 0.5 * omega**2 * x**2
        np.testing.assert_allclose(E, E[0], rtol=1e-4)


# ────────────────────────────────────────────────
# 3. Damped oscillator (exact solution)
# ────────────────────────────────────────────────

class TestDampedOscillator:
    """
    m*x'' + c*x' + k*x = 0   (under-damped)
    Exact: x(t) = A * exp(-ζ*ω_n*t) * cos(ω_d*t + φ)
    """

    def setup_method(self):
        self.m = 1.0
        self.k = 10.0
        self.c = 1.0  # under-damped: ζ = c/(2√(mk)) ≈ 0.158
        self.x0 = 1.0
        self.v0 = 0.0

    def _make_system(self):
        x = Variable("x")
        v = Variable("v")
        m, k, c = self.m, self.k, self.c
        eqs = [
            der(x) == v,
            m * der(v) == -k * x - c * v,
        ]
        from neutron_sim.core.system import System as _Sys
        s = _Sys.__new__(_Sys)
        s.components = []
        s.connections = []
        s._user_ics = {x: self.x0, v: self.v0}
        s._equations = eqs
        self._x, self._v = x, v
        return s

    def test_decay(self):
        system = self._make_system()
        result = simulate(system, t_span=(0, 20), dt=0.01)
        t = result.t
        x_num = result[self._x]

        m, k, c = self.m, self.k, self.c
        omega_n = math.sqrt(k / m)
        zeta = c / (2 * math.sqrt(m * k))
        omega_d = omega_n * math.sqrt(1 - zeta**2)
        # Full underdamped solution (x0=1, v0=0):
        # x = exp(-ζωn·t) * (A·cos(ωd·t) + B·sin(ωd·t))
        # A = x0,  B = (v0 + ζ·ωn·x0) / ωd
        A = self.x0
        B = (self.v0 + zeta * omega_n * self.x0) / omega_d
        x_exact = np.exp(-zeta * omega_n * t) * (
            A * np.cos(omega_d * t) + B * np.sin(omega_d * t)
        )
        np.testing.assert_allclose(x_num, x_exact, rtol=1e-3, atol=1e-4)

    def test_final_value_near_zero(self):
        system = self._make_system()
        result = simulate(system, t_span=(0, 50), dt=0.1)
        x = result[self._x]
        assert abs(x[-1]) < 0.01, f"Expected near-zero at t=50, got {x[-1]:.4f}"


# ────────────────────────────────────────────────
# 4. Solver auto-selection
# ────────────────────────────────────────────────

class TestSolverSelection:
    def test_non_stiff_selects_rk45(self):
        # Harmonic oscillator — moderate eigenvalues, not stiff
        x = Variable("x")
        v = Variable("v")
        omega = 1.0
        f = lambda t, y: [y[1], -(omega**2) * y[0]]
        y0 = np.array([1.0, 0.0])
        ratio = estimate_stiffness(f, 0.0, y0)
        # Non-stiff: ratio should be small
        assert ratio < 1000.0
        assert select_method(ratio) == "RK45"

    def test_stiff_selects_radau(self):
        # Very stiff system: dy1/dt = -1000*y1, dy2/dt = -y2
        f = lambda t, y: [-1000.0 * y[0], -y[1]]
        y0 = np.array([1.0, 1.0])
        ratio = estimate_stiffness(f, 0.0, y0)
        assert ratio > 900.0  # ≈ 1000, allow FD noise
        assert select_method(ratio) == "Radau"


# ────────────────────────────────────────────────
# 5. Mechanical components
# ────────────────────────────────────────────────

class TestMechanicalComponents:
    def test_mass_spring_damper_connected(self):
        """
        Build a mass-spring-damper system using components + connections.
        Verify numerical result matches direct ODE solution.
        """
        m_val, k_val, c_val = 1.0, 10.0, 1.0
        x0 = 1.0

        mass = Mass(m=m_val)
        spring = Spring(k=k_val)
        damper = Damper(c=c_val)
        wall = Fixed()

        system = System(
            components=[mass, spring, damper, wall],
            connections=[
                connect(spring.flange_a, wall.flange),
                connect(damper.flange_a, wall.flange),
                connect(spring.flange_b, damper.flange_b, mass.flange),
            ],
            initial_conditions={
                mass.x: x0,
                mass.v: 0.0,
            },
        )

        result = simulate(system, t_span=(0, 10), dt=0.01)

        # Compare to analytical solution
        omega_n = math.sqrt(k_val / m_val)
        zeta = c_val / (2 * math.sqrt(m_val * k_val))
        omega_d = omega_n * math.sqrt(1 - zeta**2)
        t = result.t
        v0 = 0.0
        A = x0
        B = (v0 + zeta * omega_n * x0) / omega_d
        x_exact = np.exp(-zeta * omega_n * t) * (
            A * np.cos(omega_d * t) + B * np.sin(omega_d * t)
        )
        x_num = result[mass.x]
        np.testing.assert_allclose(x_num, x_exact, rtol=1e-2, atol=1e-3)

    def test_mass_initial_conditions_default(self):
        mass = Mass(m=2.0)
        ics = mass.initial_conditions()
        assert ics[mass.x] == 0.0
        assert ics[mass.v] == 0.0

    def test_fixed_ground_position_zero(self):
        wall = Fixed()
        eqs = wall.equations()
        # Fixed now has 2 equations: x == 0 and v == 0
        assert len(eqs) == 2
        # x equation
        x_eq = next(e for e in eqs if wall.flange.x in e.variables())
        assert x_eq.residual({wall.flange.x.name: 0.0}) == pytest.approx(0.0)
        assert x_eq.residual({wall.flange.x.name: 1.0}) != pytest.approx(0.0)
        # v equation
        v_eq = next(e for e in eqs if wall.flange.v in e.variables())
        assert v_eq.residual({wall.flange.v.name: 0.0}) == pytest.approx(0.0)

    def test_spring_force_equation(self):
        spring = Spring(k=5.0)
        eqs = spring.equations()
        # f_a = k*(x_a - x_b) at x_a=2, x_b=0 → f_a = 10
        state = {
            spring.flange_a.x.name: 2.0,
            spring.flange_b.x.name: 0.0,
            spring.flange_a.f.name: 10.0,
        }
        assert eqs[0].residual(state) == pytest.approx(0.0)

    def test_free_oscillation_no_damping(self):
        """Spring-mass without damper: energy should be conserved."""
        mass = Mass(m=1.0)
        spring = Spring(k=4.0)
        wall = Fixed()

        system = System(
            components=[mass, spring, wall],
            connections=[
                connect(spring.flange_a, wall.flange),
                connect(spring.flange_b, mass.flange),
            ],
            initial_conditions={mass.x: 1.0, mass.v: 0.0},
        )
        # spring.flange_b and mass.flange share the same node

        result = simulate(system, t_span=(0, 10), dt=0.01)
        x = result[mass.x]
        v = result[mass.v]
        k_val = 4.0
        E = 0.5 * v**2 + 0.5 * k_val * x**2
        np.testing.assert_allclose(E, E[0], rtol=1e-3)

    def test_constant_force(self):
        """Free mass under constant force: x = 0.5*(F/m)*t^2."""
        mass = Mass(m=2.0)
        force = Force(F=4.0)

        # Direct connection: force applied to mass flange
        system = System(
            components=[mass, force],
            connections=[connect(force.flange, mass.flange)],
            initial_conditions={mass.x: 0.0, mass.v: 0.0},
        )

        result = simulate(system, t_span=(0, 5), dt=0.01)
        t = result.t
        x_exact = 0.5 * (4.0 / 2.0) * t**2
        np.testing.assert_allclose(result[mass.x], x_exact, rtol=1e-3, atol=1e-4)


# ────────────────────────────────────────────────
# 6. SimulationResult API
# ────────────────────────────────────────────────

class TestSimulationResult:
    def _simple_result(self):
        x = Variable("x")
        v = Variable("v")
        eqs = [der(x) == v, der(v) == -x]
        from neutron_sim.core.system import System as _Sys
        s = _Sys.__new__(_Sys)
        s.components = []
        s.connections = []
        s._user_ics = {x: 1.0, v: 0.0}
        s._equations = eqs
        return simulate(s, t_span=(0, 5), dt=0.1), x, v

    def test_access_by_variable(self):
        result, x, v = self._simple_result()
        arr = result[x]
        assert isinstance(arr, np.ndarray)
        assert len(arr) == len(result.t)

    def test_access_by_name(self):
        result, x, v = self._simple_result()
        assert np.array_equal(result["x"], result[x])

    def test_contains(self):
        result, x, v = self._simple_result()
        assert x in result
        assert "v" in result

    def test_summary(self):
        result, x, v = self._simple_result()
        s = result.summary()
        assert "x" in s
        assert "min" in s["x"]
        assert "max" in s["x"]
        assert "final" in s["x"]
