"""
Thermal domain tests.

Tests the HeatPort connector, ThermalResistance, ThermalCapacitance,
FixedTemperature, HeatSource, and Convection components.

Key verification:
  - RC thermal circuit exponential response matches analytical solution
  - Series resistance equivalence
  - FixedTemperature boundary condition
"""

import math
import numpy as np
import pytest

from neutron_sim import System, connect, simulate
from neutron_sim.domains.thermal import (
    HeatPort,
    ThermalCapacitance,
    ThermalResistance,
    FixedTemperature,
    HeatSource,
    Convection,
)


# ── Connector Tests ───────────────────────────────────────────────────────────

class TestHeatPort:
    def test_across_variable(self):
        port = HeatPort(prefix="test")
        across = port.across_vars()
        assert len(across) == 1
        assert across[0].name == "test.T"

    def test_through_variable(self):
        port = HeatPort(prefix="test")
        through = port.through_vars()
        assert len(through) == 1
        assert through[0].name == "test.Q"

    def test_distinct_instances(self):
        """Two ports must have independent Variable objects."""
        p1 = HeatPort(prefix="a")
        p2 = HeatPort(prefix="b")
        assert p1.T is not p2.T
        assert p1.Q is not p2.Q


# ── RC Thermal Circuit ────────────────────────────────────────────────────────

class TestRCThermalCircuit:
    """
    HeatSource -> ThermalResistance -> ThermalCapacitance

    Analytical solution for step heat input Q into R-C:
        T(t) = T0 + Q*R * (1 - exp(-t / (R*C)))

    Steady state: T_final = T0 + Q * R
    """

    def test_exponential_response(self):
        """RC circuit temperature follows exponential approach to steady state."""
        R_val = 2.0   # K/W
        C_val = 5.0   # J/K
        Q_val = 10.0  # W
        T0 = 300.0    # K (initial temperature of capacitance)
        tau = R_val * C_val  # time constant
        T_final = T0 + Q_val * R_val  # steady-state temperature

        # Build circuit: HeatSource -> ThermalResistance -> ThermalCapacitance
        source = HeatSource(Q=Q_val)
        resistance = ThermalResistance(R=R_val)
        capacitance = ThermalCapacitance(C=C_val, T0=T0)

        system = System(
            components=[source, resistance, capacitance],
            connections=[
                connect(source.port, resistance.port_a),
                connect(resistance.port_b, capacitance.port),
            ],
        )

        t_end = 5.0 * tau  # 5 time constants — nearly at steady state
        result = simulate(system, t_span=(0.0, t_end), dt=0.01)

        T_sim = result[capacitance.T]
        t_arr = result.t

        # Analytical solution: T(t) = T0 + Q*R * (1 - exp(-t/tau))
        T_analytical = T0 + Q_val * R_val * (1.0 - np.exp(-t_arr / tau))

        np.testing.assert_allclose(T_sim, T_analytical, rtol=1e-3)

    def test_initial_condition(self):
        """Temperature starts at the specified initial value."""
        T0 = 350.0
        C_val = 1.0
        Q_val = 0.0  # no heat input

        source = HeatSource(Q=Q_val)
        resistance = ThermalResistance(R=1.0)
        capacitance = ThermalCapacitance(C=C_val, T0=T0)

        system = System(
            components=[source, resistance, capacitance],
            connections=[
                connect(source.port, resistance.port_a),
                connect(resistance.port_b, capacitance.port),
            ],
        )

        result = simulate(system, t_span=(0.0, 0.1), dt=0.01)
        assert result[capacitance.T][0] == pytest.approx(T0, abs=1e-6)

    def test_steady_state(self):
        """After many time constants, temperature reaches Q*R + T0."""
        R_val = 1.0
        C_val = 1.0
        Q_val = 50.0
        T0 = 273.15
        tau = R_val * C_val

        source = HeatSource(Q=Q_val)
        resistance = ThermalResistance(R=R_val)
        capacitance = ThermalCapacitance(C=C_val, T0=T0)

        system = System(
            components=[source, resistance, capacitance],
            connections=[
                connect(source.port, resistance.port_a),
                connect(resistance.port_b, capacitance.port),
            ],
        )

        result = simulate(system, t_span=(0.0, 10.0 * tau), dt=0.01)
        T_final_expected = T0 + Q_val * R_val
        assert result[capacitance.T][-1] == pytest.approx(T_final_expected, rel=1e-2)


# ── Series Resistance ─────────────────────────────────────────────────────────

class TestSeriesResistance:
    """Two resistors in series: R_total = R1 + R2."""

    def test_series_resistance_equivalence(self):
        R1_val = 3.0
        R2_val = 7.0
        R_total = R1_val + R2_val
        C_val = 2.0
        Q_val = 20.0
        T0 = 300.0
        tau_total = R_total * C_val

        # Series circuit: Source -> R1 -> R2 -> Capacitance
        source = HeatSource(Q=Q_val)
        r1 = ThermalResistance(R=R1_val)
        r2 = ThermalResistance(R=R2_val)
        cap = ThermalCapacitance(C=C_val, T0=T0)

        system = System(
            components=[source, r1, r2, cap],
            connections=[
                connect(source.port, r1.port_a),
                connect(r1.port_b, r2.port_a),
                connect(r2.port_b, cap.port),
            ],
        )

        t_end = 5.0 * tau_total
        result = simulate(system, t_span=(0.0, t_end), dt=0.01)

        # Same analytical solution, just with R_total
        T_analytical = T0 + Q_val * R_total * (1.0 - np.exp(-result.t / tau_total))
        np.testing.assert_allclose(result[cap.T], T_analytical, rtol=1e-3)


# ── Fixed Temperature Boundary ────────────────────────────────────────────────

class TestFixedTemperature:
    def test_boundary_holds_temperature(self):
        """FixedTemperature boundary keeps port at specified temperature."""
        T_boundary = 400.0
        boundary = FixedTemperature(T=T_boundary)
        cap = ThermalCapacitance(C=1.0, T0=T_boundary)

        system = System(
            components=[boundary, cap],
            connections=[
                connect(boundary.port, cap.port),
            ],
        )

        result = simulate(system, t_span=(0.0, 1.0), dt=0.01)
        # Temperature should stay at boundary value
        np.testing.assert_allclose(result[cap.T], T_boundary, atol=1e-6)

    def test_cooling_to_boundary(self):
        """Hot capacitance cools toward fixed cold boundary through resistance."""
        T_hot = 500.0
        T_cold = 300.0
        R_val = 1.0
        C_val = 2.0
        tau = R_val * C_val

        boundary = FixedTemperature(T=T_cold)
        resistance = ThermalResistance(R=R_val)
        cap = ThermalCapacitance(C=C_val, T0=T_hot)

        system = System(
            components=[boundary, resistance, cap],
            connections=[
                connect(boundary.port, resistance.port_a),
                connect(resistance.port_b, cap.port),
            ],
        )

        t_end = 5.0 * tau
        result = simulate(system, t_span=(0.0, t_end), dt=0.01)

        # Analytical: T(t) = T_cold + (T_hot - T_cold) * exp(-t/tau)
        T_analytical = T_cold + (T_hot - T_cold) * np.exp(-result.t / tau)
        np.testing.assert_allclose(result[cap.T], T_analytical, rtol=1e-3)


# ── Convection ────────────────────────────────────────────────────────────────

class TestConvection:
    def test_convection_acts_like_resistance(self):
        """Convection(h, A) is equivalent to ThermalResistance(R = 1/(h*A))."""
        h_val = 25.0
        A_val = 0.5
        R_equiv = 1.0 / (h_val * A_val)
        C_val = 3.0
        Q_val = 15.0
        T0 = 290.0
        tau = R_equiv * C_val

        source = HeatSource(Q=Q_val)
        conv = Convection(h=h_val, A=A_val)
        cap = ThermalCapacitance(C=C_val, T0=T0)

        system = System(
            components=[source, conv, cap],
            connections=[
                connect(source.port, conv.port_a),
                connect(conv.port_b, cap.port),
            ],
        )

        t_end = 5.0 * tau
        result = simulate(system, t_span=(0.0, t_end), dt=0.01)

        T_analytical = T0 + Q_val * R_equiv * (1.0 - np.exp(-result.t / tau))
        np.testing.assert_allclose(result[cap.T], T_analytical, rtol=1e-3)
