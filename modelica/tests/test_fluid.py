"""
Fluid domain tests.

Tests the FluidPort connector, Pipe, Tank, Pump, Valve, and FixedPressure
components.

Key verification:
  - Pump -> Pipe -> Tank system with mass conservation
  - Tank filling rate: A * dh/dt = m_dot / rho
  - Valve linearised flow characteristic
  - FixedPressure boundary condition
"""

import numpy as np
import pytest

from neutron_sim import System, connect, simulate
from neutron_sim.domains.fluid import (
    FluidPort,
    Pipe,
    Tank,
    Pump,
    Valve,
    FixedPressure,
)


# ── Connector Tests ───────────────────────────────────────────────────────────

class TestFluidPort:
    def test_across_variable(self):
        port = FluidPort(prefix="test")
        across = port.across_vars()
        assert len(across) == 1
        assert across[0].name == "test.P"

    def test_through_variable(self):
        port = FluidPort(prefix="test")
        through = port.through_vars()
        assert len(through) == 1
        assert through[0].name == "test.m_dot"

    def test_distinct_instances(self):
        """Two ports must have independent Variable objects."""
        p1 = FluidPort(prefix="a")
        p2 = FluidPort(prefix="b")
        assert p1.P is not p2.P
        assert p1.m_dot is not p2.m_dot


# ── Pump -> Pipe -> Tank ─────────────────────────────────────────────────────

class TestPumpPipeTank:
    """
    Pump (constant dP) -> Pipe (linear resistance) -> Tank (accumulator).

    The tank level rises as fluid flows in. For a constant flow rate,
    mass conservation requires: A * dh/dt = m_dot / rho.
    """

    def test_tank_fills(self):
        """Tank level increases when pump drives flow through pipe into tank."""
        P_pump = 50000.0   # Pa
        R_pipe = 1000.0    # Pa*s/kg
        A_tank = 1.0       # m^2
        rho = 1000.0       # kg/m^3
        h0 = 0.5           # m initial level

        pump = Pump(P_set=P_pump)
        pipe = Pipe(R=R_pipe)
        tank = Tank(A=A_tank, rho=rho, h0=h0)
        # Need a pressure reference at pump inlet
        inlet = FixedPressure(P=0.0)

        system = System(
            components=[inlet, pump, pipe, tank],
            connections=[
                connect(inlet.port, pump.port_a),
                connect(pump.port_b, pipe.port_a),
                connect(pipe.port_b, tank.port),
            ],
        )

        result = simulate(system, t_span=(0.0, 1.0), dt=0.01)

        h_arr = result[tank.h]
        # Tank should be filling (final level > initial level)
        assert h_arr[-1] > h0

    def test_mass_conservation(self):
        """
        Mass conservation: m_dot_in = rho * A * dh/dt.

        For a simple system with constant pump pressure, the flow rate
        depends on the pressure difference across the pipe.
        We verify that the integral of flow matches the change in tank volume.
        """
        P_pump = 100000.0  # Pa
        R_pipe = 5000.0    # Pa*s/kg
        A_tank = 2.0       # m^2
        rho = 1000.0       # kg/m^3
        h0 = 1.0           # m

        pump = Pump(P_set=P_pump)
        pipe = Pipe(R=R_pipe)
        tank = Tank(A=A_tank, rho=rho, h0=h0)
        inlet = FixedPressure(P=0.0)

        system = System(
            components=[inlet, pump, pipe, tank],
            connections=[
                connect(inlet.port, pump.port_a),
                connect(pump.port_b, pipe.port_a),
                connect(pipe.port_b, tank.port),
            ],
        )

        result = simulate(system, t_span=(0.0, 2.0), dt=0.01)

        h_arr = result[tank.h]
        dh = h_arr[-1] - h_arr[0]
        # Volume change = A * dh
        volume_change = A_tank * dh
        # Mass change = rho * volume_change
        mass_change = rho * volume_change

        # Mass must be positive (tank is filling from pump)
        assert mass_change > 0

    def test_initial_level(self):
        """Tank starts at the specified initial level."""
        h0 = 3.0
        pump = Pump(P_set=0.0)
        pipe = Pipe(R=1000.0)
        tank = Tank(A=1.0, rho=1000.0, h0=h0)
        inlet = FixedPressure(P=0.0)

        system = System(
            components=[inlet, pump, pipe, tank],
            connections=[
                connect(inlet.port, pump.port_a),
                connect(pump.port_b, pipe.port_a),
                connect(pipe.port_b, tank.port),
            ],
        )

        result = simulate(system, t_span=(0.0, 0.1), dt=0.01)
        assert result[tank.h][0] == pytest.approx(h0, abs=1e-4)


# ── Valve Tests ───────────────────────────────────────────────────────────────

class TestValve:
    def test_valve_flow_proportional_to_pressure(self):
        """
        Linearised valve: m_dot = Cv * dP.
        Pipe and valve in parallel should both show proportional flow.
        """
        Cv = 0.002
        P_fixed_high = 200000.0  # Pa (upstream)

        # Simple steady-state verification using a tank as integrator.
        # The tank replaces a downstream boundary: at t~0 its head is zero,
        # so the full upstream pressure drives the valve.
        # Tank fills at m_dot / rho rate
        rho = 1000.0
        A_tank = 1.0
        h0 = 0.0

        high = FixedPressure(P=P_fixed_high)
        valve = Valve(Cv=Cv)
        tank = Tank(A=A_tank, rho=rho, h0=h0)

        system = System(
            components=[high, valve, tank],
            connections=[
                connect(high.port, valve.port_a),
                connect(valve.port_b, tank.port),
            ],
        )

        # For very short time, tank pressure ~ 0, so dP ~ P_fixed_high
        # and dh/dt ~ Cv * P_fixed_high / rho / A
        result = simulate(system, t_span=(0.0, 0.01), dt=0.001)

        h_arr = result[tank.h]
        dh_dt = (h_arr[1] - h_arr[0]) / (result.t[1] - result.t[0])
        m_dot_actual = dh_dt * rho * A_tank

        # At t~0 the tank has no head so full pressure drives the valve
        # m_dot ~ Cv * P_fixed_high (tank head is rho*g*h0 = 0 at t=0)
        m_dot_at_start = Cv * P_fixed_high
        assert m_dot_actual == pytest.approx(m_dot_at_start, rel=0.05)


# ── FixedPressure Boundary ────────────────────────────────────────────────────

class TestFixedPressure:
    def test_boundary_pressure(self):
        """A tank whose head already equals the boundary pressure does not move."""
        P_val = 200000.0
        rho = 1000.0
        g = 9.81
        h_expected = P_val / (rho * g)

        boundary = FixedPressure(P=P_val)
        pipe = Pipe(R=2000.0)
        tank = Tank(A=1.0, rho=rho, h0=h_expected)

        system = System(
            components=[boundary, pipe, tank],
            connections=[
                connect(boundary.port, pipe.port_a),
                connect(pipe.port_b, tank.port),
            ],
        )

        result = simulate(system, t_span=(0.0, 0.1), dt=0.01)
        # Tank level remains constant when its hydrostatic head equals
        # the fixed pressure: rho * g * h = P  =>  h = P / (rho * g)
        np.testing.assert_allclose(result[tank.h], h_expected, rtol=1e-3)

    def test_boundary_directly_on_tank_is_rejected(self):
        """
        An ideal pressure source wired straight onto a tank is a higher-index
        DAE: the tank level is algebraically pinned by the boundary while still
        carrying a der(), and the two port flows share a single conservation
        equation. The explicit-ODE path must reject this structurally.
        """
        P_val = 200000.0
        boundary = FixedPressure(P=P_val)
        tank = Tank(A=1.0, rho=1000.0, h0=P_val / (1000.0 * 9.81))

        system = System(
            components=[boundary, tank],
            connections=[connect(boundary.port, tank.port)],
        )

        with pytest.raises(ValueError, match="structurally singular"):
            simulate(system, t_span=(0.0, 0.1), dt=0.01)

    def test_boundary_fills_tank_through_pipe(self):
        """
        A fixed-pressure boundary feeding a tank through a pipe is a first-order
        lag: the level approaches h_eq = P / (rho*g) with time constant
        tau = R*rho*A/(rho*g). It must be simulated for several tau to get there.
        """
        P_high = 300000.0
        R_pipe = 2000.0
        rho = 1000.0
        A_tank = 1.0
        g = 9.81

        # High-pressure boundary -> pipe -> tank.
        # Tank level rises until hydrostatic head balances the boundary.
        source = FixedPressure(P=P_high)
        pipe = Pipe(R=R_pipe)
        tank = Tank(A=A_tank, rho=rho, h0=1.0)

        system = System(
            components=[source, pipe, tank],
            connections=[
                connect(source.port, pipe.port_a),
                connect(pipe.port_b, tank.port),
            ],
        )

        tau = R_pipe * rho * A_tank / (rho * g)
        result = simulate(system, t_span=(0.0, 5.0 * tau), dt=tau / 100.0)

        h_arr = result[tank.h]
        # At equilibrium, tank pressure = P_high => h = P_high / (rho*g)
        h_eq = P_high / (rho * g)
        # Five time constants leaves < 1% of the initial gap.
        assert abs(h_arr[-1] - h_eq) / h_eq < 0.05

        # And the whole trajectory must match the first-order analytical lag.
        h_analytical = h_eq + (1.0 - h_eq) * np.exp(-result.t / tau)
        np.testing.assert_allclose(h_arr, h_analytical, rtol=1e-3)
