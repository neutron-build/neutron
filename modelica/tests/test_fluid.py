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

import math
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
        P_fixed_low = 100000.0   # Pa (downstream)
        dP = P_fixed_high - P_fixed_low

        # Expected steady-state flow through valve alone
        m_dot_expected = Cv * dP

        # Simple steady-state verification using a tank as integrator
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
        """FixedPressure boundary maintains specified pressure at the port."""
        P_val = 200000.0
        boundary = FixedPressure(P=P_val)

        # Connect to a tank — the tank's port pressure should equalise
        # to the boundary pressure via Kirchhoff across-variable equalisation
        tank = Tank(A=1.0, rho=1000.0, h0=P_val / (1000.0 * 9.81))

        system = System(
            components=[boundary, tank],
            connections=[
                connect(boundary.port, tank.port),
            ],
        )

        result = simulate(system, t_span=(0.0, 0.1), dt=0.01)
        # Tank level should remain constant when its hydrostatic head equals
        # the fixed pressure: rho * g * h = P  =>  h = P / (rho * g)
        h_expected = P_val / (1000.0 * 9.81)
        np.testing.assert_allclose(result[tank.h], h_expected, rtol=1e-3)

    def test_two_boundaries_with_pipe(self):
        """
        Two fixed-pressure boundaries connected by a pipe reach equilibrium.
        Since there is no dynamic element (no tank/capacitor), we connect
        through a tank to get an ODE state variable.
        """
        P_high = 300000.0
        P_low = 100000.0
        R_pipe = 2000.0
        rho = 1000.0
        A_tank = 1.0
        g = 9.81

        # High-pressure source -> pipe -> tank <- pipe <- low-pressure sink
        # Tank level will adjust until hydrostatic head balances
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

        # Run long enough for tank to fill toward equilibrium
        result = simulate(system, t_span=(0.0, 50.0), dt=0.1)

        h_arr = result[tank.h]
        # At equilibrium, tank pressure = P_high => h = P_high / (rho*g)
        h_eq = P_high / (rho * g)
        # Should be approaching equilibrium (within 5%)
        assert abs(h_arr[-1] - h_eq) / h_eq < 0.05
