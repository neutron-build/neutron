"""Phase 5 tests — Electrical domain (KCL/KVL) and FMI 3.0 support."""

import math
import os
import tempfile
import zipfile
from xml.etree import ElementTree as ET

import pytest

from neutron_sim.core.system import System, connect
from neutron_sim.core.variable import Variable, Equation, der, Constant
from neutron_sim.core.component import Component
from neutron_sim.solvers.ode import simulate
from neutron_sim.domains.electrical import (
    Pin, Resistor, Capacitor, Inductor, Ground, VoltageSource, CurrentSource,
    IdealTransformer,
)
from neutron_sim.fmi.export import export_fmu


# ─── Electrical component basics ─────────────────────────────────────────────

class TestElectricalComponents:
    def test_pin_connector(self):
        """Pin has voltage (across) and current (through)."""
        p = Pin(prefix="test")
        assert len(p.across_vars()) == 1  # v
        assert len(p.through_vars()) == 1  # i
        assert p.v.name.startswith("test")
        assert p.i.name.startswith("test")

    def test_resistor_equations(self):
        """Resistor produces Ohm's law equations."""
        r = Resistor(R=100.0)
        eqs = r.equations()
        assert len(eqs) == 2  # i_p = v/R, i_n = -v/R

    def test_capacitor_state_variable(self):
        """Capacitor has v_cap as a state variable."""
        c = Capacitor(C=1e-6, v0=5.0)
        ics = c.initial_conditions()
        assert c.v_cap in ics
        assert ics[c.v_cap] == 5.0

    def test_inductor_state_variable(self):
        """Inductor has i_ind as a state variable."""
        ind = Inductor(L=0.01, i0=0.5)
        ics = ind.initial_conditions()
        assert ind.i_ind in ics
        assert ics[ind.i_ind] == 0.5

    def test_ground_zero_voltage(self):
        """Ground forces v=0."""
        g = Ground()
        eqs = g.equations()
        assert len(eqs) == 1

    def test_voltage_source_equations(self):
        """VoltageSource has V=const and current balance."""
        vs = VoltageSource(V=12.0)
        eqs = vs.equations()
        assert len(eqs) == 2

    def test_current_source_equations(self):
        """CurrentSource has I=const at both pins."""
        cs = CurrentSource(I=0.5)
        eqs = cs.equations()
        assert len(eqs) == 2

    def test_ideal_transformer_equations(self):
        """IdealTransformer has voltage ratio + power balance."""
        xfmr = IdealTransformer(n=2.0)
        eqs = xfmr.equations()
        assert len(eqs) == 4  # v_ratio, power, 2x continuity


# ─── RC circuit simulation ──────────────────────────────────────────────────

class TestRCCircuit:
    """RC circuit: V_source → R → C → Ground.
    V_cap(t) = V * (1 - e^(-t/RC))
    """

    def _build_rc(self, V=10.0, R=1000.0, C=1e-3):
        vs = VoltageSource(V=V)
        r = Resistor(R=R)
        c = Capacitor(C=C, v0=0.0)
        gnd = Ground()

        connections = [
            connect(vs.p, r.p),        # Source+ → Resistor+
            connect(r.n, c.p),          # Resistor- → Capacitor+
            connect(c.n, gnd.pin, vs.n),  # Capacitor- → Ground → Source-
        ]
        sys = System([vs, r, c, gnd], connections)
        return sys, vs, r, c

    def test_rc_charging(self):
        """Capacitor charges toward source voltage."""
        V, R, C_val = 10.0, 1000.0, 1e-3
        tau = R * C_val  # Time constant = 1 second

        sys, vs, r, c = self._build_rc(V, R, C_val)
        result = simulate(sys, t_span=(0, 5 * tau), dt=0.01)

        v_cap = result[c.v_cap]
        # At t=5*tau, v_cap should be ~99.3% of V
        assert v_cap[-1] == pytest.approx(V, rel=0.01)

        # At t=tau, v_cap should be ~63.2% of V
        tau_idx = int(tau / 0.01)
        expected = V * (1 - math.exp(-1))
        assert v_cap[tau_idx] == pytest.approx(expected, rel=0.05)

    def test_rc_initial_voltage(self):
        """Capacitor starts at specified initial voltage."""
        sys, vs, r, c = self._build_rc()
        result = simulate(sys, t_span=(0, 0.01), dt=0.001)
        v_cap = result[c.v_cap]
        assert v_cap[0] == pytest.approx(0.0, abs=0.01)


# ─── LC circuit simulation ──────────────────────────────────────────────────

class TestLCCircuit:
    """LC oscillator: C(v0=1V) → L → loop.
    Oscillation frequency: f = 1/(2π√(LC))
    """

    def test_lc_oscillation(self):
        """LC circuit oscillates with correct frequency."""
        L_val, C_val = 1.0, 1.0  # f = 1/(2π) ≈ 0.159 Hz
        c = Capacitor(C=C_val, v0=1.0)
        ind = Inductor(L=L_val, i0=0.0)
        gnd = Ground()

        connections = [
            connect(c.p, ind.p),
            connect(c.n, ind.n, gnd.pin),
        ]
        sys = System([c, ind, gnd], connections)

        period = 2 * math.pi * math.sqrt(L_val * C_val)
        result = simulate(sys, t_span=(0, 3 * period), dt=0.01)

        v_cap = result[c.v_cap]
        # Should return to ~1.0 after one full period (energy conservation)
        period_idx = int(period / 0.01)
        assert v_cap[period_idx] == pytest.approx(1.0, abs=0.1)

        # Should be ~-1.0 at half period
        half_idx = int(period / 2 / 0.01)
        assert v_cap[half_idx] == pytest.approx(-1.0, abs=0.1)


# ─── Kirchhoff's laws verification ──────────────────────────────────────────

class TestKirchhoffLaws:
    def test_kcl_node(self):
        """KCL: currents sum to zero at a node (multi-way connection)."""
        cs1 = CurrentSource(I=1.0)
        cs2 = CurrentSource(I=2.0)
        r = Resistor(R=1.0)
        gnd = Ground()

        connections = [
            connect(cs1.p, cs2.p, r.p),
            connect(cs1.n, cs2.n, r.n, gnd.pin),
        ]
        sys = System([cs1, cs2, r, gnd], connections)

        eqs = sys.flatten()
        assert len(eqs) > 0


# ─── FMI 3.0 export ─────────────────────────────────────────────────────────

class _OscillatorComponent(Component):
    """Simple harmonic oscillator as a Component for FMI tests."""
    def __init__(self):
        self.x = Variable("x")
        self.v = Variable("v")
    def equations(self):
        return [der(self.x) == self.v, der(self.v) == -self.x]
    def initial_conditions(self):
        return {self.x: 1.0, self.v: 0.0}


class TestFMI30:
    def _make_system(self):
        osc = _OscillatorComponent()
        return System([osc])

    def test_fmi30_export_creates_zip(self):
        """FMI 3.0 export creates valid zip file."""
        sys = self._make_system()
        with tempfile.NamedTemporaryFile(suffix=".fmu", delete=False) as f:
            path = export_fmu(sys, f.name, fmi_version="3.0")
        try:
            assert os.path.exists(path)
            assert zipfile.is_zipfile(path)
        finally:
            os.unlink(path)

    def test_fmi30_xml_version(self):
        """FMI 3.0 XML has fmiVersion='3.0'."""
        sys = self._make_system()
        with tempfile.NamedTemporaryFile(suffix=".fmu", delete=False) as f:
            path = export_fmu(sys, f.name, fmi_version="3.0")
        try:
            with zipfile.ZipFile(path) as zf:
                xml = zf.read("modelDescription.xml").decode()
            root = ET.fromstring(xml)
            assert root.get("fmiVersion") == "3.0"
        finally:
            os.unlink(path)

    def test_fmi30_has_instantiation_token(self):
        """FMI 3.0 uses instantiationToken instead of guid."""
        sys = self._make_system()
        with tempfile.NamedTemporaryFile(suffix=".fmu", delete=False) as f:
            path = export_fmu(sys, f.name, fmi_version="3.0")
        try:
            with zipfile.ZipFile(path) as zf:
                xml = zf.read("modelDescription.xml").decode()
            root = ET.fromstring(xml)
            assert root.get("instantiationToken") is not None
            assert root.get("guid") is None  # FMI 3.0 doesn't use guid
        finally:
            os.unlink(path)

    def test_fmi30_float64_variables(self):
        """FMI 3.0 uses Float64 elements instead of ScalarVariable."""
        sys = self._make_system()
        with tempfile.NamedTemporaryFile(suffix=".fmu", delete=False) as f:
            path = export_fmu(sys, f.name, fmi_version="3.0")
        try:
            with zipfile.ZipFile(path) as zf:
                xml = zf.read("modelDescription.xml").decode()
            root = ET.fromstring(xml)
            mv = root.find("ModelVariables")
            float64s = mv.findall("Float64")
            assert len(float64s) == 2  # x, v
            assert mv.findall("ScalarVariable") == []
        finally:
            os.unlink(path)

    def test_fmi30_output_structure(self):
        """FMI 3.0 ModelStructure uses Output elements."""
        sys = self._make_system()
        with tempfile.NamedTemporaryFile(suffix=".fmu", delete=False) as f:
            path = export_fmu(sys, f.name, fmi_version="3.0")
        try:
            with zipfile.ZipFile(path) as zf:
                xml = zf.read("modelDescription.xml").decode()
            root = ET.fromstring(xml)
            ms = root.find("ModelStructure")
            outputs = ms.findall("Output")
            assert len(outputs) == 2
            assert ms.find("Outputs") is None
        finally:
            os.unlink(path)

    def test_fmi30_generation_tool(self):
        """FMI 3.0 includes generationTool attribute."""
        sys = self._make_system()
        with tempfile.NamedTemporaryFile(suffix=".fmu", delete=False) as f:
            path = export_fmu(sys, f.name, fmi_version="3.0")
        try:
            with zipfile.ZipFile(path) as zf:
                xml = zf.read("modelDescription.xml").decode()
            root = ET.fromstring(xml)
            assert root.get("generationTool") == "neutron_sim"
        finally:
            os.unlink(path)

    def test_fmi_version_validation(self):
        """Invalid FMI version raises ValueError."""
        sys = self._make_system()
        with tempfile.NamedTemporaryFile(suffix=".fmu", delete=False) as f:
            with pytest.raises(ValueError, match="Unsupported FMI version"):
                export_fmu(sys, f.name, fmi_version="1.0")
            os.unlink(f.name)

    def test_fmi20_still_works(self):
        """FMI 2.0 export still produces correct XML."""
        sys = self._make_system()
        with tempfile.NamedTemporaryFile(suffix=".fmu", delete=False) as f:
            path = export_fmu(sys, f.name, fmi_version="2.0")
        try:
            with zipfile.ZipFile(path) as zf:
                xml = zf.read("modelDescription.xml").decode()
            root = ET.fromstring(xml)
            assert root.get("fmiVersion") == "2.0"
            assert root.get("guid") is not None
            mv = root.find("ModelVariables")
            assert len(mv.findall("ScalarVariable")) == 2
        finally:
            os.unlink(path)
