"""
Phase 2 tests: FMI 2.0 import/export and co-simulation.

All tests run without fmpy, without a C compiler, and without a database.
The round-trip test uses the pickled System embedded in the FMU.
"""

import os
import math
import zipfile
import tempfile
import numpy as np
import pytest

from neutron_sim import Variable, Parameter, System, connect, simulate
from neutron_sim.domains.mechanical import Mass, Spring, Damper, Fixed
from neutron_sim.fmi import export_fmu, import_fmu, ImportedFMU, CoSimulation, OdeStepper


# ── Helpers ───────────────────────────────────────────────────────────────────

def spring_mass_system(m=1.0, k=10.0, x0=1.0):
    """Simple undamped spring-mass (analytical: x(t) = x0 * cos(sqrt(k/m)*t))."""
    mass = Mass(m=m)
    spring = Spring(k=k)
    wall = Fixed()
    system = System(
        components=[mass, spring, wall],
        connections=[
            connect(spring.flange_a, wall.flange),
            connect(spring.flange_b, mass.flange),
        ],
        initial_conditions={mass.x: x0, mass.v: 0.0},
    )
    return system, mass


def damped_system(m=1.0, k=10.0, c=0.5, x0=1.0):
    mass = Mass(m=m)
    spring = Spring(k=k)
    damper = Damper(c=c)
    wall = Fixed()
    system = System(
        components=[mass, spring, damper, wall],
        connections=[
            connect(spring.flange_a, wall.flange),
            connect(spring.flange_b, damper.flange_b, mass.flange),
            connect(damper.flange_a, wall.flange),
        ],
        initial_conditions={mass.x: x0, mass.v: 0.0},
    )
    return system, mass


# ── FMU Export Tests ─────────────────────────────────────────────────────────

class TestFMUExport:
    def test_creates_zip_file(self, tmp_path):
        system, _ = spring_mass_system()
        fmu_path = str(tmp_path / "spring_mass.fmu")
        result_path = export_fmu(system, fmu_path)
        assert os.path.exists(result_path)
        assert zipfile.is_zipfile(result_path)

    def test_contains_model_description(self, tmp_path):
        system, _ = spring_mass_system()
        fmu_path = str(tmp_path / "test.fmu")
        export_fmu(system, fmu_path)
        with zipfile.ZipFile(fmu_path, "r") as zf:
            assert "modelDescription.xml" in zf.namelist()

    def test_contains_neutron_sim_artifacts(self, tmp_path):
        system, _ = spring_mass_system()
        fmu_path = str(tmp_path / "test.fmu")
        export_fmu(system, fmu_path)
        with zipfile.ZipFile(fmu_path, "r") as zf:
            names = zf.namelist()
            assert "resources/neutron_sim_system.pkl" in names
            assert "resources/neutron_sim_meta.json" in names

    def test_model_description_xml_structure(self, tmp_path):
        from xml.etree import ElementTree as ET

        system, mass = spring_mass_system(x0=2.0)
        fmu_path = str(tmp_path / "test.fmu")
        export_fmu(system, fmu_path, model_name="MyModel")

        with zipfile.ZipFile(fmu_path, "r") as zf:
            xml_str = zf.read("modelDescription.xml").decode("utf-8")
        root = ET.fromstring(xml_str)

        assert root.get("fmiVersion") == "2.0"
        assert root.get("modelName") == "MyModel"
        assert root.get("guid") is not None

        vars_el = root.findall("ModelVariables/ScalarVariable")
        assert len(vars_el) >= 1  # at least x (state var)

    def test_state_variables_in_xml(self, tmp_path):
        from xml.etree import ElementTree as ET

        system, mass = spring_mass_system()
        fmu_path = str(tmp_path / "test.fmu")
        export_fmu(system, fmu_path)

        with zipfile.ZipFile(fmu_path, "r") as zf:
            xml_str = zf.read("modelDescription.xml").decode("utf-8")
        root = ET.fromstring(xml_str)

        state_vars = system.state_variables()
        exported_names = {sv.get("name") for sv in root.findall("ModelVariables/ScalarVariable")}
        for sv in state_vars:
            assert sv.name in exported_names

    def test_meta_json_content(self, tmp_path):
        import json

        system, _ = spring_mass_system(x0=3.0)
        fmu_path = str(tmp_path / "test.fmu")
        export_fmu(system, fmu_path)

        with zipfile.ZipFile(fmu_path, "r") as zf:
            meta = json.loads(zf.read("resources/neutron_sim_meta.json"))

        assert meta["type"] == "neutron_sim_fmu"
        assert meta["fmi_version"] == "2.0"
        assert "state_variables" in meta
        assert "initial_conditions" in meta

    def test_initial_conditions_in_meta(self, tmp_path):
        import json

        system, mass = spring_mass_system(x0=5.0)
        fmu_path = str(tmp_path / "test.fmu")
        export_fmu(system, fmu_path)

        with zipfile.ZipFile(fmu_path, "r") as zf:
            meta = json.loads(zf.read("resources/neutron_sim_meta.json"))

        # The state variable for position should have IC = 5.0
        ics = meta["initial_conditions"]
        x_val = ics.get(mass.x.name, None)
        assert x_val == pytest.approx(5.0, abs=1e-9)


# ── FMU Import Tests ─────────────────────────────────────────────────────────

class TestFMUImport:
    @pytest.fixture
    def spring_fmu(self, tmp_path):
        system, mass = spring_mass_system(x0=1.0)
        fmu_path = str(tmp_path / "spring.fmu")
        export_fmu(system, fmu_path)
        return fmu_path, system, mass

    def test_import_returns_importedfmu(self, spring_fmu):
        fmu_path, _, _ = spring_fmu
        fmu = import_fmu(fmu_path)
        assert isinstance(fmu, ImportedFMU)

    def test_is_neutron_fmu(self, spring_fmu):
        fmu_path, _, _ = spring_fmu
        fmu = import_fmu(fmu_path)
        assert fmu.is_neutron_fmu

    def test_model_name(self, spring_fmu, tmp_path):
        system, _ = spring_mass_system()
        fmu_path = str(tmp_path / "my_model.fmu")
        export_fmu(system, fmu_path, model_name="SpringMassModel")
        fmu = import_fmu(fmu_path)
        assert fmu.model_name == "SpringMassModel"

    def test_output_names_match_state_vars(self, spring_fmu):
        fmu_path, system, _ = spring_fmu
        fmu = import_fmu(fmu_path)
        state_var_names = {v.name for v in system.state_variables()}
        assert set(fmu.output_names) == state_var_names

    def test_initial_conditions_preserved(self, spring_fmu):
        fmu_path, _, mass = spring_fmu
        fmu = import_fmu(fmu_path)
        ics = fmu.initial_conditions
        assert mass.x.name in ics
        assert ics[mass.x.name] == pytest.approx(1.0, abs=1e-9)

    def test_variables_have_correct_causality(self, spring_fmu):
        fmu_path, _, _ = spring_fmu
        fmu = import_fmu(fmu_path)
        for var in fmu.variables:
            assert var.causality == "output"  # state vars are outputs

    def test_guid_preserved(self, tmp_path):
        from xml.etree import ElementTree as ET

        system, _ = spring_mass_system()
        fmu_path = str(tmp_path / "guid_test.fmu")
        export_fmu(system, fmu_path)

        # Extract GUID from XML
        with zipfile.ZipFile(fmu_path, "r") as zf:
            xml_str = zf.read("modelDescription.xml").decode("utf-8")
        expected_guid = ET.fromstring(xml_str).get("guid", "").strip("{}")

        fmu = import_fmu(fmu_path)
        assert fmu.guid == expected_guid


# ── Round-Trip Simulation Tests ───────────────────────────────────────────────

class TestFMURoundTrip:
    """Export a System to FMU, import it back, simulate, and compare results."""

    def test_spring_mass_round_trip(self, tmp_path):
        """Results from FMU simulation must match direct simulation."""
        m, k, x0 = 1.0, 10.0, 1.0
        t_span = (0.0, 5.0)

        system, mass = spring_mass_system(m=m, k=k, x0=x0)

        # Direct simulation
        direct = simulate(system, t_span=t_span, dt=0.01)

        # FMU round-trip
        fmu_path = str(tmp_path / "spring.fmu")
        export_fmu(system, fmu_path)
        fmu = import_fmu(fmu_path)
        fmu_result = fmu.simulate(t_span, dt=0.01)

        # Results should be identical (same underlying System after unpickling)
        assert mass.x.name in fmu_result
        np.testing.assert_allclose(
            fmu_result[mass.x.name],
            direct[mass.x.name],
            rtol=1e-4,
        )

    def test_damped_system_round_trip(self, tmp_path):
        t_span = (0.0, 4.0)
        system, mass = damped_system(m=1.0, k=10.0, c=0.5, x0=1.0)
        direct = simulate(system, t_span=t_span, dt=0.01)

        fmu_path = str(tmp_path / "damped.fmu")
        export_fmu(system, fmu_path)
        fmu = import_fmu(fmu_path)
        fmu_result = fmu.simulate(t_span, dt=0.01)

        np.testing.assert_allclose(
            fmu_result[mass.x.name],
            direct[mass.x.name],
            rtol=1e-4,
        )

    def test_round_trip_initial_conditions_respected(self, tmp_path):
        """Initial condition x0=2.5 must be preserved through FMU round-trip."""
        system, mass = spring_mass_system(x0=2.5)
        fmu_path = str(tmp_path / "ic_test.fmu")
        export_fmu(system, fmu_path)
        fmu = import_fmu(fmu_path)
        result = fmu.simulate((0.0, 1.0), dt=0.01)

        # At t=0, x should be 2.5
        assert result[mass.x.name][0] == pytest.approx(2.5, abs=1e-3)


# ── Co-Simulation Tests ───────────────────────────────────────────────────────

class TestOdeStepper:
    def test_stepper_advances_time(self):
        system, mass = spring_mass_system()
        stepper = OdeStepper(system)
        stepper.initialize(0.0)
        assert stepper.t == pytest.approx(0.0)
        stepper.step(0.01)
        assert stepper.t == pytest.approx(0.01)

    def test_stepper_state_has_all_vars(self):
        system, mass = spring_mass_system()
        stepper = OdeStepper(system)
        stepper.initialize(0.0)
        state = stepper.state_dict()
        for sv in system.state_variables():
            assert sv.name in state

    def test_stepper_correct_dynamics(self):
        """Undamped spring-mass: x(dt) ≈ x0 + v0*dt for small dt."""
        system, mass = spring_mass_system(x0=1.0)
        stepper = OdeStepper(system)
        stepper.initialize(0.0)
        state = stepper.step(1e-4)  # tiny step
        xname = mass.x.name
        vname = mass.v.name
        # x should barely change, v should become slightly negative (spring pulls back)
        assert abs(state[xname] - 1.0) < 0.01
        assert state[vname] < 0.0  # spring accelerates mass toward origin


class TestCoSimulation:
    def test_two_independent_models(self):
        """Two independent spring-mass systems in co-sim produce same results
        as direct simulation."""
        t_span = (0.0, 2.0)
        dt = 0.001

        sys1, mass1 = spring_mass_system(m=1.0, k=10.0, x0=1.0)
        sys2, mass2 = spring_mass_system(m=2.0, k=5.0, x0=0.5)

        # Direct simulations
        ref1 = simulate(sys1, t_span=t_span, dt=dt)
        ref2 = simulate(sys2, t_span=t_span, dt=dt)

        # Co-simulation (no coupling)
        cosim = CoSimulation(
            models=[("sys1", sys1), ("sys2", sys2)],
            connections=[],
            step_size=dt,
        )
        results = cosim.run(t_span)

        assert "sys1" in results
        assert "sys2" in results

        np.testing.assert_allclose(
            results["sys1"][mass1.x.name],
            ref1[mass1.x.name],
            rtol=1e-2,  # RK4 vs solve_ivp may differ slightly
        )
        np.testing.assert_allclose(
            results["sys2"][mass2.x.name],
            ref2[mass2.x.name],
            rtol=1e-2,
        )

    def test_cosim_time_array_correct(self):
        t_span = (0.0, 0.5)
        dt = 0.01
        sys1, _ = spring_mass_system()

        cosim = CoSimulation(models=[("m", sys1)], step_size=dt)
        results = cosim.run(t_span)

        t_arr = results["m"].t
        assert t_arr[0] == pytest.approx(0.0)
        assert t_arr[-1] == pytest.approx(0.5, abs=dt * 1.1)

    def test_cosim_result_type(self):
        from neutron_sim.solvers.ode import SimulationResult

        sys1, _ = spring_mass_system()
        cosim = CoSimulation(models=[("sys", sys1)], step_size=0.01)
        results = cosim.run((0.0, 0.1))

        assert isinstance(results["sys"], SimulationResult)

    def test_cosim_energy_conservation_undamped(self):
        """Energy should be approximately conserved in an undamped system."""
        m, k, x0 = 1.0, 10.0, 1.0
        t_span = (0.0, 2.0)
        dt = 0.001

        sys1, mass1 = spring_mass_system(m=m, k=k, x0=x0)
        cosim = CoSimulation(models=[("spring_mass", sys1)], step_size=dt)
        results = cosim.run(t_span)

        r = results["spring_mass"]
        xname = mass1.x.name
        vname = mass1.v.name

        E0 = 0.5 * k * x0 ** 2  # initial potential energy
        E = 0.5 * m * r[vname] ** 2 + 0.5 * k * r[xname] ** 2
        # Energy should be within 2% of initial (RK4 error at dt=0.001)
        np.testing.assert_allclose(E, E0, rtol=0.02)
