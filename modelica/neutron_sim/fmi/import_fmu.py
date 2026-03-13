"""Import an FMU as a neutron_sim model."""

from __future__ import annotations
import json
import pickle
import zipfile
from dataclasses import dataclass
from xml.etree import ElementTree as ET


@dataclass
class FMUVariable:
    """Describes one scalar variable in an FMU."""
    name: str
    value_reference: int
    causality: str   # "input", "output", "local", "parameter", ...
    variability: str
    start: float


class ImportedFMU:
    """
    Lightweight wrapper around an imported FMU.

    For **neutron_sim FMUs** (exported by :func:`export_fmu`): the pickled
    System is loaded and used directly — no fmpy or C compiler required.

    For **external FMUs** (from Dymola, Simulink, OpenModelica, …): fmpy is
    required for execution.
    """

    def __init__(
        self,
        filename: str,
        model_name: str,
        guid: str,
        variables: list[FMUVariable],
        system=None,
        meta: dict | None = None,
    ):
        self.filename = filename
        self.model_name = model_name
        self.guid = guid
        self.variables = variables
        self._system = system       # Present only for neutron_sim FMUs
        self._meta = meta or {}
        self.is_neutron_fmu = system is not None

    # --- convenience properties ------------------------------------------

    @property
    def output_names(self) -> list[str]:
        return [v.name for v in self.variables if v.causality == "output"]

    @property
    def input_names(self) -> list[str]:
        return [v.name for v in self.variables if v.causality == "input"]

    @property
    def initial_conditions(self) -> dict[str, float]:
        return {v.name: v.start for v in self.variables}

    # --- simulation -------------------------------------------------------

    def simulate(self, t_span: tuple[float, float], dt: float | None = None):
        """Simulate the FMU over *t_span*.

        For neutron_sim FMUs uses the embedded Python System directly.
        For external FMUs delegates to fmpy (must be installed).
        """
        if self.is_neutron_fmu:
            return self._simulate_neutron(t_span, dt)
        return self._simulate_fmpy(t_span, dt)

    def _simulate_neutron(self, t_span, dt):
        from ..solvers.ode import simulate
        return simulate(self._system, t_span=t_span, dt=dt)

    def _simulate_fmpy(self, t_span, dt):
        try:
            import fmpy
        except ImportError:
            raise ImportError(
                "fmpy is required to simulate external FMUs.\n"
                "Install with: pip install fmpy"
            )
        result = fmpy.simulate_fmu(
            self.filename,
            start_time=t_span[0],
            stop_time=t_span[1],
            output_interval=dt,
        )
        import numpy as np
        from ..solvers.ode import SimulationResult
        names = [n.decode() if isinstance(n, bytes) else n for n in result.dtype.names]
        t = result["time"] if "time" in names else np.array([])
        y = np.vstack([result[n] for n in names if n != "time"])
        return SimulationResult(
            t=t,
            state_names=[n for n in names if n != "time"],
            y=y,
        )

    def __repr__(self) -> str:
        kind = "neutron_sim" if self.is_neutron_fmu else "external"
        return (
            f"ImportedFMU('{self.model_name}', {kind}, "
            f"outputs={self.output_names})"
        )


def import_fmu(filename: str) -> ImportedFMU:
    """Import an FMU from *filename* and return an :class:`ImportedFMU`.

    Reads ``modelDescription.xml`` (always present) and, for neutron_sim FMUs,
    ``resources/neutron_sim_system.pkl`` (enables direct simulation).
    """
    with zipfile.ZipFile(filename, "r") as zf:
        # ── Parse modelDescription.xml ──────────────────────────────────
        xml_bytes = zf.read("modelDescription.xml")
        root = ET.fromstring(xml_bytes)

        model_name = root.get("modelName", "unknown")
        guid = root.get("guid", "").strip("{}")

        variables: list[FMUVariable] = []
        for sv in root.findall("ModelVariables/ScalarVariable"):
            real = sv.find("Real")
            start = float(real.get("start", "0.0")) if real is not None else 0.0
            variables.append(
                FMUVariable(
                    name=sv.get("name", ""),
                    value_reference=int(sv.get("valueReference", "0")),
                    causality=sv.get("causality", "local"),
                    variability=sv.get("variability", "continuous"),
                    start=start,
                )
            )

        # ── Try to load neutron_sim System (pickle) ──────────────────────
        system = None
        meta: dict = {}
        names_in_zip = zf.namelist()

        if "resources/neutron_sim_meta.json" in names_in_zip:
            meta = json.loads(
                zf.read("resources/neutron_sim_meta.json").decode("utf-8")
            )

        if "resources/neutron_sim_system.pkl" in names_in_zip:
            system = pickle.loads(zf.read("resources/neutron_sim_system.pkl"))

    return ImportedFMU(filename, model_name, guid, variables, system, meta)
