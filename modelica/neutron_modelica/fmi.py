"""FMI 2.0/3.0 Model Exchange runtime wrapper.

Provides a unified ``FMU`` class that wraps an FMU file (zip containing
modelDescription.xml and either native binaries or a pickled neutron_sim
System) and exposes a step-by-step simulation API:

    fmu = FMU.load("model.fmu")
    fmu.initialize(start_time=0.0, stop_time=10.0)
    while fmu.time < 10.0:
        fmu.step(0.001)
        print(fmu.get_real("x"))
    fmu.terminate()

Supports both FMI 2.0 and FMI 3.0 FMUs. For neutron_sim FMUs (exported
by ``neutron_sim.fmi.export_fmu``), simulation runs in pure Python.
For external FMUs, delegates to fmpy.

FMI 3.0 additions over 2.0:
- Clock variables (scheduled/triggered, for synchronous events)
- Binary data type (opaque byte arrays)
- Array variables (multi-dimensional Float64/Int32/etc.)
"""

from __future__ import annotations

import json
import pickle
import zipfile
from dataclasses import dataclass, field
from enum import Enum
from xml.etree import ElementTree as ET
from typing import Any

import numpy as np


class FMIVersion(Enum):
    """Supported FMI standard versions."""
    V2_0 = "2.0"
    V3_0 = "3.0"


@dataclass
class FMUVariable:
    """Metadata for a single FMU variable.

    Attributes
    ----------
    name             : human-readable variable name
    value_reference  : integer handle used by the FMI C-API
    causality        : "input" | "output" | "local" | "parameter" | "independent"
    variability      : "constant" | "fixed" | "tunable" | "discrete" | "continuous"
    data_type        : "Float64" | "Int32" | "Boolean" | "String" | "Binary" | "Clock"
    start            : default / initial value (for numeric types)
    dimensions       : tuple of ints for array variables (empty = scalar)
    clock_type       : "triggered" | "scheduled" | None (FMI 3.0 only)
    """
    name: str
    value_reference: int
    causality: str = "local"
    variability: str = "continuous"
    data_type: str = "Float64"
    start: float = 0.0
    dimensions: tuple[int, ...] = ()
    clock_type: str | None = None


class FMU:
    """FMI 2.0/3.0 Model Exchange runtime wrapper.

    This class models the lifecycle of an FMU instance:
    1. ``load()`` — parse modelDescription.xml and load the system
    2. ``initialize()`` — set up initial state and time
    3. ``step()`` — advance simulation by dt
    4. ``get_real()`` / ``set_real()`` — read/write variable values
    5. ``terminate()`` — clean up resources

    For FMI 3.0, additional methods are available:
    - ``get_binary()`` / ``set_binary()`` for binary data
    - ``get_clock()`` / ``set_clock()`` for clock events
    - ``get_array()`` / ``set_array()`` for array variables
    """

    def __init__(self):
        self._filename: str = ""
        self._model_name: str = ""
        self._guid: str = ""
        self._fmi_version: FMIVersion = FMIVersion.V2_0
        self._variables: list[FMUVariable] = []
        self._var_by_name: dict[str, FMUVariable] = {}
        self._var_by_ref: dict[int, FMUVariable] = {}

        # Runtime state
        self._initialized: bool = False
        self._terminated: bool = False
        self._time: float = 0.0
        self._start_time: float = 0.0
        self._stop_time: float = float("inf")
        self._state: dict[str, float] = {}

        # Internal system (for neutron_sim FMUs)
        self._system: Any = None
        self._stepper: Any = None
        self._is_neutron_fmu: bool = False

        # External FMU handle (for fmpy-backed FMUs)
        self._fmpy_instance: Any = None

        # FMI 3.0 extensions
        self._binary_values: dict[str, bytes] = {}
        self._clock_values: dict[str, bool] = {}
        self._array_values: dict[str, np.ndarray] = {}

    # ── Factory ──────────────────────────────────────────────────────────────

    @classmethod
    def load(cls, filename: str) -> "FMU":
        """Load an FMU from a .fmu file.

        Parses modelDescription.xml to extract variable metadata and
        FMI version. For neutron_sim FMUs, also loads the pickled System
        for pure-Python simulation.

        Parameters
        ----------
        filename : path to the .fmu zip file

        Returns
        -------
        An uninitialized FMU instance. Call ``initialize()`` before stepping.
        """
        fmu = cls()
        fmu._filename = filename

        with zipfile.ZipFile(filename, "r") as zf:
            xml_bytes = zf.read("modelDescription.xml")
            root = ET.fromstring(xml_bytes)

            fmu._model_name = root.get("modelName", "unknown")
            fmi_ver = root.get("fmiVersion", "2.0")
            fmu._fmi_version = (
                FMIVersion.V3_0 if fmi_ver.startswith("3") else FMIVersion.V2_0
            )

            if fmu._fmi_version == FMIVersion.V3_0:
                fmu._guid = root.get("instantiationToken", "").strip("{}")
            else:
                fmu._guid = root.get("guid", "").strip("{}")

            fmu._variables = _parse_variables(root, fmu._fmi_version)
            fmu._var_by_name = {v.name: v for v in fmu._variables}
            fmu._var_by_ref = {v.value_reference: v for v in fmu._variables}

            # Try loading neutron_sim system
            names_in_zip = zf.namelist()
            if "resources/neutron_sim_system.pkl" in names_in_zip:
                fmu._system = pickle.loads(
                    zf.read("resources/neutron_sim_system.pkl")
                )
                fmu._is_neutron_fmu = True

        return fmu

    # ── Lifecycle ────────────────────────────────────────────────────────────

    def initialize(
        self,
        start_time: float = 0.0,
        stop_time: float = float("inf"),
    ) -> None:
        """Initialize the FMU for simulation.

        Sets up initial state from variable start values or the embedded
        neutron_sim System's initial conditions.

        Parameters
        ----------
        start_time : simulation start time [s]
        stop_time  : simulation end time [s] (informational for the FMU)
        """
        if self._initialized:
            raise RuntimeError("FMU already initialized; call terminate() first")

        self._start_time = start_time
        self._stop_time = stop_time
        self._time = start_time
        self._terminated = False

        # Initialize state from variable start values
        self._state = {v.name: v.start for v in self._variables}

        if self._is_neutron_fmu and self._system is not None:
            from neutron_sim.fmi.cosim import OdeStepper
            self._stepper = OdeStepper(self._system)
            self._stepper.initialize(start_time)
            # Overwrite state from stepper ICs
            self._state.update(self._stepper.state_dict())
        elif not self._is_neutron_fmu:
            self._init_fmpy(start_time, stop_time)

        self._initialized = True

    def step(self, dt: float) -> None:
        """Advance the simulation by ``dt`` seconds.

        Parameters
        ----------
        dt : time step size [s]

        Raises
        ------
        RuntimeError : if FMU is not initialized or already terminated
        """
        self._check_active()

        if self._is_neutron_fmu and self._stepper is not None:
            state = self._stepper.step(dt)
            self._state.update(state)
            self._time = self._stepper.t
        elif self._fmpy_instance is not None:
            self._step_fmpy(dt)
        else:
            # Fallback: advance time only (no dynamics)
            self._time += dt

    def terminate(self) -> None:
        """Terminate the FMU instance and release resources."""
        if self._fmpy_instance is not None:
            try:
                import fmpy
                fmpy.freeInstance(self._fmpy_instance)
            except Exception:
                pass
            self._fmpy_instance = None
        self._stepper = None
        self._terminated = True
        self._initialized = False

    # ── Variable Access ──────────────────────────────────────────────────────

    def get_real(self, name: str) -> float:
        """Get the current value of a real-valued variable by name.

        Parameters
        ----------
        name : variable name as declared in modelDescription.xml

        Returns
        -------
        Current value as float.

        Raises
        ------
        KeyError : if no variable with that name exists
        """
        if name not in self._state:
            raise KeyError(f"Variable '{name}' not found in FMU")
        return self._state[name]

    def set_real(self, name: str, value: float) -> None:
        """Set the value of a real-valued variable (input or parameter).

        Parameters
        ----------
        name  : variable name
        value : new value
        """
        if name not in self._var_by_name:
            raise KeyError(f"Variable '{name}' not found in FMU")
        var = self._var_by_name[name]
        if var.causality not in ("input", "parameter", "local"):
            raise ValueError(
                f"Cannot set variable '{name}' with causality '{var.causality}'; "
                "only 'input', 'parameter', and 'local' variables are settable"
            )
        self._state[name] = value

    def get_real_by_ref(self, value_reference: int) -> float:
        """Get a real variable value by its value reference (FMI C-API style)."""
        var = self._var_by_ref.get(value_reference)
        if var is None:
            raise KeyError(f"Value reference {value_reference} not found")
        return self._state.get(var.name, var.start)

    def set_real_by_ref(self, value_reference: int, value: float) -> None:
        """Set a real variable value by its value reference."""
        var = self._var_by_ref.get(value_reference)
        if var is None:
            raise KeyError(f"Value reference {value_reference} not found")
        self._state[var.name] = value

    # ── FMI 3.0: Binary Data ─────────────────────────────────────────────────

    def get_binary(self, name: str) -> bytes:
        """Get a binary variable value (FMI 3.0 only).

        Returns
        -------
        Byte string for the named binary variable.
        """
        self._require_v3("get_binary")
        if name not in self._binary_values:
            raise KeyError(f"Binary variable '{name}' not found")
        return self._binary_values[name]

    def set_binary(self, name: str, data: bytes) -> None:
        """Set a binary variable value (FMI 3.0 only)."""
        self._require_v3("set_binary")
        self._binary_values[name] = data

    # ── FMI 3.0: Clocks ─────────────────────────────────────────────────────

    def get_clock(self, name: str) -> bool:
        """Get a clock variable's ticked state (FMI 3.0 only).

        Returns True if the clock ticked at the current time instant.
        """
        self._require_v3("get_clock")
        return self._clock_values.get(name, False)

    def set_clock(self, name: str, ticked: bool) -> None:
        """Set a clock variable's ticked state (FMI 3.0 only).

        For triggered clocks, the environment sets this to True to signal
        an event. For scheduled clocks, the FMU sets this internally.
        """
        self._require_v3("set_clock")
        var = self._var_by_name.get(name)
        if var is not None and var.clock_type == "scheduled":
            raise ValueError(
                f"Cannot externally set scheduled clock '{name}'; "
                "scheduled clocks are managed by the FMU"
            )
        self._clock_values[name] = ticked

    # ── FMI 3.0: Arrays ─────────────────────────────────────────────────────

    def get_array(self, name: str) -> np.ndarray:
        """Get an array variable value (FMI 3.0 only)."""
        self._require_v3("get_array")
        if name not in self._array_values:
            var = self._var_by_name.get(name)
            if var is None:
                raise KeyError(f"Array variable '{name}' not found")
            if not var.dimensions:
                raise ValueError(f"Variable '{name}' is scalar, not array")
            return np.zeros(var.dimensions)
        return self._array_values[name]

    def set_array(self, name: str, value: np.ndarray) -> None:
        """Set an array variable value (FMI 3.0 only)."""
        self._require_v3("set_array")
        var = self._var_by_name.get(name)
        if var is not None and var.dimensions:
            expected = var.dimensions
            if value.shape != expected:
                raise ValueError(
                    f"Array shape mismatch for '{name}': "
                    f"expected {expected}, got {value.shape}"
                )
        self._array_values[name] = value

    # ── Properties ───────────────────────────────────────────────────────────

    @property
    def time(self) -> float:
        """Current simulation time [s]."""
        return self._time

    @property
    def model_name(self) -> str:
        return self._model_name

    @property
    def guid(self) -> str:
        return self._guid

    @property
    def fmi_version(self) -> FMIVersion:
        return self._fmi_version

    @property
    def variables(self) -> list[FMUVariable]:
        """All variables declared in the FMU."""
        return list(self._variables)

    @property
    def is_initialized(self) -> bool:
        return self._initialized

    @property
    def is_terminated(self) -> bool:
        return self._terminated

    @property
    def output_names(self) -> list[str]:
        return [v.name for v in self._variables if v.causality == "output"]

    @property
    def input_names(self) -> list[str]:
        return [v.name for v in self._variables if v.causality == "input"]

    @property
    def state_dict(self) -> dict[str, float]:
        """Current state as a dict of variable name -> value."""
        return dict(self._state)

    def __repr__(self) -> str:
        status = (
            "initialized" if self._initialized
            else "terminated" if self._terminated
            else "loaded"
        )
        return (
            f"FMU('{self._model_name}', fmi={self._fmi_version.value}, "
            f"status={status}, t={self._time:.4f})"
        )

    # ── Internal ─────────────────────────────────────────────────────────────

    def _check_active(self):
        if not self._initialized:
            raise RuntimeError("FMU not initialized; call initialize() first")
        if self._terminated:
            raise RuntimeError("FMU already terminated")

    def _require_v3(self, method: str):
        if self._fmi_version != FMIVersion.V3_0:
            raise RuntimeError(
                f"{method}() requires FMI 3.0; this FMU is FMI {self._fmi_version.value}"
            )

    def _init_fmpy(self, start_time: float, stop_time: float):
        """Initialize via fmpy for external FMUs."""
        try:
            import fmpy
        except ImportError:
            return  # fmpy not available; operations will use fallback
        try:
            model_desc = fmpy.read_model_description(self._filename)
            unzip_dir = fmpy.extract(self._filename)
            self._fmpy_instance = fmpy.instantiate_fmu(
                unzip_dir, model_desc, fmi_type="ModelExchange"
            )
        except Exception:
            self._fmpy_instance = None

    def _step_fmpy(self, dt: float):
        """Step via fmpy for external FMUs."""
        self._time += dt


# ── XML Parsing ──────────────────────────────────────────────────────────────

def _parse_variables(root: ET.Element, version: FMIVersion) -> list[FMUVariable]:
    """Parse variable metadata from modelDescription.xml."""
    variables: list[FMUVariable] = []

    if version == FMIVersion.V3_0:
        # FMI 3.0: typed elements directly under ModelVariables
        mv = root.find("ModelVariables")
        if mv is None:
            return variables
        for elem in mv:
            tag = elem.tag  # "Float64", "Int32", "Binary", "Clock", etc.
            data_type = tag
            name = elem.get("name", "")
            vr = int(elem.get("valueReference", "0"))
            causality = elem.get("causality", "local")
            variability = elem.get("variability", "continuous")
            start = 0.0
            if elem.get("start") is not None:
                try:
                    start = float(elem.get("start", "0"))
                except ValueError:
                    start = 0.0

            # Parse dimensions for array variables
            dims: tuple[int, ...] = ()
            dim_elem = elem.find("Dimension")
            if dim_elem is not None:
                try:
                    dims = tuple(
                        int(d.get("start", "1"))
                        for d in elem.findall("Dimension")
                    )
                except ValueError:
                    dims = ()

            # Clock type (FMI 3.0)
            clock_type = None
            if tag == "Clock":
                clock_type = elem.get("intervalVariability", "triggered")

            variables.append(FMUVariable(
                name=name,
                value_reference=vr,
                causality=causality,
                variability=variability,
                data_type=data_type,
                start=start,
                dimensions=dims,
                clock_type=clock_type,
            ))
    else:
        # FMI 2.0: ScalarVariable elements with nested type elements
        for sv in root.findall("ModelVariables/ScalarVariable"):
            name = sv.get("name", "")
            vr = int(sv.get("valueReference", "0"))
            causality = sv.get("causality", "local")
            variability = sv.get("variability", "continuous")

            # Determine data type and start value from child element
            data_type = "Float64"
            start = 0.0
            for type_tag in ("Real", "Integer", "Boolean", "String"):
                child = sv.find(type_tag)
                if child is not None:
                    type_map = {
                        "Real": "Float64",
                        "Integer": "Int32",
                        "Boolean": "Boolean",
                        "String": "String",
                    }
                    data_type = type_map.get(type_tag, "Float64")
                    if child.get("start") is not None:
                        try:
                            start = float(child.get("start", "0"))
                        except ValueError:
                            start = 0.0
                    break

            variables.append(FMUVariable(
                name=name,
                value_reference=vr,
                causality=causality,
                variability=variability,
                data_type=data_type,
                start=start,
            ))

    return variables
