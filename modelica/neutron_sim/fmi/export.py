"""Export a neutron_sim System as an FMU (Functional Mock-up Unit).

Supports FMI 2.0 and FMI 3.0 modelDescription.xml generation.
"""

from __future__ import annotations
import json
import pickle
import uuid
import zipfile
from xml.etree import ElementTree as ET

from ..core.system import System


def _build_model_description_2(
    system: System, model_name: str, guid: str, kind: str = "CoSimulation"
) -> str:
    """Generate FMI 2.0 modelDescription.xml from a System."""
    state_vars = system.state_variables()
    ics = system.initial_conditions()

    root = ET.Element("fmiModelDescription")
    root.set("xmlns:xsi", "http://www.w3.org/2001/XMLSchema-instance")
    root.set("fmiVersion", "2.0")
    root.set("modelName", model_name)
    root.set("guid", f"{{{guid}}}")
    root.set("numberOfEventIndicators", "0")

    cap = ET.SubElement(root, kind)
    cap.set("modelIdentifier", model_name.replace(" ", "_"))
    cap.set("canHandleVariableCommunicationStepSize", "true")

    mv = ET.SubElement(root, "ModelVariables")
    for i, var in enumerate(state_vars):
        sv = ET.SubElement(mv, "ScalarVariable")
        sv.set("name", var.name)
        sv.set("valueReference", str(i))
        sv.set("causality", "output")
        sv.set("variability", "continuous")
        sv.set("initial", "exact")
        real = ET.SubElement(sv, "Real")
        real.set("start", str(ics.get(var, 0.0)))

    ms = ET.SubElement(root, "ModelStructure")
    outputs = ET.SubElement(ms, "Outputs")
    for i in range(len(state_vars)):
        unk = ET.SubElement(outputs, "Unknown")
        unk.set("index", str(i + 1))
        unk.set("dependencies", "")

    ET.indent(root, space="  ")
    return ET.tostring(root, encoding="unicode", xml_declaration=True)


def _build_model_description_3(
    system: System, model_name: str, guid: str, kind: str = "CoSimulation"
) -> str:
    """Generate FMI 3.0 modelDescription.xml from a System.

    FMI 3.0 differences from 2.0:
    - fmiVersion="3.0"
    - ScalarVariable replaced by Float64 (typed elements)
    - ModelStructure uses Output (not Outputs/Unknown)
    - instantiationToken instead of guid
    - generationTool attribute
    """
    state_vars = system.state_variables()
    ics = system.initial_conditions()

    root = ET.Element("fmiModelDescription")
    root.set("xmlns:xsi", "http://www.w3.org/2001/XMLSchema-instance")
    root.set("fmiVersion", "3.0")
    root.set("modelName", model_name)
    root.set("instantiationToken", f"{{{guid}}}")
    root.set("generationTool", "neutron_sim")

    cap = ET.SubElement(root, kind)
    cap.set("modelIdentifier", model_name.replace(" ", "_"))
    if kind == "CoSimulation":
        cap.set("canHandleVariableCommunicationStepSize", "true")
        cap.set("canReturnEarlyAfterIntermediateUpdate", "false")
        cap.set("fixedInternalStepSize", "0.001")

    # FMI 3.0 uses typed elements (Float64) instead of ScalarVariable+Real
    mv = ET.SubElement(root, "ModelVariables")
    for i, var in enumerate(state_vars):
        fv = ET.SubElement(mv, "Float64")
        fv.set("name", var.name)
        fv.set("valueReference", str(i))
        fv.set("causality", "output")
        fv.set("variability", "continuous")
        fv.set("initial", "exact")
        fv.set("start", str(ics.get(var, 0.0)))

    # FMI 3.0 ModelStructure uses Output elements (not Outputs/Unknown)
    ms = ET.SubElement(root, "ModelStructure")
    for i in range(len(state_vars)):
        out = ET.SubElement(ms, "Output")
        out.set("valueReference", str(i))
        out.set("dependencies", "")

    ET.indent(root, space="  ")
    return ET.tostring(root, encoding="unicode", xml_declaration=True)


def export_fmu(
    system: System,
    filename: str,
    model_name: str | None = None,
    fmi_version: str = "2.0",
    kind: str = "CoSimulation",
) -> str:
    """Export a neutron_sim System as an FMU zip file.

    Creates a standard FMI modelDescription.xml plus a pickled copy of the
    System inside ``resources/neutron_sim_system.pkl`` so that the FMU can be
    round-tripped without a C compiler.

    Parameters
    ----------
    system     : assembled System to export
    filename   : output path (should end in .fmu)
    model_name : FMU identifier (defaults to filename stem)
    fmi_version: "2.0" or "3.0"
    kind       : "CoSimulation" or "ModelExchange"

    Returns
    -------
    Absolute path to the created .fmu file.
    """
    import os

    if fmi_version not in ("2.0", "3.0"):
        raise ValueError(f"Unsupported FMI version: {fmi_version}. Use '2.0' or '3.0'.")

    if model_name is None:
        model_name = os.path.splitext(os.path.basename(filename))[0]

    guid = str(uuid.uuid4())

    system.flatten()

    if fmi_version == "3.0":
        model_desc = _build_model_description_3(system, model_name, guid, kind)
    else:
        model_desc = _build_model_description_2(system, model_name, guid, kind)

    state_vars = system.state_variables()
    ics = system.initial_conditions()
    meta = {
        "version": "1.0",
        "type": "neutron_sim_fmu",
        "fmi_version": fmi_version,
        "model_name": model_name,
        "guid": guid,
        "state_variables": [v.name for v in state_vars],
        "initial_conditions": {v.name: ics.get(v, 0.0) for v in state_vars},
    }

    with zipfile.ZipFile(filename, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("modelDescription.xml", model_desc)
        zf.writestr("resources/neutron_sim_meta.json", json.dumps(meta, indent=2))
        zf.writestr("resources/neutron_sim_system.pkl", pickle.dumps(system))

    return os.path.abspath(filename)
