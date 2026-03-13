"""Surrogate model training from simulation data.

Trains a lightweight surrogate model (polynomial regression or Gaussian process)
from stored Nucleus TimeSeries data so that AI agents can get fast predictions
without running a full simulation.
"""

from __future__ import annotations
import numpy as np
from dataclasses import dataclass, field
from typing import Callable


@dataclass
class SurrogateModel:
    """Trained surrogate model for fast inference.

    Attributes
    ----------
    input_params : list of parameter names (model inputs)
    output_vars  : list of output variable names
    _model       : fitted sklearn estimator (one per output variable)
    _scaler_X    : input feature scaler
    _scaler_y    : output scaler (per-variable)
    """

    input_params: list[str]
    output_vars: list[str]
    _models: dict = field(default_factory=dict, repr=False)
    _scaler_X: Any = field(default=None, repr=False)
    _scalers_y: dict = field(default_factory=dict, repr=False)

    def predict(self, **inputs) -> dict[str, float]:
        """Fast inference without simulation.

        Parameters
        ----------
        **inputs : parameter values (must match input_params)

        Returns
        -------
        dict mapping output variable name → predicted value
        """
        X = np.array([[inputs[p] for p in self.input_params]])
        if self._scaler_X is not None:
            X = self._scaler_X.transform(X)

        result = {}
        for vname, model in self._models.items():
            y_scaled = model.predict(X)[0]
            if vname in self._scalers_y:
                y_scaled = self._scalers_y[vname].inverse_transform([[y_scaled]])[0][0]
            result[vname] = float(y_scaled)

        return result

    def score(self, X_test: np.ndarray, y_test: dict[str, np.ndarray]) -> dict[str, float]:
        """Return R² scores on test data."""
        scores = {}
        X = X_test
        if self._scaler_X is not None:
            X = self._scaler_X.transform(X)
        for vname, model in self._models.items():
            y_true = y_test.get(vname, np.array([]))
            if len(y_true) == 0:
                continue
            y_pred = model.predict(X)
            ss_res = np.sum((y_true - y_pred) ** 2)
            ss_tot = np.sum((y_true - y_true.mean()) ** 2)
            scores[vname] = float(1 - ss_res / ss_tot) if ss_tot > 0 else 1.0
        return scores


# Import Any for type hint compatibility
from typing import Any
SurrogateModel.__annotations__["_scaler_X"] = Any
SurrogateModel.__annotations__["_scalers_y"] = dict


def train_surrogate(
    X: np.ndarray,
    y: dict[str, np.ndarray],
    input_params: list[str],
    output_vars: list[str],
    model_type: str = "ridge",
    degree: int = 2,
) -> SurrogateModel:
    """Train a surrogate model from simulation data.

    Parameters
    ----------
    X            : (n_runs, n_params) array of input parameter values
    y            : dict mapping output variable name → (n_runs,) values
    input_params : ordered list of parameter names
    output_vars  : ordered list of output variable names
    model_type   : "ridge" (polynomial ridge), "gp" (Gaussian process), "rf" (random forest)
    degree       : polynomial degree (for "ridge" only)

    Returns
    -------
    Trained SurrogateModel
    """
    try:
        from sklearn.preprocessing import StandardScaler, PolynomialFeatures
        from sklearn.linear_model import Ridge
        from sklearn.pipeline import Pipeline
        from sklearn.ensemble import RandomForestRegressor
    except ImportError:
        raise ImportError(
            "scikit-learn is required for surrogate models.\n"
            "Install with: pip install scikit-learn"
        )

    scaler_X = StandardScaler()
    X_scaled = scaler_X.fit_transform(X)

    models: dict[str, Any] = {}
    scalers_y: dict[str, Any] = {}

    for vname in output_vars:
        y_arr = y.get(vname)
        if y_arr is None:
            continue

        scaler_y = StandardScaler()
        y_scaled = scaler_y.fit_transform(y_arr.reshape(-1, 1)).ravel()
        scalers_y[vname] = scaler_y

        if model_type == "ridge":
            pipe = Pipeline([
                ("poly", PolynomialFeatures(degree=degree, include_bias=False)),
                ("reg", Ridge(alpha=1.0)),
            ])
            pipe.fit(X_scaled, y_scaled)
            models[vname] = pipe

        elif model_type == "gp":
            from sklearn.gaussian_process import GaussianProcessRegressor
            from sklearn.gaussian_process.kernels import RBF, ConstantKernel
            kernel = ConstantKernel(1.0) * RBF(length_scale=1.0)
            gp = GaussianProcessRegressor(kernel=kernel, n_restarts_optimizer=5)
            gp.fit(X_scaled, y_scaled)
            models[vname] = gp

        elif model_type == "rf":
            rf = RandomForestRegressor(n_estimators=100, random_state=42)
            rf.fit(X_scaled, y_scaled)
            models[vname] = rf

        else:
            raise ValueError(f"Unknown model_type: '{model_type}'. Use 'ridge', 'gp', or 'rf'.")

    surrogate = SurrogateModel(
        input_params=input_params,
        output_vars=output_vars,
    )
    surrogate._models = models
    surrogate._scaler_X = scaler_X
    surrogate._scalers_y = scalers_y
    return surrogate


def train_surrogate_from_nucleus(
    conn,
    run_id_pattern: str,
    input_params: list[str],
    output_vars: list[str],
    param_extractor: Callable[[str], dict[str, float]] | None = None,
    model_type: str = "ridge",
    degree: int = 2,
) -> SurrogateModel:
    """Train a surrogate model from stored Nucleus TimeSeries runs.

    Parameters
    ----------
    conn            : psycopg3 Connection
    run_id_pattern  : pattern to match run IDs (e.g., "sweep-stiffness-*")
    input_params    : parameter names (extracted from run_id via param_extractor)
    output_vars     : statistics to predict: "max_{var}", "final_{var}", "mean_{var}"
    param_extractor : callable(run_id) → dict of param values (default: simple parsing)
    model_type      : "ridge", "gp", or "rf"
    degree          : polynomial degree for ridge

    Returns
    -------
    Trained SurrogateModel
    """
    from .load import list_runs, load_results  # noqa (nucleus module)
    # Can't import from nucleus here - use direct queries
    from ..nucleus.load import list_runs, load_results

    run_ids = list_runs(conn, pattern=run_id_pattern)

    if param_extractor is None:
        def param_extractor(run_id: str) -> dict[str, float]:
            # Try to parse "key1-val1-key2-val2" from run_id
            parts = run_id.split("-")
            params: dict[str, float] = {}
            i = 0
            while i < len(parts) - 1:
                try:
                    params[parts[i]] = float(parts[i + 1])
                    i += 2
                except ValueError:
                    i += 1
            return params

    rows_X = []
    rows_y: dict[str, list[float]] = {v: [] for v in output_vars}
    valid_runs = []

    for run_id in run_ids:
        try:
            params = param_extractor(run_id)
            if not all(p in params for p in input_params):
                continue
            x_row = [params[p] for p in input_params]
        except Exception:
            continue

        # Load output statistics for each output_var like "max_x" or "final_v"
        loaded_any = False
        for spec in output_vars:
            stat, _, vname = spec.partition("_")
            if not vname:
                continue
            _, arrays = load_results(conn, run_id, [vname])
            arr = arrays.get(vname, np.array([]))
            if len(arr) == 0:
                break
            if stat == "max":
                rows_y[spec].append(float(arr.max()))
            elif stat == "final":
                rows_y[spec].append(float(arr[-1]))
            elif stat == "mean":
                rows_y[spec].append(float(arr.mean()))
            elif stat == "min":
                rows_y[spec].append(float(arr.min()))
            else:
                rows_y[spec].append(float(arr[-1]))
            loaded_any = True

        if loaded_any:
            rows_X.append(x_row)
            valid_runs.append(run_id)

    if not rows_X:
        raise ValueError("No valid runs found matching the pattern.")

    X = np.array(rows_X, dtype=float)
    y = {k: np.array(v, dtype=float) for k, v in rows_y.items() if v}

    return train_surrogate(X, y, input_params, output_vars, model_type, degree)
