"""Basic stiff/non-stiff detection for automatic solver selection."""

from __future__ import annotations
import numpy as np
from typing import Callable


def estimate_stiffness(
    f: Callable[[float, np.ndarray], np.ndarray],
    t0: float,
    y0: np.ndarray,
    eps: float = 1e-6,
) -> float:
    """
    Estimate the stiffness ratio via finite-difference Jacobian.

    Returns the ratio max(|Re(eigenvalue)|) / min(|Re(eigenvalue)|).
    A ratio > 1000 is considered stiff.
    """
    n = len(y0)
    f0 = np.asarray(f(t0, y0), dtype=float)
    J = np.zeros((n, n))
    for i in range(n):
        yp = y0.copy()
        yp[i] += eps
        fp = np.asarray(f(t0, yp), dtype=float)
        J[:, i] = (fp - f0) / eps

    eigvals = np.linalg.eigvals(J)
    re = np.abs(np.real(eigvals))
    re = re[re > 1e-12]

    if len(re) == 0:
        return 1.0

    return float(re.max() / re.min())


def select_method(stiffness_ratio: float, threshold: float = 999.0) -> str:
    """Return 'Radau' for stiff systems (ratio ≥ threshold), 'RK45' for non-stiff."""
    return "Radau" if stiffness_ratio >= threshold else "RK45"
