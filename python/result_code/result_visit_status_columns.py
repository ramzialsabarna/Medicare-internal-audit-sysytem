# results_pipeline/result_visit_status_columns.py
# ============================================================
# VISIT_STATUS Encoding (RESULTS) — Operational Pipeline
# ------------------------------------------------------------
# LOGIC & METHODOLOGY:
# - Replicates the 'vs_' (Visit Status) namespacing logic for the results pipeline.
# - Ensures that the operational state recorded (e.g., PENDING, CLOSED) 
#   is perfectly synchronized with the structural allowed values.
# - Distinguishes the audit process stage from the final result outcome.
#
# DATA TYPE: 
# - Categorical / Ordinal Data (Operational Process States).
#
# ERRORS & NOISE RESOLVED:
# - Join Disparities: Guarantees that the visit status in the execution 
#   sheet matches the configuration sheet for consistent reporting.
# - Entry Noise: Addresses inconsistencies in how auditors record process states.
# ============================================================

from __future__ import annotations
from typing import Dict
import pandas as pd

# Reusing core utilities from the results pipeline
from results_pipeline.core_utilities_results_pandas import (
    normalize_text,
    to_uvl_code,
    is_missing_like,
    ensure_column_df,
)

def process_result_visit_status_columns(
    df: pd.DataFrame,
    visit_status_map: Dict[str, str],
) -> pd.DataFrame:
    """
    Standardizes the operational visit status using the 'vs_' prefix.
    """
    # Source column representing the current stage of the audit visit
    target_col = "VISIT_STATUS"

    if target_col not in df.columns:
        return df

    df = df.copy()

    # Initialize derived columns for cleaning and UVL coding
    ensure_column_df(df, "VISIT_STATUS_CLEAN")
    ensure_column_df(df, "VISIT_STATUS_CODE")

    def _clean(raw) -> str | None:
        """Removes whitespace and standardizes text input."""
        if is_missing_like(raw):
            return None
        txt = normalize_text(raw).strip()
        return txt if txt else None

    def _encode(clean_val: str | None) -> str | None:
        """Maps cleaned operational states to 'vs_' namespaced codes."""
        if not clean_val:
            return None
        
        # Generate slug from standardized text
        slug = to_uvl_code(clean_val) or "unknown"
        
        # Cross-reference with the operational status map in config
        mapped = visit_status_map.get(slug)
        
        # KEEPING THE PREFIX: vs_ for Operational Visit Status
        return mapped if mapped else f"vs_{slug}"

    # Apply transformations
    df["VISIT_STATUS_CLEAN"] = df[target_col].apply(_clean)
    df["VISIT_STATUS_CODE"] = df["VISIT_STATUS_CLEAN"].apply(_encode)

    return df