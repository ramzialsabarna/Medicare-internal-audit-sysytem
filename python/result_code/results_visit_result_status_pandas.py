# results_pipeline/result_visit_result_status_pandas.py
# ============================================================
# VISIT_RESULT_STATUS Encoding (RESULTS) — Workflow Pipeline
# ------------------------------------------------------------
# LOGIC & METHODOLOGY:
# - Standardizes the administrative workflow states of an audit result.
# - Replicates the 'vrs_' (Visit Result Status) namespacing logic.
# - Ensures 100% parity between the field execution data and structural definitions.
# - Critical for joining Results with UVL constraints regarding approval levels.
#
# DATA TYPE: 
# - Categorical / Ordinal Data (Workflow Decision States).
#
# ERRORS & NOISE RESOLVED:
# - Join Disparities: Guarantees match with configuration codes (e.g., approved_by_branch_director).
# - Format Divergence: Normalizes inconsistent administrative labels across different audit versions.
# ============================================================

from __future__ import annotations
from typing import Dict
import pandas as pd

# Using established utilities for the Results pipeline
from results_pipeline.core_utilities_results_pandas import (
    normalize_text,
    to_uvl_code,
    is_missing_like,
    ensure_column_df,
)

# Output column constants to prevent hardcoding errors
OUT_CLEAN = "VISIT_RESULT_STATUS_CLEAN"
OUT_CODE  = "VISIT_RESULT_STATUS_CODE"

def process_result_visit_result_status_df(
    df: pd.DataFrame,
    visit_result_status_map: Dict[str, str],
) -> pd.DataFrame:
    """
    Standardizes the administrative visit result status using the 'vrs_' prefix.
    """
    # The source column from Excel: VISIT_RESULT_STATUS
    target_col = "VISIT_RESULT_STATUS"
    
    if target_col not in df.columns:
        return df

    df = df.copy()

    # Initialize Clean and Code columns
    ensure_column_df(df, OUT_CLEAN)
    ensure_column_df(df, OUT_CODE)

    def _clean(raw) -> str | None:
        """Removes noise and standardizes administrative text structure."""
        if is_missing_like(raw):
            return None
        txt = normalize_text(raw).strip()
        return txt if txt else None

    def _encode(clean_val: str | None) -> str | None:
        """Maps cleaned workflow text to 'vrs_' namespaced codes."""
        if not clean_val:
            return None
        
        # Generate slug (e.g., "approved_by_branch_director")
        slug = to_uvl_code(clean_val) or "unknown"
        
        # Cross-reference with the master status map in config
        mapped = visit_result_status_map.get(slug)
        
        # KEEPING THE PREFIX: vrs_ for administrative/workflow results
        return mapped if mapped else f"vrs_{slug}"

    # Apply processing logic
    df[OUT_CLEAN] = df[target_col].apply(_clean)
    df[OUT_CODE]  = df[OUT_CLEAN].apply(_encode)

    return df