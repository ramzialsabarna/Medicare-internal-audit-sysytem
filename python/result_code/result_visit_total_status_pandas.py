# results_pipeline/result_visit_total_status_pandas.py
# ============================================================
# VISIT_TOTAL_STATUS Encoding (RESULTS) — Mirror Pipeline
# ------------------------------------------------------------
# LOGIC & METHODOLOGY:
# - This module processes the final overall audit outcome for the entire visit.
# - Replaces the legacy 'VISIT_RESULT_SCORE_STATUS' with the updated Excel schema.
# - Applies the 'vts_' (Visit Total Status) namespace prefix to ensure 
#   architectural distinction from operational and category-level statuses.
# - Synchronizes field-recorded outcomes with canonical UVL logic features.
#
# DATA TYPE: 
# - Binary / Categorical Data (Overall Pass/Fail Evaluation).
#
# ERRORS & NOISE RESOLVED:
# - Namespace Collision: Specifically uses 'vts_' to isolate the final visit 
#   score outcome from 'vrs_' (workflow) and 'coss_' (category) results.
# - Normalization: Cleans inconsistent manual inputs to ensure deterministic coding.
# ============================================================

from __future__ import annotations
from typing import Dict
import pandas as pd

from results_pipeline.core_utilities_results_pandas import (
    normalize_text,
    to_uvl_code,
    is_missing_like,
    ensure_column_df,
)

def process_result_visit_total_status_columns(
    df: pd.DataFrame,
    visit_result_status_map: Dict[str, str],
) -> pd.DataFrame:
    """
    Standardizes the overall visit status outcome using the 'vts_' prefix.
    """
    # The updated target column name from the new Excel schema
    target_col = "VISIT_TOTAL_STATUS"
    
    if target_col not in df.columns:
        return df

    df = df.copy()

    # Initialize Clean and Code columns
    ensure_column_df(df, f"{target_col}_CLEAN")
    ensure_column_df(df, f"{target_col}_CODE")

    def _clean(raw) -> str | None:
        if is_missing_like(raw):
            return None
        txt = normalize_text(raw).strip()
        return txt if txt else None

    def _encode(clean_val: str | None) -> str | None:
        if not clean_val:
            return None
        
        # Generate slug (e.g., "passed" or "failed")
        slug = to_uvl_code(clean_val) or "unknown"
        
        # Cross-reference with the status map in domain_config
        mapped = visit_result_status_map.get(slug)
        
        # APPLYING THE REQUESTED PREFIX: vts_ for Visit Total Status
        return mapped if mapped else f"vts_{slug}"

    df[f"{target_col}_CLEAN"] = df[target_col].apply(_clean)
    df[f"{target_col}_CODE"]  = df[f"{target_col}_CLEAN"].apply(_encode)

    return df