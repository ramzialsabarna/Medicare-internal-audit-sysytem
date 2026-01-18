# result_audit_type_pandas.py
# ============================================================
# Audit Type Encoding (RESULTS) — Mirror Pipeline
# ------------------------------------------------------------
# LOGIC & METHODOLOGY:
# - Replicates the exact Audit Type logic for the Results pipeline 
#   to ensure 1:1 synchronization with the Structure definitions.
# - Vital for joining the execution data with the structural constraints.
#
# DATA TYPE: 
# - Categorical / Nominal Data.
#
# ERRORS & NOISE RESOLVED:
# - Join Disparities: Guarantees that the execution data (Results) 
#   matches the planning data (Structure) by using identical cleaning rules.
# - Inconsistent Entry: Prevents "Join Failures" caused by differing 
#   formats or casing between the two main Excel files.
# ------------------------------------------------------------
# What this module does:
# - Normalizes Audit Type in Results for seamless joining and validation.
#
# Requires (raw columns):
# - AUDIT_TYPE
#
# Produces (derived columns):
# - AUDIT_TYPE_CLEAN
# - AUDIT_TYPE_CODE
# ============================================================

from __future__ import annotations
import pandas as pd

from core_utilities_results_pandas import (
    normalize_text,
    to_uvl_code,
    is_missing_like,
    ensure_column_df,
)

_COL_RAW = "AUDIT_TYPE"
_COL_CLEAN = "AUDIT_TYPE_CLEAN"
_COL_CODE = "AUDIT_TYPE_CODE"

def process_result_audit_type_df(df: pd.DataFrame, sheet_name: str) -> pd.DataFrame:
    """
    Mirrors Structure logic to ensure 100% matching of audit type identifiers.
    """
    if _COL_RAW not in df.columns:
        return df

    df = df.copy()

    ensure_column_df(df, _COL_CLEAN)
    ensure_column_df(df, _COL_CODE)

    def _clean(raw) -> str | None:
        if is_missing_like(raw):
            return None
        txt = normalize_text(raw).strip()
        return txt if txt else None

    df[_COL_CLEAN] = df[_COL_RAW].apply(_clean)
    df[_COL_CODE] = df[_COL_CLEAN].apply(to_uvl_code)

    return df