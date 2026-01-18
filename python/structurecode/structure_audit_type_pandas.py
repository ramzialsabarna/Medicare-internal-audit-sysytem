# structure_audit_type_pandas.py
# ============================================================
# Audit Type Encoding (STRUCTURE) — Pandas (Canonical)
# ------------------------------------------------------------
# LOGIC & METHODOLOGY:
# - This module categorizes the audit visit into one of three predefined 
#   types: PLANNED, UNPLANNED, or RE_EVALUATE.
# - It serves as the "Decision Root" in the UVL model, determining which 
#   logical constraints are activated based on the nature of the audit.
#
# DATA TYPE: 
# - Categorical / Nominal Data.
#
# ERRORS & NOISE RESOLVED:
# - Case Inconsistency: Resolved via to_uvl_code(), ensuring "Planned" 
#   and "PLANNED" map to the same code (planned).
# - Whitespace Noise: Handled via .strip() to prevent phantom categories.
# - Null-Value Handling: Detects empty entries or noise (e.g., "-", "N/A") 
#   using is_missing_like() to prevent invalid UVL features.
# ------------------------------------------------------------
# What this module does:
# - Normalizes the Audit Type label from the configuration sheet.
# - Builds a UVL-compliant identifier (AUDIT_TYPE_CODE).
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

from core_utilities_structure_pandas import (
    normalize_text,
    to_uvl_code,
    is_missing_like,
    ensure_column_df,
)

_COL_RAW = "AUDIT_TYPE"
_COL_CLEAN = "AUDIT_TYPE_CLEAN"
_COL_CODE = "AUDIT_TYPE_CODE"

def process_structure_audit_type_df(df: pd.DataFrame, sheet_name: str) -> pd.DataFrame:
    """
    Standardizes the audit classification for the UVL configuration root.
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