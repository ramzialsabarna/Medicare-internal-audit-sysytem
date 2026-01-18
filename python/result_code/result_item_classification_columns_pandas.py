# result_item_classification_columns_pandas.py
# ============================================================
# Item Classification Encoding (RESULTS) — Mirror Pipeline
# ----------------------------
# LOGIC & METHODOLOGY:
# - Replicates the Major/Minor namespacing logic for the results pipeline.
# - Ensures that during an audit execution, the risk classification of 
#   the finding matches the structural definition in the UVL.
#
# DATA TYPE: 
# - Categorical / Ordinal (Risk Severity Level).
#
# ERRORS & NOISE RESOLVED:
# - Logical Alignment: Guarantees that if an item is defined as 'major' in 
#   Structure, its corresponding finding in Results is correctly linked 
#   via the same namespaced code.
# - Data Integrity: Filters out inconsistent field entries to maintain 
#   a clean join key for reporting.
# ------------------------------------------------------------
# What this module does:
# - Mirrors item classification processing for Results parity.
#
# Requires (raw columns):
# - ITEM_CLASSIFICATION_NAME, ITEM_FEATURE_NAME
#
# Produces (derived columns):
# - ITEM_CLASSIFICATION_NAME_CLEAN
# - ITEM_CLASSIFICATION_CODE
# - ITEM_CLASSIFICATION_FEATURE
# ============================================================

from __future__ import annotations
from typing import Optional
import pandas as pd

from core_utilities_results_pandas import (
    normalize_text,
    to_uvl_code,
    is_missing_like,
    ensure_column_df,
)

_ALLOWED = {"major", "minor"}

def process_result_item_classification_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Identical mirroring of classification logic for canonical join stability.
    """
    if "ITEM_CLASSIFICATION_NAME" not in df.columns or "ITEM_FEATURE_NAME" not in df.columns:
        return df

    df = df.copy()

    ensure_column_df(df, "ITEM_CLASSIFICATION_NAME_CLEAN")
    ensure_column_df(df, "ITEM_CLASSIFICATION_CODE")
    ensure_column_df(df, "ITEM_CLASSIFICATION_FEATURE")

    def _clean_name(raw) -> Optional[str]:
        if is_missing_like(raw):
            return None
        txt = normalize_text(raw).strip()
        return txt if txt else None

    def _code_from_clean(clean_val: Optional[str]) -> Optional[str]:
        if not clean_val:
            return None
        slug = to_uvl_code(clean_val)
        return slug if slug in _ALLOWED else None

    df["ITEM_CLASSIFICATION_NAME_CLEAN"] = df["ITEM_CLASSIFICATION_NAME"].apply(_clean_name)
    df["ITEM_CLASSIFICATION_CODE"] = df["ITEM_CLASSIFICATION_NAME_CLEAN"].apply(_code_from_clean)

    def _make_feat(row) -> Optional[str]:
        item_feat = row["ITEM_FEATURE_NAME"]
        code = row["ITEM_CLASSIFICATION_CODE"]
        if not item_feat or not code:
            return None
        return f"{item_feat}__{code}"

    df["ITEM_CLASSIFICATION_FEATURE"] = df.apply(_make_feat, axis=1)
    return df