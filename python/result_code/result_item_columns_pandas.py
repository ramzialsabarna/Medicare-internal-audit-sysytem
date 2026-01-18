# result_item_columns_pandas.py
# ============================================================
# Item Encoding (RESULTS) — Mirror Pipeline
# ------------------------------------------------------------
# LOGIC & METHODOLOGY:
# - Replicates the exact composite key construction for the Results pipeline.
# - Ensures that the answer provided by an auditor is mapped to the exact 
#   structural feature identifier generated in the configuration sheet.
#
# DATA TYPE: 
# - Composite / Categorical String Identifiers.
#
# ERRORS & NOISE RESOLVED:
# - Disjointed Identity: Prevents "Mapping Errors" where an answer in the field 
#   cannot be linked back to its definition due to slight ID or text variations.
# - Excel Format Inconsistency: Guarantees that IDs recorded in different sheets 
#   are tokenized identically regardless of their original data type (string/int/float).
# ------------------------------------------------------------
# What this module does:
# - Synchronizes audit item identifiers for the Results pipeline.
#
# Requires (raw columns):
# - CHECK_ITEM_NAME, CHECK_ITEM_ID, CHECK_CATEGORY_ID, ISSUE_NUMBER
#
# Produces (derived columns):
# - ITEM_NAME_CLEAN, ITEM_TEXT_CODE, ITEM_KEY, ITEM_FEATURE_NAME
# ============================================================

from __future__ import annotations
from typing import Optional, Tuple
import pandas as pd

from core_utilities_results_pandas import (
    normalize_text,
    to_uvl_code,
    is_missing_like,
    ensure_column_df,
    require_columns_df,
    normalize_id_token,
)

def process_result_item_columns(df: pd.DataFrame, sheet_name: str = "") -> pd.DataFrame:
    """
    Mirroring the structure logic to maintain identical ITEM_FEATURE_NAME across pipelines.
    """
    require_columns_df(df, ["CHECK_ITEM_NAME", "CHECK_ITEM_ID", "CHECK_CATEGORY_ID", "ISSUE_NUMBER"], sheet_name)
    df = df.copy()

    ensure_column_df(df, "ITEM_NAME_CLEAN")
    ensure_column_df(df, "ITEM_TEXT_CODE")
    ensure_column_df(df, "ITEM_KEY")
    ensure_column_df(df, "ITEM_FEATURE_NAME")

    def _build_row(row) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[str]]:
        raw_name = row["CHECK_ITEM_NAME"]
        item_clean = normalize_text(raw_name).strip() or None if not is_missing_like(raw_name) else None
        item_text_code = to_uvl_code(item_clean) if item_clean else None

        item_id_s = normalize_id_token(row["CHECK_ITEM_ID"])
        cat_id_s = normalize_id_token(row["CHECK_CATEGORY_ID"])
        issue_s = normalize_id_token(row["ISSUE_NUMBER"])

        item_key = (f"cat{cat_id_s}__iss{issue_s}__item{item_id_s}" 
                    if (cat_id_s and issue_s and item_id_s) else None)

        if item_key and item_text_code:
            item_feature = f"item_{item_key}__{item_text_code}"
        elif item_key:
            item_feature = f"item_{item_key}"
        elif item_text_code:
            item_feature = f"item__{item_text_code}"
        else:
            item_feature = None

        return item_clean, item_text_code, item_key, item_feature

    out = df.apply(_build_row, axis=1, result_type="expand")
    out.columns = ["ITEM_NAME_CLEAN", "ITEM_TEXT_CODE", "ITEM_KEY", "ITEM_FEATURE_NAME"]

    df["ITEM_NAME_CLEAN"] = out["ITEM_NAME_CLEAN"]
    df["ITEM_TEXT_CODE"] = out["ITEM_TEXT_CODE"]
    df["ITEM_KEY"] = out["ITEM_KEY"]
    df["ITEM_FEATURE_NAME"] = out["ITEM_FEATURE_NAME"]

    return df