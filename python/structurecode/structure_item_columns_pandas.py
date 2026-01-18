# structure_item_columns_pandas.py
# ============================================================
# Item Encoding (STRUCTURE) — Pandas (STRICT / CANONICAL)
# ------------------------------------------------------------
# STRICT POLICY:
# - Do NOT create ITEM_KEY unless (CHECK_CATEGORY_ID + ISSUE_NUMBER + CHECK_ITEM_ID) are all present.
# - Do NOT create ITEM_FEATURE_NAME unless (ITEM_KEY + ITEM_TEXT_CODE) are both present.
# - No fallback features like: item_{item_key} or item__{text_code}.
#   Missing inputs => derived feature fields remain None.
# ============================================================

from __future__ import annotations
from typing import Optional, Tuple
import pandas as pd

from core_utilities_structure_pandas import (
    normalize_text,
    to_uvl_code,
    is_missing_like,
    ensure_column_df,
    require_columns_df,
    normalize_id_token,
)

def process_item_columns(df: pd.DataFrame, sheet_name: str = "") -> pd.DataFrame:
    require_columns_df(df, ["CHECK_ITEM_NAME", "CHECK_ITEM_ID", "CHECK_CATEGORY_ID", "ISSUE_NUMBER"], sheet_name)
    df = df.copy()

    ensure_column_df(df, "ITEM_NAME_CLEAN")
    ensure_column_df(df, "ITEM_TEXT_CODE")
    ensure_column_df(df, "ITEM_KEY")
    ensure_column_df(df, "ITEM_FEATURE_NAME")

    def _build_row(row) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[str]]:
        # ---- Clean item name ----
        raw_name = row["CHECK_ITEM_NAME"]
        if is_missing_like(raw_name):
            item_clean = None
        else:
            item_clean = normalize_text(raw_name).strip() or None

        item_text_code = to_uvl_code(item_clean) if item_clean else None

        # ---- Normalize required ID tokens ----
        item_id_s = normalize_id_token(row["CHECK_ITEM_ID"])
        cat_id_s  = normalize_id_token(row["CHECK_CATEGORY_ID"])
        issue_s   = normalize_id_token(row["ISSUE_NUMBER"])

        # ---- STRICT: composite key must be complete ----
        if not (cat_id_s and issue_s and item_id_s):
            # No key => no feature
            return item_clean, item_text_code, None, None

        item_key = f"cat{cat_id_s}__iss{issue_s}__item{item_id_s}"

        # ---- STRICT: feature name requires BOTH key and text code ----
        if not item_text_code:
            # Keep the key for tracking, but DO NOT create feature name
            return item_clean, item_text_code, item_key, None

        item_feature = f"item_{item_key}__{item_text_code}"
        return item_clean, item_text_code, item_key, item_feature

    out = df.apply(_build_row, axis=1, result_type="expand")
    out.columns = ["ITEM_NAME_CLEAN", "ITEM_TEXT_CODE", "ITEM_KEY", "ITEM_FEATURE_NAME"]

    df["ITEM_NAME_CLEAN"] = out["ITEM_NAME_CLEAN"]
    df["ITEM_TEXT_CODE"] = out["ITEM_TEXT_CODE"]
    df["ITEM_KEY"] = out["ITEM_KEY"]
    df["ITEM_FEATURE_NAME"] = out["ITEM_FEATURE_NAME"]

    return df
