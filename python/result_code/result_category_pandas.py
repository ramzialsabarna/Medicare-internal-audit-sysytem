# result_category_pandas.py
# ============================================================
# Category Encoding — RESULTS Sheet (Mirror Pipeline)
# ------------------------------------------------------------
# What this module does:
# - Replicates the Harmonization logic for results data.
# - Ensures that Categories in the field results match the Structure categories.
#
# Methodology:
# - Uses the same spelling map to prevent "Join Failures" caused by inconsistent naming.
# ============================================================

from __future__ import annotations
from typing import Dict
import pandas as pd

from core_utilities_results_pandas import (
    normalize_text,
    to_uvl_code,
    is_missing_like,
    ensure_column_df,
    require_columns_df,
)

def process_result_category_df(
    df: pd.DataFrame,
    sheet_name: str,
    category_spelling_map: Dict[str, str],
) -> pd.DataFrame:
    # In Results, we assume the same column name for consistency; if different, adjust here.
    require_columns_df(df, ["CHECK_CATEGORY_NAME"], sheet_name)
    df = df.copy()

    ensure_column_df(df, "CATEGORY_NAME_CLEAN")
    ensure_column_df(df, "CATEGORY_NAME_HARMONIZED")
    ensure_column_df(df, "CATEGORY_CODE")

    def _clean(raw) -> str | None:
        if is_missing_like(raw): return None
        txt = normalize_text(raw).strip()
        return txt if txt else None

    df["CATEGORY_NAME_CLEAN"] = df["CHECK_CATEGORY_NAME"].apply(_clean)

    def _harmonize(clean_val: str | None) -> str | None:
        if not clean_val: return None
        return category_spelling_map.get(clean_val, clean_val)

    df["CATEGORY_NAME_HARMONIZED"] = df["CATEGORY_NAME_CLEAN"].apply(_harmonize)
    df["CATEGORY_CODE"] = df["CATEGORY_NAME_HARMONIZED"].apply(to_uvl_code)

    return df