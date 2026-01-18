# structure_category_pandas.py
# ============================================================
# Category Encoding (STRUCTURE) — Pandas (Canonical)
# ------------------------------------------------------------
# LOGIC & METHODOLOGY:
# - This module implements "Entity Resolution" at the category level.
# - It uses a centralized 'category_spelling_map' to perform data 
#   harmonization, merging different naming variants into a single state.
# - It generates a stable CATEGORY_CODE which acts as a logical 
#   container for audit items in the UVL hierarchy.
#
# DATA TYPE: 
# - Categorical / Nominal (Departmental Labels).
#
# ERRORS & NOISE RESOLVED:
# - Spelling Variance: Unifies synonyms (e.g., "Hematology" vs "Haematology") 
#   using the harmonization map to prevent duplicate logic features.
# - Whitespace/Casing Noise: Normalizes raw text to ensure consistency.
# - Identifier Instability: Creates a safe slug (CATEGORY_CODE) that 
#   avoids special character errors in formal logic models.
# ------------------------------------------------------------
# What this module does:
# - Normalizes and harmonizes audit category names.
# - Produces a unique UVL-safe code for each department.
#
# Requires (raw columns):
# - CHECK_CATEGORY_NAME
#
# Produces (derived columns):
# - CATEGORY_NAME_CLEAN
# - CATEGORY_NAME_HARMONIZED
# - CATEGORY_CODE
# ============================================================

from __future__ import annotations
from typing import Dict
import pandas as pd

from core_utilities_structure_pandas import (
    normalize_text,
    to_uvl_code,
    is_missing_like,
    ensure_column_df,
    require_columns_df,
)

def process_structure_category_df(
    df: pd.DataFrame,
    sheet_name: str,
    category_spelling_map: Dict[str, str],
) -> pd.DataFrame:
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