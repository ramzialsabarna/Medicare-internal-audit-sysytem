# result_answer_columns_pandas.py
# ============================================================
# Answer Encoding (RESULTS) — Mirror Pipeline
# ------------------------------------------------------------
# LOGIC & METHODOLOGY:
# - Replicates the item-namespacing logic for the execution data (Results).
# - This ensures that when an auditor selects "Yes" for a specific item, 
#   the system maps it to the exact unique feature code generated in 
#   the structural configuration.
#
# DATA TYPE: 
# - Boolean/Categorical Options (Selection Logic).
#
# ERRORS & NOISE RESOLVED:
# - Mapping Disconnect: Ensures that field answers (Results) are 
#   perfectly joinable with the logical constraints defined in Structure.
# - Ambiguity Resolution: Guarantees that even if "Yes" is chosen a 
#   thousand times, each instance is tied to its specific context (item).
# ------------------------------------------------------------
# What this module does:
# - Standardizes answer selection in Results for UVL parity.
#
# Requires (raw columns):
# - ITEM_FEATURE_NAME, CHOICE_VALUE_OPTION_NAME
#
# Produces (derived columns):
# - ANSWER_TEXT_CLEAN, ANSWER_CODE, ANSWER_FEATURE_NAME
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
)

def process_result_answer_columns(df: pd.DataFrame, sheet_name: str = "") -> pd.DataFrame:
    """
    Identical mirroring of namespacing logic to ensure canonical parity.
    """
    require_columns_df(df, ["ITEM_FEATURE_NAME", "CHOICE_VALUE_OPTION_NAME"], sheet_name)
    df = df.copy()

    ensure_column_df(df, "ANSWER_TEXT_CLEAN")
    ensure_column_df(df, "ANSWER_CODE")
    ensure_column_df(df, "ANSWER_FEATURE_NAME")

    def _encode_row(row) -> Tuple[Optional[str], Optional[str], Optional[str]]:
        item_feat = row["ITEM_FEATURE_NAME"]
        raw_ans = row["CHOICE_VALUE_OPTION_NAME"]

        if is_missing_like(item_feat) or is_missing_like(raw_ans):
            return None, None, None

        ans_clean = normalize_text(raw_ans).strip() or None
        if not ans_clean:
            return None, None, None

        ans_code = to_uvl_code(ans_clean)
        if not ans_code:
            return None, None, None

        ans_feat = f"{item_feat}__{ans_code}"
        return ans_clean, ans_code, ans_feat

    encoded = df.apply(_encode_row, axis=1, result_type="expand")
    encoded.columns = ["ANSWER_TEXT_CLEAN", "ANSWER_CODE", "ANSWER_FEATURE_NAME"]

    df["ANSWER_TEXT_CLEAN"] = encoded["ANSWER_TEXT_CLEAN"]
    df["ANSWER_CODE"] = encoded["ANSWER_CODE"]
    df["ANSWER_FEATURE_NAME"] = encoded["ANSWER_FEATURE_NAME"]

    return df