# results_pipeline/result_bulk_identifier_tokens_pandas.py
# ============================================================
# BULK IDENTIFIER TOKENS PROCESSING (RESULTS)
# ------------------------------------------------------------
# LOGIC & METHODOLOGY:
# - Standardizes primary and foreign keys (IDs) into stable string tokens.
# - Prevents "Numeric Drift" where Excel converts long IDs into floats (e.g., 10.0).
# - Strips '.0' artifacts to ensure perfect parity between Structure and Results.
# - These columns are treated as IDENTIFIERS for joining, not for math.
#
# DATA TYPE: 
# - String / Object (Canonical Tokens).
#
# COLUMNS TREATED (Full List of Identifiers):
# - VISIT_ID, VISIT_RESULT_ID, BRANCH_ID, CATEGORY_ID, ISSUE_NUMBER,
#   LAB_SECTION_ID, CHECK_ITEM_ID, CHOICE_ID, CHOICE_VALUE_OPTION_ID.
# ============================================================

from __future__ import annotations
import pandas as pd

# Standard utilities for missing data and ID normalization
from results_pipeline.core_utilities_results_pandas import (
    is_missing_like,
    ensure_column_df,
    normalize_id_token,
)

def process_result_bulk_identifier_tokens(df: pd.DataFrame) -> pd.DataFrame:
    """
    Standardizes all ID columns into clean string tokens to ensure join stability.
    """
    # Exhaustive list of identifier columns provided by the partner
    id_columns = [
        "VISIT_ID",
        "VISIT_RESULT_ID",
        "BRANCH_ID",
        "CATEGORY_ID",
        "ISSUE_NUMBER",
        "LAB_SECTION_ID",
        "CHECK_ITEM_ID",
        "CHOICE_ID",
        "CHOICE_VALUE_OPTION_ID"
    ]

    df = df.copy()

    for col in id_columns:
        if col not in df.columns:
            continue

        clean_col = f"{col}_CLEAN"
        ensure_column_df(df, clean_col)

        # Methodology: Use normalize_id_token to strip decimals and force string type
        # We don't use 0.0 here; if an ID is missing, it remains None/NA
        df[clean_col] = df[col].apply(lambda x: normalize_id_token(x) if not is_missing_like(x) else None)

    return df