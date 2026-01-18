# results_pipeline/result_bulk_numeric_metrics_pandas.py
# ============================================================
# BULK NUMERIC METRICS PROCESSING (RESULTS)
# ------------------------------------------------------------
# LOGIC & METHODOLOGY:
# - Standardizes all mathematical scoring and weight columns into Float64.
# - Processes 10 specific audit metrics to ensure they are calculation-ready.
# - Implements 'FINAL_ITEM_SCORE' as the primary source of truth for items.
# - Coerces errors (text/symbols) to 0.0 to prevent pipeline crashes.
#
# DATA TYPE: 
# - Float64 (Mathematical Numeric).
#
# COLUMNS TREATED:
# - CATEGORY_GENERAL_SCORE, CATEGORY_MIN_ACC_SCORE, WEIGHT_PERCENTAGE,
#   C_ITEM_MIN_ACC_SCORE, OPTION_VALUE, ITEM_SCORE, CATEGORY_WEIGHT_SUM,
#   CATEGORY_OVERALL_SCORE, V_RESULT_MIN_ACC_SCORE, OVERALL_VISIT_RESULT_SCORE.
# ============================================================

from __future__ import annotations
import pandas as pd

# Reusing core utilities for missingness detection and column assurance
from results_pipeline.core_utilities_results_pandas import (
    is_missing_like,
    ensure_column_df,
)

def process_result_bulk_numeric_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """
    Standardizes a predefined list of numerical metrics into clean float formats.
    """
    # Exact list of columns provided by the partner
    metric_columns = [
        "CATEGORY_GENERAL_SCORE",
        "CATEGORY_MIN_ACC_SCORE",
        "WEIGHT_PERCENTAGE",
        "C_ITEM_MIN_ACC_SCORE",
        "OPTION_VALUE",
        "ITEM_SCORE",
        "CATEGORY_WEIGHT_SUM",
        "CATEGORY_OVERALL_SCORE",
        "V_RESULT_MIN_ACC_SCORE",
        "OVERALL_VISIT_RESULT_SCORE"
    ]

    df = df.copy()

    for col in metric_columns:
        if col not in df.columns:
            continue

        clean_col = f"{col}_CLEAN"
        ensure_column_df(df, clean_col)

        def _to_clean_float(val):
            """Internal logic to force numeric conversion and handle missing tokens."""
            if is_missing_like(val):
                return 0.0
            try:
                # Converts strings, ints, or floats into a stable Float64
                return float(pd.to_numeric(val, errors='coerce'))
            except (ValueError, TypeError):
                return 0.0

        # Bulk processing of the column
        df[clean_col] = df[col].apply(_to_clean_float).fillna(0.0)

    # --- FINAL_ITEM_SCORE LOGIC (The primary reference for calculations) ---
    # We prioritize ITEM_SCORE_CLEAN as it represents the audited result.
    if "ITEM_SCORE_CLEAN" in df.columns:
        df["FINAL_ITEM_SCORE"] = df["ITEM_SCORE_CLEAN"]
    elif "OPTION_VALUE_CLEAN" in df.columns:
        df["FINAL_ITEM_SCORE"] = df["OPTION_VALUE_CLEAN"]
    else:
        df["FINAL_ITEM_SCORE"] = 0.0

    return df