# results_pipeline/result_bulk_datetime_processing_pandas.py
# ============================================================
# BULK DATETIME PROCESSING (RESULTS)
# ------------------------------------------------------------
# LOGIC & METHODOLOGY:
# - Standardizes visit and approval timestamps into formal Datetime objects.
# - Handles complex Oracle-style formats including month names and fractional seconds.
# - Coerces invalid or corrupt date entries into NaT (Not a Time) for clean analysis.
# - Ensures these columns are ready for time-series grouping and duration calculations.
#
# DATA TYPE: 
# - Datetime64 (Temporal Object).
#
# COLUMNS TREATED:
# - VISIT_DATE, DATE_VISIT_ENTRY, DATE_VISIT_APPROVED.
# ============================================================

from __future__ import annotations
import pandas as pd

# Importing core utilities for missingness detection and column assurance
from results_pipeline.core_utilities_results_pandas import (
    is_missing_like,
    ensure_column_df,
)

def process_result_bulk_datetime_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Standardizes a predefined list of date and time columns into clean Datetime formats.
    """
    # List of date-related columns provided by the partner
    date_columns = [
        "VISIT_DATE",
        "DATE_VISIT_ENTRY",
        "DATE_VISIT_APPROVED"
    ]

    df = df.copy()

    for col in date_columns:
        if col not in df.columns:
            continue

        clean_col = f"{col}_CLEAN"
        ensure_column_df(df, clean_col)

        # Methodology: Using to_datetime with errors='coerce' to handle mixed formats.
        # The logic detects strings like '17-DEC-2025 13:37:51.749000' automatically.
        if not pd.api.types.is_datetime64_any_dtype(df[col]):
            # pd.to_numeric handles standard ISO and Oracle formatted strings
            df[clean_col] = pd.to_datetime(df[col], errors='coerce')
        else:
            # If already datetime (e.g., from Excel reader), just copy it
            df[clean_col] = df[col]

    return df