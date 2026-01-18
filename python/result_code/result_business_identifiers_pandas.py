# results_pipeline/result_business_identifiers_pandas.py
# ============================================================
# BUSINESS IDENTIFIERS PROCESSING (RESULTS)
# ------------------------------------------------------------
# LOGIC & METHODOLOGY:
# - Standardizes complex business codes (RESULTCODE & SOLUTION_CODE).
# - Removes all internal whitespaces and enforces Uppercase formatting.
# - Preserves essential structural delimiters (/ , _ , -) while stripping
#   non-alphanumeric noise.
# - Ensures consistency across multi-part identifiers for precise tracing.
#
# DATA TYPE: 
# - String / Token (Business Identifier).
#
# COLUMNS TREATED:
# - RESULTCODE: The unique audit record identifier.
# - SOLUTION_CODE: The linked corrective action/solution identifier.
# ============================================================

from __future__ import annotations
import pandas as pd
import re

# Standard utilities for missing data and column management
from results_pipeline.core_utilities_results_pandas import (
    is_missing_like,
    ensure_column_df,
)

def process_result_business_identifiers(df: pd.DataFrame) -> pd.DataFrame:
    """
    Standardizes RESULTCODE and SOLUTION_CODE by removing noise and unifying format.
    """
    target_columns = ["RESULTCODE", "SOLUTION_CODE"]
    
    df = df.copy()

    for col in target_columns:
        if col not in df.columns:
            continue

        clean_col = f"{col}_CLEAN"
        ensure_column_df(df, clean_col)

        def _clean_business_code(val):
            """
            Internal logic to sanitize business identifiers:
            1. Forces Uppercase and strips outer spaces.
            2. Removes all internal whitespaces.
            3. Retains only alphanumeric chars and delimiters (/, _, -).
            """
            if is_missing_like(val):
                return None
            
            # Convert to string and force Uppercase
            text = str(val).upper().strip()
            
            # Remove internal whitespaces
            text = re.sub(r'\s+', '', text)
            
            # Retain only Alphanumeric and essential delimiters (/ , _ , -)
            # This prevents symbols or hidden characters from breaking the ID
            text = re.sub(r'[^A-Z0-9/_/-]', '', text)
            
            return text if text else None

        # Apply the regex-based cleaning
        df[clean_col] = df[col].apply(_clean_business_code)

    return df