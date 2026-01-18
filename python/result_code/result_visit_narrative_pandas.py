# results_pipeline/result_visit_narrative_pandas.py
# ============================================================
# VISIT NARRATIVE PROCESSING (RESULTS)
# ------------------------------------------------------------
# LOGIC & METHODOLOGY:
# - Standardizes the 'VISIT' descriptive string.
# - Cleans non-standard characters and collapses multiple whitespaces.
# - This column serves as a "Human-Readable Key" for reporting.
# - IMPORTANT: This column is NOT used for UVL logic mapping as it 
#   contains redundant concatenated data.
#
# DATA TYPE: 
# - String / Text (Narrative).
#
# ERRORS & NOISE RESOLVED:
# - Encoding Noise: Normalizes Arabic/English text using NFKC.
# - Whitespace issues: Removes leading/trailing spaces and internal double spacing.
# ============================================================

from __future__ import annotations
import pandas as pd

# Standard utilities for text normalization
from results_pipeline.core_utilities_results_pandas import (
    normalize_text,
    is_missing_like,
    ensure_column_df,
)

def process_result_visit_narrative_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Standardizes the VISIT descriptive column for reporting purposes.
    """
    target_col = "VISIT"

    if target_col not in df.columns:
        return df

    df = df.copy()

    ensure_column_df(df, f"{target_col}_CLEAN")

    def _clean_narrative(val):
        """Cleans and normalizes the narrative text."""
        if is_missing_like(val):
            return None
        # Normalize text (NFKC) and strip spaces
        txt = normalize_text(val).strip()
        return txt if txt else None

    # Apply cleaning
    df[f"{target_col}_CLEAN"] = df[target_col].apply(_clean_narrative)

    return df