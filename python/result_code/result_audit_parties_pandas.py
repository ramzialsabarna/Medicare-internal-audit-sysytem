# results_pipeline/result_audit_parties_pandas.py
# ============================================================
# AUDIT PARTIES PROCESSING (RESULTS)
# ------------------------------------------------------------
# LOGIC & METHODOLOGY:
# - AUDITOR: Processed as Categorical Data (Fixed list). Standardized 
#   for UVL feature mapping to enable auditor-performance analysis.
# - AUDITEE: Processed as Narrative/Text Data (Free text). Cleaned 
#   using NFKC normalization to reduce manual entry noise.
# - Ensures consistent representation of names across all audit sheets.
#
# DATA TYPE: 
# - AUDITOR: Categorical / Feature-ready.
# - AUDITEE: String / Narrative.
# ============================================================

from __future__ import annotations
import pandas as pd

# Reusing core utilities for normalization
from results_pipeline.core_utilities_results_pandas import (
    normalize_text,
    to_uvl_code,
    is_missing_like,
    ensure_column_df,
)

def process_result_audit_parties_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Standardizes names of auditors and auditees for reporting and logic modeling.
    """
    df = df.copy()

    # 1. Processing AUDITOR (The Fixed List)
    if "AUDITOR" in df.columns:
        ensure_column_df(df, "AUDITOR_CLEAN")
        ensure_column_df(df, "AUDITOR_CODE") # Ready for UVL
        
        df["AUDITOR_CLEAN"] = df["AUDITOR"].apply(lambda x: normalize_text(x).strip() if not is_missing_like(x) else None)
        df["AUDITOR_CODE"] = df["AUDITOR_CLEAN"].apply(lambda x: f"aud_{to_uvl_code(x)}" if x else None)

    # 2. Processing AUDITEE (The Free-Text List)
    if "AUDITEE" in df.columns:
        ensure_column_df(df, "AUDITEE_CLEAN")
        
        # We only clean the text, we don't generate a UVL code yet to avoid "Feature Explosion"
        df["AUDITEE_CLEAN"] = df["AUDITEE"].apply(lambda x: normalize_text(x).strip() if not is_missing_like(x) else None)

    return df