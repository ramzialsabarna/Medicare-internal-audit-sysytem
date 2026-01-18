# results_pipeline/result_item_score_status_pandas.py
# ============================================================
# ITEM SCORE STATUS (RESULTS) - Unique Hierarchical Mapping
# ------------------------------------------------------------
# LOGIC: Matches the actual audit result to the unique UVL node.
# Format: iss_[ITEM_KEY]_[STATUS]
# ============================================================

from __future__ import annotations
from typing import Dict
import pandas as pd
from results_pipeline.core_utilities_results_pandas import (
    normalize_text, to_uvl_code, is_missing_like, ensure_column_df
)

def process_result_item_score_status_columns(
    df: pd.DataFrame, 
    visit_result_status_map: Dict[str, str]
) -> pd.DataFrame:
    """
    Maps the audit result to the unique 'iss_' feature name.
    """
    target_col = "ITEM_SCORE_STATUS"
    key_col = "ITEM_KEY"

    if target_col not in df.columns or key_col not in df.columns:
        return df

    df = df.copy()
    ensure_column_df(df, f"{target_col}_CODE")

    def _encode_result(row):
        raw_val = row[target_col]
        item_key = row[key_col]
        
        if is_missing_like(raw_val) or not item_key:
            return None
        
        # تنظيف الحالة (تحويل 'ناجح' أو 'Passed' إلى 'pass')
        clean_status = to_uvl_code(normalize_text(raw_val))
        mapped_status = visit_result_status_map.get(clean_status, clean_status)
        
        # النتيجة: iss_cat29__iss2__item633696_pass
        return f"iss_{item_key}_{mapped_status}"

    df[f"{target_col}_CODE"] = df.apply(_encode_result, axis=1)
    return df