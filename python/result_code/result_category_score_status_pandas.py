# results_pipeline/result_category_score_status_pandas.py
# ============================================================
# CATEGORY_OVERALL_SCORE_STATUS Encoding (RESULTS) - Unique
# ------------------------------------------------------------
# LOGIC & METHODOLOGY:
# - Standardizes category outcomes (Passed/Failed).
# - Ensures UNIQUENESS by merging Category ID with the status.
# - Format: coss_cat[ID]_[status]
# ============================================================

from __future__ import annotations
from typing import Dict
import pandas as pd
from results_pipeline.core_utilities_results_pandas import (
    normalize_text,
    to_uvl_code,
    is_missing_like,
    ensure_column_df,
    normalize_id_token,
)

def process_result_category_score_status_columns(
    df: pd.DataFrame,
    visit_result_status_map: Dict[str, str],
) -> pd.DataFrame:
    """
    Standardizes category performance into UNIQUE UVL-compliant codes.
    """
    target_col = "CATEGORY_OVERALL_SCORE_STATUS"
    cat_id_col = "CHECK_CATEGORY_ID" # المعرف لضمان الفرادة
    
    if target_col not in df.columns:
        return df

    df = df.copy()

    ensure_column_df(df, f"{target_col}_CLEAN")
    ensure_column_df(df, f"{target_col}_CODE")

    def _clean(raw) -> str | None:
        if is_missing_like(raw):
            return None
        txt = normalize_text(raw).strip()
        return txt if txt else None

    # تطبيق التنظيف
    df[f"{target_col}_CLEAN"] = df[target_col].apply(_clean)

    def _encode_unique_cat(row) -> str | None:
        clean_val = row[f"{target_col}_CLEAN"]
        cat_id = normalize_id_token(row.get(cat_id_col))
        
        if not clean_val or not cat_id:
            return None
        
        # تحويل الحالة لـ slug (مثلاً: passed -> pass)
        slug = to_uvl_code(clean_val) or "unknown"
        mapped = visit_result_status_map.get(slug, slug)
        
        # النتيجة النهائية الفريدة: coss_cat29_pass
        return f"coss_cat{cat_id}_{mapped}"

    df[f"{target_col}_CODE"] = df.apply(_encode_unique_cat, axis=1)

    return df