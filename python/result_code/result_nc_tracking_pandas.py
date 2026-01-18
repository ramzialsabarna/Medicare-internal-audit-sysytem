# results_pipeline/result_nc_tracking_pandas.py
# ============================================================
# NC TRACKING & INTEGRITY LOGIC (RESULTS) — Hierarchical Unique Mapping
# ------------------------------------------------------------
# LOGIC & METHODOLOGY:
# 1. CLEANING: Standardizes 6 NC-related narrative columns.
# 2. UNIQUE UVL MAPPING: Generates feature names that match the 
#    structure: nc_[ITEM_KEY]_[STEP_CODE].
# 3. HIERARCHICAL INTEGRITY: Prepares data to be validated strictly 
#    under the 'Fail' outcome of each specific item.
# ============================================================

from __future__ import annotations
import pandas as pd
from results_pipeline.core_utilities_results_pandas import (
    normalize_text,
    is_missing_like,
    ensure_column_df,
    to_uvl_code,
)

def process_result_nc_tracking(df: pd.DataFrame) -> pd.DataFrame:
    """
    Processes NC columns to create unique UVL features that mirror the structure.
    """
    # الأعمدة الستة كما هي في الإكسيل الخام
    nc_columns = [
        "NC_RESPONSIBLE_PERSON",
        "NC_EXPECTED_COMPLETION_DATE",
        "NC_ROOT_CAUSE",
        "NC_PREVENTIVE_ACTION",
        "NC_CORRECTIVE_ACTION",
        "NC_FOLLOW_UP_EFFECTIVENESS"
    ]

    df = df.copy()
    key_col = "ITEM_KEY" # المفتاح الذي يضمن عدم التكرار (cat__iss__item)

    # التحقق من وجود المفتاح قبل البدء
    if key_col not in df.columns:
        return df

    # --- الجزء الأول: التنظيف والتحويل إلى ميزات فريدة (Unique Features) ---
    for col in nc_columns:
        if col not in df.columns:
            continue

        clean_col = f"{col}_CLEAN"
        # هذا العمود سيحمل الاسم النهائي الذي سيبحث عنه الـ UVL
        uvl_feature_col = f"UVL_{col}_FEATURE" 

        ensure_column_df(df, clean_col)
        ensure_column_df(df, uvl_feature_col)

        def _process_nc_row(row):
            val = row[col]
            item_key = row[key_col]
            
            # 1. تنظيف النص للتأكد من وجود محتوى حقيقي
            if is_missing_like(val):
                clean_txt = None
                has_data = False
            else:
                clean_txt = normalize_text(val).strip()
                has_data = True if clean_txt else False
            
            # 2. بناء اسم الميزة الفريد (يجب أن يطابق كود الستركشر تماماً)
            # مثال: nc_cat29__iss2__item633696_root_cause
            if has_data:
                # تحويل اسم العمود إلى كود نظيف (مثل root_cause)
                step_slug = to_uvl_code(col.replace("NC_", ""))
                feature_name = f"nc_{item_key}_{step_slug}"
            else:
                feature_name = None
            
            return clean_txt, feature_name

        results = df.apply(_process_nc_row, axis=1)
        df[clean_col] = results.apply(lambda x: x[0])
        df[uvl_feature_col] = results.apply(lambda x: x[1])

    # --- الجزء الثاني: منطق كشف "مخالفة النزاهة" (Violation Logic) ---
    # نربط النتيجة (Fail) بوجود الإجراء التصحيحي (Corrective Action)
    res_col = "CHOICE_VALUE_CODE" 
    violation_col = "NC_INTEGRITY_VIOLATION" 
    violation_uvl_col = "UVL_NC_VIOLATION_FEATURE"

    ensure_column_df(df, violation_col)
    ensure_column_df(df, violation_uvl_col)

    def _detect_violation(row):
        result_val = str(row.get(res_col, "")).lower()
        # نعتمد على العمود النظيف لضمان الدقة
        has_corrective = not is_missing_like(row.get("NC_CORRECTIVE_ACTION_CLEAN"))
        item_key = row.get(key_col)
        
        # إذا كانت النتيجة راسبة ولم نجد إجراءً تصحيحياً
        if "fail" in result_val and not has_corrective:
            # ننشئ فيشر فريد للمخالفة أيضاً للتحليل لاحقاً
            violation_feat = f"violation_{item_key}_missing_action"
            return True, violation_feat
        return False, None

    if res_col in df.columns:
        violation_results = df.apply(_detect_violation, axis=1)
        df[violation_col] = violation_results.apply(lambda x: x[0])
        df[violation_uvl_col] = violation_results.apply(lambda x: x[1])

    return df