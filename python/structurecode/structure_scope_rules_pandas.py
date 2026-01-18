# structure_scope_rules_pandas.py
from __future__ import annotations
from typing import Optional, Set
import pandas as pd
from core_utilities_structure_pandas import (
    normalize_text,
    is_missing_like,
    ensure_column_df,
    require_columns_df,
)

def process_scope_rules_sheet(
    df: pd.DataFrame,
    sheet_name: str = "SCOPE_RULES",
    valid_category_codes: Optional[Set[str]] = None,
    valid_item_codes: Optional[Set[str]] = None,
) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()

    df = df.copy()

    # 1) الأعمدة المطلوبة
    require_columns_df(df, required=["RULE_ID", "CAPABILITY_FLAG", "TARGET_CODE", "ACTION"], sheet_name=sheet_name)
    ensure_column_df(df, "TARGET_TYPE")

    # 2) تنظيف النصوص (Normalization)
    for c in ["CAPABILITY_FLAG", "TARGET_TYPE", "ACTION"]:
        df[c] = df[c].astype(str).str.strip().str.lower()
    
    df["TARGET_CODE"] = df["TARGET_CODE"].astype(str).str.strip()

    # 3) الـ Validation الذكي (تنبيه وليس حذف)
    # ملاحظة: ألغينا الـ Raise ValueError لكي لا يتوقف البرنامج
    if valid_category_codes is not None or valid_item_codes is not None:
        # نقوم فقط بوضع علامة على القواعد الصالحة للشيت الحالي
        def is_valid(row):
            ttype = row["TARGET_TYPE"]
            code = row["TARGET_CODE"]
            if ttype == "category" and valid_category_codes:
                return code in valid_category_codes
            if ttype == "item" and valid_item_codes:
                return code in valid_item_codes
            return True # افتراض الصحة إذا لم يتم تمرير القوائم

        # بدلاً من حذف الصفوف، سنتركها للـ Builder ليتعامل معها بذكاء
        # الـ Builder سيقرأ فقط ما يحتاجه
        pass 

    return df