# structure_branch_profile_pandas.py
from __future__ import annotations
from typing import Dict, Set, List, Any, Optional
import pandas as pd

from core_utilities_structure_pandas import (
    ensure_column_df,
    require_columns_df,
    normalize_flag01,
)
from structure_branch_columns import process_branch_columns

_ALLOWED_ENTITY_TYPES = {"branch", "dep", "org"}
_FLAG_COLS = ["iso_active", "micro_active", "path_active"]

def process_branch_profile_sheet(
    df: pd.DataFrame,
    branch_overrides: Dict[str, str],
    dep_labels: Set[str],
    sheet_name: str = "BRANCH_PROFILE",
) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame() # إرجاع فريم فارغ بدل الانهيار

    df = df.copy()

    # 1) التأكد من وجود الأعمدة الأساسية
    require_columns_df(df, required=["BRANCH_ID", "BRANCH_NAME"], sheet_name=sheet_name)
    ensure_column_df(df, "notes")
    ensure_column_df(df, "ENTITY_TYPE")
    ensure_column_df(df, "BRANCH_FEATURE_CODE")

    # 2) معالجة الهوية (Regenerate identity)
    df = process_branch_columns(df=df, branch_overrides=branch_overrides, dep_labels=dep_labels)

    # 3) تطبيع القيم (Normalization) بدلاً من الـ Validation القاتل
    for c in _FLAG_COLS:
        if c in df.columns:
            # تحويل القيم المفقودة لـ 0 بدلاً من رفع Error
            df[c] = df[c].fillna(0).apply(normalize_flag01)
        else:
            df[c] = 0 # إنشاء العمود بصفر إذا لم يوجد

    # 4) التحقق من ENTITY_TYPE (تصحيح بدل حذف)
    if "ENTITY_TYPE" in df.columns:
        df["ENTITY_TYPE"] = df["ENTITY_TYPE"].fillna("branch")

    return df