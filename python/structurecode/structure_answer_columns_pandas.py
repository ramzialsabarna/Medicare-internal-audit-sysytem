# structure_answer_columns_pandas.py
from __future__ import annotations
from typing import Optional, Tuple
import pandas as pd

from core_utilities_structure_pandas import (
    normalize_text,
    to_uvl_code,
    is_missing_like,
    ensure_column_df,
    require_columns_df,
)

def process_answer_columns(df: pd.DataFrame, sheet_name: str = "") -> pd.DataFrame:
    # ✅ نضيف ITEM_KEY كمتطلب (لضمان أن البند أصلاً صالح ومُعرّف)
    require_columns_df(df, ["ITEM_FEATURE_NAME", "CHOICE_VALUE_OPTION_NAME", "ITEM_KEY"], sheet_name)
    df = df.copy()

    ensure_column_df(df, "ANSWER_TEXT_CLEAN")
    ensure_column_df(df, "ANSWER_CODE")
    ensure_column_df(df, "ANSWER_FEATURE_NAME")

    def _encode_row(row) -> Tuple[Optional[str], Optional[str], Optional[str]]:
        item_key = row["ITEM_KEY"]
        item_feat = row["ITEM_FEATURE_NAME"]
        raw_ans = row["CHOICE_VALUE_OPTION_NAME"]

        # STRICT: لازم item_key + item_feat + raw_ans
        if is_missing_like(item_key) or is_missing_like(item_feat) or is_missing_like(raw_ans):
            return None, None, None

        ans_clean = normalize_text(raw_ans).strip() or None
        if not ans_clean:
            return None, None, None

        ans_code = to_uvl_code(ans_clean)
        if not ans_code:
            return None, None, None

        # Build unique namespaced feature
        ans_feat = f"{item_feat}__{ans_code}"
        return ans_clean, ans_code, ans_feat

    encoded = df.apply(_encode_row, axis=1, result_type="expand")
    encoded.columns = ["ANSWER_TEXT_CLEAN", "ANSWER_CODE", "ANSWER_FEATURE_NAME"]

    df["ANSWER_TEXT_CLEAN"] = encoded["ANSWER_TEXT_CLEAN"]
    df["ANSWER_CODE"] = encoded["ANSWER_CODE"]
    df["ANSWER_FEATURE_NAME"] = encoded["ANSWER_FEATURE_NAME"]

    return df
