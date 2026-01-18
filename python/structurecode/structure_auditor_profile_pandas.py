# structure_auditor_profile_pandas.py
# ============================================================
# USERS / AUDITOR CAPABILITIES (STRUCTURE) — Pandas (PRESERVE ALL ROWS)
# ============================================================
# OFFICIAL DESIGN (UPDATED):
# - Source sheet: users (Single Source of Truth)
# - PRESERVE all original rows (do NOT filter out non-auditors)
# - ID is the authoritative key
# - FULL_NAME_EN is the authoritative label
# - is_auditor determines whether AUDITOR_FEATURE_CODE is generated
# - Flags are MANUAL decisions (QA Manager)
# - Code only normalizes + validates (0/1) + overwrites derived columns
# - Non-auditors: force capability flags to 0 (safety)
# ============================================================

from __future__ import annotations
from typing import Any, Optional
import pandas as pd

from core_utilities_structure_pandas import (
    normalize_text,
    to_uvl_code,
    is_missing_like,
    ensure_column_df,
    require_columns_df,
    normalize_id_token,
    normalize_flag01,
)

_FLAG_COLS = ["iso_auditor", "micro_auditor", "path_auditor", "senior_auditor"]


def process_users_auditor_profile_sheet(
    df: pd.DataFrame,
    sheet_name: str = "users",
) -> pd.DataFrame:
    if df is None or df.empty:
        raise ValueError("CRITICAL ERROR: users sheet is empty.")

    df = df.copy()

    # ---------------------------------------------------------
    # 1) Required columns
    # ---------------------------------------------------------
    require_columns_df(
        df,
        required=["ID", "FULL_NAME_EN", "is_auditor", *_FLAG_COLS],
        sheet_name=sheet_name,
    )

    ensure_column_df(df, "notes")

    # ---------------------------------------------------------
    # 2) Normalize is_auditor flag (ALL rows must be explicit 0/1)
    # ---------------------------------------------------------
    df["is_auditor"] = df["is_auditor"].apply(normalize_flag01)
    if df["is_auditor"].isna().any():
        raise ValueError(
            f"CRITICAL ERROR: Null/invalid values in '{sheet_name}.is_auditor'. "
            f"Fill all rows with 0 or 1."
        )

    # ---------------------------------------------------------
    # 3) Normalize capability flags (datatype only)
    #    - ALL rows must be explicit 0/1
    #    - Non-auditors are forced to 0 (safety)
    # ---------------------------------------------------------
    for c in _FLAG_COLS:
        df[c] = df[c].apply(normalize_flag01)
        if df[c].isna().any():
            raise ValueError(
                f"CRITICAL ERROR: Auditor flag '{sheet_name}.{c}' contains null/invalid values. "
                f"All rows must have explicit 0/1."
            )
        # Safety: non-auditors cannot keep capabilities
        df.loc[df["is_auditor"] == 0, c] = 0

    # ---------------------------------------------------------
    # 4) Build derived identity columns (OVERWRITE / IDEMPOTENT)
    # ---------------------------------------------------------
    ensure_column_df(df, "AUDITOR_ID_TOKEN")
    ensure_column_df(df, "AUDITOR_LABEL_CLEAN")
    ensure_column_df(df, "AUDITOR_LABEL_SLUG")
    ensure_column_df(df, "AUDITOR_FEATURE_CODE")

    def _id_token(val: Any) -> Optional[str]:
        tok = normalize_id_token(val)
        return tok if tok else None

    df["AUDITOR_ID_TOKEN"] = df["ID"].apply(_id_token)

    def _clean_label(val: Any) -> Optional[str]:
        if is_missing_like(val):
            return None
        s = normalize_text(val).strip()
        return s if s else None

    df["AUDITOR_LABEL_CLEAN"] = df["FULL_NAME_EN"].apply(_clean_label)
    df["AUDITOR_LABEL_SLUG"] = df["AUDITOR_LABEL_CLEAN"].apply(to_uvl_code)

    def _make_feature(row) -> Optional[str]:
        # Only auditors have a feature identity in FM
        if int(row["is_auditor"]) != 1:
            return None
        if not row["AUDITOR_ID_TOKEN"]:
            return None
        slug = row["AUDITOR_LABEL_SLUG"]
        if slug:
            return f"auditor_{row['AUDITOR_ID_TOKEN']}__{slug}"
        return f"auditor_{row['AUDITOR_ID_TOKEN']}"

    df["AUDITOR_FEATURE_CODE"] = df.apply(_make_feature, axis=1)

    # ---------------------------------------------------------
    # 5) Final validations (FATAL) for auditors only
    # ---------------------------------------------------------
    aud = df[df["is_auditor"] == 1].copy()
    if aud.empty:
        raise ValueError(
            "CRITICAL ERROR: No auditors found (is_auditor=1). "
            "System cannot build auditor constraints."
        )

    if aud["AUDITOR_FEATURE_CODE"].isna().any():
        raise ValueError(
            "CRITICAL ERROR: Failed to generate AUDITOR_FEATURE_CODE for some auditors. "
            "Check ID and FULL_NAME_EN."
        )

    if aud["AUDITOR_FEATURE_CODE"].duplicated().any():
        dups = aud.loc[aud["AUDITOR_FEATURE_CODE"].duplicated(keep=False), "AUDITOR_FEATURE_CODE"].tolist()
        raise ValueError(f"CRITICAL ERROR: Duplicate AUDITOR_FEATURE_CODE among auditors: {dups}")

    # ---------------------------------------------------------
    # 6) Output: FULL users sheet preserved (NO row deletion)
    # ---------------------------------------------------------
    return df
