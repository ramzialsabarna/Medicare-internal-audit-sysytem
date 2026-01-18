# core_utilities_results_pandas.py
# ============================================================
# RESULTS Utilities (Pandas) — Canonical Parity Layer
# ------------------------------------------------------------
# LOGIC & METHODOLOGY:
# - This module is an exact functional mirror of the structure utilities.
# - It ensures that data extracted from the field (Results) is normalized 
#   using the same NFKC and Slugification logic as the configuration (Structure).
# - This parity is critical for successful joins between Results and UVL features.
#
# DATA INTEGRITY:
# - Standardizes Arabic text normalization to handle lab supervisor input.
# - Ensures numeric IDs (Branches, Items) are tokenized identically to 
#   avoid "Key Mismatches" caused by Excel's float formatting.
# ============================================================

from __future__ import annotations
from typing import Any, List, Optional
import re
import unicodedata
import pandas as pd

# Standard missing tokens shared across the entire project
_MISSING_TOKENS = {"", "nan", "none", "null", "na", "n/a", "-", "--", "?"}

def normalize_text(val: Any) -> str:
    """
    Mirror: Normalizes results text (Arabic/English) using NFKC.
    """
    if val is None:
        return ""
    try:
        if pd.isna(val):
            return ""
    except Exception:
        pass

    s = str(val).strip()
    s = unicodedata.normalize('NFKC', s)
    
    if not s:
        return ""
    s = re.sub(r"\s+", " ", s)
    return s

def is_missing_like(val: Any) -> bool:
    """
    Mirror: Detects missing data in audit results sheets.
    """
    if val is None:
        return True
    try:
        if pd.isna(val):
            return True
    except Exception:
        pass

    s = normalize_text(val)
    if not s:
        return True
    return s.strip().lower() in _MISSING_TOKENS

def to_uvl_code(val: Any) -> Optional[str]:
    """
    Mirror: Generates identical slugs for result answers to match UVL features.
    """
    if is_missing_like(val):
        return None

    s = normalize_text(val)
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = s.lower()

    s = re.sub(r"[^a-z0-9_]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")

    if not s:
        return None
    if not re.match(r"^[a-z_]", s):
        s = f"_{s}"
    return s

def ensure_column_df(df: pd.DataFrame, col_name: str) -> pd.DataFrame:
    """Safely adds columns to results dataframe."""
    if col_name not in df.columns:
        df[col_name] = None
    return df

def require_columns_df(df: pd.DataFrame, required: List[str], sheet_name: str = "") -> None:
    """Enforces header integrity for results sheets."""
    missing = [c for c in required if c not in df.columns]
    if missing:
        where = f" in result sheet '{sheet_name}'" if sheet_name else ""
        raise ValueError(f"Missing required columns{where}: {missing}")

def normalize_id_token(val: Any) -> Optional[str]:
    """
    Mirror: Ensures Branch/Item IDs in Results match those in Structure.
    """
    if val is None:
        return None
    try:
        if pd.isna(val):
            return None
    except Exception:
        pass

    s = str(val).strip()
    if s.lower() in _MISSING_TOKENS:
        return None

    if isinstance(val, (int, float)):
        f_val = float(val)
        if f_val.is_integer():
            return str(int(f_val))
        return s.rstrip("0").rstrip(".")

    try:
        f = float(s)
        if f.is_integer():
            return str(int(f))
        return str(f).rstrip("0").rstrip(".")
    except Exception:
        return s