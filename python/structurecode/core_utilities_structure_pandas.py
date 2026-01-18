# structurecode/core_utilities_structure_pandas.py
from __future__ import annotations
from typing import Any, List, Optional
import re
import unicodedata
import pandas as pd

# missing lists
_MISSING_TOKENS = {"", "nan", "none", "null", "na", "n/a", "-", "--", "?"}

def normalize_text(val: Any) -> str:
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
    if col_name not in df.columns:
        df[col_name] = None
    return df

def require_columns_df(df: pd.DataFrame, required: List[str], sheet_name: str = "") -> None:
    missing = [c for c in required if c not in df.columns]
    if missing:
        where = f" in sheet '{sheet_name}'" if sheet_name else ""
        raise ValueError(f"CRITICAL ERROR: Missing columns{where}: {missing}")

def normalize_id_token(val: Any) -> Optional[str]:
    if val is None: return None
    try:
        if pd.isna(val): return None
    except Exception: pass
    s = str(val).strip()
    if s.lower() in _MISSING_TOKENS: return None
    if isinstance(val, (int, float)):
        f_val = float(val)
        if f_val.is_integer(): return str(int(f_val))
        return s.rstrip("0").rstrip(".")
    try:
        f = float(s)
        if f.is_integer(): return str(int(f))
        return str(f).rstrip("0").rstrip(".")
    except Exception:
        return s

# --- الدالة المضافة لحل مشكلة الرنر الصارم ---
def normalize_flag01(val: Any) -> int:
    """
    تحويل قيم الأعلام (iso_active, micro_active, etc) إلى 0 أو 1 حصراً.
    تتعامل مع المدخلات: 'Active', 1, 1.0, 'Yes', '1' -> تعيد 1
    تتعامل مع: NaN, 'No', 0, 'Inactive' -> تعيد 0
    """
    if is_missing_like(val):
        return 0
    s = str(val).strip().lower()
    # الحالات التي نعتبرها True/Active
    if s in {'1', '1.0', 'true', 'active', 'yes', 'y', 'enabled'}:
        return 1
    return 0