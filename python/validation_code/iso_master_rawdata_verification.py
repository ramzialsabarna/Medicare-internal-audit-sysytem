#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
iso_master_rawdata_verification.py
----------------------------------
One script → one Excel report containing all RAW DATA verification checks:

A) ID Verification
   - Missing values (raw + normalized)
   - Numeric drift signals (.0 / scientific notation)
   - Values changed by normalization
   - Uniqueness/duplicates
   - Composite duplicates across ID columns
   - Dependency checks with samples

B) Audit Type / Audit Plan Verification (FIXED rule)
   - AUDIT_PLAN required ONLY for planned
   - AUDIT_PLAN allowed missing for: unplanned, reevaluate
   - Distribution + planned-missing-plan metric + violations sample

C) Structure Name Verification
   - CHECK_CATEGORY_NAME / CHECK_ITEM_NAME / CHOICE_VALUE_OPTION_NAME
   - Missing counts, dependency checks
   - Unique counts (normalized text)
   - Category -> distinct items
   - (Category, Item) -> distinct answers
   - Missing samples

D) Numeric Columns Verification
   - GENERAL_SCORE / CATEGORY_MIN_ACCEPTABLE_SCORE / WEIGHT_PERCENTAGE / CI_MIN_ACCEPTABLE_SCORE / OPTION_VALUE
   - Missing + non-numeric + numeric stats
   - Samples

Run (defaults expect ISO_DATA.xlsx in current folder):
  python .\iso_master_rawdata_verification.py

Or:
  python .\iso_master_rawdata_verification.py --excel .\ISO_DATA.xlsx --out .\ISO_Master_RAWDATA_Verification_Report.xlsx --prefix ISO_Check_categor
"""

from __future__ import annotations
import argparse
import math
import os
import re
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

import pandas as pd


# =============================
# Common utilities
# =============================
_MISSING_STRINGS = {"", "nan", "none", "null", "na", "n/a", "nil", "-"}

def is_missing_like(x: Any) -> bool:
    if x is None:
        return True
    try:
        if pd.isna(x):
            return True
    except Exception:
        pass
    if isinstance(x, str):
        return x.strip().lower() in _MISSING_STRINGS
    return False

def norm_colname(c: str) -> str:
    return re.sub(r"\s+", "", str(c).strip().upper())

def find_column(df: pd.DataFrame, target: str) -> Optional[str]:
    t = norm_colname(target)
    mapping = {norm_colname(c): c for c in df.columns}
    return mapping.get(t)

def _ensure_excel_exists(excel_arg: str) -> Path:
    """
    Robust resolver:
    - If provided file exists -> use it.
    - Else if ISO_DATA.xlsx exists in current dir -> use it.
    - Else show available xlsx files and raise.
    """
    p = Path(excel_arg)
    if p.exists():
        return p

    fallback = Path("ISO_DATA.xlsx")
    if fallback.exists():
        print(f"⚠ ملف غير موجود: {excel_arg} | سيتم استخدام: {fallback.name}")
        return fallback

    xlsx_files = sorted([f for f in Path(".").glob("*.xlsx")])
    raise FileNotFoundError(
        f"Excel file not found: {excel_arg}\n"
        f"Working directory: {Path('.').resolve()}\n"
        f"Available .xlsx files: {[f.name for f in xlsx_files]}"
    )

def safe_sheet_name(name: str) -> str:
    """Excel sheet name max length 31."""
    name = str(name).strip()
    return name[:31] if len(name) > 31 else name

def infer_value_type(series: pd.Series) -> str:
    vals = [v for v in series.tolist() if not is_missing_like(v)]
    if not vals:
        return "empty"
    if all(isinstance(v, (pd.Timestamp,)) for v in vals):
        return "datetime"

    def can_num(v: Any) -> bool:
        try:
            float(str(v).strip().replace("%", ""))
            return True
        except Exception:
            return False

    if all(can_num(v) for v in vals):
        return "numeric_like"
    if all(isinstance(v, str) for v in vals):
        return "string"
    return "mixed"


# =============================
# A) ID Verification
# =============================
_SCI_NOTATION_RE = re.compile(r"^[+-]?\d+(\.\d+)?[eE][+-]?\d+$")
_FLOAT_ARTIFACT_RE = re.compile(r"^[+-]?\d+\.0+$")

def _decimal_from_str(s: str) -> Optional[Decimal]:
    try:
        return Decimal(s)
    except (InvalidOperation, ValueError):
        return None

def normalize_id_token(x: Any) -> Optional[str]:
    """Normalize ID values to stable string tokens (avoid 54.0 / scientific notation)."""
    if is_missing_like(x):
        return None

    if isinstance(x, (int,)) and not isinstance(x, bool):
        return str(x)

    if isinstance(x, (float,)) and not isinstance(x, bool):
        if math.isnan(x) or math.isinf(x):
            return None
        if abs(x - round(x)) < 1e-9:
            return str(int(round(x)))
        d = _decimal_from_str(repr(x)) or _decimal_from_str(str(x))
        if d is None:
            return str(x).strip()
        return format(d.normalize(), "f")

    s = str(x).strip()
    if s == "":
        return None

    if _FLOAT_ARTIFACT_RE.match(s):
        return s.split(".", 1)[0]

    if _SCI_NOTATION_RE.match(s):
        d = _decimal_from_str(s)
        if d is None:
            return s
        if d == d.to_integral_value():
            return str(int(d))
        return format(d.normalize(), "f")

    if re.match(r"^[+-]?\d+\.\d+$", s):
        d = _decimal_from_str(s)
        if d is None:
            return s
        if d == d.to_integral_value():
            return str(int(d))
        return format(d.normalize(), "f")

    return s

def _count_numeric_drift_signals(series: pd.Series) -> int:
    n = 0
    for v in series.dropna():
        if isinstance(v, float) and not math.isnan(v) and abs(v - round(v)) < 1e-9:
            n += 1
            continue
        if isinstance(v, str):
            s = v.strip()
            if _FLOAT_ARTIFACT_RE.match(s) or _SCI_NOTATION_RE.match(s):
                n += 1
    return n

ID_COLUMNS = [
    "BRANCH_ID",
    "CHECK_CATEGORY_ID",
    "ISSUE_NUMBER",
    "CHECK_ITEM_ID",
    "LAB_SECTION_ID",
    "CHOICE_ID",
    "CHOICE_VALUE_OPTION_ID",
]

ID_DEP_RULES = [
    ("CHOICE_VALUE_OPTION_ID present but CHOICE_ID missing", "CHOICE_VALUE_OPTION_ID", "CHOICE_ID"),
    ("CHOICE_ID present but CHECK_ITEM_ID missing", "CHOICE_ID", "CHECK_ITEM_ID"),
    ("CHECK_ITEM_ID present but CHECK_CATEGORY_ID missing", "CHECK_ITEM_ID", "CHECK_CATEGORY_ID"),
    ("CHECK_CATEGORY_ID present but BRANCH_ID missing", "CHECK_CATEGORY_ID", "BRANCH_ID"),
]

def id_verify_sheet(df: pd.DataFrame, sheet: str, max_dep_samples: int = 80) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Returns:
      - ID_Details (one row per ID col)
      - ID_Sheet_Summary (one row)
      - ID_Dependency_Samples (rows)
    """
    rows = len(df)
    present = [c for c in ID_COLUMNS if find_column(df, c) is not None]
    # build normalized columns in memory
    norm_map: Dict[str, pd.Series] = {}
    raw_map: Dict[str, pd.Series] = {}

    for c in present:
        rc = find_column(df, c)
        raw = df[rc]
        norm = raw.apply(normalize_id_token)
        raw_map[c] = raw
        norm_map[c] = norm

    detail_rows = []
    for c in present:
        raw = raw_map[c]
        norm = norm_map[c]

        missing_raw = int(raw.apply(is_missing_like).sum())
        missing_norm = int(norm.isna().sum())
        drift = _count_numeric_drift_signals(raw)

        changed = 0
        sample_changes = []
        for r, n in zip(raw, norm):
            if is_missing_like(r):
                continue
            rr = str(r).strip()
            nn = "" if n is None else str(n).strip()
            if rr != nn:
                changed += 1
                if len(sample_changes) < 5:
                    sample_changes.append(f"{rr} -> {nn}")

        non_null = norm.dropna().astype(str)
        uniq = int(non_null.nunique())
        dup = int(non_null.duplicated().sum())

        detail_rows.append({
            "SHEET": sheet,
            "COLUMN": c,
            "ROWS": rows,
            "MISSING_RAW": missing_raw,
            "MISSING_RAW_PCT": (missing_raw / rows * 100.0) if rows else 0.0,
            "MISSING_NORM": missing_norm,
            "MISSING_NORM_PCT": (missing_norm / rows * 100.0) if rows else 0.0,
            "NUMERIC_DRIFT_SIGNALS": drift,
            "CHANGED_BY_NORMALIZATION": changed,
            "UNIQUE_NORM_NON_NULL": uniq,
            "DUPLICATE_NORM_NON_NULL": dup,
            "RAW_VALUE_TYPE": infer_value_type(raw),
            "SAMPLE_CHANGES": "; ".join(sample_changes),
        })

    id_details = pd.DataFrame(detail_rows)

    # composite duplicates across available normalized columns
    composite_dup = 0
    if present:
        comp = pd.DataFrame({c: norm_map[c] for c in present})
        composite_dup = int(comp.duplicated().sum())

    # dependency samples
    dep_samples: List[Dict[str, Any]] = []
    for rule_name, when_col, must_col in ID_DEP_RULES:
        when_real = find_column(df, when_col)
        must_real = find_column(df, must_col)
        if when_real is None or must_real is None:
            continue
        when_present = df[when_real].apply(is_missing_like) == False
        must_missing = df[must_real].apply(is_missing_like)
        bad_idx = df.index[(when_present & must_missing)].tolist()[:max_dep_samples]
        for idx in bad_idx:
            dep_samples.append({
                "SHEET": sheet,
                "ROW_INDEX": int(idx) if isinstance(idx, int) else str(idx),
                "RULE": rule_name,
                when_col: df.at[idx, when_real],
                must_col: df.at[idx, must_real],
            })

    dep_df = pd.DataFrame(dep_samples)

    sheet_summary = pd.DataFrame([{
        "SHEET": sheet,
        "TOTAL_ROWS": rows,
        "ID_COLUMNS_PRESENT": ", ".join(present),
        "COMPOSITE_DUPLICATE_ROWS_ON_IDS": composite_dup,
        "NOTES": "IDs normalized to stable strings to avoid Excel drift (.0/scientific notation).",
    }])

    return id_details, sheet_summary, dep_df


# =============================
# B) Audit Type/Plan Verification (FIXED)
# =============================
ALLOWED_MISSING_TYPES = {"unplanned", "reevaluate"}

def canon_audit_type(x: Any) -> Optional[str]:
    if is_missing_like(x):
        return None
    s = str(x).strip().lower()
    s = re.sub(r"[\s_-]+", "", s)
    return s

def audit_verify_sheet(df: pd.DataFrame, sheet: str, max_violation_rows: int = 500) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Returns:
      - summary (1 row)
      - dist (rows)
      - violations sample (rows)
    """
    type_col = find_column(df, "AUDIT_TYPE")
    plan_col = find_column(df, "AUDIT_PLAN")

    total = len(df)
    if type_col is None or plan_col is None:
        return (
            pd.DataFrame([{
                "SHEET": sheet,
                "TOTAL_ROWS": total,
                "AUDIT_TYPE_PRESENT": type_col is not None,
                "AUDIT_PLAN_PRESENT": plan_col is not None,
                "NOTES": "Missing AUDIT_TYPE or AUDIT_PLAN columns.",
            }]),
            pd.DataFrame([{"SHEET": sheet, "AUDIT_TYPE_CANON": None, "COUNT": None, "PCT": None}]),
            pd.DataFrame()
        )

    s_type = df[type_col]
    s_plan = df[plan_col]

    type_canon = s_type.apply(canon_audit_type)
    plan_missing = s_plan.apply(is_missing_like)
    type_missing = s_type.apply(is_missing_like)

    is_allowed = type_canon.isin(ALLOWED_MISSING_TYPES)
    is_planned = type_canon.eq("planned")

    allowed_missing = int((is_allowed & plan_missing).sum())
    violations = int(((~is_allowed) & plan_missing).sum())

    planned_rows = int(is_planned.sum())
    planned_missing = int((is_planned & plan_missing).sum())
    planned_missing_pct = (planned_missing / planned_rows * 100.0) if planned_rows else 0.0

    # distribution
    dist_counts = type_canon.fillna("MISSING").value_counts(dropna=False)
    dist = pd.DataFrame({
        "SHEET": sheet,
        "AUDIT_TYPE_CANON": dist_counts.index.tolist(),
        "COUNT": dist_counts.values.tolist(),
    })
    dist["PCT"] = dist["COUNT"].apply(lambda c: (c / total * 100.0) if total else 0.0)

    # violations sample
    ctx_candidates = [
        "BRANCH_ID", "CHECK_CATEGORY_ID", "ISSUE_NUMBER", "CHECK_ITEM_ID",
        "LAB_SECTION_ID", "CHOICE_ID", "CHOICE_VALUE_OPTION_ID"
    ]
    ctx_cols = []
    for c in ctx_candidates:
        real = find_column(df, c)
        if real is not None:
            ctx_cols.append(real)

    bad_idx = df.index[((~is_allowed) & plan_missing)].tolist()[:max_violation_rows]
    viol_rows = []
    for idx in bad_idx:
        row = {
            "SHEET": sheet,
            "ROW_INDEX": int(idx) if isinstance(idx, int) else str(idx),
            "AUDIT_TYPE": df.at[idx, type_col],
            "AUDIT_PLAN": df.at[idx, plan_col],
            "RULE": f"AUDIT_PLAN missing while AUDIT_TYPE not in {sorted(ALLOWED_MISSING_TYPES)} (planned requires plan)",
        }
        for c in ctx_cols:
            row[c] = df.at[idx, c]
        viol_rows.append(row)

    viol_df = pd.DataFrame(viol_rows)

    summary = pd.DataFrame([{
        "SHEET": sheet,
        "TOTAL_ROWS": total,
        "AUDIT_TYPE_MISSING_COUNT": int(type_missing.sum()),
        "AUDIT_PLAN_MISSING_COUNT": int(plan_missing.sum()),
        "AUDIT_PLAN_ALLOWED_MISSING_COUNT": allowed_missing,
        "AUDIT_PLAN_VIOLATION_MISSING_COUNT": violations,
        "PLANNED_ROWS": planned_rows,
        "PLANNED_PLAN_MISSING_COUNT": planned_missing,
        "PLANNED_PLAN_MISSING_PCT": float(planned_missing_pct),
        "NOTES": "Rule: AUDIT_PLAN required only for planned; missing allowed for unplanned+reevaluate.",
    }])

    return summary, dist, viol_df


# =============================
# C) Structure Name Verification
# =============================
_SPACE_RE = re.compile(r"\s+")

def normalize_text_value(x: Any) -> Optional[str]:
    if is_missing_like(x):
        return None
    s = str(x).strip().lower()
    s = _SPACE_RE.sub(" ", s)
    return s if s else None

def choose_display_map(raw: pd.Series, norm: pd.Series) -> Dict[str, str]:
    raw_trim = raw.fillna("").astype(str).map(lambda z: str(z).strip())
    tmp = pd.DataFrame({"raw": raw_trim, "norm": norm})
    tmp = tmp[~tmp["norm"].isna() & (tmp["norm"] != "")]
    if tmp.empty:
        return {}
    freq = tmp.groupby(["norm", "raw"]).size().reset_index(name="n")
    idx = freq.sort_values(["norm", "n"], ascending=[True, False]).drop_duplicates("norm")
    return dict(zip(idx["norm"], idx["raw"]))

def structure_verify_sheet(df: pd.DataFrame, sheet: str, max_missing_samples: int = 300) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    col_cat = find_column(df, "CHECK_CATEGORY_NAME")
    col_item = find_column(df, "CHECK_ITEM_NAME")
    col_ans = find_column(df, "CHOICE_VALUE_OPTION_NAME")
    total = len(df)

    if col_cat is None or col_item is None or col_ans is None:
        summary = pd.DataFrame([{
            "SHEET": sheet,
            "TOTAL_ROWS": total,
            "CATEGORY_COL_PRESENT": col_cat is not None,
            "ITEM_COL_PRESENT": col_item is not None,
            "ANSWER_COL_PRESENT": col_ans is not None,
            "NOTES": "Missing one or more required structure name columns.",
        }])
        return summary, pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    s_cat_raw = df[col_cat]
    s_item_raw = df[col_item]
    s_ans_raw = df[col_ans]

    cat = s_cat_raw.apply(normalize_text_value)
    item = s_item_raw.apply(normalize_text_value)
    ans = s_ans_raw.apply(normalize_text_value)

    cat_missing = int(cat.isna().sum())
    item_missing = int(item.isna().sum())
    ans_missing = int(ans.isna().sum())

    unique_cats = int(cat.dropna().nunique())
    unique_items = int(item.dropna().nunique())
    unique_ans = int(ans.dropna().nunique())

    dep_cat_item = int((cat.notna() & item.isna()).sum())
    dep_item_ans = int((item.notna() & ans.isna()).sum())
    dep_ans_item = int((ans.notna() & item.isna()).sum())

    cat_display = choose_display_map(s_cat_raw, cat)
    item_display = choose_display_map(s_item_raw, item)

    # Category -> distinct items
    tmp_ci = pd.DataFrame({"SHEET": sheet, "CATEGORY_NORM": cat, "ITEM_NORM": item})
    tmp_ci = tmp_ci[tmp_ci["CATEGORY_NORM"].notna() & tmp_ci["ITEM_NORM"].notna()]
    ci = tmp_ci.groupby(["SHEET", "CATEGORY_NORM"])["ITEM_NORM"].nunique().reset_index(name="DISTINCT_ITEMS")
    if not ci.empty:
        ci["CATEGORY_DISPLAY"] = ci["CATEGORY_NORM"].map(cat_display).fillna(ci["CATEGORY_NORM"])

    # (Category, Item) -> distinct answers
    tmp_ia = pd.DataFrame({"SHEET": sheet, "CATEGORY_NORM": cat, "ITEM_NORM": item, "ANSWER_NORM": ans})
    tmp_ia = tmp_ia[tmp_ia["CATEGORY_NORM"].notna() & tmp_ia["ITEM_NORM"].notna() & tmp_ia["ANSWER_NORM"].notna()]
    ia = tmp_ia.groupby(["SHEET", "CATEGORY_NORM", "ITEM_NORM"])["ANSWER_NORM"].nunique().reset_index(name="DISTINCT_ANSWERS")
    if not ia.empty:
        ia["CATEGORY_DISPLAY"] = ia["CATEGORY_NORM"].map(cat_display).fillna(ia["CATEGORY_NORM"])
        ia["ITEM_DISPLAY"] = ia["ITEM_NORM"].map(item_display).fillna(ia["ITEM_NORM"])

    # Missing samples
    missing_mask = cat.isna() | item.isna() | ans.isna()
    ms = df.loc[missing_mask, [col_cat, col_item, col_ans]].copy()
    ms.insert(0, "ROW_INDEX", ms.index.astype(str))
    ms.insert(0, "SHEET", sheet)

    def _what(row) -> str:
        miss = []
        if is_missing_like(row.get(col_cat, None)): miss.append("CATEGORY")
        if is_missing_like(row.get(col_item, None)): miss.append("ITEM")
        if is_missing_like(row.get(col_ans, None)): miss.append("ANSWER")
        return ",".join(miss)

    ms["MISSING_WHAT"] = ms.apply(_what, axis=1)
    ms = ms.rename(columns={col_cat: "CHECK_CATEGORY_NAME", col_item: "CHECK_ITEM_NAME", col_ans: "CHOICE_VALUE_OPTION_NAME"})
    ms = ms.head(max_missing_samples)

    summary = pd.DataFrame([{
        "SHEET": sheet,
        "TOTAL_ROWS": total,
        "CATEGORY_MISSING_COUNT": cat_missing,
        "CATEGORY_MISSING_PCT": (cat_missing / total * 100.0) if total else 0.0,
        "ITEM_MISSING_COUNT": item_missing,
        "ITEM_MISSING_PCT": (item_missing / total * 100.0) if total else 0.0,
        "ANSWER_MISSING_COUNT": ans_missing,
        "ANSWER_MISSING_PCT": (ans_missing / total * 100.0) if total else 0.0,
        "UNIQUE_CATEGORIES": unique_cats,
        "UNIQUE_ITEMS": unique_items,
        "UNIQUE_ANSWERS": unique_ans,
        "DEP_CAT_PRESENT_ITEM_MISSING": dep_cat_item,
        "DEP_ITEM_PRESENT_ANS_MISSING": dep_item_ans,
        "DEP_ANS_PRESENT_ITEM_MISSING": dep_ans_item,
        "NOTES": "Text normalized (strip+lower+collapse spaces).",
    }])

    return summary, ci, ia, ms


# =============================
# D) Numeric Columns Verification
# =============================
NUM_COLUMNS = [
    "GENERAL_SCORE",
    "CATEGORY_MIN_ACCEPTABLE_SCORE",
    "WEIGHT_PERCENTAGE",
    "CI_MIN_ACCEPTABLE_SCORE",
    "OPTION_VALUE",
]

def coerce_numeric_series(s: pd.Series) -> pd.Series:
    def _prep(v: Any) -> Any:
        if is_missing_like(v):
            return None
        if isinstance(v, (int, float)) and not isinstance(v, bool):
            return v
        txt = str(v).strip()
        if txt == "":
            return None
        txt = txt.replace("%", "").strip()
        txt = re.sub(r"\s+", "", txt)
        # comma decimal or thousands
        if "," in txt and "." not in txt:
            parts = txt.split(",")
            if len(parts) == 2 and len(parts[1]) == 3 and parts[0].isdigit() and parts[1].isdigit():
                txt = parts[0] + parts[1]
            else:
                txt = txt.replace(",", ".")
        else:
            txt = txt.replace(",", "")
        return txt
    prepared = s.map(_prep)
    return pd.to_numeric(prepared, errors="coerce")

def numeric_verify_sheet(df: pd.DataFrame, sheet: str, max_samples_per_col: int = 60) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    total = len(df)

    ctx_candidates = [
        "BRANCH_ID", "CHECK_CATEGORY_ID", "CHECK_CATEGORY_NAME",
        "ISSUE_NUMBER", "CHECK_ITEM_ID", "CHECK_ITEM_NAME",
        "CHOICE_ID", "CHOICE_VALUE_OPTION_NAME"
    ]
    ctx_cols = []
    for c in ctx_candidates:
        real = find_column(df, c)
        if real is not None:
            ctx_cols.append(real)

    summary_rows = []
    nonnumeric_rows = []
    missing_rows = []

    for target in NUM_COLUMNS:
        col = find_column(df, target)
        if col is None:
            summary_rows.append({
                "SHEET": sheet, "COLUMN": target, "PRESENT": False, "TOTAL_ROWS": total,
                "MISSING_COUNT": None, "MISSING_PCT": None,
                "NON_NUMERIC_COUNT": None, "NON_NUMERIC_PCT": None,
                "NUMERIC_COUNT": None, "NUMERIC_PCT": None,
                "NUMERIC_MIN": None, "NUMERIC_MAX": None, "NUMERIC_MEAN": None,
                "RAW_VALUE_TYPE": None,
                "NOTES": "Column not found."
            })
            continue

        raw = df[col]
        miss = raw.map(is_missing_like)
        missing_count = int(miss.sum())
        missing_pct = (missing_count / total * 100.0) if total else 0.0

        num = coerce_numeric_series(raw)
        nonnumeric_mask = (~miss) & (num.isna())
        nonnumeric_count = int(nonnumeric_mask.sum())
        nonnumeric_pct = (nonnumeric_count / total * 100.0) if total else 0.0

        numeric_mask = (~miss) & (~num.isna())
        numeric_count = int(numeric_mask.sum())
        numeric_pct = (numeric_count / total * 100.0) if total else 0.0

        valid = num[numeric_mask]
        nmin = float(valid.min()) if len(valid) else None
        nmax = float(valid.max()) if len(valid) else None
        nmean = float(valid.mean()) if len(valid) else None

        summary_rows.append({
            "SHEET": sheet, "COLUMN": target, "PRESENT": True, "TOTAL_ROWS": total,
            "MISSING_COUNT": missing_count, "MISSING_PCT": float(missing_pct),
            "NON_NUMERIC_COUNT": nonnumeric_count, "NON_NUMERIC_PCT": float(nonnumeric_pct),
            "NUMERIC_COUNT": numeric_count, "NUMERIC_PCT": float(numeric_pct),
            "NUMERIC_MIN": nmin, "NUMERIC_MAX": nmax, "NUMERIC_MEAN": nmean,
            "RAW_VALUE_TYPE": infer_value_type(raw),
            "NOTES": "Numeric coercion handles %, comma decimals, and thousands separators.",
        })

        # samples
        for idx in df.index[nonnumeric_mask].tolist()[:max_samples_per_col]:
            row = {"SHEET": sheet, "ROW_INDEX": str(idx), "COLUMN": target, "RAW_VALUE": df.at[idx, col], "ISSUE": "NON_NUMERIC"}
            for c in ctx_cols:
                row[c] = df.at[idx, c]
            nonnumeric_rows.append(row)

        for idx in df.index[miss].tolist()[:max_samples_per_col]:
            row = {"SHEET": sheet, "ROW_INDEX": str(idx), "COLUMN": target, "RAW_VALUE": df.at[idx, col], "ISSUE": "MISSING"}
            for c in ctx_cols:
                row[c] = df.at[idx, c]
            missing_rows.append(row)

    return pd.DataFrame(summary_rows), pd.DataFrame(nonnumeric_rows), pd.DataFrame(missing_rows)


# =============================
# Master runner
# =============================
def run_master(excel_path: Path, out_path: Path, prefix: str) -> None:
    xls = pd.ExcelFile(excel_path, engine="openpyxl")
    target_sheets = [s for s in xls.sheet_names if str(s).startswith(prefix)]
    if not target_sheets:
        raise ValueError(f"No sheets found with prefix '{prefix}'. Sheets: {xls.sheet_names}")

    # Collect outputs
    id_details_all, id_summary_all, id_dep_all = [], [], []
    audit_sum_all, audit_dist_all, audit_viol_all = [], [], []
    struct_sum_all, ci_all, ia_all, struct_miss_all = [], [], [], []
    num_sum_all, num_non_all, num_miss_all = [], [], []

    # For dashboard totals
    total_rows_all = 0

    print(f"✅ Using Excel: {excel_path.resolve()}")
    print(f"📌 Target sheets ({len(target_sheets)}): {target_sheets}")

    for sheet in target_sheets:
        df = pd.read_excel(excel_path, sheet_name=sheet, dtype=object, engine="openpyxl")
        total_rows_all += len(df)

        # A) IDs
        id_details, id_sum, id_dep = id_verify_sheet(df, sheet)
        id_details_all.append(id_details)
        id_summary_all.append(id_sum)
        if not id_dep.empty:
            id_dep_all.append(id_dep)

        # B) Audit
        a_sum, a_dist, a_viol = audit_verify_sheet(df, sheet)
        audit_sum_all.append(a_sum)
        audit_dist_all.append(a_dist)
        if not a_viol.empty:
            audit_viol_all.append(a_viol)

        # C) Structure names
        s_sum, ci, ia, s_miss = structure_verify_sheet(df, sheet)
        struct_sum_all.append(s_sum)
        if not ci.empty: ci_all.append(ci)
        if not ia.empty: ia_all.append(ia)
        if not s_miss.empty: struct_miss_all.append(s_miss)

        # D) Numeric
        n_sum, n_non, n_miss = numeric_verify_sheet(df, sheet)
        num_sum_all.append(n_sum)
        if not n_non.empty: num_non_all.append(n_non)
        if not n_miss.empty: num_miss_all.append(n_miss)

    # concat
    ID_Details = pd.concat(id_details_all, ignore_index=True) if id_details_all else pd.DataFrame()
    ID_Summary = pd.concat(id_summary_all, ignore_index=True) if id_summary_all else pd.DataFrame()
    ID_Dep_Samples = pd.concat(id_dep_all, ignore_index=True) if id_dep_all else pd.DataFrame()

    Audit_Summary = pd.concat(audit_sum_all, ignore_index=True) if audit_sum_all else pd.DataFrame()
    Audit_Dist = pd.concat(audit_dist_all, ignore_index=True) if audit_dist_all else pd.DataFrame()
    Audit_Violations = pd.concat(audit_viol_all, ignore_index=True) if audit_viol_all else pd.DataFrame()

    Struct_Summary = pd.concat(struct_sum_all, ignore_index=True) if struct_sum_all else pd.DataFrame()
    Cat_Item_Counts = pd.concat(ci_all, ignore_index=True) if ci_all else pd.DataFrame()
    Item_Ans_Counts = pd.concat(ia_all, ignore_index=True) if ia_all else pd.DataFrame()
    Struct_Missing = pd.concat(struct_miss_all, ignore_index=True) if struct_miss_all else pd.DataFrame()

    Num_Summary = pd.concat(num_sum_all, ignore_index=True) if num_sum_all else pd.DataFrame()
    Num_NonNumeric = pd.concat(num_non_all, ignore_index=True) if num_non_all else pd.DataFrame()
    Num_Missing = pd.concat(num_miss_all, ignore_index=True) if num_miss_all else pd.DataFrame()

    # Overall dashboard (key metrics)
    # Planned missing plan total across sheets:
    planned_missing_total = None
    planned_rows_total = None
    if not Audit_Summary.empty and "PLANNED_PLAN_MISSING_COUNT" in Audit_Summary.columns:
        planned_missing_total = int(Audit_Summary["PLANNED_PLAN_MISSING_COUNT"].fillna(0).sum())
        planned_rows_total = int(Audit_Summary["PLANNED_ROWS"].fillna(0).sum())
    planned_missing_pct = (planned_missing_total / planned_rows_total * 100.0) if planned_rows_total else None

    dashboard = pd.DataFrame([{
        "EXCEL_FILE": str(excel_path.name),
        "PREFIX": prefix,
        "SHEETS_SCANNED": len(target_sheets),
        "TOTAL_ROWS_SCANNED": int(total_rows_all),
        "PLANNED_ROWS_TOTAL": planned_rows_total,
        "PLANNED_MISSING_AUDIT_PLAN_TOTAL": planned_missing_total,
        "PLANNED_MISSING_AUDIT_PLAN_PCT": planned_missing_pct,
        "NOTES": "Master report aggregates RAW DATA verifications before downstream modeling.",
    }])

    # Write report
    with pd.ExcelWriter(out_path, engine="openpyxl") as w:
        dashboard.to_excel(w, index=False, sheet_name="Dashboard")

        ID_Summary.to_excel(w, index=False, sheet_name="ID_Summary")
        ID_Details.to_excel(w, index=False, sheet_name="ID_Details")
        (ID_Dep_Samples if not ID_Dep_Samples.empty else pd.DataFrame([{"NOTES":"No dependency issues found."}])) \
            .to_excel(w, index=False, sheet_name="ID_Dep_Samples")

        Audit_Summary.to_excel(w, index=False, sheet_name="Audit_Summary")
        Audit_Dist.to_excel(w, index=False, sheet_name="Audit_Dist")
        (Audit_Violations if not Audit_Violations.empty else pd.DataFrame([{"NOTES":"No audit-plan violations found."}])) \
            .to_excel(w, index=False, sheet_name="Audit_Violations")

        Struct_Summary.to_excel(w, index=False, sheet_name="Struct_Summary")
        (Cat_Item_Counts if not Cat_Item_Counts.empty else pd.DataFrame([{"NOTES":"No category-item pairs found."}])) \
            .to_excel(w, index=False, sheet_name="Cat_Item_Counts")
        (Item_Ans_Counts if not Item_Ans_Counts.empty else pd.DataFrame([{"NOTES":"No item-answer pairs found."}])) \
            .to_excel(w, index=False, sheet_name="Item_Ans_Counts")
        (Struct_Missing if not Struct_Missing.empty else pd.DataFrame([{"NOTES":"No missing structure rows found."}])) \
            .to_excel(w, index=False, sheet_name="Struct_Missing")

        Num_Summary.to_excel(w, index=False, sheet_name="Num_Summary")
        (Num_NonNumeric if not Num_NonNumeric.empty else pd.DataFrame([{"NOTES":"No non-numeric values found."}])) \
            .to_excel(w, index=False, sheet_name="Num_NonNumeric")
        (Num_Missing if not Num_Missing.empty else pd.DataFrame([{"NOTES":"No missing numeric values found."}])) \
            .to_excel(w, index=False, sheet_name="Num_Missing")

    print(f"✅ Master report written to: {out_path.resolve()}")


def build_argparser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Master RAW DATA verification for ISO_DATA.xlsx (single report).")
    p.add_argument("--excel", default="ISO_DATA.xlsx", help="Path to ISO_DATA.xlsx (default: ISO_DATA.xlsx)")
    p.add_argument("--out", default="ISO_Master_RAWDATA_Verification_Report.xlsx",
                   help="Output Excel report (default: ISO_Master_RAWDATA_Verification_Report.xlsx)")
    p.add_argument("--prefix", default="ISO_Check_categor",
                   help="Target sheet prefix (default: ISO_Check_categor)")
    return p


if __name__ == "__main__":
    args = build_argparser().parse_args()
    excel_path = _ensure_excel_exists(args.excel)
    out_path = Path(args.out)
    run_master(excel_path, out_path, args.prefix)
