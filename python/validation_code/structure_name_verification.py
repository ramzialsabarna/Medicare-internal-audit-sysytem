#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
structure_name_verification.py
------------------------------
Verifies RAW structure name columns in ISO_DATA.xlsx:

Target columns:
  - CHECK_CATEGORY_NAME
  - CHECK_ITEM_NAME
  - CHOICE_VALUE_OPTION_NAME

Because data is row-level (Category + Item + Answer), repetition is expected.
This script produces an Excel report that includes:

1) Summary per sheet:
   - Total rows
   - Missing count/% for each column
   - Unique counts (distinct normalized) for category/item/answer
   - Dependency checks (e.g., category present but item missing)

2) Overall summary across all targeted sheets.

3) Per-category item counts:
   - For each category: number of distinct items under it (per sheet + overall)

4) Per-item answer counts:
   - For each (category, item): number of distinct answers (per sheet + overall)

5) Missing samples:
   - A sample of rows where any of the target columns are missing (for manual review)

Run (defaults expect ISO_DATA.xlsx in current folder):
  python .\structure_name_verification.py

Or:
  python .\structure_name_verification.py --excel .\ISO_DATA.xlsx --out .\ISO_Structure_Name_Verification_Report.xlsx --prefix ISO_Check_categor
"""

from __future__ import annotations
import argparse
import os
import re
from typing import Any, Optional, List, Dict, Tuple

import pandas as pd


# -----------------------------
# Missing + normalization helpers
# -----------------------------
_MISSING_STRINGS = {"", "nan", "none", "null", "na", "n/a", "nil", "-"}

def is_missing_like(x: Any) -> bool:
    """Robust missing detector for Excel-ingested cells."""
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
    """Normalize column names for matching (ignore case/whitespace)."""
    return re.sub(r"\s+", "", str(c).strip().upper())

def find_column(df: pd.DataFrame, target: str) -> Optional[str]:
    """Find a column in df matching target name ignoring case and whitespace."""
    t = norm_colname(target)
    mapping = {norm_colname(c): c for c in df.columns}
    return mapping.get(t)

_SPACE_RE = re.compile(r"\s+")

def normalize_text_value(x: Any) -> Optional[str]:
    """
    Normalize text for counting distinct values:
      - missing -> None
      - strip
      - lower (case-insensitive)
      - collapse multiple spaces into one
    """
    if is_missing_like(x):
        return None
    s = str(x).strip().lower()
    s = _SPACE_RE.sub(" ", s)
    return s if s != "" else None

def choose_display_name(series_raw: pd.Series, series_norm: pd.Series) -> pd.Series:
    """
    For each normalized token, choose a representative display name
    (most frequent raw trimmed form).
    """
    raw_trim = series_raw.fillna("").astype(str).map(lambda z: str(z).strip())
    tmp = pd.DataFrame({"raw": raw_trim, "norm": series_norm})
    tmp = tmp[~tmp["norm"].isna() & (tmp["norm"] != "")]
    if tmp.empty:
        return pd.Series(dtype=str)

    # Count frequency of raw within each norm
    freq = tmp.groupby(["norm", "raw"]).size().reset_index(name="n")
    # Pick the most frequent raw for each norm
    idx = freq.sort_values(["norm", "n"], ascending=[True, False]).drop_duplicates("norm")
    mapping = dict(zip(idx["norm"], idx["raw"]))
    return pd.Series(mapping)

def infer_value_type(series: pd.Series) -> str:
    """Infer value type ignoring missing: empty/string/numeric/datetime/mixed."""
    vals = [v for v in series.tolist() if not is_missing_like(v)]
    if not vals:
        return "empty"
    if all(isinstance(v, (pd.Timestamp,)) for v in vals):
        return "datetime"
    def can_num(v: Any) -> bool:
        try:
            float(str(v).strip())
            return True
        except Exception:
            return False
    if all(can_num(v) for v in vals):
        return "numeric"
    if all(isinstance(v, str) for v in vals):
        return "string"
    return "mixed"


# -----------------------------
# Core per-sheet verification
# -----------------------------
TARGET_COLS = [
    "CHECK_CATEGORY_NAME",
    "CHECK_ITEM_NAME",
    "CHOICE_VALUE_OPTION_NAME",
]

def verify_structure_sheet(
    df: pd.DataFrame,
    sheet_name: str,
    max_missing_samples: int = 300
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Returns:
      - summary_row (1 row df)
      - category_item_counts (df)
      - item_answer_counts (df)
      - missing_samples (df)
    """
    # Find real columns (robust matching)
    col_cat = find_column(df, "CHECK_CATEGORY_NAME")
    col_item = find_column(df, "CHECK_ITEM_NAME")
    col_ans = find_column(df, "CHOICE_VALUE_OPTION_NAME")

    total_rows = len(df)

    # If any missing columns, return summary only
    missing_cols = []
    for need, col in [("CHECK_CATEGORY_NAME", col_cat), ("CHECK_ITEM_NAME", col_item), ("CHOICE_VALUE_OPTION_NAME", col_ans)]:
        if col is None:
            missing_cols.append(need)

    if missing_cols:
        summary = pd.DataFrame([{
            "SHEET": sheet_name,
            "TOTAL_ROWS": total_rows,
            "CATEGORY_COL_PRESENT": col_cat is not None,
            "ITEM_COL_PRESENT": col_item is not None,
            "ANSWER_COL_PRESENT": col_ans is not None,
            "CATEGORY_VALUE_TYPE": None,
            "ITEM_VALUE_TYPE": None,
            "ANSWER_VALUE_TYPE": None,
            "CATEGORY_MISSING_COUNT": None,
            "CATEGORY_MISSING_PCT": None,
            "ITEM_MISSING_COUNT": None,
            "ITEM_MISSING_PCT": None,
            "ANSWER_MISSING_COUNT": None,
            "ANSWER_MISSING_PCT": None,
            "UNIQUE_CATEGORIES": None,
            "UNIQUE_ITEMS": None,
            "UNIQUE_ANSWERS": None,
            "DEP_CAT_PRESENT_ITEM_MISSING": None,
            "DEP_ITEM_PRESENT_ANS_MISSING": None,
            "DEP_ANS_PRESENT_ITEM_MISSING": None,
            "NOTES": f"Missing required columns: {', '.join(missing_cols)}",
        }])

        empty_ci = pd.DataFrame(columns=["SHEET", "CATEGORY_NORM", "CATEGORY_DISPLAY", "DISTINCT_ITEMS"])
        empty_ia = pd.DataFrame(columns=["SHEET", "CATEGORY_NORM", "CATEGORY_DISPLAY", "ITEM_NORM", "ITEM_DISPLAY", "DISTINCT_ANSWERS"])
        empty_ms = pd.DataFrame(columns=["SHEET", "ROW_INDEX", "CHECK_CATEGORY_NAME", "CHECK_ITEM_NAME", "CHOICE_VALUE_OPTION_NAME", "MISSING_WHAT"])
        return summary, empty_ci, empty_ia, empty_ms

    # Normalize values
    s_cat_raw = df[col_cat]
    s_item_raw = df[col_item]
    s_ans_raw = df[col_ans]

    cat_norm = s_cat_raw.apply(normalize_text_value)
    item_norm = s_item_raw.apply(normalize_text_value)
    ans_norm = s_ans_raw.apply(normalize_text_value)

    # Missing counts
    cat_missing = int(cat_norm.isna().sum())
    item_missing = int(item_norm.isna().sum())
    ans_missing = int(ans_norm.isna().sum())

    cat_missing_pct = (cat_missing / total_rows * 100.0) if total_rows else 0.0
    item_missing_pct = (item_missing / total_rows * 100.0) if total_rows else 0.0
    ans_missing_pct = (ans_missing / total_rows * 100.0) if total_rows else 0.0

    # Unique counts (normalized)
    unique_cats = int(pd.Series(cat_norm.dropna()).nunique())
    unique_items = int(pd.Series(item_norm.dropna()).nunique())
    unique_ans = int(pd.Series(ans_norm.dropna()).nunique())

    # Dependency checks
    dep_cat_present_item_missing = int((cat_norm.notna() & item_norm.isna()).sum())
    dep_item_present_ans_missing = int((item_norm.notna() & ans_norm.isna()).sum())
    dep_ans_present_item_missing = int((ans_norm.notna() & item_norm.isna()).sum())

    # Representative display names
    cat_display_map = choose_display_name(s_cat_raw, cat_norm)  # norm -> display
    item_display_map = choose_display_name(s_item_raw, item_norm)  # norm -> display

    # Category -> distinct items
    tmp_ci = pd.DataFrame({
        "SHEET": sheet_name,
        "CATEGORY_NORM": cat_norm,
        "ITEM_NORM": item_norm
    })
    tmp_ci = tmp_ci[tmp_ci["CATEGORY_NORM"].notna() & tmp_ci["ITEM_NORM"].notna()]
    if tmp_ci.empty:
        category_item_counts = pd.DataFrame(columns=["SHEET", "CATEGORY_NORM", "CATEGORY_DISPLAY", "DISTINCT_ITEMS"])
    else:
        category_item_counts = (
            tmp_ci.groupby(["SHEET", "CATEGORY_NORM"])["ITEM_NORM"]
            .nunique()
            .reset_index(name="DISTINCT_ITEMS")
        )
        category_item_counts["CATEGORY_DISPLAY"] = category_item_counts["CATEGORY_NORM"].map(cat_display_map).fillna(category_item_counts["CATEGORY_NORM"])

    # (Category, Item) -> distinct answers
    tmp_ia = pd.DataFrame({
        "SHEET": sheet_name,
        "CATEGORY_NORM": cat_norm,
        "ITEM_NORM": item_norm,
        "ANSWER_NORM": ans_norm
    })
    tmp_ia = tmp_ia[tmp_ia["CATEGORY_NORM"].notna() & tmp_ia["ITEM_NORM"].notna() & tmp_ia["ANSWER_NORM"].notna()]
    if tmp_ia.empty:
        item_answer_counts = pd.DataFrame(columns=["SHEET", "CATEGORY_NORM", "CATEGORY_DISPLAY", "ITEM_NORM", "ITEM_DISPLAY", "DISTINCT_ANSWERS"])
    else:
        item_answer_counts = (
            tmp_ia.groupby(["SHEET", "CATEGORY_NORM", "ITEM_NORM"])["ANSWER_NORM"]
            .nunique()
            .reset_index(name="DISTINCT_ANSWERS")
        )
        item_answer_counts["CATEGORY_DISPLAY"] = item_answer_counts["CATEGORY_NORM"].map(cat_display_map).fillna(item_answer_counts["CATEGORY_NORM"])
        item_answer_counts["ITEM_DISPLAY"] = item_answer_counts["ITEM_NORM"].map(item_display_map).fillna(item_answer_counts["ITEM_NORM"])

    # Missing samples (rows where any of the three is missing)
    missing_mask = cat_norm.isna() | item_norm.isna() | ans_norm.isna()
    miss_df = df.loc[missing_mask, [col_cat, col_item, col_ans]].copy()
    miss_df.insert(0, "ROW_INDEX", miss_df.index.astype(str))
    miss_df.insert(0, "SHEET", sheet_name)

    def missing_what(row) -> str:
        miss = []
        if is_missing_like(row.get(col_cat, None)): miss.append("CATEGORY")
        if is_missing_like(row.get(col_item, None)): miss.append("ITEM")
        if is_missing_like(row.get(col_ans, None)): miss.append("ANSWER")
        return ",".join(miss)

    miss_df["MISSING_WHAT"] = miss_df.apply(missing_what, axis=1)
    miss_df = miss_df.rename(columns={
        col_cat: "CHECK_CATEGORY_NAME",
        col_item: "CHECK_ITEM_NAME",
        col_ans: "CHOICE_VALUE_OPTION_NAME",
    })
    missing_samples = miss_df.head(max_missing_samples)

    summary = pd.DataFrame([{
        "SHEET": sheet_name,
        "TOTAL_ROWS": total_rows,
        "CATEGORY_COL_PRESENT": True,
        "ITEM_COL_PRESENT": True,
        "ANSWER_COL_PRESENT": True,
        "CATEGORY_VALUE_TYPE": infer_value_type(s_cat_raw),
        "ITEM_VALUE_TYPE": infer_value_type(s_item_raw),
        "ANSWER_VALUE_TYPE": infer_value_type(s_ans_raw),
        "CATEGORY_MISSING_COUNT": cat_missing,
        "CATEGORY_MISSING_PCT": float(cat_missing_pct),
        "ITEM_MISSING_COUNT": item_missing,
        "ITEM_MISSING_PCT": float(item_missing_pct),
        "ANSWER_MISSING_COUNT": ans_missing,
        "ANSWER_MISSING_PCT": float(ans_missing_pct),
        "UNIQUE_CATEGORIES": unique_cats,
        "UNIQUE_ITEMS": unique_items,
        "UNIQUE_ANSWERS": unique_ans,
        "DEP_CAT_PRESENT_ITEM_MISSING": dep_cat_present_item_missing,
        "DEP_ITEM_PRESENT_ANS_MISSING": dep_item_present_ans_missing,
        "DEP_ANS_PRESENT_ITEM_MISSING": dep_ans_present_item_missing,
        "NOTES": "Values normalized using strip + lower + collapse spaces; missing tokens include: '', NA, N/A, null, none, '-'.",
    }])

    return summary, category_item_counts, item_answer_counts, missing_samples


# -----------------------------
# Runner (all sheets by prefix + overall summary)
# -----------------------------
def run(excel_path: str, out_path: str, prefix: str, max_missing_samples: int = 300) -> None:
    if not os.path.exists(excel_path):
        cwd = os.getcwd()
        excel_files = [f for f in os.listdir(cwd) if f.lower().endswith((".xlsx", ".xlsm", ".xls"))]
        raise FileNotFoundError(
            f"[ERROR] Excel file not found: {excel_path}\n"
            f"Current directory: {cwd}\n"
            f"Excel files here: {excel_files}\n"
            f"Tip: run with --excel <path_to_your_file.xlsx>"
        )

    xls = pd.ExcelFile(excel_path, engine="openpyxl")
    # case-sensitive prefix like earlier (your naming is consistent)
    target_sheets = [s for s in xls.sheet_names if str(s).startswith(prefix)]

    if not target_sheets:
        with pd.ExcelWriter(out_path, engine="openpyxl") as w:
            pd.DataFrame([{"NOTES": f"No sheets found with prefix: {prefix}"}]).to_excel(w, index=False, sheet_name="Summary_BySheet")
        return

    all_summary = []
    all_ci = []
    all_ia = []
    all_ms = []

    # For overall summary we will accumulate normalized columns across sheets
    overall_rows = 0
    overall_cat_norm = []
    overall_item_norm = []
    overall_ans_norm = []

    # Dependency overall counters
    overall_dep_cat_item = 0
    overall_dep_item_ans = 0
    overall_dep_ans_item = 0

    for sheet in target_sheets:
        df = pd.read_excel(excel_path, sheet_name=sheet, dtype=object, engine="openpyxl")
        summary, ci, ia, ms = verify_structure_sheet(df, sheet, max_missing_samples=max_missing_samples)

        all_summary.append(summary)
        if not ci.empty:
            all_ci.append(ci)
        if not ia.empty:
            all_ia.append(ia)
        if not ms.empty:
            all_ms.append(ms)

        # Build overall normalized sets (using same normalization)
        col_cat = find_column(df, "CHECK_CATEGORY_NAME")
        col_item = find_column(df, "CHECK_ITEM_NAME")
        col_ans = find_column(df, "CHOICE_VALUE_OPTION_NAME")
        if col_cat and col_item and col_ans:
            cat_norm = df[col_cat].apply(normalize_text_value)
            item_norm = df[col_item].apply(normalize_text_value)
            ans_norm = df[col_ans].apply(normalize_text_value)

            overall_rows += len(df)
            overall_cat_norm.append(cat_norm)
            overall_item_norm.append(item_norm)
            overall_ans_norm.append(ans_norm)

            overall_dep_cat_item += int((cat_norm.notna() & item_norm.isna()).sum())
            overall_dep_item_ans += int((item_norm.notna() & ans_norm.isna()).sum())
            overall_dep_ans_item += int((ans_norm.notna() & item_norm.isna()).sum())

    summary_df = pd.concat(all_summary, ignore_index=True)

    # Overall summary
    if overall_rows > 0 and overall_cat_norm:
        cat_all = pd.concat(overall_cat_norm, ignore_index=True)
        item_all = pd.concat(overall_item_norm, ignore_index=True)
        ans_all = pd.concat(overall_ans_norm, ignore_index=True)

        overall_summary = pd.DataFrame([{
            "TOTAL_ROWS_ALL_SHEETS": int(overall_rows),
            "CATEGORY_MISSING_COUNT": int(cat_all.isna().sum()),
            "CATEGORY_MISSING_PCT": float(cat_all.isna().sum() / overall_rows * 100.0),
            "ITEM_MISSING_COUNT": int(item_all.isna().sum()),
            "ITEM_MISSING_PCT": float(item_all.isna().sum() / overall_rows * 100.0),
            "ANSWER_MISSING_COUNT": int(ans_all.isna().sum()),
            "ANSWER_MISSING_PCT": float(ans_all.isna().sum() / overall_rows * 100.0),
            "UNIQUE_CATEGORIES_ALL": int(cat_all.dropna().nunique()),
            "UNIQUE_ITEMS_ALL": int(item_all.dropna().nunique()),
            "UNIQUE_ANSWERS_ALL": int(ans_all.dropna().nunique()),
            "DEP_CAT_PRESENT_ITEM_MISSING_ALL": int(overall_dep_cat_item),
            "DEP_ITEM_PRESENT_ANS_MISSING_ALL": int(overall_dep_item_ans),
            "DEP_ANS_PRESENT_ITEM_MISSING_ALL": int(overall_dep_ans_item),
            "SHEETS_INCLUDED": ", ".join(target_sheets),
            "NOTES": "Overall counts computed after text normalization (strip+lower+collapse spaces).",
        }])
    else:
        overall_summary = pd.DataFrame([{"NOTES": "No usable sheets found for overall summary."}])

    # Combine detail tables
    ci_df = pd.concat(all_ci, ignore_index=True) if all_ci else pd.DataFrame()
    ia_df = pd.concat(all_ia, ignore_index=True) if all_ia else pd.DataFrame()
    ms_df = pd.concat(all_ms, ignore_index=True) if all_ms else pd.DataFrame()

    # Write report
    with pd.ExcelWriter(out_path, engine="openpyxl") as w:
        summary_df.to_excel(w, index=False, sheet_name="Summary_BySheet")
        overall_summary.to_excel(w, index=False, sheet_name="Summary_Overall")

        if not ci_df.empty:
            # keep readable ordering
            ci_df = ci_df.sort_values(["SHEET", "DISTINCT_ITEMS"], ascending=[True, False])
            ci_df.to_excel(w, index=False, sheet_name="Category_Item_Counts")
        else:
            pd.DataFrame([{"NOTES": "No category-item pairs found."}]).to_excel(w, index=False, sheet_name="Category_Item_Counts")

        if not ia_df.empty:
            ia_df = ia_df.sort_values(["SHEET", "DISTINCT_ANSWERS"], ascending=[True, False])
            ia_df.to_excel(w, index=False, sheet_name="Item_Answer_Counts")
        else:
            pd.DataFrame([{"NOTES": "No item-answer pairs found."}]).to_excel(w, index=False, sheet_name="Item_Answer_Counts")

        if not ms_df.empty:
            ms_df.to_excel(w, index=False, sheet_name="Missing_Samples")
        else:
            pd.DataFrame([{"NOTES": "No missing rows found in target columns."}]).to_excel(w, index=False, sheet_name="Missing_Samples")

    print(f"✅ Report written to: {out_path}")
    print(f"📌 Sheets scanned: {target_sheets}")


def build_argparser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Verify Category/Item/Answer name columns in ISO RAW structure sheets.")
    p.add_argument("--excel", default="ISO_DATA.xlsx", help="Path to ISO_DATA.xlsx (default: ISO_DATA.xlsx)")
    p.add_argument("--out", default="ISO_Structure_Name_Verification_Report.xlsx",
                   help="Output report Excel (default: ISO_Structure_Name_Verification_Report.xlsx)")
    p.add_argument("--prefix", default="ISO_Check_categor",
                   help="Sheet name prefix to target (default: ISO_Check_categor)")
    p.add_argument("--max_missing_samples", type=int, default=300,
                   help="Max missing sample rows per sheet (default: 300)")
    return p


if __name__ == "__main__":
    args = build_argparser().parse_args()
    run(args.excel, args.out, args.prefix, args.max_missing_samples)
