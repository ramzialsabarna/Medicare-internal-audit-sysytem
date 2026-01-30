#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
traceability_random_sample.py
-----------------------------
Generates a RANDOM (but reproducible) traceability sample from ALL structure sheets:
ISO_Check_category1..5 + ISO_Check_category2025

Sample mix:
- 3 Categories   (CATEGORY_CODE)
- 5 Items        (ITEM_FEATURE_NAME)
- 2 Answers      (ANSWER_FEATURE_NAME)

Fix included:
- Cleans ID-like fields to avoid Excel showing 54.0 / 22.0, etc.
  (If value is integer-like float => convert to '54' not '54.0')

Output:
- Traceability_10_Random sheet
- Derivation_Notes sheet

Robustness:
- If --excel points to a missing file, it falls back to ISO_DATA.xlsx (if present in current dir).
- If still not found, it lists available .xlsx files.
"""

from __future__ import annotations
import argparse
from pathlib import Path
from typing import List, Dict
import pandas as pd


STRUCTURE_SHEETS_DEFAULT = [
    "ISO_Check_category1",
    "ISO_Check_category2",
    "ISO_Check_category3",
    "ISO_Check_category4",
    "ISO_Check_category5",
    "ISO_Check_category2025",
]


# ---------------------------
# Helpers
# ---------------------------
def _pick_col(df: pd.DataFrame, candidates: List[str]) -> str:
    """Return first existing column from candidates (case-insensitive)."""
    lower_map = {str(c).strip().lower(): c for c in df.columns}
    for cand in candidates:
        key = cand.strip().lower()
        if key in lower_map:
            return lower_map[key]
    return ""


def _clean_id(v) -> str:
    """
    Convert ID-like values to clean strings:
    - 54.0 -> "54"
    - 54   -> "54"
    - NaN  -> ""
    - "54.0" -> "54"
    """
    if v is None or pd.isna(v):
        return ""
    # numeric
    if isinstance(v, (int,)):
        return str(v)
    if isinstance(v, float):
        return str(int(v)) if float(v).is_integer() else str(v)

    # string-like
    s = str(v).strip()
    if s == "":
        return ""
    try:
        f = float(s)
        return str(int(f)) if f.is_integer() else s
    except Exception:
        return s


def _safe_get_text(r: pd.Series, col: str) -> str:
    if not col:
        return ""
    v = r.get(col, "")
    if pd.isna(v):
        return ""
    return str(v)


def _resolve_excel_path(excel_arg: str) -> Path:
    """Robust file resolver with fallback to ISO_DATA.xlsx."""
    p = Path(excel_arg)
    if p.exists():
        return p

    fallback = Path("ISO_DATA.xlsx")
    if fallback.exists():
        print(f"⚠ '{excel_arg}' غير موجود. سيتم استخدام الملف الافتراضي: {fallback.name}")
        return fallback

    xlsx_files = list(Path(".").glob("*.xlsx"))
    if len(xlsx_files) == 1:
        print(f"⚠ '{excel_arg}' غير موجود. تم العثور على ملف Excel واحد فقط وسيتم استخدامه: {xlsx_files[0].name}")
        return xlsx_files[0]

    raise FileNotFoundError(
        f"Excel file not found: {excel_arg}\n"
        f"Available .xlsx files here: {[f.name for f in xlsx_files]}"
    )


def _load_structure_sheets(excel_path: Path, sheet_names: List[str]) -> pd.DataFrame:
    """Load and concat all existing structure sheets from workbook."""
    xls = pd.ExcelFile(excel_path)
    existing = [s for s in sheet_names if s in xls.sheet_names]
    if not existing:
        raise ValueError(
            f"None of the expected structure sheets were found.\n"
            f"Expected one of: {sheet_names}\n"
            f"Sheets found: {xls.sheet_names}"
        )

    frames = []
    for s in existing:
        df = pd.read_excel(excel_path, sheet_name=s)
        df.columns = [str(c).strip() for c in df.columns]
        df["_SOURCE_SHEET"] = s
        frames.append(df)

    return pd.concat(frames, ignore_index=True)


# ---------------------------
# Main
# ---------------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--excel", default="ISO_DATA.xlsx", help="Path to ISO_DATA.xlsx (default: ISO_DATA.xlsx)")
    ap.add_argument("--out", default="Traceability_Sample_Random10.xlsx", help="Output Excel report path")
    ap.add_argument("--seed", type=int, default=2025, help="Random seed for reproducibility")
    ap.add_argument("--n-categories", type=int, default=3)
    ap.add_argument("--n-items", type=int, default=5)
    ap.add_argument("--n-answers", type=int, default=2)
    ap.add_argument("--sheets", nargs="*", default=STRUCTURE_SHEETS_DEFAULT, help="Structure sheet names")
    args = ap.parse_args()

    excel_path = _resolve_excel_path(args.excel)

    # Debug: show sheets
    xls = pd.ExcelFile(excel_path)
    print(f"✅ Using Excel: {excel_path.resolve()}")
    print(f"📄 Sheets found: {xls.sheet_names}")

    df_all = _load_structure_sheets(excel_path, args.sheets)

    # Feature-name columns
    col_cat_code = _pick_col(df_all, ["CATEGORY_CODE"])
    col_item_feat = _pick_col(df_all, ["ITEM_FEATURE_NAME"])
    col_ans_feat = _pick_col(df_all, ["ANSWER_FEATURE_NAME"])

    # Traceability keys (IDs + names)
    col_cat_id = _pick_col(df_all, ["CHECK_CATEGORY_ID", "CATEGORY_ID"])
    col_cat_name = _pick_col(df_all, ["CHECK_CATEGORY_NAME", "CATEGORY_NAME"])
    col_issue = _pick_col(df_all, ["ISSUE_NUMBER", "ISSUE_NO"])
    col_item_id = _pick_col(df_all, ["CHECK_ITEM_ID", "ITEM_ID"])
    col_item_name = _pick_col(df_all, ["CHECK_ITEM_NAME", "ITEM_NAME"])
    col_choice_id = _pick_col(df_all, ["CHOICE_ID"])
    col_choice_name = _pick_col(df_all, ["CHOICE_VALUE_OPTION_NAME", "CHOICE_NAME", "CHOICE_VALUE"])

    # Validate minimum required feature columns
    missing = []
    if not col_cat_code:
        missing.append("CATEGORY_CODE")
    if not col_item_feat:
        missing.append("ITEM_FEATURE_NAME")
    if not col_ans_feat:
        missing.append("ANSWER_FEATURE_NAME")
    if missing:
        raise ValueError(
            f"Missing required feature-name columns in structure sheets: {missing}\n"
            f"Columns found: {list(df_all.columns)}"
        )

    # Unique representatives for each level
    cats = df_all.dropna(subset=[col_cat_code]).copy()
    cats[col_cat_code] = cats[col_cat_code].astype(str).str.strip()
    cats = cats[cats[col_cat_code] != ""]
    cats_unique = cats.drop_duplicates(subset=[col_cat_code])

    items = df_all.dropna(subset=[col_item_feat]).copy()
    items[col_item_feat] = items[col_item_feat].astype(str).str.strip()
    items = items[items[col_item_feat] != ""]
    items_unique = items.drop_duplicates(subset=[col_item_feat])

    answers = df_all.dropna(subset=[col_ans_feat]).copy()
    answers[col_ans_feat] = answers[col_ans_feat].astype(str).str.strip()
    answers = answers[answers[col_ans_feat] != ""]
    answers_unique = answers.drop_duplicates(subset=[col_ans_feat])

    # Random sample with fixed seed (reproducible)
    cats_s = cats_unique.sample(n=min(args.n_categories, len(cats_unique)), random_state=args.seed) if len(cats_unique) else cats_unique
    items_s = items_unique.sample(n=min(args.n_items, len(items_unique)), random_state=args.seed + 1) if len(items_unique) else items_unique
    answers_s = answers_unique.sample(n=min(args.n_answers, len(answers_unique)), random_state=args.seed + 2) if len(answers_unique) else answers_unique

    rows: List[Dict[str, str]] = []

    # Categories (3)
    for _, r in cats_s.iterrows():
        rows.append({
            "Feature_Level": "Category",
            "Feature_Name": _safe_get_text(r, col_cat_code),
            "Source_Sheet": _safe_get_text(r, "_SOURCE_SHEET"),
            "CHECK_CATEGORY_ID": _clean_id(r.get(col_cat_id, "")),
            "CHECK_CATEGORY_NAME": _safe_get_text(r, col_cat_name),
            "ISSUE_NUMBER": _clean_id(r.get(col_issue, "")),
            "CHECK_ITEM_ID": "",
            "CHECK_ITEM_NAME": "",
            "CHOICE_ID": "",
            "CHOICE_VALUE_OPTION_NAME": "",
            "ITEM_FEATURE_NAME": "",
            "ANSWER_FEATURE_NAME": "",
        })

    # Items (5)
    for _, r in items_s.iterrows():
        rows.append({
            "Feature_Level": "Item",
            "Feature_Name": _safe_get_text(r, col_item_feat),
            "Source_Sheet": _safe_get_text(r, "_SOURCE_SHEET"),
            "CHECK_CATEGORY_ID": _clean_id(r.get(col_cat_id, "")),
            "CHECK_CATEGORY_NAME": _safe_get_text(r, col_cat_name),
            "ISSUE_NUMBER": _clean_id(r.get(col_issue, "")),
            "CHECK_ITEM_ID": _clean_id(r.get(col_item_id, "")),
            "CHECK_ITEM_NAME": _safe_get_text(r, col_item_name),
            "CHOICE_ID": "",
            "CHOICE_VALUE_OPTION_NAME": "",
            "ITEM_FEATURE_NAME": _safe_get_text(r, col_item_feat),
            "ANSWER_FEATURE_NAME": "",
        })

    # Answers (2)
    for _, r in answers_s.iterrows():
        rows.append({
            "Feature_Level": "Answer",
            "Feature_Name": _safe_get_text(r, col_ans_feat),
            "Source_Sheet": _safe_get_text(r, "_SOURCE_SHEET"),
            "CHECK_CATEGORY_ID": _clean_id(r.get(col_cat_id, "")),
            "CHECK_CATEGORY_NAME": _safe_get_text(r, col_cat_name),
            "ISSUE_NUMBER": _clean_id(r.get(col_issue, "")),
            "CHECK_ITEM_ID": _clean_id(r.get(col_item_id, "")),
            "CHECK_ITEM_NAME": _safe_get_text(r, col_item_name),
            "CHOICE_ID": _clean_id(r.get(col_choice_id, "")),
            "CHOICE_VALUE_OPTION_NAME": _safe_get_text(r, col_choice_name),
            "ITEM_FEATURE_NAME": _safe_get_text(r, col_item_feat),
            "ANSWER_FEATURE_NAME": _safe_get_text(r, col_ans_feat),
        })

    df_trace = pd.DataFrame(rows)

    derivation = pd.DataFrame([
        {
            "Feature_Level": "Category",
            "Feature_Name_Source_Column": "CATEGORY_CODE",
            "Traceability_Keys": "CHECK_CATEGORY_ID (+ CHECK_CATEGORY_NAME), ISSUE_NUMBER (if present)",
            "Notes": "CATEGORY_CODE is the stable category feature code used in the model."
        },
        {
            "Feature_Level": "Item",
            "Feature_Name_Source_Column": "ITEM_FEATURE_NAME",
            "Traceability_Keys": "CHECK_ITEM_ID (+ CHECK_ITEM_NAME) and CHECK_CATEGORY_ID",
            "Notes": "ITEM_FEATURE_NAME is the UVL item feature name; repeated result rows do not affect traceability."
        },
        {
            "Feature_Level": "Answer",
            "Feature_Name_Source_Column": "ANSWER_FEATURE_NAME",
            "Traceability_Keys": "CHOICE_ID (+ CHOICE_VALUE_OPTION_NAME) and CHECK_ITEM_ID",
            "Notes": "ANSWER_FEATURE_NAME maps back to choice identifiers used in the source data."
        },
        {
            "Feature_Level": "Reproducibility",
            "Feature_Name_Source_Column": "",
            "Traceability_Keys": "",
            "Notes": f"Random sampling is reproducible using a fixed seed = {args.seed}. Change seed to generate a different sample."
        },
    ])

    out_path = Path(args.out)
    with pd.ExcelWriter(out_path, engine="openpyxl") as w:
        df_trace.to_excel(w, index=False, sheet_name="Traceability_10_Random")
        derivation.to_excel(w, index=False, sheet_name="Derivation_Notes")

    print(f"✅ Traceability sample written to: {out_path.resolve()}")
    print(f"   Sample mix: {len(cats_s)} categories, {len(items_s)} items, {len(answers_s)} answers (seed={args.seed})")


if __name__ == "__main__":
    main()
