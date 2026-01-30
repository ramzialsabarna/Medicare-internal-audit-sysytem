#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
iso_numeric_columns_verification.py
-----------------------------------
Verify numeric columns in ISO raw structure sheets.

Target columns:
  - GENERAL_SCORE
  - CATEGORY_MIN_ACCEPTABLE_SCORE
  - WEIGHT_PERCENTAGE
  - CI_MIN_ACCEPTABLE_SCORE
  - OPTION_VALUE

Scans all sheets starting with a prefix (default: ISO_Check_categor).

Outputs a single Excel report with multiple sheets:
  - Summary_BySheet
  - Summary_Overall
  - NonNumeric_Samples
  - Missing_Samples

Run (defaults expect ISO_DATA.xlsx in current folder):
  python .\iso_numeric_columns_verification.py

Or:
  python .\iso_numeric_columns_verification.py --excel .\ISO_DATA.xlsx --out .\ISO_Numeric_Verification_Report.xlsx --prefix ISO_Check_categor
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

def coerce_numeric_series(s: pd.Series) -> pd.Series:
    """
    Convert a series to numeric robustly:
    - trims strings
    - removes percent sign
    - handles comma decimal (e.g., '12,5' -> '12.5' when no dot is present)
    - removes thousands separators (e.g., '1,234' -> '1234' when dot exists or typical thousands)
    Returns float series with NaN for non-convertible values.
    """
    def _prep(v: Any) -> Any:
        if is_missing_like(v):
            return None
        # Already numeric?
        if isinstance(v, (int, float)) and not isinstance(v, bool):
            return v
        txt = str(v).strip()
        if txt == "":
            return None
        # remove % sign
        txt = txt.replace("%", "").strip()

        # normalize spaces
        txt = re.sub(r"\s+", "", txt)

        # If contains comma but no dot => treat comma as decimal separator
        # Example: "12,5" -> "12.5"
        if "," in txt and "." not in txt:
            # but if it looks like thousands (e.g., 1,234) we should remove comma
            # heuristic: if there are exactly 3 digits after comma, likely thousands
            parts = txt.split(",")
            if len(parts) == 2 and len(parts[1]) == 3 and parts[0].isdigit() and parts[1].isdigit():
                txt = parts[0] + parts[1]  # "1,234" -> "1234"
            else:
                txt = txt.replace(",", ".")
        else:
            # remove thousands separators commas
            txt = txt.replace(",", "")

        return txt

    prepared = s.map(_prep)
    return pd.to_numeric(prepared, errors="coerce")


def infer_value_type(series: pd.Series) -> str:
    """Infer raw value type ignoring missing: empty/string/numeric/datetime/mixed."""
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


# -----------------------------
# Verification logic
# -----------------------------
TARGET_NUM_COLS = [
    "GENERAL_SCORE",
    "CATEGORY_MIN_ACCEPTABLE_SCORE",
    "WEIGHT_PERCENTAGE",
    "CI_MIN_ACCEPTABLE_SCORE",
    "OPTION_VALUE",
]


def verify_numeric_sheet(
    df: pd.DataFrame,
    sheet_name: str,
    max_samples_per_col: int = 60
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Returns:
      - summary rows (multi-row df; one row per target column)
      - non_numeric_samples (rows)
      - missing_samples (rows)
    """
    total_rows = len(df)

    summary_rows: List[Dict[str, Any]] = []
    non_numeric_rows: List[Dict[str, Any]] = []
    missing_rows: List[Dict[str, Any]] = []

    # context columns (helpful for manual review if present)
    context_candidates = [
        "BRANCH_ID", "CHECK_CATEGORY_ID", "CHECK_CATEGORY_NAME",
        "ISSUE_NUMBER", "CHECK_ITEM_ID", "CHECK_ITEM_NAME",
        "LAB_SECTION_ID", "CHOICE_ID", "CHOICE_VALUE_OPTION_NAME"
    ]
    context_cols: List[str] = []
    for c in context_candidates:
        real = find_column(df, c)
        if real is not None:
            context_cols.append(real)

    for target in TARGET_NUM_COLS:
        col = find_column(df, target)
        if col is None:
            summary_rows.append({
                "SHEET": sheet_name,
                "COLUMN": target,
                "PRESENT": False,
                "TOTAL_ROWS": total_rows,
                "RAW_VALUE_TYPE": None,
                "MISSING_COUNT": None,
                "MISSING_PCT": None,
                "NON_NUMERIC_COUNT": None,
                "NON_NUMERIC_PCT": None,
                "NUMERIC_COUNT": None,
                "NUMERIC_PCT": None,
                "NUMERIC_MIN": None,
                "NUMERIC_MAX": None,
                "NUMERIC_MEAN": None,
                "NOTES": "Column not found in this sheet.",
            })
            continue

        raw = df[col]
        raw_type = infer_value_type(raw)

        # Missing in raw (based on missing tokens)
        is_miss = raw.map(is_missing_like)
        missing_count = int(is_miss.sum())
        missing_pct = (missing_count / total_rows * 100.0) if total_rows else 0.0

        # Numeric coercion
        num = coerce_numeric_series(raw)
        # Non-numeric means: not missing in raw BUT coercion resulted NaN
        non_numeric_mask = (~is_miss) & (num.isna())
        non_numeric_count = int(non_numeric_mask.sum())
        non_numeric_pct = (non_numeric_count / total_rows * 100.0) if total_rows else 0.0

        numeric_mask = (~is_miss) & (~num.isna())
        numeric_count = int(numeric_mask.sum())
        numeric_pct = (numeric_count / total_rows * 100.0) if total_rows else 0.0

        # Stats on numeric values only
        num_valid = num[numeric_mask]
        num_min = float(num_valid.min()) if len(num_valid) else None
        num_max = float(num_valid.max()) if len(num_valid) else None
        num_mean = float(num_valid.mean()) if len(num_valid) else None

        summary_rows.append({
            "SHEET": sheet_name,
            "COLUMN": target,
            "PRESENT": True,
            "TOTAL_ROWS": total_rows,
            "RAW_VALUE_TYPE": raw_type,
            "MISSING_COUNT": missing_count,
            "MISSING_PCT": float(missing_pct),
            "NON_NUMERIC_COUNT": non_numeric_count,
            "NON_NUMERIC_PCT": float(non_numeric_pct),
            "NUMERIC_COUNT": numeric_count,
            "NUMERIC_PCT": float(numeric_pct),
            "NUMERIC_MIN": num_min,
            "NUMERIC_MAX": num_max,
            "NUMERIC_MEAN": num_mean,
            "NOTES": "Missing tokens: '', NA, N/A, null, none, '-'. Numeric coercion handles % and comma decimals.",
        })

        # Collect samples
        # Non-numeric samples
        idxs = df.index[non_numeric_mask].tolist()[:max_samples_per_col]
        for idx in idxs:
            row = {
                "SHEET": sheet_name,
                "ROW_INDEX": int(idx) if isinstance(idx, int) else str(idx),
                "COLUMN": target,
                "RAW_VALUE": df.at[idx, col],
                "ISSUE": "NON_NUMERIC_VALUE",
            }
            for cc in context_cols:
                row[cc] = df.at[idx, cc]
            non_numeric_rows.append(row)

        # Missing samples
        idxs_m = df.index[is_miss].tolist()[:max_samples_per_col]
        for idx in idxs_m:
            row = {
                "SHEET": sheet_name,
                "ROW_INDEX": int(idx) if isinstance(idx, int) else str(idx),
                "COLUMN": target,
                "RAW_VALUE": df.at[idx, col],
                "ISSUE": "MISSING_VALUE",
            }
            for cc in context_cols:
                row[cc] = df.at[idx, cc]
            missing_rows.append(row)

    summary_df = pd.DataFrame(summary_rows)
    non_numeric_df = pd.DataFrame(non_numeric_rows)
    missing_df = pd.DataFrame(missing_rows)
    return summary_df, non_numeric_df, missing_df


def run(excel_path: str, out_path: str, prefix: str, max_samples_per_col: int = 60) -> None:
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
    target_sheets = [s for s in xls.sheet_names if str(s).startswith(prefix)]

    if not target_sheets:
        with pd.ExcelWriter(out_path, engine="openpyxl") as w:
            pd.DataFrame([{"NOTES": f"No sheets found with prefix: {prefix}"}]).to_excel(w, index=False, sheet_name="Summary_BySheet")
        return

    all_summary = []
    all_non_numeric = []
    all_missing = []

    for sheet in target_sheets:
        df = pd.read_excel(excel_path, sheet_name=sheet, dtype=object, engine="openpyxl")
        summary_df, non_numeric_df, missing_df = verify_numeric_sheet(df, sheet, max_samples_per_col=max_samples_per_col)
        all_summary.append(summary_df)
        if not non_numeric_df.empty:
            all_non_numeric.append(non_numeric_df)
        if not missing_df.empty:
            all_missing.append(missing_df)

    summary_all = pd.concat(all_summary, ignore_index=True)
    non_numeric_all = pd.concat(all_non_numeric, ignore_index=True) if all_non_numeric else pd.DataFrame()
    missing_all = pd.concat(all_missing, ignore_index=True) if all_missing else pd.DataFrame()

    # Overall summary aggregated by COLUMN across all sheets
    overall_rows = []
    for col in TARGET_NUM_COLS:
        sub = summary_all[summary_all["COLUMN"] == col]
        if sub.empty:
            continue
        # Sum across sheets (only where column present)
        present = sub[sub["PRESENT"] == True]
        if present.empty:
            overall_rows.append({
                "COLUMN": col,
                "PRESENT_IN_ANY_SHEET": False,
                "TOTAL_ROWS_SUM": int(sub["TOTAL_ROWS"].fillna(0).sum()),
                "MISSING_COUNT_SUM": None,
                "NON_NUMERIC_COUNT_SUM": None,
                "NUMERIC_COUNT_SUM": None,
                "MISSING_PCT_OVERALL": None,
                "NON_NUMERIC_PCT_OVERALL": None,
                "NUMERIC_PCT_OVERALL": None,
                "NOTES": "Column not present in any targeted sheet.",
            })
            continue

        total_rows_sum = int(present["TOTAL_ROWS"].sum())
        missing_sum = int(present["MISSING_COUNT"].fillna(0).sum())
        non_numeric_sum = int(present["NON_NUMERIC_COUNT"].fillna(0).sum())
        numeric_sum = int(present["NUMERIC_COUNT"].fillna(0).sum())

        overall_rows.append({
            "COLUMN": col,
            "PRESENT_IN_ANY_SHEET": True,
            "TOTAL_ROWS_SUM": total_rows_sum,
            "MISSING_COUNT_SUM": missing_sum,
            "NON_NUMERIC_COUNT_SUM": non_numeric_sum,
            "NUMERIC_COUNT_SUM": numeric_sum,
            "MISSING_PCT_OVERALL": float(missing_sum / total_rows_sum * 100.0) if total_rows_sum else None,
            "NON_NUMERIC_PCT_OVERALL": float(non_numeric_sum / total_rows_sum * 100.0) if total_rows_sum else None,
            "NUMERIC_PCT_OVERALL": float(numeric_sum / total_rows_sum * 100.0) if total_rows_sum else None,
            "NOTES": "Overall percentages computed as sums across sheets where the column is present.",
        })

    overall_df = pd.DataFrame(overall_rows)

    with pd.ExcelWriter(out_path, engine="openpyxl") as w:
        summary_all.to_excel(w, index=False, sheet_name="Summary_BySheet")
        overall_df.to_excel(w, index=False, sheet_name="Summary_Overall")
        if not non_numeric_all.empty:
            non_numeric_all.to_excel(w, index=False, sheet_name="NonNumeric_Samples")
        else:
            pd.DataFrame([{"NOTES": "No non-numeric values found (after coercion) in targeted columns."}]).to_excel(
                w, index=False, sheet_name="NonNumeric_Samples"
            )
        if not missing_all.empty:
            missing_all.to_excel(w, index=False, sheet_name="Missing_Samples")
        else:
            pd.DataFrame([{"NOTES": "No missing values found in targeted columns."}]).to_excel(
                w, index=False, sheet_name="Missing_Samples"
            )

    print(f"✅ Report written to: {out_path}")
    print(f"📌 Sheets scanned: {target_sheets}")


def build_argparser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Verify numeric columns (scores/weights/options) in ISO RAW structure sheets.")
    p.add_argument("--excel", default="ISO_DATA.xlsx", help="Path to ISO_DATA.xlsx (default: ISO_DATA.xlsx)")
    p.add_argument("--out", default="ISO_Numeric_Verification_Report.xlsx",
                   help="Output report Excel (default: ISO_Numeric_Verification_Report.xlsx)")
    p.add_argument("--prefix", default="ISO_Check_categor",
                   help="Sheet prefix to target (default: ISO_Check_categor)")
    p.add_argument("--max_samples_per_col", type=int, default=60,
                   help="Max sample rows per column per sheet (default: 60)")
    return p


if __name__ == "__main__":
    args = build_argparser().parse_args()
    run(args.excel, args.out, args.prefix, args.max_samples_per_col)
    print(f"[OK] Wrote report: {args.out}")
