# -*- coding: utf-8 -*-
"""
ISO RAW DATA - ID Verification Utilities
=======================================

Purpose
-------
1) Select sheets whose name starts with a given prefix (default: "ISO_Check_categor").
2) Standardize identifier (ID) columns into stable string tokens (prevents Excel numeric drift).
3) Produce a verification report:
   - Missing IDs (raw + after normalization)
   - Numeric drift / scientific notation signals
   - Values changed by normalization
   - Duplicates (per column + composite over all available ID columns)
   - Simple dependency checks (e.g., CHOICE_VALUE_OPTION_ID present but CHOICE_ID missing)

This module is self-contained (no external project imports required).
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
import math
import re
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

import pandas as pd


# -----------------------------
# Helpers: missing + ID tokenization
# -----------------------------

_MISSING_STRINGS = {"", "nan", "none", "null", "na", "n/a", "#n/a", "#na"}

_SCI_NOTATION_RE = re.compile(r"^[+-]?\d+(\.\d+)?[eE][+-]?\d+$")
_FLOAT_ARTIFACT_RE = re.compile(r"^[+-]?\d+\.0+$")  # e.g., "10.0", "123.0"


def is_missing_like(x) -> bool:
    """True if x should be considered missing."""
    if x is None:
        return True
    # pandas NA / numpy NaN
    try:
        if pd.isna(x):
            return True
    except Exception:
        pass
    if isinstance(x, str):
        s = x.strip().lower()
        return s in _MISSING_STRINGS
    return False


def _decimal_from_str(s: str) -> Optional[Decimal]:
    """Parse Decimal safely from a string; return None on failure."""
    try:
        return Decimal(s)
    except (InvalidOperation, ValueError):
        return None


def normalize_id_token(x) -> Optional[str]:
    """
    Convert an ID-like value into a stable string token.

    Handles:
    - ints -> "123"
    - floats like 123.0 -> "123"
    - strings like "123.0" -> "123"
    - scientific notation like "1.234E+05" -> "123400" (if integer)
    - strips whitespace
    Returns None if missing-like.
    """
    if is_missing_like(x):
        return None

    # Integers
    if isinstance(x, (int,)) and not isinstance(x, bool):
        return str(x)

    # Numpy / pandas ints
    if isinstance(x, (pd.Int64Dtype,)):  # type: ignore[attr-defined]
        return str(int(x))

    # Floats
    if isinstance(x, (float,)) and not isinstance(x, bool):
        if math.isnan(x) or math.isinf(x):
            return None
        # Treat near-integers as integers (Excel often makes 10 -> 10.0)
        if abs(x - round(x)) < 1e-9:
            return str(int(round(x)))
        # Otherwise keep full precision without trailing zeros
        d = _decimal_from_str(repr(x)) or _decimal_from_str(str(x))
        if d is None:
            return str(x).strip()
        s = format(d.normalize(), "f")
        return s

    # Anything else -> string path
    s = str(x).strip()
    if s == "":
        return None

    # Fix classic Excel float artifact in strings: "10.0" -> "10"
    if _FLOAT_ARTIFACT_RE.match(s):
        s = s.split(".", 1)[0]
        return s

    # Scientific notation (string)
    if _SCI_NOTATION_RE.match(s):
        d = _decimal_from_str(s)
        if d is None:
            return s
        # If integer-like, cast to int to avoid ".0"
        if d == d.to_integral_value():
            return str(int(d))
        return format(d.normalize(), "f")

    # Plain numeric with trailing zeros after decimal: "12.3400" -> "12.34"
    if re.match(r"^[+-]?\d+\.\d+$", s):
        d = _decimal_from_str(s)
        if d is None:
            return s
        # If integer-like, remove decimal part
        if d == d.to_integral_value():
            return str(int(d))
        return format(d.normalize(), "f")

    return s


# -----------------------------
# Verification logic
# -----------------------------

@dataclass(frozen=True)
class IdCheckConfig:
    sheet_prefix: str = "ISO_Check_categor"   # matches "ISO_Check_category..." too
    case_insensitive: bool = True
    id_columns: Tuple[str, ...] = (
        "BRANCH_ID",
        "CHECK_CATEGORY_ID",
        "ISSUE_NUMBER",
        "CHECK_ITEM_ID",
        "LAB_SECTION_ID",
        "CHOICE_ID",
        "CHOICE_VALUE_OPTION_ID",
    )
    clean_suffix: str = "_CLEAN"


def _sheet_matches(name: str, prefix: str, case_insensitive: bool) -> bool:
    if case_insensitive:
        return name.lower().startswith(prefix.lower())
    return name.startswith(prefix)


def standardize_identifier_columns(
    df: pd.DataFrame,
    id_columns: Sequence[str],
    clean_suffix: str = "_CLEAN",
) -> pd.DataFrame:
    """
    Adds <COL>_CLEAN for each identifier column that exists in df.
    """
    out = df.copy()
    for col in id_columns:
        if col not in out.columns:
            continue
        clean_col = f"{col}{clean_suffix}"
        out[clean_col] = out[col].apply(normalize_id_token)
    return out


def _count_numeric_drift_signals(series: pd.Series) -> int:
    """
    Counts rows where the raw value looks like it suffered Excel drift:
    - float with .0
    - string ending with .0
    - string in scientific notation
    """
    n = 0
    for v in series.dropna():
        if isinstance(v, float) and abs(v - round(v)) < 1e-9 and not math.isnan(v):
            # This is common but still a drift signal if it was an ID
            n += 1
            continue
        if isinstance(v, str):
            s = v.strip()
            if _FLOAT_ARTIFACT_RE.match(s) or _SCI_NOTATION_RE.match(s):
                n += 1
    return n


def verify_identifier_columns(
    df: pd.DataFrame,
    sheet_name: str,
    cfg: IdCheckConfig,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Returns:
      (detail_df, sheet_summary_df)
    """
    rows = len(df)
    details = []

    present_id_cols = [c for c in cfg.id_columns if c in df.columns]
    clean_cols = [f"{c}{cfg.clean_suffix}" for c in present_id_cols]

    for col in present_id_cols:
        clean_col = f"{col}{cfg.clean_suffix}"

        raw = df[col]
        clean = df[clean_col] if clean_col in df.columns else pd.Series([None] * rows)

        missing_raw = int(raw.apply(is_missing_like).sum())
        missing_clean = int(clean.isna().sum())

        drift_signals = _count_numeric_drift_signals(raw)

        # changed after normalization (only where raw is not missing-like)
        changed = 0
        sample_changes = []
        for r, c in zip(raw, clean):
            if is_missing_like(r):
                continue
            rr = str(r).strip()
            cc = "" if c is None else str(c).strip()
            if rr != cc:
                changed += 1
                if len(sample_changes) < 5:
                    sample_changes.append(f"{rr} -> {cc}")

        # uniqueness/duplicates (on clean)
        non_null = clean.dropna().astype(str)
        unique_clean = int(non_null.nunique())
        dup_clean = int(non_null.duplicated().sum())

        # type profile (raw)
        type_counts = raw.dropna().map(lambda x: type(x).__name__).value_counts().to_dict()

        details.append({
            "sheet": sheet_name,
            "column": col,
            "present": True,
            "rows": rows,
            "missing_raw": missing_raw,
            "missing_raw_pct": (missing_raw / rows * 100.0) if rows else 0.0,
            "missing_clean": missing_clean,
            "missing_clean_pct": (missing_clean / rows * 100.0) if rows else 0.0,
            "numeric_drift_signals": drift_signals,
            "changed_by_normalization": changed,
            "unique_clean_non_null": unique_clean,
            "duplicate_clean_non_null": dup_clean,
            "raw_type_profile": type_counts,
            "sample_changes": "; ".join(sample_changes),
        })

    # Composite duplication across all present ID columns (clean)
    composite_dup = 0
    composite_cols = [f"{c}{cfg.clean_suffix}" for c in present_id_cols if f"{c}{cfg.clean_suffix}" in df.columns]
    if composite_cols:
        composite_dup = int(df[composite_cols].duplicated().sum())

    # Simple dependency checks
    dep_issues = []
    def _dep(rule_name: str, when_col: str, must_have_col: str):
        if when_col in df.columns and must_have_col in df.columns:
            when = df[when_col].apply(is_missing_like) == False
            must_missing = df[must_have_col].apply(is_missing_like)
            bad = int((when & must_missing).sum())
            if bad:
                dep_issues.append(f"{rule_name}: {bad}")

    _dep("CHOICE_VALUE_OPTION_ID present but CHOICE_ID missing", "CHOICE_VALUE_OPTION_ID", "CHOICE_ID")
    _dep("CHOICE_ID present but CHECK_ITEM_ID missing", "CHOICE_ID", "CHECK_ITEM_ID")
    _dep("CHECK_ITEM_ID present but CHECK_CATEGORY_ID missing", "CHECK_ITEM_ID", "CHECK_CATEGORY_ID")
    _dep("CHECK_CATEGORY_ID present but BRANCH_ID missing", "CHECK_CATEGORY_ID", "BRANCH_ID")

    sheet_summary = pd.DataFrame([{
        "sheet": sheet_name,
        "rows": rows,
        "id_columns_present": ", ".join(present_id_cols),
        "composite_duplicate_rows_on_ids": composite_dup,
        "dependency_issues": " | ".join(dep_issues) if dep_issues else "",
    }])

    return pd.DataFrame(details), sheet_summary


def build_iso_id_verification_report(
    excel_path: str,
    output_excel_path: Optional[str] = None,
    cfg: Optional[IdCheckConfig] = None,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Main entrypoint.

    Parameters
    ----------
    excel_path:
        Path to the ISO_DATA Excel file.
    output_excel_path:
        If provided, writes an Excel report with two sheets:
        - "Summary"
        - "ID_Details"
    cfg:
        IdCheckConfig (prefix + id columns + suffix).

    Returns
    -------
    (summary_df, details_df)
    """
    cfg = cfg or IdCheckConfig()

    xls = pd.ExcelFile(excel_path)
    target_sheets = [s for s in xls.sheet_names if _sheet_matches(s, cfg.sheet_prefix, cfg.case_insensitive)]

    all_details = []
    all_summaries = []

    for sheet in target_sheets:
        df = pd.read_excel(excel_path, sheet_name=sheet)
        df2 = standardize_identifier_columns(df, cfg.id_columns, cfg.clean_suffix)

        details_df, summary_df = verify_identifier_columns(df2, sheet, cfg)
        all_details.append(details_df)
        all_summaries.append(summary_df)

    details_out = pd.concat(all_details, ignore_index=True) if all_details else pd.DataFrame()
    summary_out = pd.concat(all_summaries, ignore_index=True) if all_summaries else pd.DataFrame()

    if output_excel_path:
        with pd.ExcelWriter(output_excel_path, engine="openpyxl") as writer:
            summary_out.to_excel(writer, sheet_name="Summary", index=False)
            details_out.to_excel(writer, sheet_name="ID_Details", index=False)

    return summary_out, details_out


if __name__ == "__main__":
    import argparse
    import os
    import sys

    p = argparse.ArgumentParser(
        description=(
            "Generate ID verification report for ISO raw Excel data.\n\n"
            "If you run the script with no arguments (python iso_id_verification.py), "
            "it will assume ISO_DATA.xlsx is in the current folder, and will output "
            "ISO_ID_Verification_Report.xlsx."
        )
    )

    # Defaults allow: python iso_id_verification.py
    p.add_argument(
        "--excel",
        default="ISO_DATA.xlsx",
        help="Path to ISO_DATA.xlsx (default: ISO_DATA.xlsx in current folder)",
    )
    p.add_argument(
        "--out",
        default="ISO_ID_Verification_Report.xlsx",
        help="Output report xlsx path (default: ISO_ID_Verification_Report.xlsx)",
    )
    p.add_argument(
        "--prefix",
        default="ISO_Check_categor",
        help="Sheet name prefix to scan (default: ISO_Check_categor)",
    )
    args = p.parse_args()

    # Friendly error if the default file isn't in the working directory
    if not os.path.exists(args.excel):
        cwd = os.getcwd()
        xlsx_files = [f for f in os.listdir(cwd) if f.lower().endswith((".xlsx", ".xls"))]
        msg = [
            f"لم يتم العثور على ملف الإكسل: {args.excel}",
            f"المسار الحالي (Working Directory): {cwd}",
        ]
        if xlsx_files:
            msg.append("ملفات Excel الموجودة هنا:")
            msg.extend([f"- {f}" for f in sorted(xlsx_files)])
        msg.append("\nجرّب أحد الأوامر التالية:")
        msg.append("  python iso_id_verification.py --excel ISO_DATA.xlsx")
        msg.append("  python iso_id_verification.py --excel /path/to/ISO_DATA.xlsx --out report.xlsx")
        print("\n".join(msg))
        sys.exit(2)

    cfg = IdCheckConfig(sheet_prefix=args.prefix)
    build_iso_id_verification_report(args.excel, output_excel_path=args.out, cfg=cfg)
    print(f"Saved report to: {args.out}")
