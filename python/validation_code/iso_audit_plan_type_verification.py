#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
ISO Audit Plan/Type Verification (Fixed)
----------------------------------------
Business rule:
- AUDIT_PLAN is REQUIRED only for planned visits.
- AUDIT_PLAN is ALLOWED to be missing for: unplanned, reevaluate.

Report outputs:
- Summary per sheet:
  * Total rows
  * Audit type distribution counts
  * AUDIT_PLAN missing overall
  * Allowed missing (unplanned + reevaluate)
  * Violations (planned missing plan)
  * Planned-only missing percentage (key metric for system weakness evidence)
"""

from __future__ import annotations
import argparse
import os
import re
from typing import Any, Optional, Tuple, List

import pandas as pd


# -----------------------------
# Helpers
# -----------------------------
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

def norm_text(x: Any) -> Optional[str]:
    if is_missing_like(x):
        return None
    return str(x).strip()

def canon_audit_type(x: Any) -> Optional[str]:
    """
    Canonicalize AUDIT_TYPE:
    - lower
    - remove spaces/_/-
    Examples:
      'RE_EVALUATE' -> 'reevaluate'
      're-evaluate' -> 'reevaluate'
    """
    s = norm_text(x)
    if s is None:
        return None
    s = s.lower()
    s = re.sub(r"[\s_-]+", "", s)
    return s

def infer_value_type(series: pd.Series) -> str:
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
# Core verification
# -----------------------------
ALLOWED_MISSING_TYPES = {"unplanned", "reevaluate"}  # <-- FIX

def verify_sheet(df: pd.DataFrame, sheet_name: str, max_violation_rows: int = 500) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    audit_type_col = find_column(df, "AUDIT_TYPE")
    audit_plan_col = find_column(df, "AUDIT_PLAN")

    total_rows = len(df)
    missing_cols: List[str] = []
    if audit_type_col is None:
        missing_cols.append("AUDIT_TYPE")
    if audit_plan_col is None:
        missing_cols.append("AUDIT_PLAN")

    if missing_cols:
        summary = pd.DataFrame([{
            "SHEET": sheet_name,
            "TOTAL_ROWS": total_rows,
            "AUDIT_TYPE_PRESENT": audit_type_col is not None,
            "AUDIT_PLAN_PRESENT": audit_plan_col is not None,
            "AUDIT_TYPE_VALUE_TYPE": None,
            "AUDIT_PLAN_VALUE_TYPE": None,
            "AUDIT_TYPE_MISSING_COUNT": None,
            "AUDIT_TYPE_MISSING_PCT": None,
            "AUDIT_PLAN_MISSING_COUNT": None,
            "AUDIT_PLAN_MISSING_PCT": None,
            "AUDIT_PLAN_ALLOWED_MISSING_COUNT": None,
            "AUDIT_PLAN_VIOLATION_MISSING_COUNT": None,
            "PLANNED_ROWS": None,
            "PLANNED_PLAN_MISSING_COUNT": None,
            "PLANNED_PLAN_MISSING_PCT": None,
            "NOTES": f"Missing columns: {', '.join(missing_cols)}",
        }])
        dist = pd.DataFrame([{"SHEET": sheet_name, "AUDIT_TYPE_CANON": None, "COUNT": None, "PCT": None}])
        viol = pd.DataFrame(columns=["SHEET", "ROW_INDEX", "AUDIT_TYPE", "AUDIT_PLAN", "RULE"])
        return summary, dist, viol

    s_type = df[audit_type_col].copy()
    s_plan = df[audit_plan_col].copy()

    type_missing = int(s_type.apply(is_missing_like).sum())
    plan_missing = int(s_plan.apply(is_missing_like).sum())
    type_missing_pct = (type_missing / total_rows * 100.0) if total_rows else 0.0
    plan_missing_pct = (plan_missing / total_rows * 100.0) if total_rows else 0.0

    type_canon = s_type.apply(canon_audit_type)
    plan_is_missing = s_plan.apply(is_missing_like)

    is_allowed_missing_type = type_canon.isin(ALLOWED_MISSING_TYPES)
    is_planned = type_canon.eq("planned")

    allowed_missing = int((is_allowed_missing_type & plan_is_missing).sum())
    violations_missing = int(((~is_allowed_missing_type) & plan_is_missing).sum())

    planned_rows = int(is_planned.sum())
    planned_plan_missing = int((is_planned & plan_is_missing).sum())
    planned_plan_missing_pct = (planned_plan_missing / planned_rows * 100.0) if planned_rows else 0.0

    # Distribution
    dist_counts = type_canon.fillna("MISSING").value_counts(dropna=False)
    dist = pd.DataFrame({
        "SHEET": sheet_name,
        "AUDIT_TYPE_CANON": dist_counts.index.tolist(),
        "COUNT": dist_counts.values.tolist(),
    })
    dist["PCT"] = dist["COUNT"].apply(lambda c: (c / total_rows * 100.0) if total_rows else 0.0)

    # violations sample (planned missing plan, and any other non-allowed types)
    context_candidates = [
        "BRANCH_ID", "CHECK_CATEGORY_ID", "ISSUE_NUMBER", "CHECK_ITEM_ID",
        "LAB_SECTION_ID", "CHOICE_ID", "CHOICE_VALUE_OPTION_ID"
    ]
    context_cols: List[str] = []
    for c in context_candidates:
        real = find_column(df, c)
        if real is not None:
            context_cols.append(real)

    violation_idx = df.index[((~is_allowed_missing_type) & plan_is_missing)].tolist()[:max_violation_rows]
    viol_rows = []
    for idx in violation_idx:
        row = {
            "SHEET": sheet_name,
            "ROW_INDEX": int(idx) if isinstance(idx, int) else str(idx),
            "AUDIT_TYPE": df.at[idx, audit_type_col],
            "AUDIT_PLAN": df.at[idx, audit_plan_col],
            "RULE": f"AUDIT_PLAN missing while AUDIT_TYPE not in {sorted(ALLOWED_MISSING_TYPES)}",
        }
        for c in context_cols:
            row[c] = df.at[idx, c]
        viol_rows.append(row)
    violations = pd.DataFrame(viol_rows)

    summary = pd.DataFrame([{
        "SHEET": sheet_name,
        "TOTAL_ROWS": total_rows,
        "AUDIT_TYPE_PRESENT": True,
        "AUDIT_PLAN_PRESENT": True,
        "AUDIT_TYPE_VALUE_TYPE": infer_value_type(s_type),
        "AUDIT_PLAN_VALUE_TYPE": infer_value_type(s_plan),
        "AUDIT_TYPE_MISSING_COUNT": type_missing,
        "AUDIT_TYPE_MISSING_PCT": float(type_missing_pct),
        "AUDIT_PLAN_MISSING_COUNT": plan_missing,
        "AUDIT_PLAN_MISSING_PCT": float(plan_missing_pct),
        "AUDIT_PLAN_ALLOWED_MISSING_COUNT": allowed_missing,
        "AUDIT_PLAN_VIOLATION_MISSING_COUNT": violations_missing,
        "PLANNED_ROWS": planned_rows,
        "PLANNED_PLAN_MISSING_COUNT": planned_plan_missing,
        "PLANNED_PLAN_MISSING_PCT": float(planned_plan_missing_pct),
        "NOTES": f"AUDIT_PLAN missing allowed only when AUDIT_TYPE in {sorted(ALLOWED_MISSING_TYPES)}; violations represent planned (or other) types missing plan.",
    }])

    return summary, dist, violations


def run(excel_path: str, out_path: str, prefix: str, max_violation_rows: int = 500) -> None:
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
        summary = pd.DataFrame([{
            "SHEET": None,
            "TOTAL_ROWS": None,
            "NOTES": f"No sheets found with prefix: {prefix}",
        }])
        with pd.ExcelWriter(out_path, engine="openpyxl") as w:
            summary.to_excel(w, index=False, sheet_name="Summary")
        return

    all_summary = []
    all_dist = []
    all_viol = []

    for sheet in target_sheets:
        df = pd.read_excel(excel_path, sheet_name=sheet, dtype=object, engine="openpyxl")
        summary, dist, viol = verify_sheet(df, sheet_name=sheet, max_violation_rows=max_violation_rows)
        all_summary.append(summary)
        all_dist.append(dist)
        if not viol.empty:
            all_viol.append(viol)

    summary_df = pd.concat(all_summary, ignore_index=True)
    dist_df = pd.concat(all_dist, ignore_index=True)
    viol_df = pd.concat(all_viol, ignore_index=True) if all_viol else pd.DataFrame()

    with pd.ExcelWriter(out_path, engine="openpyxl") as w:
        summary_df.to_excel(w, index=False, sheet_name="Summary")
        dist_df.to_excel(w, index=False, sheet_name="AuditType_Dist")
        if not viol_df.empty:
            viol_df.to_excel(w, index=False, sheet_name="Violations")
        else:
            pd.DataFrame([{"NOTES": "No violations found (planned missing plan = 0)."}]).to_excel(
                w, index=False, sheet_name="Violations"
            )


def build_argparser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Verify AUDIT_PLAN and AUDIT_TYPE in ISO raw Excel sheets.")
    p.add_argument("--excel", default="ISO_DATA.xlsx", help="Path to ISO_DATA Excel file (default: ISO_DATA.xlsx)")
    p.add_argument("--out", default="ISO_Audit_Verification_Report_FIXED.xlsx",
                   help="Output report (default: ISO_Audit_Verification_Report_FIXED.xlsx)")
    p.add_argument("--prefix", default="ISO_Check_categor",
                   help="Sheet name prefix to target (default: ISO_Check_categor)")
    p.add_argument("--max_violation_rows", type=int, default=500,
                   help="Max violation rows to include (default: 500)")
    return p


if __name__ == "__main__":
    args = build_argparser().parse_args()
    run(args.excel, args.out, args.prefix, args.max_violation_rows)
    print(f"[OK] Wrote report: {args.out}")
