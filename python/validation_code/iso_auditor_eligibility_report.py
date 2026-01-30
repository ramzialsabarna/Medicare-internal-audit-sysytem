#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
iso_auditor_eligibility_report.py
---------------------------------
Generates an Excel report that answers:

- For each CATEGORY_CODE: what auditor capability flags are REQUIRED (from SCOPE_RULES)?
- How many eligible auditors exist in users sheet (is_auditor=1 and required flags=1)?
- Warn when 0 eligible auditors exist.

Designed to work with ISO_DATA.xlsx that contains:
- Sheet: users  (columns: ID, FULL_NAME_EN, is_auditor, iso_auditor, micro_auditor, path_auditor, senior_auditor, ...)
- Sheet: SCOPE_RULES (columns: CAPABILITY_FLAG, TARGET_TYPE, TARGET_CODE, ACTION, ...)
"""

from __future__ import annotations
import argparse
from typing import List, Dict, Set
import pandas as pd


AUDITOR_CAP_FLAGS = ["iso_auditor", "micro_auditor", "path_auditor", "senior_auditor"]


def _norm_str(x) -> str:
    if x is None or pd.isna(x):
        return ""
    return str(x).strip()


def _norm_lower(x) -> str:
    return _norm_str(x).lower()


def _to01(series: pd.Series) -> pd.Series:
    # coerce values to 0/1; invalid -> 0 (reporting script should not crash your pipeline)
    s = pd.to_numeric(series, errors="coerce").fillna(0).astype(int)
    return (s != 0).astype(int)


def load_users(excel_path: str, sheet: str) -> pd.DataFrame:
    df = pd.read_excel(excel_path, sheet_name=sheet)
    df.columns = [str(c).strip() for c in df.columns]

    required = {"ID", "FULL_NAME_EN", "is_auditor", *AUDITOR_CAP_FLAGS}
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns in '{sheet}': {missing}")

    df = df.copy()

    # normalize flags
    df["is_auditor"] = _to01(df["is_auditor"])
    for c in AUDITOR_CAP_FLAGS:
        df[c] = _to01(df[c])

    # safety: non-auditors cannot have capabilities
    for c in AUDITOR_CAP_FLAGS:
        df.loc[df["is_auditor"] == 0, c] = 0

    df["ID"] = df["ID"].apply(_norm_str)
    df["FULL_NAME_EN"] = df["FULL_NAME_EN"].apply(_norm_str)

    return df


def load_scope_rules(excel_path: str, sheet: str) -> pd.DataFrame:
    df = pd.read_excel(excel_path, sheet_name=sheet)
    df.columns = [str(c).strip() for c in df.columns]

    required = {"CAPABILITY_FLAG", "TARGET_TYPE", "TARGET_CODE", "ACTION"}
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns in '{sheet}': {missing}")

    df = df.copy()
    df["CAPABILITY_FLAG"] = df["CAPABILITY_FLAG"].apply(_norm_lower)
    df["TARGET_TYPE"] = df["TARGET_TYPE"].apply(_norm_lower)
    df["TARGET_CODE"] = df["TARGET_CODE"].apply(_norm_str)
    df["ACTION"] = df["ACTION"].apply(_norm_lower)

    return df


def build_required_caps_by_category(df_rules: pd.DataFrame) -> Dict[str, Set[str]]:
    """
    Use ONLY rules:
      TARGET_TYPE = 'category'
      ACTION = 'require'
      CAPABILITY_FLAG in auditor capability flags
    """
    df = df_rules[
        (df_rules["TARGET_TYPE"] == "category")
        & (df_rules["ACTION"] == "require")
        & (df_rules["CAPABILITY_FLAG"].isin(AUDITOR_CAP_FLAGS))
    ].copy()

    req: Dict[str, Set[str]] = {}
    for _, r in df.iterrows():
        cat = r["TARGET_CODE"]
        cap = r["CAPABILITY_FLAG"]
        if not cat or not cap:
            continue
        req.setdefault(cat, set()).add(cap)
    return req


def eligible_auditors(users: pd.DataFrame, required_caps: Set[str]) -> pd.DataFrame:
    aud = users[users["is_auditor"] == 1].copy()
    for cap in sorted(required_caps):
        if cap not in aud.columns:
            # if a required cap column doesn't exist, then nobody is eligible
            return aud.iloc[0:0]
        aud = aud[aud[cap] == 1]
    return aud


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--excel", required=True, help="Path to ISO_DATA.xlsx (or any workbook containing users + SCOPE_RULES)")
    ap.add_argument("--out", required=True, help="Output Excel report path")
    ap.add_argument("--users-sheet", default="users", help="Sheet name for users")
    ap.add_argument("--rules-sheet", default="SCOPE_RULES", help="Sheet name for scope rules")
    ap.add_argument("--max-names", type=int, default=25, help="Max eligible auditor names to list per category")
    args = ap.parse_args()

    users = load_users(args.excel, args.users_sheet)
    rules = load_scope_rules(args.excel, args.rules_sheet)

    req_map = build_required_caps_by_category(rules)

    # If you want to report also categories that do not have requirements,
    # you can extend this later by scanning structure sheets. Here we report what rules declare.
    rows = []
    for cat, caps in sorted(req_map.items(), key=lambda x: x[0]):
        eligible = eligible_auditors(users, caps)
        count = int(len(eligible))
        names = eligible["FULL_NAME_EN"].dropna().astype(str).tolist()[: args.max_names]
        ids = eligible["ID"].dropna().astype(str).tolist()[: args.max_names]

        status = "OK" if count > 0 else "NO_ELIGIBLE_AUDITOR"

        rows.append(
            {
                "CATEGORY_CODE": cat,
                "REQUIRED_CAPABILITIES": ", ".join(sorted(caps)),
                "ELIGIBLE_AUDITOR_COUNT": count,
                "ELIGIBLE_AUDITOR_IDS_SAMPLE": ", ".join(ids),
                "ELIGIBLE_AUDITOR_NAMES_SAMPLE": ", ".join(names),
                "STATUS": status,
            }
        )

    df_report = pd.DataFrame(rows)

    # Summary sheet: counts of auditors by capability
    auditors_only = users[users["is_auditor"] == 1].copy()
    summary = {
        "TOTAL_USERS": [len(users)],
        "TOTAL_AUDITORS": [len(auditors_only)],
    }
    for cap in AUDITOR_CAP_FLAGS:
        summary[f"AUDITORS_WITH_{cap.upper()}"] = [int(auditors_only[cap].sum())]

    df_summary = pd.DataFrame(summary)

    # Write output
    with pd.ExcelWriter(args.out, engine="openpyxl") as w:
        df_report.to_excel(w, index=False, sheet_name="Category_Eligibility")
        df_summary.to_excel(w, index=False, sheet_name="Summary")

        # Optional: include the extracted requirement rules for traceability
        trace = rules[
            (rules["TARGET_TYPE"] == "category")
            & (rules["ACTION"] == "require")
            & (rules["CAPABILITY_FLAG"].isin(AUDITOR_CAP_FLAGS))
        ].copy()
        if not trace.empty:
            trace.to_excel(w, index=False, sheet_name="Rules_Trace")

    print(f"✅ Report written to: {args.out}")


if __name__ == "__main__":
    main()
