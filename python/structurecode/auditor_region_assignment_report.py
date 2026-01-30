#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations
import argparse
from pathlib import Path
from typing import Dict, Set
import pandas as pd

AUDITOR_CAP_FLAGS = ["iso_auditor", "micro_auditor", "path_auditor", "senior_auditor"]

REGION_FLAGS = {
    "NorthRegion": "region_north",
    "CentralRegion": "region_central",
    "SouthRegion": "region_south",
}

# ---------------------------
# Helpers
# ---------------------------
def _norm_str(x) -> str:
    if x is None or pd.isna(x):
        return ""
    return str(x).strip()

def _norm_lower(x) -> str:
    return _norm_str(x).lower()

def _to01(series: pd.Series) -> pd.Series:
    s = pd.to_numeric(series, errors="coerce").fillna(0).astype(int)
    return (s != 0).astype(int)

def _norm_region(x: str) -> str:
    v = _norm_str(x)
    if v in REGION_FLAGS:
        return v
    vl = v.lower().replace(" ", "")
    if "north" in vl or "شمال" in vl:
        return "NorthRegion"
    if "central" in vl or "center" in vl or "وسط" in vl:
        return "CentralRegion"
    if "south" in vl or "جنوب" in vl:
        return "SouthRegion"
    return ""

def _find_sheet(xls: pd.ExcelFile, desired: str) -> str:
    """Find sheet name case-insensitively; if not found, raise with available sheets."""
    desired_l = desired.strip().lower()
    for s in xls.sheet_names:
        if s.strip().lower() == desired_l:
            return s
    raise ValueError(f"لم أجد شيت '{desired}'. الشيتات الموجودة: {xls.sheet_names}")

def _resolve_excel_path(excel_arg: str) -> Path:
    """
    Robust file resolver:
    - If excel_arg exists => use it.
    - Else if ISO_DATA.xlsx exists in current dir => use it.
    - Else if exactly one .xlsx exists => use it.
    - Else raise with list of xlsx files.
    """
    p = Path(excel_arg)
    if p.exists():
        return p

    fallback = Path("ISO_DATA.xlsx")
    if fallback.exists():
        print(f"⚠ الملف '{excel_arg}' غير موجود. سيتم استخدام الملف الافتراضي: {fallback.name}")
        return fallback

    xlsx_files = list(Path(".").glob("*.xlsx"))
    if len(xlsx_files) == 1:
        print(f"⚠ الملف '{excel_arg}' غير موجود. تم العثور على ملف Excel واحد فقط وسيتم استخدامه: {xlsx_files[0].name}")
        return xlsx_files[0]

    raise FileNotFoundError(
        f"Excel file not found: {excel_arg}\n"
        f"Available .xlsx files here: {[f.name for f in xlsx_files]}"
    )

def _pick_first_existing_col(df: pd.DataFrame, candidates: list[str]) -> str:
    cols_lower = {c.strip().lower(): c for c in df.columns}
    for cand in candidates:
        key = cand.strip().lower()
        if key in cols_lower:
            return cols_lower[key]
    return ""


# ---------------------------
# Loaders (robust)
# ---------------------------
def load_users(excel_path: Path, sheet: str) -> pd.DataFrame:
    xls = pd.ExcelFile(excel_path)
    sheet_real = _find_sheet(xls, sheet)
    df = pd.read_excel(excel_path, sheet_name=sheet_real)
    df.columns = [str(c).strip() for c in df.columns]

    # robust column mapping
    col_id = _pick_first_existing_col(df, ["ID", "USER_ID"])
    col_name = _pick_first_existing_col(df, ["FULL_NAME_EN", "USER_NAME", "NAME"])
    col_is_aud = _pick_first_existing_col(df, ["is_auditor", "IS_AUDITOR"])

    if not col_id or not col_name or not col_is_aud:
        raise ValueError(
            f"Sheet '{sheet_real}' must contain ID/USER_ID, FULL_NAME_EN/USER_NAME/NAME, is_auditor.\n"
            f"Columns found: {list(df.columns)}"
        )

    # ensure required capability columns exist
    missing_caps = [c for c in AUDITOR_CAP_FLAGS if c not in df.columns]
    if missing_caps:
        raise ValueError(f"Missing capability columns in users sheet: {missing_caps}")

    # ensure region columns exist
    missing_regions = [c for c in REGION_FLAGS.values() if c not in df.columns]
    if missing_regions:
        raise ValueError(
            f"Missing region flag columns in users sheet: {missing_regions}\n"
            f"Expected: {list(REGION_FLAGS.values())}"
        )

    df = df.copy()
    df.rename(columns={col_id: "ID", col_name: "FULL_NAME_EN", col_is_aud: "is_auditor"}, inplace=True)

    df["is_auditor"] = _to01(df["is_auditor"])
    for c in AUDITOR_CAP_FLAGS:
        df[c] = _to01(df[c])
    for rf in REGION_FLAGS.values():
        df[rf] = _to01(df[rf])

    # safety: non-auditors cannot have capabilities/regions
    for c in AUDITOR_CAP_FLAGS:
        df.loc[df["is_auditor"] == 0, c] = 0
    for rf in REGION_FLAGS.values():
        df.loc[df["is_auditor"] == 0, rf] = 0

    df["ID"] = df["ID"].apply(_norm_str)
    df["FULL_NAME_EN"] = df["FULL_NAME_EN"].apply(_norm_str)
    return df


def load_branch_profile(excel_path: Path, sheet: str) -> pd.DataFrame:
    xls = pd.ExcelFile(excel_path)
    sheet_real = _find_sheet(xls, sheet)
    df = pd.read_excel(excel_path, sheet_name=sheet_real)
    df.columns = [str(c).strip() for c in df.columns]

    col_branch_id = _pick_first_existing_col(df, ["BRANCH_ID"])
    col_branch_name = _pick_first_existing_col(df, ["BRANCH_NAME", "NAME"])
    col_region = _pick_first_existing_col(df, ["branch_region", "BRANCH_REGION"])

    if not col_branch_id or not col_branch_name or not col_region:
        raise ValueError(
            f"Sheet '{sheet_real}' must contain BRANCH_ID, BRANCH_NAME, branch_region.\n"
            f"Columns found: {list(df.columns)}"
        )

    df = df.copy()
    df.rename(columns={col_branch_id: "BRANCH_ID", col_branch_name: "BRANCH_NAME", col_region: "branch_region"}, inplace=True)

    df["BRANCH_ID"] = pd.to_numeric(df["BRANCH_ID"], errors="coerce").astype("Int64")
    df["BRANCH_NAME"] = df["BRANCH_NAME"].apply(_norm_str)
    df["branch_region"] = df["branch_region"].apply(_norm_region)

    # normalize common capability flags if present
    for cap in ["iso_active", "micro_active", "path_active"]:
        if cap in df.columns:
            df[cap] = _to01(df[cap])

    return df


def load_scope_rules(excel_path: Path, sheet: str) -> pd.DataFrame:
    xls = pd.ExcelFile(excel_path)
    sheet_real = _find_sheet(xls, sheet)
    df = pd.read_excel(excel_path, sheet_name=sheet_real)
    df.columns = [str(c).strip() for c in df.columns]

    required = {"CAPABILITY_FLAG", "TARGET_TYPE", "TARGET_CODE", "ACTION"}
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns in '{sheet_real}': {missing}")

    df = df.copy()
    df["CAPABILITY_FLAG"] = df["CAPABILITY_FLAG"].apply(_norm_lower)
    df["TARGET_TYPE"] = df["TARGET_TYPE"].apply(_norm_lower)
    df["TARGET_CODE"] = df["TARGET_CODE"].apply(_norm_str)
    df["ACTION"] = df["ACTION"].apply(_norm_lower)
    return df


# ---------------------------
# Rules to mappings
# ---------------------------
def build_required_caps_by_category(df_rules: pd.DataFrame) -> Dict[str, Set[str]]:
    df = df_rules[
        (df_rules["TARGET_TYPE"] == "category")
        & (df_rules["ACTION"] == "require")
        & (df_rules["CAPABILITY_FLAG"].isin(AUDITOR_CAP_FLAGS))
    ].copy()

    req: Dict[str, Set[str]] = {}
    for _, r in df.iterrows():
        cat = _norm_str(r["TARGET_CODE"])
        cap = _norm_lower(r["CAPABILITY_FLAG"])
        if cat and cap:
            req.setdefault(cat, set()).add(cap)
    return req


def build_forbidden_categories_by_branchcap(df_rules: pd.DataFrame) -> Dict[str, Set[str]]:
    df = df_rules[
        (df_rules["TARGET_TYPE"] == "category")
        & (df_rules["ACTION"] == "forbid")
    ].copy()

    m: Dict[str, Set[str]] = {}
    for _, r in df.iterrows():
        cap_flag = _norm_lower(r["CAPABILITY_FLAG"])
        cat = _norm_str(r["TARGET_CODE"])
        if cap_flag and cat:
            m.setdefault(cap_flag, set()).add(cat)
    return m


# ---------------------------
# Eligibility
# ---------------------------
def eligible_auditors(users: pd.DataFrame, required_caps: Set[str]) -> pd.DataFrame:
    aud = users[users["is_auditor"] == 1].copy()
    for cap in sorted(required_caps):
        if cap not in aud.columns:
            return aud.iloc[0:0]
        aud = aud[aud[cap] == 1]
    return aud

def eligible_auditors_in_region(users: pd.DataFrame, required_caps: Set[str], region: str) -> pd.DataFrame:
    aud = eligible_auditors(users, required_caps)
    if not region:
        return aud
    rf = REGION_FLAGS.get(region, "")
    if not rf:
        return aud
    return aud[aud[rf] == 1]

def category_applicable_for_branch(cat: str, branch_row: pd.Series, forbid_map: Dict[str, Set[str]]) -> bool:
    for cap_flag, forbidden_cats in forbid_map.items():
        if cat in forbidden_cats:
            if cap_flag in branch_row.index:
                if int(branch_row[cap_flag]) == 0:
                    return False
            else:
                return False
    return True


# ---------------------------
# Main
# ---------------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--excel", default="ISO_DATA.xlsx", help="Path to ISO_DATA.xlsx (default: ISO_DATA.xlsx)")
    ap.add_argument("--out", default="Auditor_Region_Assignment_Report.xlsx", help="Output report path")
    ap.add_argument("--users-sheet", default="users")
    ap.add_argument("--rules-sheet", default="SCOPE_RULES")
    ap.add_argument("--branch-sheet", default="BRANCH_PROFILE")
    ap.add_argument("--max-names", type=int, default=20)
    args = ap.parse_args()

    excel_path = _resolve_excel_path(args.excel)

    # Debug info (helps you immediately)
    try:
        xls = pd.ExcelFile(excel_path)
        print(f"✅ Using Excel: {excel_path.resolve()}")
        print(f"📄 Sheets found: {xls.sheet_names}")
    except Exception as e:
        raise RuntimeError(f"Failed to open Excel file: {excel_path}\nError: {e}")

    users = load_users(excel_path, args.users_sheet)
    rules = load_scope_rules(excel_path, args.rules_sheet)
    branches = load_branch_profile(excel_path, args.branch_sheet)

    req_map = build_required_caps_by_category(rules)
    forbid_map = build_forbidden_categories_by_branchcap(rules)

    # Sheet 1: category eligibility by region
    rows_cat = []
    for cat, caps in sorted(req_map.items(), key=lambda x: x[0]):
        base_eligible = eligible_auditors(users, caps)
        row = {
            "CATEGORY_CODE": cat,
            "REQUIRED_CAPABILITIES": ", ".join(sorted(caps)),
            "ELIGIBLE_TOTAL": int(len(base_eligible)),
        }
        for region, rf in REGION_FLAGS.items():
            elig_r = eligible_auditors_in_region(users, caps, region)
            row[f"ELIGIBLE_{region}_COUNT"] = int(len(elig_r))
            row[f"ELIGIBLE_{region}_NAMES_SAMPLE"] = ", ".join(
                elig_r["FULL_NAME_EN"].dropna().astype(str).tolist()[: args.max_names]
            )
        rows_cat.append(row)
    df_cat_region = pd.DataFrame(rows_cat)

    # Sheet 2: branch + category readiness
    rows_branch = []
    for _, b in branches.iterrows():
        branch_id = b["BRANCH_ID"]
        branch_name = b["BRANCH_NAME"]
        branch_region = _norm_region(b.get("branch_region", ""))

        for cat, caps in sorted(req_map.items(), key=lambda x: x[0]):
            applicable = category_applicable_for_branch(cat, b, forbid_map)
            if not applicable:
                rows_branch.append({
                    "BRANCH_ID": branch_id,
                    "BRANCH_NAME": branch_name,
                    "BRANCH_REGION": branch_region,
                    "CATEGORY_CODE": cat,
                    "REQUIRED_CAPABILITIES": ", ".join(sorted(caps)),
                    "SAME_REGION_ELIGIBLE_COUNT": 0,
                    "BACKUP_ELIGIBLE_COUNT": 0,
                    "STATUS": "NOT_APPLICABLE",
                    "SAME_REGION_NAMES_SAMPLE": "",
                    "BACKUP_NAMES_SAMPLE": "",
                })
                continue

            eligible_total = eligible_auditors(users, caps)
            elig_same = eligible_auditors_in_region(users, caps, branch_region)

            if branch_region and branch_region in REGION_FLAGS:
                rf_same = REGION_FLAGS[branch_region]
                elig_backup = eligible_total[eligible_total[rf_same] == 0]
            else:
                elig_backup = eligible_total

            same_count = int(len(elig_same))
            backup_count = int(len(elig_backup))
            total_count = int(len(eligible_total))

            if total_count == 0:
                status = "NO_ELIGIBLE_ANYWHERE"
            elif same_count == 0 and backup_count > 0:
                status = "NEEDS_BACKUP"
            else:
                status = "OK"

            rows_branch.append({
                "BRANCH_ID": branch_id,
                "BRANCH_NAME": branch_name,
                "BRANCH_REGION": branch_region,
                "CATEGORY_CODE": cat,
                "REQUIRED_CAPABILITIES": ", ".join(sorted(caps)),
                "SAME_REGION_ELIGIBLE_COUNT": same_count,
                "BACKUP_ELIGIBLE_COUNT": backup_count,
                "STATUS": status,
                "SAME_REGION_NAMES_SAMPLE": ", ".join(
                    elig_same["FULL_NAME_EN"].dropna().astype(str).tolist()[: args.max_names]
                ),
                "BACKUP_NAMES_SAMPLE": ", ".join(
                    elig_backup["FULL_NAME_EN"].dropna().astype(str).tolist()[: args.max_names]
                ),
            })
    df_branch_assign = pd.DataFrame(rows_branch)

    # Sheet 3: Summary
    aud_only = users[users["is_auditor"] == 1].copy()
    summary = {
        "TOTAL_USERS": [len(users)],
        "TOTAL_AUDITORS": [len(aud_only)],
    }
    for cap in AUDITOR_CAP_FLAGS:
        summary[f"AUDITORS_WITH_{cap.upper()}"] = [int(aud_only[cap].sum())]
    for region, rf in REGION_FLAGS.items():
        summary[f"AUDITORS_COVERING_{region.upper()}"] = [int(aud_only[rf].sum())]
    df_summary = pd.DataFrame(summary)

    out_path = Path(args.out)
    with pd.ExcelWriter(out_path, engine="openpyxl") as w:
        df_cat_region.to_excel(w, index=False, sheet_name="Category_Eligibility_ByRegion")
        df_branch_assign.to_excel(w, index=False, sheet_name="Branch_Category_Assignment")
        df_summary.to_excel(w, index=False, sheet_name="Summary")

    print(f"✅ Report written to: {out_path.resolve()}")


if __name__ == "__main__":
    main()
