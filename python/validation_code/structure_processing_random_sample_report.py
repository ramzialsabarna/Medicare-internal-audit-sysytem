#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# ------------------------------------------------------------
# # Purpose (Manual Verification Sampling Report - Stratified)
# ------------------------------------------------------------
# - Reads ISO_DATA.xlsx and targets all structure sheets whose names start with a prefix
#   (default: ISO_Check_categor), e.g., ISO_Check_category1..5 and ISO_Check_category2025.
# - For each transformation group (Category / Item / Answer / Branch / AuditType / AuditPlan):
#     * Select original + derived columns
#     * Perform STRATIFIED sampling across ALL sheets (not only 2025)
#       so each sheet contributes to the sample.
# - Sampling strategy:
#     * Use a stable key column if present (e.g., *_CODE / *_FEATURE_NAME)
#     * Deduplicate within each sheet on that key (so we sample distinct values per sheet)
#     * Take a quota per sheet (equal split by default) then combine
# - Adds manual verification columns:
#     * VERIFICATION_STATUS  (fill with: Done / Verified)
#     * VERIFICATION_NOTES   (optional notes)
# - Output: one Excel file with multiple sheets, one per group, plus Summary.
# ------------------------------------------------------------

from __future__ import annotations
import argparse
from pathlib import Path
from typing import List, Dict, Optional, Tuple

import pandas as pd


# -----------------------------
# Config: Column groups
# -----------------------------
GROUPS: Dict[str, Dict[str, List[str]]] = {
    "Categories": {
        "columns": [
            "CHECK_CATEGORY_NAME",
            "CATEGORY_NAME_CLEAN",
            "CATEGORY_NAME_HARMONIZED",
            "CATEGORY_CODE",
        ],
        "sample_key_candidates": ["CATEGORY_CODE", "CATEGORY_NAME_HARMONIZED", "CATEGORY_NAME_CLEAN", "CHECK_CATEGORY_NAME"],
    },
    "Items": {
        "columns": [
            "CHECK_ITEM_NAME",
            "ITEM_NAME_CLEAN",
            "ITEM_TEXT_CODE",
            "ITEM_KEY",
            "ITEM_FEATURE_NAME",
        ],
        "sample_key_candidates": ["ITEM_FEATURE_NAME", "ITEM_KEY", "ITEM_TEXT_CODE", "ITEM_NAME_CLEAN", "CHECK_ITEM_NAME"],
    },
    "Answers": {
        "columns": [
            "CHOICE_VALUE_OPTION_NAME",
            "ANSWER_TEXT_CLEAN",
            "ANSWER_CODE",
            "ANSWER_FEATURE_NAME",
        ],
        "sample_key_candidates": ["ANSWER_FEATURE_NAME", "ANSWER_CODE", "ANSWER_TEXT_CLEAN", "CHOICE_VALUE_OPTION_NAME"],
    },
    "Branches": {
        "columns": [
            "BRANCH_NAME",
            "BRANCH_LABEL_CLEAN",
            "BRANCH_LABEL_SLUG",
            "BRANCH_ID_TOKEN",
            "BRANCH_FEATURE_CODE",
            "ENTITY_TYPE",
        ],
        "sample_key_candidates": ["BRANCH_FEATURE_CODE", "BRANCH_ID_TOKEN", "BRANCH_LABEL_SLUG", "BRANCH_LABEL_CLEAN", "BRANCH_NAME"],
    },
    "AuditType": {
        "columns": [
            "AUDIT_TYPE",
            "AUDIT_TYPE_CLEAN",
            "AUDIT_TYPE_CODE",
        ],
        "sample_key_candidates": ["AUDIT_TYPE_CODE", "AUDIT_TYPE_CLEAN", "AUDIT_TYPE"],
    },
    "AuditPlan": {
        "columns": [
            "AUDIT_PLAN",
            "AUDIT_PLAN_CLEAN",
            "AUDIT_PLAN_CODE",
        ],
        "sample_key_candidates": ["AUDIT_PLAN_CODE", "AUDIT_PLAN_CLEAN", "AUDIT_PLAN"],
    },
}

META_COLS = ["SHEET", "ROW_INDEX"]


# -----------------------------
# Helpers
# -----------------------------
def _ensure_excel_exists(excel_arg: str) -> Path:
    p = Path(excel_arg)
    if p.exists():
        return p

    fallback = Path("ISO_DATA.xlsx")
    if fallback.exists():
        print(f"⚠ File not found: {excel_arg} | Using fallback: {fallback.name}")
        return fallback

    available = sorted([f.name for f in Path(".").glob("*.xlsx")])
    raise FileNotFoundError(
        f"Excel file not found: {excel_arg}\n"
        f"Working dir: {Path('.').resolve()}\n"
        f"Available xlsx files: {available}"
    )

def _safe_sheet_name(name: str) -> str:
    name = str(name).strip()
    return name[:31] if len(name) > 31 else name

def _is_missing_like(x) -> bool:
    if x is None:
        return True
    try:
        if pd.isna(x):
            return True
    except Exception:
        pass
    if isinstance(x, str):
        return str(x).strip() == ""
    return False

def _pick_sample_key(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    for c in candidates:
        if c in df.columns:
            return c
    return None

def _sample_distinct_within_sheet(df: pd.DataFrame, key_col: Optional[str], k: int, seed: int) -> pd.DataFrame:
    """
    Sample up to k rows from ONE sheet.
    - If key_col exists: drop duplicates by key_col within this sheet, sample distinct values.
    - Else: random sample rows.
    """
    if df.empty or k <= 0:
        return df.iloc[0:0]

    work = df.copy()

    if key_col and key_col in work.columns:
        work = work[~work[key_col].map(_is_missing_like)].copy()
        if work.empty:
            return work
        work = work.drop_duplicates(subset=[key_col], keep="first")
        k_take = min(k, len(work))
        return work.sample(n=k_take, random_state=seed) if k_take > 0 else work.iloc[0:0]

    # fallback: random rows
    k_take = min(k, len(work))
    return work.sample(n=k_take, random_state=seed) if k_take > 0 else work.iloc[0:0]


# -----------------------------
# Main logic
# -----------------------------
def load_structure_sheets(excel_path: Path, prefix: str) -> Tuple[pd.DataFrame, List[str]]:
    xls = pd.ExcelFile(excel_path, engine="openpyxl")
    sheets = [s for s in xls.sheet_names if str(s).startswith(prefix)]
    if not sheets:
        raise ValueError(f"No sheets found with prefix '{prefix}'. Existing: {xls.sheet_names}")

    frames = []
    for s in sheets:
        df = pd.read_excel(excel_path, sheet_name=s, dtype=object, engine="openpyxl")
        df = df.copy()
        df.insert(0, "ROW_INDEX", df.index.astype(int))
        df.insert(0, "SHEET", s)
        frames.append(df)

    combined = pd.concat(frames, ignore_index=True)
    return combined, sheets


def build_group_sheet_stratified(df_all: pd.DataFrame, sheets: List[str], group_name: str, n_total: int, seed: int) -> pd.DataFrame:
    cfg = GROUPS[group_name]
    cols = cfg["columns"]
    key_candidates = cfg["sample_key_candidates"]

    # Ensure group columns exist in df_all (if not, create empty)
    work = df_all.copy()
    for c in cols:
        if c not in work.columns:
            work[c] = pd.NA

    out_cols = META_COLS + cols
    work = work[out_cols].copy()

    # Choose a sample key (global decision)
    key_col = _pick_sample_key(work, key_candidates)

    # Equal quota per sheet (ensures representation)
    m = max(1, len(sheets))
    base = n_total // m
    remainder = n_total % m

    parts = []
    for i, sh in enumerate(sheets):
        quota = base + (1 if i < remainder else 0)
        df_sheet = work[work["SHEET"] == sh].copy()

        # Use different seed per sheet but still reproducible
        sheet_seed = seed + (i * 10007)

        part = _sample_distinct_within_sheet(df_sheet, key_col=key_col, k=quota, seed=sheet_seed)
        parts.append(part)

    sample = pd.concat(parts, ignore_index=True) if parts else work.iloc[0:0]

    # If some sheets had no available rows, we may get less than n_total; that's okay.
    # Add manual verification columns
    sample["VERIFICATION_STATUS"] = ""   # Done / Verified
    sample["VERIFICATION_NOTES"] = ""    # optional

    sample = sample.sort_values(["SHEET", "ROW_INDEX"], ascending=[True, True]).reset_index(drop=True)
    return sample


def run(excel_path: str, out_path: str, prefix: str, n: int, seed: int) -> None:
    excel_file = _ensure_excel_exists(excel_path)
    df_all, sheets = load_structure_sheets(excel_file, prefix=prefix)

    summary_rows = [{
        "EXCEL_FILE": excel_file.name,
        "PREFIX": prefix,
        "STRUCTURE_SHEETS_SCANNED": len(sheets),
        "SHEETS": ", ".join(sheets),
        "TOTAL_ROWS_COMBINED": int(len(df_all)),
        "SAMPLE_SIZE_TOTAL_PER_GROUP": int(n),
        "RANDOM_SEED": int(seed),
        "SAMPLING_MODE": "STRATIFIED_PER_SHEET (equal quota)",
        "NOTES": "Each group sheet samples from ALL structure sheets to support accurate manual verification.",
    }]
    df_summary = pd.DataFrame(summary_rows)

    outputs: Dict[str, pd.DataFrame] = {"Summary": df_summary}
    for group_name in GROUPS.keys():
        outputs[group_name] = build_group_sheet_stratified(df_all, sheets, group_name, n_total=n, seed=seed)

    out = Path(out_path)
    with pd.ExcelWriter(out, engine="openpyxl") as writer:
        for name, df in outputs.items():
            df.to_excel(writer, index=False, sheet_name=_safe_sheet_name(name))

    print(f"✅ Report written to: {out.resolve()}")
    print(f"📌 Sheets scanned: {sheets}")


def build_argparser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Stratified random sampling report for manual verification of processed structure columns.")
    p.add_argument("--excel", default="ISO_DATA.xlsx", help="Path to ISO_DATA.xlsx (default: ISO_DATA.xlsx)")
    p.add_argument("--out", default="Structure_Processing_RandomSample_Report.xlsx",
                   help="Output Excel report (default: Structure_Processing_RandomSample_Report.xlsx)")
    p.add_argument("--prefix", default="ISO_Check_categor",
                   help="Structure sheet prefix to scan (default: ISO_Check_categor)")
    p.add_argument("--n", type=int, default=60,
                   help="TOTAL sample size per group across ALL sheets (default: 60). "
                        "If 6 sheets => ~10 per sheet.")
    p.add_argument("--seed", type=int, default=2025, help="Random seed (default: 2025)")
    return p


if __name__ == "__main__":
    args = build_argparser().parse_args()
    run(args.excel, args.out, args.prefix, args.n, args.seed)
