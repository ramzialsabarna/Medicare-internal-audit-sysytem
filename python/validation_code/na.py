import pandas as pd
from pathlib import Path

# =========================
# CONFIG
# =========================
excel_path = Path("ISO_DATA.xlsx")

visit_result_sheets = ["VISIT_RESULT1", "VISIT_RESULT2", "VISIT_RESULT3", "VISIT_RESULT4", "VISIT_RESULT5"]
branch_profile_sheet = "BRANCH_PROFILE"
scope_rules_sheet = "SCOPE_RULES"

# Structure sheets (only used to map CHECK_ITEM_ID -> ITEM_FEATURE_NAME if available)
structure_sheets = [
    "ISO_Check_category2025",
    "ISO_Check_category1",
    "ISO_Check_category2",
    "ISO_Check_category3",
    "ISO_Check_category4",
    "ISO_Check_category5",
]

out_report = Path("NA_Captured_2025_Report.xlsx")

# Columns in VISIT_RESULT*
COL_BRANCH_ID = "BRANCH_ID"
COL_CATEGORY_ID = "CATEGORY_ID"
COL_CATEGORY_NAME = "CHECK_CATEGORY_NAME"
COL_ITEM_ID = "CHECK_ITEM_ID"
COL_ITEM_NAME = "CHECK_ITEM_NAME"
COL_CHOICE_NAME = "CHOICE_VALUE_OPTION_NAME"
COL_VISIT_DATE = "VISIT_DATE"

# Columns in BRANCH_PROFILE
COL_ISO_ACTIVE = "iso_active"

# Columns in SCOPE_RULES
SR_RULE_ID = "RULE_ID"
SR_CAP_FLAG = "CAPABILITY_FLAG"
SR_TARGET_TYPE = "TARGET_TYPE"
SR_TARGET_CODE = "TARGET_CODE"
SR_ACTION = "ACTION"
SR_NOTES = "NOTES"


# =========================
# HELPERS
# =========================
def normalize_text(series: pd.Series) -> pd.Series:
    """Lowercase + strip for safe comparisons (handles NaN)."""
    return (
        series.fillna("")
              .astype(str)
              .str.strip()
              .str.lower()
    )

def is_na_choice(choice_series: pd.Series) -> pd.Series:
    """NA is any value containing 'not applicable' (case-insensitive)."""
    txt = normalize_text(choice_series)
    return txt.str.contains("not applicable", na=False)

def safe_int(series: pd.Series) -> pd.Series:
    """Convert to pandas nullable Int64 if possible."""
    return pd.to_numeric(series, errors="coerce").astype("Int64")

def ensure_datetime(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce")


# =========================
# LOAD DATA
# =========================
if not excel_path.exists():
    raise FileNotFoundError(f"Excel file not found: {excel_path.resolve()}")

xls = pd.ExcelFile(excel_path)

# Validate sheets exist
missing_vr = [s for s in visit_result_sheets if s not in xls.sheet_names]
if missing_vr:
    raise ValueError(f"Missing VISIT_RESULT sheets: {missing_vr}")

if branch_profile_sheet not in xls.sheet_names:
    raise ValueError(f"Missing sheet: {branch_profile_sheet}")

if scope_rules_sheet not in xls.sheet_names:
    raise ValueError(f"Missing sheet: {scope_rules_sheet}")

# 1) Read visit results
vr_frames = []
usecols_vr = [
    COL_BRANCH_ID, COL_CATEGORY_ID, COL_CATEGORY_NAME, COL_ITEM_ID, COL_ITEM_NAME,
    COL_CHOICE_NAME, "CHOICE_ID", COL_VISIT_DATE
]

for sh in visit_result_sheets:
    df = pd.read_excel(xls, sheet_name=sh, usecols=usecols_vr)
    df["SOURCE_SHEET"] = sh
    vr_frames.append(df)

vr = pd.concat(vr_frames, ignore_index=True)

# Fix types
vr[COL_BRANCH_ID] = safe_int(vr[COL_BRANCH_ID])
vr[COL_CATEGORY_ID] = safe_int(vr[COL_CATEGORY_ID])
vr[COL_ITEM_ID] = safe_int(vr[COL_ITEM_ID])
vr[COL_VISIT_DATE] = ensure_datetime(vr[COL_VISIT_DATE])

# Filter 2025
vr_2025 = vr[vr[COL_VISIT_DATE].dt.year == 2025].copy()

# 2) Read branch profile (iso_active)
bp = pd.read_excel(xls, sheet_name=branch_profile_sheet, usecols=[COL_BRANCH_ID, COL_ISO_ACTIVE]).copy()
bp[COL_BRANCH_ID] = safe_int(bp[COL_BRANCH_ID])
bp[COL_ISO_ACTIVE] = pd.to_numeric(bp[COL_ISO_ACTIVE], errors="coerce").fillna(0).astype(int)

vr_2025 = vr_2025.merge(bp, on=COL_BRANCH_ID, how="left")
vr_2025[COL_ISO_ACTIVE] = vr_2025[COL_ISO_ACTIVE].fillna(0).astype(int)
vr_2025["IS_NON_ISO_BRANCH"] = (vr_2025[COL_ISO_ACTIVE] == 0)

# 3) Read SCOPE_RULES and keep ISO forbid rules
sr = pd.read_excel(
    xls, sheet_name=scope_rules_sheet,
    usecols=[SR_RULE_ID, SR_CAP_FLAG, SR_TARGET_TYPE, SR_TARGET_CODE, SR_ACTION, SR_NOTES]
).copy()

sr[SR_CAP_FLAG] = normalize_text(sr[SR_CAP_FLAG])
sr[SR_TARGET_TYPE] = normalize_text(sr[SR_TARGET_TYPE])
sr[SR_TARGET_CODE] = normalize_text(sr[SR_TARGET_CODE])
sr[SR_ACTION] = normalize_text(sr[SR_ACTION])

sr_iso_forbid = sr[
    (sr[SR_CAP_FLAG] == "iso_active") &
    (sr[SR_ACTION] == "forbid") &
    (sr[SR_TARGET_TYPE].isin(["category", "item"]))
].copy()

forbid_categories = set(sr_iso_forbid.loc[sr_iso_forbid[SR_TARGET_TYPE] == "category", SR_TARGET_CODE].dropna().unique())
forbid_items = set(sr_iso_forbid.loc[sr_iso_forbid[SR_TARGET_TYPE] == "item", SR_TARGET_CODE].dropna().unique())

# 4) Build CHECK_ITEM_ID -> ITEM_FEATURE_NAME mapping (if those columns exist in structure sheets)
item_id_to_feature = None
item_map_frames = []

for sh in structure_sheets:
    if sh not in xls.sheet_names:
        continue
    tmp = pd.read_excel(xls, sheet_name=sh)
    cols = set(map(str, tmp.columns))
    if "CHECK_ITEM_ID" in cols and "ITEM_FEATURE_NAME" in cols:
        m = tmp[["CHECK_ITEM_ID", "ITEM_FEATURE_NAME"]].dropna().copy()
        m["CHECK_ITEM_ID"] = safe_int(m["CHECK_ITEM_ID"])
        m["ITEM_FEATURE_NAME_N"] = normalize_text(m["ITEM_FEATURE_NAME"])
        item_map_frames.append(m[["CHECK_ITEM_ID", "ITEM_FEATURE_NAME_N"]])

if item_map_frames:
    item_id_to_feature = (
        pd.concat(item_map_frames, ignore_index=True)
          .dropna(subset=["CHECK_ITEM_ID", "ITEM_FEATURE_NAME_N"])
          .drop_duplicates(subset=["CHECK_ITEM_ID"])
          .copy()
    )
    vr_2025 = vr_2025.merge(
        item_id_to_feature,
        left_on=COL_ITEM_ID,
        right_on="CHECK_ITEM_ID",
        how="left"
    )
else:
    # Fallback: use CHECK_ITEM_NAME directly (less reliable)
    vr_2025["ITEM_FEATURE_NAME_N"] = normalize_text(vr_2025[COL_ITEM_NAME])

# =========================
# FLAGS
# =========================
vr_2025["IS_NA"] = is_na_choice(vr_2025[COL_CHOICE_NAME])

# Category rule match: CHECK_CATEGORY_NAME == TARGET_CODE (normalized)
vr_2025["CATEGORY_CODE_N"] = normalize_text(vr_2025[COL_CATEGORY_NAME])
vr_2025["IN_FORBID_CATEGORY_RULES"] = vr_2025["CATEGORY_CODE_N"].isin(forbid_categories)

# Item rule match: ITEM_FEATURE_NAME_N == TARGET_CODE (normalized)
vr_2025["ITEM_FEATURE_NAME_N"] = vr_2025["ITEM_FEATURE_NAME_N"].fillna("")
vr_2025["IN_FORBID_ITEM_RULES"] = vr_2025["ITEM_FEATURE_NAME_N"].isin(forbid_items)

# Removed by current rules: only for non-ISO branches
vr_2025["REMOVED_BY_RULES"] = (
    vr_2025["IS_NON_ISO_BRANCH"] &
    (vr_2025["IN_FORBID_CATEGORY_RULES"] | vr_2025["IN_FORBID_ITEM_RULES"])
)

# =========================
# SUMMARY (Two topics)
# =========================
total_rows = int(len(vr_2025))
total_na = int(vr_2025["IS_NA"].sum())

# Topic A: current partial rules
removed_rows_rules = int(vr_2025["REMOVED_BY_RULES"].sum())
scope_reduction_rules_pct = (removed_rows_rules / total_rows * 100) if total_rows else 0.0

na_in_removed_scope = int((vr_2025["IS_NA"] & vr_2025["REMOVED_BY_RULES"]).sum())
na_captured_rules_pct = (na_in_removed_scope / total_na * 100) if total_na else 0.0

# Topic B: all NA (baseline + upper bound)
na_overall_rate_pct = (total_na / total_rows * 100) if total_rows else 0.0
na_non_iso = int((vr_2025["IS_NA"] & vr_2025["IS_NON_ISO_BRANCH"]).sum())
na_non_iso_share_of_all_na_pct = (na_non_iso / total_na * 100) if total_na else 0.0

summary = pd.DataFrame([{
    "Year": 2025,
    "TotalRows": total_rows,
    "TotalNA": total_na,
    "NA_Overall_Rate_%": round(na_overall_rate_pct, 4),

    "NA_in_NonISO_Branches": na_non_iso,
    "NA_NonISO_Share_of_All_NA_%": round(na_non_iso_share_of_all_na_pct, 4),

    "Rules_Partial_RemovedRows": removed_rows_rules,
    "Rules_Partial_ScopeReduction_%": round(scope_reduction_rules_pct, 4),

    "Rules_Partial_NA_in_RemovedScope": na_in_removed_scope,
    "Rules_Partial_NA_Captured_%": round(na_captured_rules_pct, 4),
}])

# =========================
# BY-CATEGORY BREAKDOWN (clean + stable, no % in agg names)
# =========================
# Base aggregation
by_cat = (
    vr_2025
    .groupby("CATEGORY_CODE_N", dropna=False)
    .agg({
        "CATEGORY_CODE_N": "size",
        "IS_NA": "sum",
        "REMOVED_BY_RULES": "sum"
    })
    .rename(columns={
        "CATEGORY_CODE_N": "TotalRows",
        "IS_NA": "NA_Count",
        "REMOVED_BY_RULES": "RemovedRows_Rules"
    })
    .reset_index()
    .rename(columns={"CATEGORY_CODE_N": "CategoryCode"})
)

# Rates
by_cat["NA_Rate_%"] = (by_cat["NA_Count"] / by_cat["TotalRows"] * 100).round(4)
by_cat["RemovedRate_Rules_%"] = (by_cat["RemovedRows_Rules"] / by_cat["TotalRows"] * 100).round(4)

# Non-ISO NA count per category
non_iso_na = (
    vr_2025[vr_2025["IS_NON_ISO_BRANCH"] & vr_2025["IS_NA"]]
    .groupby("CATEGORY_CODE_N")
    .size()
    .rename("NonISO_NA_Count")
    .reset_index()
    .rename(columns={"CATEGORY_CODE_N": "CategoryCode"})
)

# NA captured by rules per category
na_in_removed = (
    vr_2025[vr_2025["IS_NA"] & vr_2025["REMOVED_BY_RULES"]]
    .groupby("CATEGORY_CODE_N")
    .size()
    .rename("NA_in_RemovedScope_Rules")
    .reset_index()
    .rename(columns={"CATEGORY_CODE_N": "CategoryCode"})
)

by_cat = (
    by_cat
    .merge(non_iso_na, on="CategoryCode", how="left")
    .merge(na_in_removed, on="CategoryCode", how="left")
    .fillna(0)
)

# Ensure ints for counts
for c in ["NA_Count", "RemovedRows_Rules", "NonISO_NA_Count", "NA_in_RemovedScope_Rules"]:
    by_cat[c] = by_cat[c].astype(int)

# Sort: highest NA first
by_cat = by_cat.sort_values(["NA_Count", "TotalRows"], ascending=False)

# =========================
# EXPORT
# =========================
rules_used = sr_iso_forbid[[SR_RULE_ID, SR_CAP_FLAG, SR_TARGET_TYPE, SR_TARGET_CODE, SR_ACTION, SR_NOTES]].copy()

with pd.ExcelWriter(out_report, engine="openpyxl") as writer:
    summary.to_excel(writer, sheet_name="Summary", index=False)
    by_cat.to_excel(writer, sheet_name="ByCategory", index=False)
    rules_used.to_excel(writer, sheet_name="RulesUsed_ISO_forbid", index=False)

    # Optional: sample rows to validate quickly
    audit_cols = [
        "SOURCE_SHEET", COL_VISIT_DATE, COL_BRANCH_ID, COL_ISO_ACTIVE,
        COL_CATEGORY_NAME, COL_CATEGORY_ID, COL_ITEM_ID, COL_ITEM_NAME,
        COL_CHOICE_NAME, "IS_NA", "REMOVED_BY_RULES",
        "IN_FORBID_CATEGORY_RULES", "IN_FORBID_ITEM_RULES"
    ]
    audit_cols = [c for c in audit_cols if c in vr_2025.columns]
    vr_2025[audit_cols].head(5000).to_excel(writer, sheet_name="SampleRows_Head5000", index=False)

print(f"✅ Report generated: {out_report.resolve()}")
print(summary.to_string(index=False))
