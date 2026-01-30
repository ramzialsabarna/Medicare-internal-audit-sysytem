import pandas as pd
from pathlib import Path

# =========================
# CONFIG
# =========================
EXCEL_PATH = Path("ISO_DATA.xlsx")

STRUCTURE_SHEETS = [
    "ISO_Check_category2025",
    "ISO_Check_category1",
    "ISO_Check_category2",
    "ISO_Check_category3",
    "ISO_Check_category4",
    "ISO_Check_category5",
]

OUT_PATH = Path("Category_Score_Validation_Report.xlsx")

# Expected column names (as you stated)
COL_CATEGORY_ID = "CATEGORY_ID"  # optional, if present
COL_CATEGORY_NAME = "CHECK_CATEGORY_NAME"  # must match category code/name
COL_ITEM_ID = "CHECK_ITEM_ID"
COL_WEIGHT = "WEIGHT_PERCENTAGE"
COL_GENERAL_SCORE = "GENERAL_SCORE"
COL_MIN_ACCEPT = "CATEGORY_MIN_ACCEPTABLE_SCORE"

# Tolerance: allow tiny differences due to formatting
TOL = 1e-9

# =========================
# HELPERS
# =========================
def require_cols(df: pd.DataFrame, sheet: str, required: list[str]) -> None:
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(
            f"Sheet '{sheet}' is missing required columns: {missing}\n"
            f"Available columns: {list(df.columns)}"
        )

def to_number(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")

def safe_int(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").astype("Int64")

# =========================
# LOAD & CONCAT STRUCTURE SHEETS
# =========================
if not EXCEL_PATH.exists():
    raise FileNotFoundError(f"File not found: {EXCEL_PATH.resolve()}")

xls = pd.ExcelFile(EXCEL_PATH)
frames = []

for sh in STRUCTURE_SHEETS:
    if sh not in xls.sheet_names:
        # ignore if a sheet doesn't exist
        continue

    df = pd.read_excel(xls, sheet_name=sh)
    # Minimal required columns to compute
    required = [COL_CATEGORY_NAME, COL_ITEM_ID, COL_WEIGHT, COL_GENERAL_SCORE, COL_MIN_ACCEPT]
    require_cols(df, sh, required)

    df["SOURCE_SHEET"] = sh

    # Normalize types
    df[COL_ITEM_ID] = safe_int(df[COL_ITEM_ID])
    df[COL_WEIGHT] = to_number(df[COL_WEIGHT])
    df[COL_GENERAL_SCORE] = to_number(df[COL_GENERAL_SCORE])
    df[COL_MIN_ACCEPT] = to_number(df[COL_MIN_ACCEPT])

    if COL_CATEGORY_ID in df.columns:
        df[COL_CATEGORY_ID] = safe_int(df[COL_CATEGORY_ID])

    # Keep only needed columns (plus optional CATEGORY_ID)
    keep_cols = ["SOURCE_SHEET", COL_CATEGORY_NAME, COL_ITEM_ID, COL_WEIGHT, COL_GENERAL_SCORE, COL_MIN_ACCEPT]
    if COL_CATEGORY_ID in df.columns:
        keep_cols.insert(2, COL_CATEGORY_ID)

    frames.append(df[keep_cols].copy())

if not frames:
    raise ValueError(
        "No structure sheets were loaded. Check sheet names in STRUCTURE_SHEETS and the Excel file."
    )

data = pd.concat(frames, ignore_index=True)

# Remove rows with no category name or no item id
data = data.dropna(subset=[COL_CATEGORY_NAME, COL_ITEM_ID]).copy()

# =========================
# STEP 1: DETECT INCONSISTENT ITEM WEIGHTS (within same category)
# (If the same CHECK_ITEM_ID appears multiple times in the same category with different WEIGHT_PERCENTAGE)
# =========================
weight_inconsistency = (
    data.groupby([COL_CATEGORY_NAME, COL_ITEM_ID], dropna=False)[COL_WEIGHT]
        .nunique(dropna=True)
        .reset_index(name="DistinctWeightValues")
)
weight_inconsistency = weight_inconsistency[weight_inconsistency["DistinctWeightValues"] > 1]

# Provide details of those inconsistencies
inconsistency_details = pd.DataFrame()
if not weight_inconsistency.empty:
    inconsistency_details = (
        data.merge(weight_inconsistency[[COL_CATEGORY_NAME, COL_ITEM_ID]], on=[COL_CATEGORY_NAME, COL_ITEM_ID], how="inner")
            .sort_values([COL_CATEGORY_NAME, COL_ITEM_ID, "SOURCE_SHEET"])
    )

# =========================
# STEP 2: COMPUTE CATEGORY SCORE = sum of UNIQUE items per category
# We de-duplicate by (category, item_id). For weight we take MAX (safer if duplicates exist).
# =========================
unique_items = (
    data.groupby([COL_CATEGORY_NAME, COL_ITEM_ID], dropna=False, as_index=False)
        .agg({
            COL_WEIGHT: "max",
            COL_GENERAL_SCORE: "max",          # manual value (should be same)
            COL_MIN_ACCEPT: "max",             # manual value (should be same)
        })
)

computed = (
    unique_items.groupby(COL_CATEGORY_NAME, dropna=False, as_index=False)
        .agg(
            Computed_GENERAL_SCORE=(COL_WEIGHT, "sum"),
            Manual_GENERAL_SCORE=(COL_GENERAL_SCORE, "max"),
            Manual_MIN_ACCEPTABLE=(COL_MIN_ACCEPT, "max"),
            Unique_Items_Count=(COL_ITEM_ID, "nunique"),
        )
)

computed["Computed_MIN_ACCEPTABLE_80pct"] = (computed["Computed_GENERAL_SCORE"] * 0.80)

# Differences
computed["Diff_GENERAL_SCORE"] = computed["Manual_GENERAL_SCORE"] - computed["Computed_GENERAL_SCORE"]
computed["AbsDiff_GENERAL_SCORE"] = computed["Diff_GENERAL_SCORE"].abs()

computed["Diff_MIN_ACCEPTABLE"] = computed["Manual_MIN_ACCEPTABLE"] - computed["Computed_MIN_ACCEPTABLE_80pct"]
computed["AbsDiff_MIN_ACCEPTABLE"] = computed["Diff_MIN_ACCEPTABLE"].abs()

# Status flags
def status_row(r):
    issues = []
    if pd.isna(r["Manual_GENERAL_SCORE"]):
        issues.append("Manual_GENERAL_SCORE_Missing")
    if pd.isna(r["Manual_MIN_ACCEPTABLE"]):
        issues.append("Manual_MIN_ACCEPTABLE_Missing")
    if pd.isna(r["Computed_GENERAL_SCORE"]):
        issues.append("Computed_GENERAL_SCORE_Missing")
    if pd.isna(r["Computed_MIN_ACCEPTABLE_80pct"]):
        issues.append("Computed_MIN_ACCEPTABLE_Missing")

    if (not pd.isna(r["AbsDiff_GENERAL_SCORE"])) and (r["AbsDiff_GENERAL_SCORE"] > TOL):
        issues.append("GENERAL_SCORE_Mismatch")
    if (not pd.isna(r["AbsDiff_MIN_ACCEPTABLE"])) and (r["AbsDiff_MIN_ACCEPTABLE"] > TOL):
        issues.append("MIN_ACCEPTABLE_Mismatch")

    return "OK" if not issues else ";".join(issues)

computed["Status"] = computed.apply(status_row, axis=1)

# Sort most problematic first
by_category = computed.sort_values(["Status", "AbsDiff_GENERAL_SCORE", "AbsDiff_MIN_ACCEPTABLE"], ascending=[True, False, False])

# =========================
# SUMMARY
# =========================
total_categories = len(by_category)
mismatch_general = int((by_category["AbsDiff_GENERAL_SCORE"] > TOL).sum())
mismatch_min = int((by_category["AbsDiff_MIN_ACCEPTABLE"] > TOL).sum())
ok_count = int((by_category["Status"] == "OK").sum())

summary = pd.DataFrame([{
    "Total_Categories": total_categories,
    "OK_Categories": ok_count,
    "GENERAL_SCORE_Mismatch_Categories": mismatch_general,
    "MIN_ACCEPTABLE_Mismatch_Categories": mismatch_min,
    "ItemWeight_Inconsistency_Pairs": int(len(weight_inconsistency)),
    "Avg_AbsDiff_GENERAL_SCORE": float(by_category["AbsDiff_GENERAL_SCORE"].mean(skipna=True)),
    "Max_AbsDiff_GENERAL_SCORE": float(by_category["AbsDiff_GENERAL_SCORE"].max(skipna=True)),
    "Avg_AbsDiff_MIN_ACCEPTABLE": float(by_category["AbsDiff_MIN_ACCEPTABLE"].mean(skipna=True)),
    "Max_AbsDiff_MIN_ACCEPTABLE": float(by_category["AbsDiff_MIN_ACCEPTABLE"].max(skipna=True)),
}])

# Optional: categories with any mismatch
mismatch_rows = by_category[by_category["Status"] != "OK"].copy()

# =========================
# EXPORT REPORT
# =========================
with pd.ExcelWriter(OUT_PATH, engine="openpyxl") as writer:
    summary.to_excel(writer, sheet_name="Summary", index=False)
    by_category.to_excel(writer, sheet_name="ByCategory", index=False)
    mismatch_rows.to_excel(writer, sheet_name="MismatchesOnly", index=False)

    # Evidence: unique item weights used in computation
    unique_items.sort_values([COL_CATEGORY_NAME, COL_ITEM_ID]).to_excel(writer, sheet_name="UniqueItemsUsed", index=False)

    # Weight inconsistencies
    weight_inconsistency.to_excel(writer, sheet_name="WeightInconsistencies", index=False)
    if not inconsistency_details.empty:
        inconsistency_details.to_excel(writer, sheet_name="WeightInconsistencyDetails", index=False)

print(f"✅ Report generated: {OUT_PATH.resolve()}")
print(summary.to_string(index=False))
