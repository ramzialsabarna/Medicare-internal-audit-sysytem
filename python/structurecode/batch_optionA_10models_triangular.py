import re
import random
from pathlib import Path
import pandas as pd

# =========================
# CONFIG
# =========================
FILE_PREFIX = "ISO_DATA"  # ISO_DATA*.xlsx
STRUCT_PREFIXES = ("ISO_CHECK_CATEG", "ISO_CHECK_CATEGORY")

SEED = 42
MANUAL_SAMPLE_N = 40  # number of rows in Manual sheet

# 10 Models only
MODEL_IDS = [f"M{str(i).zfill(2)}" for i in range(1, 11)]

# Cumulative categories per model (target) -> dynamic min with available
CATEGORY_CUM_TARGETS = [2, 4, 6, 8, 10, 12, 14, 16, 17, 18]

# Items rule: items per category = min(5, category_age_in_models)
ITEMS_PER_CATEGORY_CAP = 5

# Domain expansion targets (dynamic min with available)
AUDIT_TYPE_TARGETS = [1, 2, 3, 3, 3, 3, 3, 3, 3, 3]         # total available=3
AUDIT_PLAN_TARGETS = [2, 4, 6, 8, 10, 10, 10, 10, 10, 10]   # total available=10
BRANCH_TARGETS     = [5, 10, 15, 20, 25, 30, 35, 40, 40, 40] # total available=40

# Output sheet prefixes (no new columns, only new sheets)
REDUCED_PREFIX = "Reduced"
MANUAL_PREFIX  = "Manual"

# Required scope columns (case-insensitive match, no renaming)
REQ_SCOPE_UPPER = ["CATEGORY_CODE", "ITEM_KEY", "ITEM_FEATURE_NAME", "ANSWER_FEATURE_NAME"]

# Optional domain columns (case-insensitive if exist)
COL_AUDIT_TYPE = "AUDIT_TYPE_CODE"
COL_AUDIT_PLAN = "AUDIT_PLAN_CODE"
COL_BRANCH     = "BRANCH_FEATURE_CODE"
COL_ENTITYTYPE = "ENTITY_TYPE"  # to filter only branches if present


# =========================
# Helpers
# =========================
def is_structure_sheet(name: str) -> bool:
    s = str(name).strip().upper()
    return any(s.startswith(p) for p in STRUCT_PREFIXES)

def excel_sheet(name: str) -> str:
    return name[:31]

def find_col(df: pd.DataFrame, upper_name: str) -> str:
    target = upper_name.strip().upper()
    for c in df.columns:
        if str(c).strip().upper() == target:
            return c
    raise KeyError(f"Missing required column: {upper_name}. Available: {list(df.columns)}")

def find_col_optional(df: pd.DataFrame, upper_name: str) -> str | None:
    target = upper_name.strip().upper()
    for c in df.columns:
        if str(c).strip().upper() == target:
            return c
    return None

def safe_tag_from_sheet(sheet_name: str) -> str:
    s = str(sheet_name).strip()
    su = s.upper()
    m = re.search(r"CATEGORY(\d+)$", su)
    if m:
        return f"c{m.group(1)}"
    m = re.search(r"CATEG(\d+)$", su)
    if m:
        return f"c{m.group(1)}"
    cleaned = re.sub(r"[^A-Za-z0-9]+", "", su)
    return (cleaned[-6:] or "sx").lower()

def safe_unique_list(series: pd.Series) -> list[str]:
    if series is None:
        return []
    s = series.dropna().astype(str).str.strip()
    out = [x for x in s if x and x.lower() not in ("nan", "none")]
    return sorted(set(out))

def shuffled(lst: list[str], seed: int) -> list[str]:
    r = random.Random(seed)
    x = list(lst)
    r.shuffle(x)
    return x

def build_domain_stub_rows(df_columns: list, audit_types: list[str], plans: list[str], branches: list[str],
                           col_audit_type: str | None, col_audit_plan: str | None, col_branch: str | None) -> pd.DataFrame:
    """
    Create rows with ONLY domain columns filled (AUDIT_TYPE_CODE / AUDIT_PLAN_CODE / BRANCH_FEATURE_CODE).
    All other columns are NA. No new columns.
    """
    rows = []

    def make_blank_row():
        return {c: pd.NA for c in df_columns}

    # one row per audit type
    if col_audit_type and audit_types:
        for v in audit_types:
            r = make_blank_row()
            r[col_audit_type] = v
            rows.append(r)

    # one row per plan
    if col_audit_plan and plans:
        for v in plans:
            r = make_blank_row()
            r[col_audit_plan] = v
            rows.append(r)

    # one row per branch
    if col_branch and branches:
        for v in branches:
            r = make_blank_row()
            r[col_branch] = v
            rows.append(r)

    if not rows:
        return pd.DataFrame(columns=df_columns)

    return pd.DataFrame(rows, columns=df_columns)

def model_category_targets(available_n: int) -> list[int]:
    # dynamic min with available categories
    return [min(t, available_n) for t in CATEGORY_CUM_TARGETS]

def model_domain_targets(avail_types: int, avail_plans: int, avail_branches: int):
    t_types = [min(t, avail_types) for t in AUDIT_TYPE_TARGETS]
    t_plans = [min(t, avail_plans) for t in AUDIT_PLAN_TARGETS]
    t_br    = [min(t, avail_branches) for t in BRANCH_TARGETS]
    return t_types, t_plans, t_br

def category_first_model_build(df: pd.DataFrame, seed: int):
    # Required scope columns (case-insensitive)
    c_cat = find_col(df, "CATEGORY_CODE")
    c_item_key = find_col(df, "ITEM_KEY")
    c_item_feat = find_col(df, "ITEM_FEATURE_NAME")
    c_ans_feat = find_col(df, "ANSWER_FEATURE_NAME")

    # Optional domain cols
    c_audit_type = find_col_optional(df, COL_AUDIT_TYPE)
    c_audit_plan = find_col_optional(df, COL_AUDIT_PLAN)
    c_branch     = find_col_optional(df, COL_BRANCH)
    c_entity     = find_col_optional(df, COL_ENTITYTYPE)

    all_cols = df.columns.tolist()

    # 1) exact row dedup (safe, doesn't break relations)
    before = len(df)
    df = df.drop_duplicates(keep="first").copy()
    removed = before - len(df)

    # 2) Build category list (from actual column)
    categories = safe_unique_list(df[c_cat])
    categories = shuffled(categories, seed)

    # 3) For each category, build item list (unique by ITEM_KEY+ITEM_FEATURE_NAME inside category)
    #    Shuffle deterministically per category (seed + hash)
    items_by_cat: dict[str, list[str]] = {}  # cat -> list of ITEM_KEY
    for cat in categories:
        sub = df[df[c_cat].astype(str).str.strip() == cat].copy()
        core = sub.dropna(subset=[c_item_key, c_item_feat]).drop_duplicates(subset=[c_item_key, c_item_feat])
        ikeys = safe_unique_list(core[c_item_key])
        # stable shuffle per category
        cat_seed = seed + (abs(hash(cat)) % 10_000)
        items_by_cat[cat] = shuffled(ikeys, cat_seed)

    # 4) Domain lists (from columns if exist)
    audit_types_all = safe_unique_list(df[c_audit_type]) if c_audit_type else []
    audit_plans_all = safe_unique_list(df[c_audit_plan]) if c_audit_plan else []

    # branches: if ENTITY_TYPE exists, keep only entity_type == 'branch' (case-insensitive)
    branches_all = []
    if c_branch:
        if c_entity:
            tmp = df.dropna(subset=[c_branch, c_entity]).copy()
            tmp_ent = tmp[c_entity].astype(str).str.strip().str.lower()
            tmp = tmp[tmp_ent == "branch"]
            branches_all = safe_unique_list(tmp[c_branch])
        else:
            branches_all = safe_unique_list(df[c_branch])

    audit_types_all = shuffled(audit_types_all, seed)
    audit_plans_all = shuffled(audit_plans_all, seed + 1)
    branches_all    = shuffled(branches_all, seed + 2)

    # 5) Per-model targets (dynamic min with available)
    cat_targets = model_category_targets(len(categories))
    t_types, t_plans, t_branches = model_domain_targets(
        avail_types=len(audit_types_all),
        avail_plans=len(audit_plans_all),
        avail_branches=len(branches_all),
    )

    # Track when each category was introduced (for "age" rule)
    # We'll introduce categories as first N in categories list by cat_targets
    subsets = {}
    manuals = {}
    reports = []

    for i, mid in enumerate(MODEL_IDS):
        n_cats = cat_targets[i]
        selected_cats = categories[:n_cats]

        # Determine category "age": if cat enters at model j, then age in current model = (i - j + 1)
        # Entry model index for cat = first model where it's included (based on cat_targets).
        # Since selected_cats are prefix, entry is simply its position in category list -> maps to model where prefix reaches it.
        # We'll compute entry_model for each selected cat by scanning cat_targets once.
        entry_model_index = {}
        for idx_cat, cat in enumerate(categories):
            # find first model where cat is included
            for m_idx, tgt in enumerate(cat_targets):
                if idx_cat < tgt:
                    entry_model_index[cat] = m_idx
                    break

        # Items per category: min(5, age)
        selected_item_keys = set()
        for cat in selected_cats:
            age = (i - entry_model_index.get(cat, i) + 1)
            n_items = min(ITEMS_PER_CATEGORY_CAP, max(1, age))
            ikeys = items_by_cat.get(cat, [])
            selected_item_keys.update(ikeys[:min(n_items, len(ikeys))])

        # Scope subset rows: keep all answers rows for selected items inside selected categories
        if selected_cats and selected_item_keys:
            sub = df[df[c_cat].isin(selected_cats) & df[c_item_key].isin(list(selected_item_keys))].copy()
        else:
            sub = df.iloc[0:0].copy()

        # Domain selections (prefixes)
        sel_types    = audit_types_all[:t_types[i]] if audit_types_all else []
        sel_plans    = audit_plans_all[:t_plans[i]] if audit_plans_all else []
        sel_branches = branches_all[:t_branches[i]] if branches_all else []

        # Append domain stubs to ensure UVL builder sees audit types/plans/branches even if not present in selected rows
        stub = build_domain_stub_rows(
            df_columns=all_cols,
            audit_types=sel_types,
            plans=sel_plans,
            branches=sel_branches,
            col_audit_type=c_audit_type,
            col_audit_plan=c_audit_plan,
            col_branch=c_branch,
        )

        out = pd.concat([sub.reindex(columns=all_cols), stub], ignore_index=True)

        # Manual sample from scope rows only (not from stubs)
        manual = sub.reindex(columns=all_cols).copy()
        if len(manual) > MANUAL_SAMPLE_N:
            manual = manual.sample(n=MANUAL_SAMPLE_N, random_state=seed)

        subsets[mid] = out
        manuals[mid] = manual

        reports.append({
            "model": mid,
            "cats_selected": len(selected_cats),
            "items_selected": len(selected_item_keys),
            "rows_scope": len(sub),
            "rows_total_with_domain_stubs": len(out),
            "audit_types_selected": len(sel_types),
            "audit_plans_selected": len(sel_plans),
            "branches_selected": len(sel_branches),
        })

    meta = {
        "removed_exact_duplicates": removed,
        "available_categories": len(categories),
        "available_audit_types": len(audit_types_all),
        "available_audit_plans": len(audit_plans_all),
        "available_branches": len(branches_all),
        "columns": all_cols,
        "col_map": {
            "CATEGORY_CODE": c_cat,
            "ITEM_KEY": c_item_key,
            "ITEM_FEATURE_NAME": c_item_feat,
            "ANSWER_FEATURE_NAME": c_ans_feat,
            "AUDIT_TYPE_CODE": c_audit_type,
            "AUDIT_PLAN_CODE": c_audit_plan,
            "BRANCH_FEATURE_CODE": c_branch,
            "ENTITY_TYPE": c_entity,
        }
    }
    return df, subsets, manuals, reports, meta


def run_batch(folder="."):
    folder_path = Path(folder)
    files = sorted(folder_path.glob(f"{FILE_PREFIX}*.xlsx"))
    if not files:
        print(f"❌ No files found matching {FILE_PREFIX}*.xlsx in {folder_path.resolve()}")
        return

    for xlsx_path in files:
        print("\n" + "=" * 110)
        print(f"📘 FILE: {xlsx_path.name}")
        print("=" * 110)

        xl = pd.ExcelFile(xlsx_path)
        struct_sheets = [s for s in xl.sheet_names if is_structure_sheet(s)]
        if not struct_sheets:
            print("⚠️  No structure sheets found.")
            continue

        with pd.ExcelWriter(xlsx_path, engine="openpyxl", mode="a", if_sheet_exists="replace") as writer:
            for sheet in struct_sheets:
                print(f"\n--- STRUCTURE SHEET: {sheet}")

                df = pd.read_excel(xlsx_path, sheet_name=sheet)

                # Validate required scope columns exist (case-insensitive)
                for req in REQ_SCOPE_UPPER:
                    _ = find_col(df, req)

                tag = safe_tag_from_sheet(sheet)
                _, subsets, manuals, reports, meta = category_first_model_build(df, SEED)

                print(f"✅ Exact duplicate rows removed inside '{sheet}': {meta['removed_exact_duplicates']}")
                print(f"✅ Available: categories={meta['available_categories']}, audit_types={meta['available_audit_types']}, "
                      f"plans={meta['available_audit_plans']}, branches={meta['available_branches']}")

                # Write outputs
                for rep in reports:
                    mid = rep["model"]
                    reduced_name = excel_sheet(f"{REDUCED_PREFIX}_{tag}_{mid}")
                    manual_name  = excel_sheet(f"{MANUAL_PREFIX}_{tag}_{mid}")

                    subsets[mid].to_excel(writer, index=False, sheet_name=reduced_name)
                    manuals[mid].to_excel(writer, index=False, sheet_name=manual_name)

                print("✅ Models written:")
                for r in reports:
                    print("  -", r)

        print(f"\n✅ Done: wrote Reduced/Manual for all structure sheets into {xlsx_path.name}")


if __name__ == "__main__":
    run_batch(".")
