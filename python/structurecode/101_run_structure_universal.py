# structurecode/101_run_structure_universal.py
from __future__ import annotations

from pathlib import Path
import pandas as pd
import sys

# =============================================================================
# 1) Paths so local modules are visible (project root + structurecode)
# =============================================================================
current_dir = Path(__file__).resolve().parent
root_dir = current_dir.parent

for p in [root_dir, current_dir]:
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))

# =============================================================================
# 2) Imports (keep exactly as your pipeline expects)
# =============================================================================
try:
    import structure_category_pandas as scp
    import structure_item_columns_pandas as sicp
    import structure_answer_columns_pandas as sacp
    import structure_audit_type_pandas as satp
    import structure_audit_plan_pandas as sapp
    import structure_branch_columns as sbc
    import structure_branch_profile_pandas as sbpp
    import uvl_builder as ub

    # NOTE: keep as-is if this matches your project layout
    from config.domain_config import *  # noqa: F403,F401
except ImportError as e:
    print(f"❌ [IMPORT ERROR] {e}")
    sys.exit(1)

# =============================================================================
# 3) Helpers
# =============================================================================
def _update_excel_sheet(xlsx_path: Path, sheet_name: str, df_out: pd.DataFrame) -> None:
    with pd.ExcelWriter(xlsx_path, engine="openpyxl", mode="a", if_sheet_exists="replace") as writer:
        df_out.to_excel(writer, sheet_name=sheet_name, index=False)


def _enrich_with_branch_profile_safe(df: pd.DataFrame, df_profile: pd.DataFrame, sheet_name: str) -> pd.DataFrame:
    """
    Safe enrichment: LEFT JOIN so we never drop scope rows
    even if a branch code is missing or unmatched.
    """
    df = df.copy()

    if "BRANCH_FEATURE_CODE" not in df.columns:
        # Nothing to enrich yet
        return df

    # Prevent suffix collisions if these already exist
    cols_to_drop = [c for c in ["ENTITY_TYPE", "iso_active", "micro_active", "path_active"] if c in df.columns]
    if cols_to_drop:
        df.drop(columns=cols_to_drop, inplace=True)

    needed_cols = ["BRANCH_FEATURE_CODE", "ENTITY_TYPE", "iso_active", "micro_active", "path_active"]
    prof = df_profile.copy()
    missing_in_profile = [c for c in needed_cols if c not in prof.columns]
    if missing_in_profile:
        # If profile is missing, return without crashing
        print(f"⚠️  [WARN] BRANCH_PROFILE missing columns {missing_in_profile}. Skipping enrichment for {sheet_name}.")
        return df

    df = df.merge(
        prof[needed_cols],
        on="BRANCH_FEATURE_CODE",
        how="left",
    )

    # Fill NA flags with 0 to keep model constraints stable
    for col in ["iso_active", "micro_active", "path_active"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)

    return df


def _find_iso_data_files(search_dir: Path) -> list[Path]:
    """
    Return ALL Excel files matching ISO_DATA*.xlsx in the given directory (non-recursive).
    """
    return sorted(search_dir.glob("ISO_DATA*.xlsx"))


# =============================================================================
# 4) Main
# =============================================================================
def main() -> None:
    print("\n" + "=" * 80)
    print("🚀 EXECUTING: MASTER STRUCTURE RUNNER (MULTI-FILE MODE)")
    print("=" * 80)

    try:
        # --- Find ALL files that start with ISO_DATA in the current working directory ---
        work_dir = Path(".").resolve()
        xlsx_files = _find_iso_data_files(work_dir)

        if not xlsx_files:
            print(f"❌ No files found in {work_dir} matching: ISO_DATA*.xlsx")
            return

        # UVL output directory from domain_config (kept as-is)
        # You already have UVL_OUTPUT_DIR and UVL_NAMESPACE in config.domain_config
        UVL_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)  # noqa: F405

        for xlsx_path in xlsx_files:
            print("\n" + "=" * 80)
            print(f"📘 Processing file: {xlsx_path.name}")
            print("=" * 80)

            if not xlsx_path.exists():
                print(f"⚠️  Skipping missing file: {xlsx_path}")
                continue

            # -------------------------
            # A) Master sheets
            # -------------------------
            # BRANCH_PROFILE
            try:
                df_p_raw = pd.read_excel(xlsx_path, sheet_name="BRANCH_PROFILE")
                df_p_out = sbpp.process_branch_profile_sheet(df_p_raw, BRANCH_NAME_OVERRIDES, DEP_LABELS)  # noqa: F405
                _update_excel_sheet(xlsx_path, "BRANCH_PROFILE", df_p_out)
            except Exception as e:
                print(f"⚠️  [WARN] BRANCH_PROFILE not processed for {xlsx_path.name}: {e}")
                df_p_out = pd.DataFrame()

            # users
            try:
                df_u_raw = pd.read_excel(xlsx_path, sheet_name="users")
                df_u_out = saudp.process_users_auditor_profile_sheet(df_u_raw)
                _update_excel_sheet(xlsx_path, "users", df_u_out)
            except Exception as e:
                print(f"⚠️  [WARN] users not processed for {xlsx_path.name}: {e}")

            # -------------------------
            # B) Process all ISO_Check_ sheets in this file
            # -------------------------
            xls = pd.ExcelFile(xlsx_path)
            sheets = [s for s in xls.sheet_names if str(s).startswith("ISO_Check_")]

            if not sheets:
                print(f"⚠️  No sheets starting with ISO_Check_ in {xlsx_path.name}.")
                continue

            for sheet in sheets:
                print(f"📦 Working on: {sheet}")
                df = pd.read_excel(xlsx_path, sheet_name=sheet)

                # Structural pipeline (kept in same order)
                df = scp.process_structure_category_df(df, sheet, CATEGORY_SPELLING_MAP)  # noqa: F405
                df = sicp.process_item_columns(df, sheet)
                df = sacp.process_answer_columns(df, sheet)
                df = satp.process_structure_audit_type_df(df, sheet)
                df = sapp.process_structure_audit_plan_df(df, sheet)
                df = sbc.process_branch_columns(df, BRANCH_NAME_OVERRIDES, DEP_LABELS)  # noqa: F405
                
                
                

                # Enrich with branch profile (safe)
                if isinstance(df_p_out, pd.DataFrame) and not df_p_out.empty:
                    df = _enrich_with_branch_profile_safe(df, df_p_out, sheet)

                # Write processed structure back into same file
                _update_excel_sheet(xlsx_path, sheet, df)

                # -------------------------
                # UVL generation
                # Use file stem in UVL name to avoid collisions across multiple ISO_DATA files
                # -------------------------
                uvl_path = UVL_OUTPUT_DIR / f"{xlsx_path.stem}__{sheet}.uvl"  # noqa: F405
                ub.build_uvl_from_structure(str(xlsx_path), sheet, str(uvl_path), UVL_NAMESPACE)  # noqa: F405
                print(f"   -> ✅ UVL Ready: {uvl_path.name}")

        print("\n" + "=" * 80)
        print("✨ ALL FILES PROCESSED SUCCESSFULLY (NO DATA DELETED)")
        print("=" * 80)

    except Exception as e:
        print(f"\n❌ CRITICAL ERROR: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
