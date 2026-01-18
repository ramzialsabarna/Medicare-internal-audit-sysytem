# structurecode/uvl_builder.py
import pandas as pd
import re
from collections import defaultdict


def _indent(level: int) -> str:
    return " " * (4 * level)


def _safe_sorted_unique(series):
    if series is None:
        return []
    s = series.dropna().astype(str).str.strip()
    return sorted(set(x for x in s if x and x.lower() != "nan"))


def _is_constraint_valid(expr: str, valid_features: set) -> bool:
    tokens = re.findall(r"[A-Za-z_][A-Za-z0-9_]*", expr)
    blacklist = {"and", "or", "not", "true", "false", "implies"}
    features_in_expr = [t for t in tokens if t not in blacklist]
    return all(f in valid_features for f in features_in_expr)


def _require_columns(df: pd.DataFrame, cols: list[str], sheet_name: str = "") -> None:
    missing = [c for c in cols if c not in df.columns]
    if missing:
        s = f" (sheet={sheet_name})" if sheet_name else ""
        raise ValueError(f"Missing required columns{s}: {missing}")


def _clean_upper_cols(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(c).strip().upper() for c in df.columns]
    for c in ["CATEGORY_CODE", "ITEM_KEY", "ITEM_FEATURE_NAME", "ANSWER_FEATURE_NAME", "BRANCH_FEATURE_CODE"]:
        if c in df.columns:
            df[c] = df[c].astype(str).str.strip()
            df.loc[df[c].str.lower().isin(["nan", "none", ""]), c] = pd.NA
    return df


def _is_missing(x) -> bool:
    if x is None or pd.isna(x):
        return True
    s = str(x).strip().lower()
    return s in ("", "nan", "none")


def _detect_fatal_mapping_conflicts(df: pd.DataFrame) -> None:
    core = df.dropna(subset=["ITEM_KEY", "ITEM_FEATURE_NAME"]).copy()
    if core.empty:
        return

    g1 = core.groupby("ITEM_KEY")["ITEM_FEATURE_NAME"].nunique()
    bad1 = g1[g1 > 1]

    g2 = core.groupby("ITEM_FEATURE_NAME")["ITEM_KEY"].nunique()
    bad2 = g2[g2 > 1]

    if not bad1.empty or not bad2.empty:
        msg = ["FATAL: Item mapping conflicts detected (data corruption):"]
        if not bad1.empty:
            msg.append(f"- ITEM_KEY with multiple ITEM_FEATURE_NAME: {bad1.index.tolist()[:10]} ...")
        if not bad2.empty:
            msg.append(f"- ITEM_FEATURE_NAME with multiple ITEM_KEY: {bad2.index.tolist()[:10]} ...")
        raise ValueError("\n".join(msg))


def _print_build_report(report: dict, max_examples: int = 20) -> None:
    total_skipped = sum(len(v) for v in report.values())
    if total_skipped == 0:
        print("✅ UVL BUILD REPORT: No skipped rows. All required features were present.")
        return

    print("\n" + "=" * 80)
    print("⚠️  UVL BUILD REPORT: Skipped elements (missing required feature fields)")
    print("=" * 80)
    for reason, examples in report.items():
        print(f"\n- {reason}: {len(examples)}")
        for ex in examples[:max_examples]:
            print(f"  • {ex}")
        if len(examples) > max_examples:
            print(f"  ... (+{len(examples) - max_examples} more)")
    print("\nSUMMARY:")
    print(f"- Total skipped: {total_skipped}")
    print("=" * 80 + "\n")


def _read_branch_profile(input_xlsx: str) -> pd.DataFrame:
    try:
        bp = pd.read_excel(input_xlsx, sheet_name="BRANCH_PROFILE")
        bp = _clean_upper_cols(bp)
        return bp
    except Exception:
        return pd.DataFrame()


def _branches_with_flag(profile_df: pd.DataFrame, flag_col_upper: str) -> list[str]:
    if profile_df is None or profile_df.empty:
        return []
    if "BRANCH_FEATURE_CODE" not in profile_df.columns or flag_col_upper not in profile_df.columns:
        return []

    tmp = profile_df.copy()
    tmp[flag_col_upper] = pd.to_numeric(tmp[flag_col_upper], errors="coerce").fillna(0).astype(int)

    s = tmp.loc[tmp[flag_col_upper] == 1, "BRANCH_FEATURE_CODE"].dropna().astype(str).str.strip()
    return sorted(set([x for x in s if x and x.lower() != "nan"]))


def build_uvl_from_structure(
    input_xlsx: str,
    sheet_name: str,
    uvl_out_path: str,
    namespace: str = "MedicareAuditStructure",
    report_to_terminal: bool = True,
    require_answers: bool = True,
) -> None:
    # =========================
    # Read & clean RAW sheet
    # =========================
    df_raw = pd.read_excel(input_xlsx, sheet_name=sheet_name)
    df_raw = _clean_upper_cols(df_raw)

    _require_columns(
        df_raw,
        ["CATEGORY_CODE", "ITEM_KEY", "ITEM_FEATURE_NAME", "ANSWER_FEATURE_NAME", "BRANCH_FEATURE_CODE"],
        sheet_name=sheet_name,
    )

    report = defaultdict(list)

    # =========================
    # (A) Branch Universe (INDEPENDENT)
    # =========================
    branches_universe = _safe_sorted_unique(df_raw.get("BRANCH_FEATURE_CODE"))

    # =========================
    # (B) Content Universe (DEDUP ONLY FOR STRUCTURE)
    # Deduplicate only the structure semantics:
    # same category + item + answer should appear once, regardless of branch repeats
    # =========================
    df_content = df_raw.drop_duplicates(
        subset=["CATEGORY_CODE", "ITEM_KEY", "ITEM_FEATURE_NAME", "ANSWER_FEATURE_NAME"],
        keep="first",
    ).copy()

    # mapping conflicts should be checked on the content (not raw repeats)
    _detect_fatal_mapping_conflicts(df_content)

    # =========================
    # Extract dimensions FROM CONTENT (except branches)
    # =========================
    categories_all = _safe_sorted_unique(df_content.get("CATEGORY_CODE"))
    audit_types = _safe_sorted_unique(df_content.get("AUDIT_TYPE_CODE"))
    audit_plans = _safe_sorted_unique(df_content.get("AUDIT_PLAN_CODE"))

    branch_cap_flags = ["iso_active", "micro_active", "path_active"]

    item_features = _safe_sorted_unique(
        df_content.dropna(subset=["ITEM_FEATURE_NAME"]).get("ITEM_FEATURE_NAME")
    )

    CONTAINER_FEATURES = {"AuditPlan", "RelatedVisit"}

    valid_features = set(
        branch_cap_flags
        + audit_types
        + audit_plans
        + branches_universe
        + categories_all
        + item_features
        + list(CONTAINER_FEATURES)
    )

    branch_profile = _read_branch_profile(input_xlsx)

    # =========================
    # UVL builder
    # =========================
    uvl_lines: list[str] = []

    def emit(line: str) -> None:
        uvl_lines.append(line)

    def emit_feature(name: str, level: int, abstract: bool = False) -> None:
        tag = " {abstract}" if abstract else ""
        emit(_indent(level) + f"{name}{tag}")

    # Header
    emit(f"namespace {namespace}")
    emit("")
    emit("features")

    emit_feature("InternalAuditSystem", 1, abstract=True)
    emit(_indent(2) + "mandatory")

    # BranchCapabilities
    emit_feature("BranchCapabilities", 3, abstract=True)
    emit(_indent(4) + "optional")
    for f in branch_cap_flags:
        emit_feature(f, 5, abstract=False)

    # AuditType
    if audit_types:
        emit_feature("AuditType", 3, abstract=True)
        emit(_indent(4) + "alternative")
        for t in audit_types:
            emit_feature(t, 5, abstract=False)

    # AuditedEntity (ALL branches from RAW universe)
    if branches_universe:
        emit_feature("AuditedEntity", 3, abstract=True)
        emit(_indent(4) + "alternative")
        for b in branches_universe:
            emit_feature(b, 5, abstract=False)

    # AuditScope container
    emit_feature("AuditScope", 3, abstract=True)
    emit(_indent(4) + "or")

    # =========================
    # Categories / Items / Answers (from CONTENT)
    # =========================
    for cat in categories_all:
        cat_rows = df_content[df_content["CATEGORY_CODE"].astype(str).str.strip() == cat].copy()

        cat_valid_items = cat_rows.dropna(subset=["ITEM_KEY", "ITEM_FEATURE_NAME"]).copy()
        cat_valid_items = cat_valid_items.drop_duplicates(subset=["ITEM_KEY", "ITEM_FEATURE_NAME"], keep="first")

        if cat_valid_items.empty:
            report["SKIP_CATEGORY_NO_VALID_ITEMS"].append(
                f"CATEGORY={cat} (all rows missing ITEM_KEY/ITEM_FEATURE_NAME)"
            )
            continue

        emit_feature(cat, 5, abstract=True)
        emit(_indent(6) + "mandatory")

        for _, row in cat_valid_items.iterrows():
            ikey = row["ITEM_KEY"]
            item_feat = row["ITEM_FEATURE_NAME"]

            if _is_missing(ikey) or _is_missing(item_feat):
                report["SKIP_ITEM_MISSING_KEY_OR_FEATURE"].append(
                    f"CATEGORY={cat} ITEM_KEY={ikey} ITEM_FEATURE_NAME={item_feat}"
                )
                continue

            ikey = str(ikey).strip()
            item_feat = str(item_feat).strip()

            rel = cat_rows[cat_rows["ITEM_KEY"].astype(str).str.strip() == ikey].copy()
            rel_answers = rel.dropna(subset=["ANSWER_FEATURE_NAME"]).copy()
            choices = _safe_sorted_unique(rel_answers.get("ANSWER_FEATURE_NAME"))

            if not choices:
                if require_answers:
                    report["SKIP_ITEM_NO_VALID_ANSWERS"].append(
                        f"CATEGORY={cat} ITEM_KEY={ikey} ITEM_FEATURE_NAME={item_feat}"
                    )
                    continue
                else:
                    report["WARN_ITEM_NO_VALID_ANSWERS_BUILT_ITEM_ONLY"].append(
                        f"CATEGORY={cat} ITEM_KEY={ikey} ITEM_FEATURE_NAME={item_feat}"
                    )

            emit_feature(item_feat, 7, abstract=True)
            emit(_indent(8) + "mandatory")

            if choices:
                answers_node = f"Answers__{ikey}"
                emit_feature(answers_node, 9, abstract=True)
                emit(_indent(10) + "alternative")
                valid_features.add(answers_node)

                for ans_feat in choices:
                    if _is_missing(ans_feat):
                        report["SKIP_ANSWER_MISSING_FEATURE"].append(
                            f"CATEGORY={cat} ITEM_KEY={ikey} ANSWER_FEATURE_NAME={ans_feat}"
                        )
                        continue
                    ans_feat = str(ans_feat).strip()
                    emit_feature(ans_feat, 11, abstract=False)
                    valid_features.add(ans_feat)

    # Optional block: AuditPlan + RelatedVisit
    emit(_indent(2) + "optional")

    if audit_plans:
        emit_feature("AuditPlan", 3, abstract=True)
        emit(_indent(4) + "alternative")
        for p in audit_plans:
            emit_feature(p, 5, abstract=False)

    emit_feature("RelatedVisit", 3, abstract=True)

    # =========================
    # Constraints
    # =========================
    emit("")
    emit("constraints")

    for flag_upper in ["ISO_ACTIVE", "MICRO_ACTIVE", "PATH_ACTIVE"]:
        flag_name = flag_upper.lower()
        branches_on = _branches_with_flag(branch_profile, flag_upper)

        if branches_on:
            expr = f"{flag_name} <=> (" + " | ".join(branches_on) + ")"
            if _is_constraint_valid(expr, valid_features):
                emit(_indent(1) + expr)
        else:
            emit(_indent(1) + f"!{flag_name}")

    if "planned" in valid_features:
        emit(_indent(1) + "planned => AuditPlan")
        emit(_indent(1) + "!planned => !AuditPlan")

    if "re_evaluate" in valid_features:
        emit(_indent(1) + "re_evaluate => RelatedVisit")
        emit(_indent(1) + "!re_evaluate => !RelatedVisit")

    # Scope rules (unchanged)
    try:
        df_rules = pd.read_excel(input_xlsx, sheet_name="SCOPE_RULES")
        df_rules = _clean_upper_cols(df_rules)

        if all(c in df_rules.columns for c in ["CAPABILITY_FLAG", "TARGET_CODE", "ACTION"]):
            for _, r in df_rules.iterrows():
                target = str(r.get("TARGET_CODE", "")).strip()
                flag = str(r.get("CAPABILITY_FLAG", "")).strip()
                action = str(r.get("ACTION", "")).strip().lower()

                if not target or not flag or not action:
                    continue

                flag = flag.lower()

                if target not in valid_features or flag not in valid_features:
                    continue

                if action == "forbid" or action == "require":
                    expr1 = f"{target} => {flag}"
                    expr2 = f"!{flag} => !{target}"
                    if _is_constraint_valid(expr1, valid_features):
                        emit(_indent(1) + expr1)
                    if _is_constraint_valid(expr2, valid_features):
                        emit(_indent(1) + expr2)

                elif action in ("allow", "permit", "none"):
                    continue

    except Exception:
        pass

    # Save
    with open(uvl_out_path, "w", encoding="utf-8") as f:
        f.write("\n".join(uvl_lines))

    if report_to_terminal:
        _print_build_report(report, max_examples=20)

    print(f"✅ UVL written to: {uvl_out_path}")
