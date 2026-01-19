# final_logic_analyzer.py
# ============================================================
# FINAL LOGIC ANALYZER (ONE-EXCEL REPORT, STABLE ARTIFACT)
# Core truth: compares KR (clean vs injected) directly.
# Optional enrichment: reads SAT/time/NF from existing CSV/Excel if available.
#
# Outputs:
#  - FINAL_Verification_Report.xlsx (multiple stable sheets)
#  - final_phd_validation_results_sat.csv (compatibility)
#
# Root is FIXED: InternalAuditSystem
# ============================================================

from __future__ import annotations

import re
from pathlib import Path
from dataclasses import dataclass
from typing import Dict, List, Optional, Set, Tuple

import pandas as pd


# ----------------------------
# Fixed Root
# ----------------------------
ROOT_FEATURE = "InternalAuditSystem"

# ----------------------------
# Default KR folders (stable)
# ----------------------------
CLEAN_KR_DIR_NAME = "kr_outputs_10models"
INJECTED_KR_DIR_NAME = "prolog_facts_v5"

# ----------------------------
# Stable filenames (do not change)
# ----------------------------
SAT_RESULTS_CSV_NAME = "final_phd_validation_results_sat.csv"
FINAL_REPORT_XLSX_NAME = "FINAL_Verification_Report.xlsx"

# ----------------------------
# Stable sheet names (do not change)
# ----------------------------
SHEET_00_SUMMARY = "00_Summary"
SHEET_01_ALL = "01_RunTable_All"
SHEET_02_CLEAN = "02_RunTable_Clean"
SHEET_03_INJ = "03_RunTable_Injected"
SHEET_04_VERIFY = "04_Verification_InjectionVsDete"
SHEET_05_DET_MISS = "05_DetectorMisses"
SHEET_06_INVALID = "06_InvalidTargets"
SHEET_07_MISS_INJ = "07_MissingInjection"
SHEET_08_UNSAT = "08_VoidUnsat"
SHEET_09_FP = "09_FP_Clean"
SHEET_10_FN = "10_FN_Injected"
SHEET_11_DELTA = "11_DeltaTime"

# Canonical columns (as you showed)
CANON_COLS = [
    "Group",
    "Model",
    "Root",
    "NF",
    "Constraints",
    "SAT",
    "N_Dead",
    "N_FalseOptional",
    "TimeSec",
    "Defects",
]


# ============================================================
# KR parsing
# ============================================================

_RE_FEATURE = re.compile(r"^\s*feature\(\s*([^)]+?)\s*\)\.\s*$", re.MULTILINE)
_RE_GROUP = re.compile(
    r"^\s*group\(\s*([^,]+?)\s*,\s*([a-z_]+)\s*,\s*\[(.*?)\]\s*\)\.\s*$",
    re.MULTILINE,
)
_RE_IMP = re.compile(
    r"^\s*imp\(\s*([^,]+?)\s*,\s*(.+?)\s*\)\.\s*$",
    re.MULTILINE,
)


def derive_model_key(filename: str) -> str:
    """
    Robust ModelKey derivation for pairing clean/injected.
    Handles:
      - SCIENTIFIC_V5_ prefix (older runs)
      - _injected suffix
      - tokens: clean/original/baseline/injected/defective
      - extensions: .kr.pl, .pl, .uvl
    """
    name = str(filename).replace("\\", "/").split("/")[-1]

    # remove extensions
    name = name.replace(".kr.pl", "").replace(".pl", "").replace(".uvl", "")

    # remove common labels
    name = re.sub(r"(?i)\b(clean|original|baseline)\b", "", name)
    name = re.sub(r"(?i)\b(injected|defective)\b", "", name)

    # remove your old prefixes if present
    name = re.sub(r"(?i)^scientific_v\d+_", "", name)  # SCIENTIFIC_V5_
    name = re.sub(r"(?i)^scientific_", "", name)

    # remove suffix patterns
    name = re.sub(r"(?i)_injected$", "", name)

    # cleanup
    name = re.sub(r"__+", "_", name).strip("_- ")

    return name


def parse_literal(expr: str) -> Tuple[bool, str]:
    """
    Returns (is_positive, feature_name)
      - x -> (True, x)
      - not(x) -> (False, x)
    """
    s = expr.strip()
    if s.startswith("not(") and s.endswith(")"):
        inner = s[4:-1].strip()
        return (False, inner)
    return (True, s)


@dataclass
class KRModel:
    path: Path
    features: Set[str]
    groups: List[Tuple[str, str, List[str]]]  # (parent, gtype, children)
    imps: List[Tuple[str, str]]               # raw (lhs, rhs)


def load_kr_model(path: Path) -> KRModel:
    text = path.read_text(encoding="utf-8", errors="ignore")
    features = set(_RE_FEATURE.findall(text))

    groups: List[Tuple[str, str, List[str]]] = []
    for parent, gtype, children_blob in _RE_GROUP.findall(text):
        parent = parent.strip()
        gtype = gtype.strip()
        children = [c.strip() for c in children_blob.split(",") if c.strip()]
        groups.append((parent, gtype, children))

    imps: List[Tuple[str, str]] = []
    for lhs, rhs in _RE_IMP.findall(text):
        imps.append((lhs.strip(), rhs.strip()))

    return KRModel(path=path, features=features, groups=groups, imps=imps)


def index_kr_files(root_dir: Path) -> Dict[str, Path]:
    mapping: Dict[str, Path] = {}
    if not root_dir.exists():
        return mapping
    for p in root_dir.rglob("*.kr.pl"):
        mapping[p.name] = p
    return mapping


# ============================================================
# KR analysis
# ============================================================

@dataclass
class KRAnalysis:
    model_key: str
    filename: str
    nf: int
    constraints: int
    audit_type_options_count: int
    dead_features: Set[str]
    false_optional: Set[str]
    defects_raw: List[str]
    defects_filtered: List[str]
    is_visit_type_artifact: bool


def compute_structural_always(root: str, groups: List[Tuple[str, str, List[str]]]) -> Set[str]:
    """
    Compute features that are structurally always-selected:
    - root always
    - mandatory-group children of always-parent are always
    - alternative/or with single child under always-parent -> child always
    """
    by_parent: Dict[str, List[Tuple[str, List[str]]]] = {}
    for parent, gtype, children in groups:
        by_parent.setdefault(parent, []).append((gtype, children))

    always: Set[str] = {root}
    changed = True
    while changed:
        changed = False
        for parent in list(always):
            for gtype, children in by_parent.get(parent, []):
                if gtype == "mandatory":
                    for c in children:
                        if c not in always:
                            always.add(c)
                            changed = True
                elif gtype in {"alternative", "or"} and len(children) == 1:
                    c = children[0]
                    if c not in always:
                        always.add(c)
                        changed = True
    return always


def analyze_kr_for_defects(kr: KRModel, model_key: str, root: str = ROOT_FEATURE) -> KRAnalysis:
    nf = len(kr.features)
    constraints = len(kr.imps)

    # AuditType options
    audit_type_options_count = 0
    for parent, gtype, children in kr.groups:
        if parent == "AuditType" and gtype == "alternative":
            audit_type_options_count = len(children)
            break

    # Root optional children (for false optional)
    root_optional: Set[str] = set()
    for parent, gtype, children in kr.groups:
        if parent == root and gtype == "optional":
            root_optional.update(children)

    # Compute always + forward-chaining on implications
    always = compute_structural_always(root, kr.groups) if root in kr.features else set()
    forbidden: Set[str] = set()

    changed = True
    while changed:
        changed = False
        for lhs_raw, rhs_raw in kr.imps:
            lhs_pos, lhs_feat = parse_literal(lhs_raw)
            if not lhs_pos:
                continue
            if lhs_feat not in always:
                continue

            rhs_pos, rhs_feat = parse_literal(rhs_raw)
            if rhs_pos:
                if rhs_feat not in always:
                    always.add(rhs_feat)
                    changed = True
            else:
                if rhs_feat not in forbidden:
                    forbidden.add(rhs_feat)
                    changed = True

    dead_features = set(forbidden)
    false_optional = (root_optional & always) if root in kr.features else set()

    defects_raw: List[str] = []
    for d in sorted(dead_features):
        defects_raw.append(f"DF:{d}")
    for fo in sorted(false_optional):
        defects_raw.append(f"FO:{fo}")

    # Artifact filter (your known artifact)
    is_visit_type_artifact = (audit_type_options_count == 1) and ("AuditPlan" in false_optional)
    defects_filtered = list(defects_raw)
    if is_visit_type_artifact:
        defects_filtered = [x for x in defects_filtered if x != "FO:AuditPlan"]

    return KRAnalysis(
        model_key=model_key,
        filename=kr.path.name,
        nf=nf,
        constraints=constraints,
        audit_type_options_count=audit_type_options_count,
        dead_features=dead_features,
        false_optional=false_optional,
        defects_raw=defects_raw,
        defects_filtered=defects_filtered,
        is_visit_type_artifact=is_visit_type_artifact,
    )


def imp_set(kr: KRModel) -> Set[Tuple[str, str]]:
    return {(lhs.strip(), rhs.strip()) for lhs, rhs in kr.imps}


def infer_targets_from_new_imps(new_imps: Set[Tuple[str, str]]) -> List[str]:
    targets: List[str] = []
    for _, rhs in sorted(new_imps):
        pos, feat = parse_literal(rhs)
        if not pos:
            targets.append(feat)

    seen = set()
    out = []
    for t in targets:
        if t not in seen:
            out.append(t)
            seen.add(t)
    return out


# ============================================================
# Optional enrichment: load prior SAT/time results if present
# ============================================================

def load_optional_results(script_dir: Path) -> pd.DataFrame:
    """
    Tries to load:
      1) final_phd_validation_results_sat.csv
      2) FINAL_Verification_Report.xlsx sheet 01_RunTable_All
    If none exist -> returns empty DataFrame with canonical columns.
    """
    csv_path = script_dir / SAT_RESULTS_CSV_NAME
    xlsx_path = script_dir / FINAL_REPORT_XLSX_NAME

    df: Optional[pd.DataFrame] = None
    if csv_path.exists():
        df = pd.read_csv(csv_path, encoding="utf-8-sig")
    elif xlsx_path.exists():
        try:
            df = pd.read_excel(xlsx_path, sheet_name=SHEET_01_ALL)
        except Exception:
            df = None

    if df is None:
        return pd.DataFrame(columns=CANON_COLS)

    # Normalize columns
    rename_map = {
        "Execution_Time": "TimeSec",
        "Time": "TimeSec",
        "Constraint": "Constraints",
        "NDF": "N_Dead",
    }
    df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})

    # Ensure canonical columns exist
    for c in CANON_COLS:
        if c not in df.columns:
            df[c] = "" if c in ("Defects", "SAT", "Group", "Model", "Root") else 0

    # Standardize group labels
    df["Group"] = df["Group"].astype(str).str.strip()
    df["Group"] = df["Group"].replace({"Original": "Clean"})

    # Ensure numeric
    for c in ["NF", "Constraints", "N_Dead", "N_FalseOptional", "TimeSec"]:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)

    # Root fix if missing
    df["Root"] = df["Root"].astype(str)
    df.loc[df["Root"].str.strip().eq(""), "Root"] = ROOT_FEATURE

    return df


# ============================================================
# Build report (KR-first)
# ============================================================

def build_pairs_from_kr(clean_index: Dict[str, Path], inj_index: Dict[str, Path]) -> List[Dict[str, str]]:
    clean_keys = {derive_model_key(fn): fn for fn in clean_index.keys()}
    inj_keys = {derive_model_key(fn): fn for fn in inj_index.keys()}

    all_keys = sorted(set(clean_keys.keys()) | set(inj_keys.keys()))
    pairs: List[Dict[str, str]] = []
    for mk in all_keys:
        pairs.append(
            {
                "ModelKey": mk,
                "CleanFile": clean_keys.get(mk, ""),
                "InjectedFile": inj_keys.get(mk, ""),
            }
        )
    return pairs


def _sanity_print_folders(clean_dir: Path, inj_dir: Path, clean_index: Dict[str, Path], inj_index: Dict[str, Path]) -> None:
    print("============================================================")
    print("📌 SANITY CHECK: KR FOLDERS")
    print("============================================================")
    print(f"Root Feature          : {ROOT_FEATURE}")
    print(f"Clean KR folder       : {clean_dir}")
    print(f"Injected KR folder    : {inj_dir}")
    print(f"Clean KR files found  : {len(clean_index)}")
    print(f"Injected KR files found: {len(inj_index)}")

    if not clean_index:
        print("❌ CLEAN KR folder is empty or missing. Expected .kr.pl files in kr_outputs_10models")
    if not inj_index:
        print("❌ INJECTED KR folder is empty or missing. Expected .kr.pl files in prolog_facts_v5")

    if clean_index:
        ex = list(sorted(clean_index.keys()))[:5]
        print("Sample clean files    :", ex)
    if inj_index:
        ex = list(sorted(inj_index.keys()))[:5]
        print("Sample injected files :", ex)
    print("============================================================")


def build_final_report() -> Path:
    script_dir = Path(__file__).resolve().parent
    base_dir = script_dir.parent

    clean_dir = base_dir / CLEAN_KR_DIR_NAME
    inj_dir = base_dir / INJECTED_KR_DIR_NAME

    clean_index = index_kr_files(clean_dir)
    inj_index = index_kr_files(inj_dir)

    _sanity_print_folders(clean_dir, inj_dir, clean_index, inj_index)

    if not clean_index and not inj_index:
        raise FileNotFoundError(
            f"KR folders not found or empty.\n"
            f"Expected:\n  - {clean_dir}\n  - {inj_dir}\n"
            f"Or change CLEAN_KR_DIR_NAME / INJECTED_KR_DIR_NAME in the script."
        )

    # Optional enrichment
    opt_df = load_optional_results(script_dir).copy()
    if not opt_df.empty:
        opt_df["ModelKey"] = opt_df["Model"].astype(str).apply(derive_model_key)
        opt_df["Group_norm"] = opt_df["Group"].astype(str).str.lower().str.strip()
    else:
        opt_df = pd.DataFrame(columns=CANON_COLS + ["ModelKey", "Group_norm"])

    # Pairs KR-first
    pairs = build_pairs_from_kr(clean_index, inj_index)
    df_pairs = pd.DataFrame(pairs)

    # Analyze KR
    clean_analysis: Dict[str, KRAnalysis] = {}
    inj_analysis: Dict[str, KRAnalysis] = {}

    for row in pairs:
        mk = row["ModelKey"]

        cf = row["CleanFile"]
        if cf:
            kr_c = load_kr_model(clean_index[cf])
            clean_analysis[mk] = analyze_kr_for_defects(kr_c, mk, root=ROOT_FEATURE)

        inf = row["InjectedFile"]
        if inf:
            kr_i = load_kr_model(inj_index[inf])
            inj_analysis[mk] = analyze_kr_for_defects(kr_i, mk, root=ROOT_FEATURE)

    # Build RunTable rows
    run_rows: List[Dict[str, object]] = []
    for row in pairs:
        mk = row["ModelKey"]
        for grp in ["Clean", "Injected"]:
            filename = row["CleanFile"] if grp == "Clean" else row["InjectedFile"]
            if not filename:
                run_rows.append(
                    {
                        "Group": grp,
                        "Model": "",
                        "Root": ROOT_FEATURE,
                        "NF": 0,
                        "Constraints": 0,
                        "SAT": "",
                        "N_Dead": 0,
                        "N_FalseOptional": 0,
                        "TimeSec": 0.0,
                        "Defects": "",
                        "ModelKey": mk,
                    }
                )
                continue

            ana = clean_analysis.get(mk) if grp == "Clean" else inj_analysis.get(mk)
            nf_kr = ana.nf if ana else 0
            cons_kr = ana.constraints if ana else 0

            sat_val = ""
            tsec = 0.0
            nf_val = nf_kr
            cons_val = cons_kr
            n_dead = len(ana.dead_features) if ana else 0
            n_fo = len(ana.false_optional) if ana else 0
            defects = ";".join(ana.defects_raw) if ana else ""

            # Enrich from opt results if exists
            if not opt_df.empty:
                m = opt_df[(opt_df["ModelKey"] == mk) & (opt_df["Group_norm"] == grp.lower())]
                if not m.empty:
                    r = m.iloc[0]
                    sat_val = r.get("SAT", "")
                    tsec = float(r.get("TimeSec", 0.0))
                    try:
                        nf_opt = int(r.get("NF", 0))
                        if nf_opt > 0:
                            nf_val = nf_opt
                    except Exception:
                        pass
                    try:
                        c_opt = int(r.get("Constraints", 0))
                        if c_opt > 0:
                            cons_val = c_opt
                    except Exception:
                        pass
                    if str(r.get("Defects", "")).strip():
                        defects = str(r.get("Defects", "")).strip()

            run_rows.append(
                {
                    "Group": grp,
                    "Model": filename,
                    "Root": ROOT_FEATURE,
                    "NF": nf_val,
                    "Constraints": cons_val,
                    "SAT": sat_val,
                    "N_Dead": int(n_dead),
                    "N_FalseOptional": int(n_fo),
                    "TimeSec": float(tsec),
                    "Defects": defects,
                    "ModelKey": mk,
                }
            )

    df_all = pd.DataFrame(run_rows)
    df_clean = df_all[df_all["Group"].str.lower() == "clean"].copy()
    df_inj = df_all[df_all["Group"].str.lower() == "injected"].copy()

    # Verification: KR diff
    verify_rows: List[Dict[str, object]] = []
    for row in pairs:
        mk = row["ModelKey"]
        cf = row["CleanFile"]
        inf = row["InjectedFile"]

        injection_present = False
        new_imps: Set[Tuple[str, str]] = set()
        removed_imps: Set[Tuple[str, str]] = set()
        inferred_targets: List[str] = []
        invalid_targets: List[str] = []

        sat_from_results = ""
        nf_from_results = 0

        inj_row = df_inj[df_inj["ModelKey"] == mk]
        clean_row = df_clean[df_clean["ModelKey"] == mk]
        if not inj_row.empty:
            nf_from_results = int(inj_row.iloc[0].get("NF", 0))
            sat_from_results = inj_row.iloc[0].get("SAT", "")
        elif not clean_row.empty:
            nf_from_results = int(clean_row.iloc[0].get("NF", 0))
            sat_from_results = clean_row.iloc[0].get("SAT", "")
        else:
            nf_from_results = (
                inj_analysis.get(mk).nf if mk in inj_analysis
                else clean_analysis.get(mk).nf if mk in clean_analysis
                else 0
            )

        if cf and inf:
            kr_c = load_kr_model(clean_index[cf])
            kr_i = load_kr_model(inj_index[inf])
            s_c = imp_set(kr_c)
            s_i = imp_set(kr_i)
            new_imps = s_i - s_c
            removed_imps = s_c - s_i
            injection_present = len(new_imps) > 0

            inferred_targets = infer_targets_from_new_imps(new_imps)
            for t in inferred_targets:
                if t not in kr_i.features:
                    invalid_targets.append(t)

        ana_i = inj_analysis.get(mk)
        detected_raw = ";".join(ana_i.defects_raw) if ana_i else ""
        detected_filtered = ";".join(ana_i.defects_filtered) if ana_i else ""
        detected_any = bool(detected_filtered.strip())

        if not injection_present:
            status = "MISSING_INJECTION"
            reason = "No new KR implications detected between Clean and Injected (injection likely missing)."
        elif invalid_targets:
            status = "INVALID_TARGET"
            reason = "Injection targets are not present in injected model features."
        elif injection_present and not detected_any:
            status = "DETECTOR_MISS"
            reason = "Injection present (KR diff) but detector did not report defects."
        else:
            status = "OK"
            reason = "Injection present and defects detected."

        verify_rows.append(
            {
                "ModelKey": mk,
                "CleanFile": cf,
                "InjectedFile": inf,
                "NF_from_results": nf_from_results,
                "SAT_from_results": sat_from_results,
                "InjectionPresent": injection_present,
                "NewImpCount": len(new_imps),
                "RemovedImpCount": len(removed_imps),
                "NewImp": "; ".join([f"imp({a},{b})" for (a, b) in sorted(new_imps)])[:32000],
                "RemovedImp": "; ".join([f"imp({a},{b})" for (a, b) in sorted(removed_imps)])[:32000],
                "InferredTargets": ";".join(inferred_targets),
                "InvalidTargets": ";".join(invalid_targets),
                "DetectedDefects_RAW": detected_raw,
                "DetectedDefects_FILTERED": detected_filtered,
                "Status": status,
                "Reason": reason,
            }
        )

    df_verify = pd.DataFrame(verify_rows)

    # Tables for misses
    df_det_miss = df_verify[df_verify["Status"] == "DETECTOR_MISS"].copy()
    df_invalid = df_verify[df_verify["Status"] == "INVALID_TARGET"].copy()
    df_miss_inj = df_verify[df_verify["Status"] == "MISSING_INJECTION"].copy()
    df_unsat = df_verify[df_verify["Status"] == "UNSAT"].copy()  # placeholder

    # FP table (clean defects)
    fp_rows = []
    for mk, ana in clean_analysis.items():
        detected_filtered = ";".join(ana.defects_filtered)
        if detected_filtered.strip():
            fp_rows.append(
                {
                    "ModelKey": mk,
                    "Model": ana.filename,
                    "DetectedDefects_FILTERED": detected_filtered,
                    "ArtifactFlag": ana.is_visit_type_artifact,
                    "AuditType_OptionsCount": ana.audit_type_options_count,
                }
            )
    df_fp = pd.DataFrame(fp_rows)

    # FN: injection present but detector miss
    df_fn = df_det_miss.copy()

    # Delta time
    delta_rows = []
    for mk in sorted(set(df_clean["ModelKey"]) & set(df_inj["ModelKey"])):
        c = df_clean[df_clean["ModelKey"] == mk]
        i = df_inj[df_inj["ModelKey"] == mk]
        if c.empty or i.empty:
            continue
        tc = float(c.iloc[0].get("TimeSec", 0.0))
        ti = float(i.iloc[0].get("TimeSec", 0.0))
        nf = int(i.iloc[0].get("NF", 0))
        inj_present = False
        v = df_verify[df_verify["ModelKey"] == mk]
        if not v.empty:
            inj_present = bool(v.iloc[0].get("InjectionPresent", False))

        delta_rows.append(
            {
                "ModelKey": mk,
                "NF": nf,
                "TimeSec_Clean": tc,
                "TimeSec_Injected": ti,
                "DeltaTimeSec (Injected - Clean)": ti - tc,
                "InjectionPresent": inj_present,
            }
        )
    df_delta = pd.DataFrame(delta_rows)

    # Summary
    n_clean = int((df_all["Group"].str.lower() == "clean").sum())
    n_inj = int((df_all["Group"].str.lower() == "injected").sum())
    n_pairs = int(len(df_pairs))

    n_missing_injection = int((df_verify["Status"] == "MISSING_INJECTION").sum())
    n_detector_miss = int((df_verify["Status"] == "DETECTOR_MISS").sum())
    n_invalid = int((df_verify["Status"] == "INVALID_TARGET").sum())
    n_ok = int((df_verify["Status"] == "OK").sum())

    df_summary = pd.DataFrame(
        [
            {"Metric": "Root", "Value": ROOT_FEATURE},
            {"Metric": "Pairs(ModelKey)", "Value": n_pairs},
            {"Metric": "Rows_Clean", "Value": n_clean},
            {"Metric": "Rows_Injected", "Value": n_inj},
            {"Metric": "OK", "Value": n_ok},
            {"Metric": "MISSING_INJECTION", "Value": n_missing_injection},
            {"Metric": "DETECTOR_MISS", "Value": n_detector_miss},
            {"Metric": "INVALID_TARGET", "Value": n_invalid},
            {
                "Metric": "Note_FP_Artifact",
                "Value": "FO:AuditPlan can appear as false optional in Clean when AuditType has only 1 option (sampling artifact).",
            },
            {
                "Metric": "FutureWork_Strong",
                "Value": "Enforce minimum two options when sampling from optional/alternative groups (avoid singleton-induced logical artifacts).",
            },
        ]
    )

    # Write compatibility CSV
    out_csv = script_dir / SAT_RESULTS_CSV_NAME
    df_all[CANON_COLS].to_csv(out_csv, index=False, encoding="utf-8-sig")

    # Write ONE Excel workbook
    out_xlsx = script_dir / FINAL_REPORT_XLSX_NAME
    with pd.ExcelWriter(out_xlsx, engine="openpyxl") as writer:
        df_summary.to_excel(writer, sheet_name=SHEET_00_SUMMARY, index=False)
        df_all.to_excel(writer, sheet_name=SHEET_01_ALL, index=False)
        df_clean.to_excel(writer, sheet_name=SHEET_02_CLEAN, index=False)
        df_inj.to_excel(writer, sheet_name=SHEET_03_INJ, index=False)
        df_verify.to_excel(writer, sheet_name=SHEET_04_VERIFY, index=False)
        df_det_miss.to_excel(writer, sheet_name=SHEET_05_DET_MISS, index=False)
        df_invalid.to_excel(writer, sheet_name=SHEET_06_INVALID, index=False)
        df_miss_inj.to_excel(writer, sheet_name=SHEET_07_MISS_INJ, index=False)
        df_unsat.to_excel(writer, sheet_name=SHEET_08_UNSAT, index=False)
        df_fp.to_excel(writer, sheet_name=SHEET_09_FP, index=False)
        df_fn.to_excel(writer, sheet_name=SHEET_10_FN, index=False)
        df_delta.to_excel(writer, sheet_name=SHEET_11_DELTA, index=False)

    return out_xlsx


def main() -> None:
    out = build_final_report()
    print(f"✅ FINAL report generated: {out}")
    print(f"✅ Compatibility CSV written: {out.parent / SAT_RESULTS_CSV_NAME}")
    print("✅ Stable artifact: sheet names and core columns remain fixed.")


if __name__ == "__main__":
    main()
