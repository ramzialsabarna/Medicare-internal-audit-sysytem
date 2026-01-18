# final_logic_analyzer.py
# -------------------------------------------------------------
# SAT-based defect detection for KR(Prolog) Feature Models
# Detects:
#   - VOID model (UNSAT)
#   - Dead Feature (DF): UNSAT when forcing X=True
#   - False Optional (FO): structurally optional but UNSAT when forcing X=False
#
# Root handling (IMPORTANT):
#   - Canonical root is ALWAYS "InternalAuditSystem" if it exists as a feature.
#   - Only if missing, fallback to graph-based inference from p(child,parent).
#
# Optional (OFF by default): redundancy check of CNF clauses (expensive)
#
# Expected facts (examples):
#   feature(name).
#   p(child,parent).
#   group(parent,mandatory,[c1,c2]).
#   group(parent,optional,[x,y]).
#   group(parent,alternative,[u,v]).
#   group(parent,or,[m,n]).
#   imp(a,b).
#   imp(a,not(x)).
#
# Install:
#   pip install python-sat
# -------------------------------------------------------------

import re
import time
from pathlib import Path
from typing import List, Tuple, Dict, Set, Optional

import pandas as pd
from pysat.formula import CNF
from pysat.solvers import Solver


# ===================== USER SETTINGS =====================

BASE_DIR = Path(r"C:\Users\pc\Desktop\phd file draft\phd new\جامعه اشبيليه\برنامج الايزو\vs code\medicareinternalaudit")

KR_ORIGINAL_DIR = BASE_DIR / "kr_outputs_10models"
KR_INJECTED_DIR = BASE_DIR / "prolog_facts_v5"

OUTPUT_CSV = BASE_DIR / "structurecode" / "final_phd_validation_results_sat.csv"

# Canonical fixed root (your requirement)
CANONICAL_ROOT = "InternalAuditSystem"

# If True: formal redundancy test of CNF clauses (VERY expensive)
CHECK_REDUNDANCY = False

# SAT solver backends to try (fallback)
SOLVER_CANDIDATES = ["glucose3", "minisat22", "cadical153", "lingeling"]

# =========================================================


# -------------------- Robust parsing --------------------

_TOKEN = re.compile(r"[A-Za-z0-9_]+")

def extract_feature_facts(text: str) -> Set[str]:
    # feature(name).
    return set(re.findall(r"feature\(\s*([A-Za-z0-9_]+)\s*\)\s*\.", text))

def extract_p_edges(text: str) -> List[Tuple[str, str]]:
    # p(child,parent).
    pairs = re.findall(r"p\(\s*([A-Za-z0-9_]+)\s*,\s*([A-Za-z0-9_]+)\s*\)\s*\.", text)
    return [(c, p) for c, p in pairs]

def extract_groups(text: str) -> List[Tuple[str, str, List[str]]]:
    """
    group(parent,mandatory,[a,b,c]).
    group(parent,optional,[x,y]).
    group(parent,alternative,[u,v]).
    group(parent,or,[m,n]).
    """
    groups: List[Tuple[str, str, List[str]]] = []
    for parent, gtype, inside in re.findall(
        r"group\(\s*([A-Za-z0-9_]+)\s*,\s*(mandatory|optional|alternative|or)\s*,\s*\[(.*?)\]\s*\)\s*\.",
        text,
        flags=re.DOTALL
    ):
        children = _TOKEN.findall(inside)
        if children:
            groups.append((parent, gtype, children))
    return groups

def parse_term(term: str) -> Tuple[str, str]:
    """
    Parse terms in imp():
      - "x"      -> ("pos","x")
      - "not(x)" -> ("neg","x")
    """
    term = term.strip()
    m = re.match(r"not\(\s*([A-Za-z0-9_]+)\s*\)\s*$", term)
    if m:
        return ("neg", m.group(1))
    m2 = re.match(r"^\s*([A-Za-z0-9_]+)\s*$", term)
    if m2:
        return ("pos", m2.group(1))
    # unexpected format
    return ("pos", term)

def extract_imps(text: str) -> List[Tuple[Tuple[str, str], Tuple[str, str]]]:
    """
    imp(a,b). where a and b can be X or not(X)
    Returns [ ((polA,featA),(polB,featB)), ... ]
    """
    imps_raw = re.findall(r"imp\(\s*([^,]+?)\s*,\s*([^)]+?)\s*\)\s*\.", text)
    imps: List[Tuple[Tuple[str, str], Tuple[str, str]]] = []
    for a, b in imps_raw:
        imps.append((parse_term(a), parse_term(b)))
    return imps


# -------------------- Root inference (with canonical lock) --------------------

def infer_root(features: Set[str], p_edges: List[Tuple[str, str]]) -> str:
    """
    Root policy:
      1) If CANONICAL_ROOT exists as a feature, it is ALWAYS the root.
      2) Otherwise (rare/invalid KR), fallback to graph inference from p(child,parent).
      3) Otherwise, fallback to any feature or a default string.
    """
    if CANONICAL_ROOT in features:
        return CANONICAL_ROOT

    # Fallback: graph inference
    if p_edges:
        children = {c for c, _ in p_edges}
        parents = {p for _, p in p_edges}
        candidates = list(parents - children)

        for r in candidates:
            if r in features:
                return r
        if candidates:
            return candidates[0]

    return next(iter(features), CANONICAL_ROOT)


# -------------------- SAT helper --------------------

def make_solver(cnf: CNF) -> Solver:
    last_err = None
    for name in SOLVER_CANDIDATES:
        try:
            return Solver(name=name, bootstrap_with=cnf)
        except Exception as e:
            last_err = e
    raise RuntimeError(f"No SAT solver backend available. Last error: {last_err}")

def sat_is_satisfiable(cnf: CNF) -> bool:
    s = make_solver(cnf)
    try:
        return s.solve()
    finally:
        s.delete()

def sat_with_assumption(cnf: CNF, lit: int) -> bool:
    s = make_solver(cnf)
    try:
        return s.solve(assumptions=[lit])
    finally:
        s.delete()


# -------------------- SAT encoding --------------------

class FMEncoder:
    def __init__(self, features: Set[str]):
        self.features = sorted(features)
        self.var: Dict[str, int] = {f: i + 1 for i, f in enumerate(self.features)}

    def lit(self, pol: str, feat: str) -> Optional[int]:
        if feat not in self.var:
            return None
        v = self.var[feat]
        return v if pol == "pos" else -v

    def add_imp(self, cnf: CNF, a: Tuple[str, str], b: Tuple[str, str]) -> None:
        """
        a -> b  encoded as (~a v b)
        where a,b can be X or not(X)
        """
        la = self.lit(a[0], a[1])
        lb = self.lit(b[0], b[1])

        if la is None or lb is None:
            return

        cnf.append([-la, lb])

    def encode(
        self,
        root: str,
        p_edges: List[Tuple[str, str]],
        groups: List[Tuple[str, str, List[str]]],
        imps: List[Tuple[Tuple[str, str], Tuple[str, str]]],
    ) -> Tuple[CNF, Dict]:
        cnf = CNF()
        meta = {
            "root": root,
            "struct_optional": set(),
            "struct_mandatory": set(),
            "constraints_count": 0,
        }

        # Root must be selected (HARD constraint)
        if root in self.var:
            cnf.append([self.var[root]])
            meta["constraints_count"] += 1

        # Structure edges: child -> parent
        for child, parent in p_edges:
            if child in self.var and parent in self.var:
                cnf.append([-self.var[child], self.var[parent]])
                meta["constraints_count"] += 1

        # Groups semantics
        for parent, gtype, children in groups:
            if gtype == "optional":
                meta["struct_optional"].update([c for c in children if c in self.var])
            if gtype == "mandatory":
                meta["struct_mandatory"].update([c for c in children if c in self.var])

            # each child -> parent
            if parent in self.var:
                for c in children:
                    if c in self.var:
                        cnf.append([-self.var[c], self.var[parent]])
                        meta["constraints_count"] += 1

            # mandatory: parent -> child
            if gtype == "mandatory" and parent in self.var:
                for c in children:
                    if c in self.var:
                        cnf.append([-self.var[parent], self.var[c]])
                        meta["constraints_count"] += 1

            # alternative: if parent then exactly one child
            if gtype == "alternative" and parent in self.var:
                lits = [self.var[c] for c in children if c in self.var]
                if lits:
                    # at least one
                    cnf.append([-self.var[parent]] + lits)
                    meta["constraints_count"] += 1
                    # at most one
                    for i in range(len(lits)):
                        for j in range(i + 1, len(lits)):
                            cnf.append([-lits[i], -lits[j]])
                            meta["constraints_count"] += 1

            # or-group: if parent then at least one child
            if gtype == "or" and parent in self.var:
                lits = [self.var[c] for c in children if c in self.var]
                if lits:
                    cnf.append([-self.var[parent]] + lits)
                    meta["constraints_count"] += 1

        # Cross-tree implications
        for a, b in imps:
            self.add_imp(cnf, a, b)
            meta["constraints_count"] += 1

        return cnf, meta


# -------------------- Defect checks --------------------

def find_dead_features(cnf: CNF, enc: FMEncoder) -> List[str]:
    dead: List[str] = []
    for f, v in enc.var.items():
        if not sat_with_assumption(cnf, v):
            dead.append(f)
    return dead

def find_false_optional(cnf: CNF, enc: FMEncoder, struct_optional: Set[str]) -> List[str]:
    fo: List[str] = []
    for f in sorted(struct_optional):
        v = enc.var.get(f)
        if not v:
            continue
        if not sat_with_assumption(cnf, -v):
            fo.append(f)
    return fo

def check_redundant_clauses(cnf: CNF) -> int:
    redundant = 0
    clauses = list(cnf.clauses)

    for idx, clause in enumerate(clauses):
        cnf_wo = CNF()
        cnf_wo.extend([c for j, c in enumerate(clauses) if j != idx])

        for lit in clause:
            cnf_wo.append([-lit])

        if not sat_is_satisfiable(cnf_wo):
            redundant += 1

    return redundant


# -------------------- Per-file analysis --------------------

def analyze_kr_text(text: str) -> Dict:
    features = extract_feature_facts(text)
    p_edges = extract_p_edges(text)
    groups = extract_groups(text)
    imps = extract_imps(text)

    root = infer_root(features, p_edges)

    # Warning if canonical root missing (should not happen)
    if root != CANONICAL_ROOT:
        # If the canonical root is missing, we still proceed, but we flag this in output.
        root_note = f"WARNING:canonical_root_missing({CANONICAL_ROOT})"
    else:
        root_note = ""

    enc = FMEncoder(features)
    cnf, meta = enc.encode(root=root, p_edges=p_edges, groups=groups, imps=imps)

    is_sat = sat_is_satisfiable(cnf)

    result = {
        "Root": root,
        "RootNote": root_note,
        "NF": len(features),
        "Constraints": meta["constraints_count"],
        "SAT": is_sat,
        "DeadFeatures": [],
        "FalseOptional": [],
        "RedundantClauses": None,
    }

    if not is_sat:
        return result

    result["DeadFeatures"] = find_dead_features(cnf, enc)
    result["FalseOptional"] = find_false_optional(cnf, enc, meta["struct_optional"])

    if CHECK_REDUNDANCY:
        result["RedundantClauses"] = check_redundant_clauses(cnf)

    return result


# -------------------- Batch runner --------------------

def run_analysis():
    targets = [
        {"path": KR_ORIGINAL_DIR, "type": "Original (Clean)"},
        {"path": KR_INJECTED_DIR, "type": "Injected (Scientific)"},
    ]

    all_rows = []

    for target in targets:
        folder = target["path"]
        if not folder.exists():
            print(f"⚠️ Skipping missing folder: {folder}")
            continue

        kr_files = sorted(folder.glob("*.kr.pl"))
        if not kr_files:
            print(f"⚠️ No .kr.pl files found in: {folder}")
            continue

        for kr_file in kr_files:
            t0 = time.perf_counter()
            content = kr_file.read_text(encoding="utf-8", errors="replace")

            try:
                info = analyze_kr_text(content)
                elapsed = time.perf_counter() - t0

                defects = []
                if not info["SAT"]:
                    defects.append("VOID_MODEL")
                else:
                    defects.extend([f"DF:{f}" for f in info["DeadFeatures"]])
                    defects.extend([f"FO:{f}" for f in info["FalseOptional"]])
                    if info["RedundantClauses"] is not None:
                        defects.append(f"RE_CLAUSES:{info['RedundantClauses']}")

                if info["RootNote"]:
                    defects.append(info["RootNote"])

                all_rows.append({
                    "Group": target["type"],
                    "Model": kr_file.name,
                    "Root": info["Root"],
                    "NF": info["NF"],
                    "Constraints": info["Constraints"],
                    "SAT": "SAT" if info["SAT"] else "UNSAT",
                    "N_Dead": len(info["DeadFeatures"]) if info["SAT"] else "",
                    "N_FalseOptional": len(info["FalseOptional"]) if info["SAT"] else "",
                    "TimeSec": f"{elapsed:.6f}",
                    "Defects": "; ".join(defects) if defects else "None",
                })

            except Exception as e:
                elapsed = time.perf_counter() - t0
                all_rows.append({
                    "Group": target["type"],
                    "Model": kr_file.name,
                    "Root": "",
                    "NF": "",
                    "Constraints": "",
                    "SAT": "ERROR",
                    "N_Dead": "",
                    "N_FalseOptional": "",
                    "TimeSec": f"{elapsed:.6f}",
                    "Defects": f"ERROR:{type(e).__name__}:{e}",
                })

    df = pd.DataFrame(all_rows)
    OUTPUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUTPUT_CSV, index=False, encoding="utf-8-sig")
    print(f"✅ Results saved to: {OUTPUT_CSV}")


if __name__ == "__main__":
    run_analysis()
