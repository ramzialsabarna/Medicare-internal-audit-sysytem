# batch_uvl_to_kr.py
# ------------------------------------------------------------
# Batch Transformer: UVL (*.uvl) -> KR Facts (*.kr.pl)
# - Recursively scans input directory
# - Parses UVL (features tree + groups + constraints)
# - Writes KR as Prolog-like facts
# - Produces summary CSV
# ------------------------------------------------------------

from __future__ import annotations
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Dict, Optional, Tuple
import re
import csv
import argparse


GROUP_KINDS = {"mandatory", "optional", "alternative", "or"}


@dataclass
class FeatureNode:
    name: str
    is_abstract: bool = False
    parent: Optional[str] = None


@dataclass
class Group:
    parent: str
    kind: str  # mandatory|optional|alternative|or
    children: List[str] = field(default_factory=list)


def _strip_inline_comment(s: str) -> str:
    # Remove inline comments if any style appears
    s = s.split("//", 1)[0]
    s = s.split("#", 1)[0]
    return s.rstrip("\n")


def _parse_feature_decl(line: str) -> Tuple[str, bool]:
    # Examples:
    #   InternalAuditSystem {abstract}
    #   planned
    m = re.match(r"^([A-Za-z_][A-Za-z0-9_]*)\s*(\{abstract\})?\s*$", line.strip())
    if not m:
        raise ValueError(f"Cannot parse feature declaration: {line!r}")
    return m.group(1), bool(m.group(2))


def _normalize_constraint(line: str) -> Optional[str]:
    s = line.strip()
    if not s:
        return None

    # Normalize "!X" -> "not(X)" for tokens
    s = re.sub(r"!\s*([A-Za-z_][A-Za-z0-9_]*)", r"not(\1)", s)

    # Normalize operators spacing
    s = re.sub(r"\s+", " ", s).strip()
    return s


def parse_uvl(uvl_path: str) -> Tuple[Dict[str, FeatureNode], List[Group], List[str], str]:
    """
    Returns:
      features: dict name -> FeatureNode(parent, abstract)
      groups: list of Group(parent, kind, children)
      constraints: list of normalized constraints strings
      namespace: extracted namespace
    """
    path = Path(uvl_path)
    lines = path.read_text(encoding="utf-8").splitlines()

    namespace = ""
    section: Optional[str] = None  # None | "features" | "constraints"

    # Stack of (indent, feature_name)
    stack: List[Tuple[int, str]] = []

    # pending group kind per indent level (indent where group keyword appears)
    pending_group_kind: Dict[int, str] = {}

    features: Dict[str, FeatureNode] = {}
    groups: List[Group] = []
    constraints: List[str] = []

    def current_parent(indent: int) -> Optional[str]:
        # closest feature with smaller indent
        for i in range(len(stack) - 1, -1, -1):
            if stack[i][0] < indent:
                return stack[i][1]
        return None

    def nearest_pending_group_indent(child_indent: int) -> Optional[int]:
        cands = [gi for gi in pending_group_kind.keys() if gi < child_indent]
        if not cands:
            return None
        return max(cands)

    def add_child_to_group(parent: str, kind: str, child: str) -> None:
        for g in groups:
            if g.parent == parent and g.kind == kind:
                if child not in g.children:
                    g.children.append(child)
                return
        groups.append(Group(parent=parent, kind=kind, children=[child]))

    for raw in lines:
        raw = raw.rstrip("\n")
        stripped = _strip_inline_comment(raw).strip()
        if not stripped:
            continue

        if stripped.lower().startswith("namespace "):
            namespace = stripped.split(None, 1)[1].strip()
            continue

        if stripped.lower() == "features":
            section = "features"
            continue

        if stripped.lower() == "constraints":
            section = "constraints"
            continue

        if section == "constraints":
            c = _normalize_constraint(stripped)
            if c:
                constraints.append(c)
            continue

        if section != "features":
            continue

        # indent = leading spaces count
        indent = len(raw) - len(raw.lstrip(" "))

        # group keyword line
        token = stripped
        if token in GROUP_KINDS:
            pending_group_kind[indent] = token
            continue

        # feature declaration line
        feat_name, is_abs = _parse_feature_decl(token)
        parent = current_parent(indent)

        # pop stack to correct depth
        while stack and stack[-1][0] >= indent:
            stack.pop()
        stack.append((indent, feat_name))

        if feat_name not in features:
            features[feat_name] = FeatureNode(name=feat_name, is_abstract=is_abs, parent=parent)
        else:
            # update if later line marks abstract or parent found
            if is_abs:
                features[feat_name].is_abstract = True
            if parent:
                features[feat_name].parent = parent

        # attach to nearest pending group
        if parent:
            gi = nearest_pending_group_indent(indent)
            if gi is not None:
                kind = pending_group_kind[gi]
                add_child_to_group(parent=parent, kind=kind, child=feat_name)

    # Sort children inside each group for stable output
    for g in groups:
        g.children = sorted(set(g.children))

    return features, groups, constraints, namespace


def emit_kr_facts(features: Dict[str, FeatureNode], groups: List[Group], constraints: List[str], namespace: str) -> str:
    out: List[str] = []

    if namespace:
        out.append(f"% namespace: {namespace}")

    # Features
    for fname in sorted(features.keys()):
        out.append(f"feature({fname}).")
        if features[fname].is_abstract:
            out.append(f"abstract({fname}).")

    # Parent links
    for fname in sorted(features.keys()):
        p = features[fname].parent
        if p:
            out.append(f"p({fname},{p}).")

    # Groups
    for g in sorted(groups, key=lambda x: (x.parent, x.kind)):
        children = ",".join(g.children)
        out.append(f"group({g.parent},{g.kind},[{children}]).")

    # Constraints -> facts (simple mapping + fallback raw)
    for c in constraints:
        # A => B  (A or not(A), same for B)
        m = re.match(
            r"^\s*(not\([A-Za-z_][A-Za-z0-9_]*\)|[A-Za-z_][A-Za-z0-9_]*)\s*=>\s*"
            r"(not\([A-Za-z_][A-Za-z0-9_]*\)|[A-Za-z_][A-Za-z0-9_]*)\s*$",
            c,
        )
        if m:
            out.append(f"imp({m.group(1)},{m.group(2)}).")
            continue

        # A <=> (B | C | ...)
        m2 = re.match(
            r"^\s*(not\([A-Za-z_][A-Za-z0-9_]*\)|[A-Za-z_][A-Za-z0-9_]*)\s*<=>\s*\((.+)\)\s*$",
            c,
        )
        if m2:
            left = m2.group(1)
            rhs = m2.group(2)
            parts = [x.strip() for x in rhs.split("|") if x.strip()]
            for r in parts:
                out.append(f"equiv_or({left},{r}).")
            continue

        # fallback
        out.append(f"constraint_raw({c!r}).")

    return "\n".join(out) + "\n"


def transform_one(uvl_file: Path, out_dir: Path) -> Dict[str, int]:
    features, groups, constraints, namespace = parse_uvl(str(uvl_file))
    kr_text = emit_kr_facts(features, groups, constraints, namespace)

    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{uvl_file.stem}.kr.pl"
    out_path.write_text(kr_text, encoding="utf-8")

    return {
        "features": len(features),
        "groups": len(groups),
        "constraints": len(constraints),
    }


def run_batch(input_dir: str, output_dir: str, pattern: str = "*.uvl") -> None:
    in_dir = Path(input_dir)
    out_dir = Path(output_dir)

    if not in_dir.exists():
        raise FileNotFoundError(f"Input directory not found: {in_dir.resolve()}")

    uvl_files = sorted(in_dir.rglob(pattern))
    if not uvl_files:
        print(f"⚠️ No UVL files found under: {in_dir.resolve()} with pattern: {pattern}")
        return

    summary_rows = []

    print(f"📂 Input : {in_dir.resolve()}")
    print(f"📁 Output: {out_dir.resolve()}")
    print(f"🔎 Found : {len(uvl_files)} UVL files")
    print("-" * 80)

    ok = 0
    fail = 0

    for f in uvl_files:
        try:
            stats = transform_one(f, out_dir)
            ok += 1
            print(f"✅ {f.name} -> {f.stem}.kr.pl | "
                  f"features={stats['features']} groups={stats['groups']} constraints={stats['constraints']}")
            summary_rows.append({
                "uvl_file": str(f),
                "kr_file": str((out_dir / f"{f.stem}.kr.pl")),
                **stats
            })
        except Exception as e:
            fail += 1
            print(f"❌ FAIL {f.name}: {e}")
            summary_rows.append({
                "uvl_file": str(f),
                "kr_file": "",
                "features": 0,
                "groups": 0,
                "constraints": 0,
                "error": str(e),
            })

    # Write CSV summary
    csv_path = out_dir / "kr_summary.csv"
    fieldnames = ["uvl_file", "kr_file", "features", "groups", "constraints", "error"]
    with csv_path.open("w", newline="", encoding="utf-8") as fp:
        w = csv.DictWriter(fp, fieldnames=fieldnames)
        w.writeheader()
        for r in summary_rows:
            if "error" not in r:
                r["error"] = ""
            w.writerow({k: r.get(k, "") for k in fieldnames})

    print("-" * 80)
    print(f"✅ Done. success={ok} fail={fail}")
    print(f"📄 Summary CSV: {csv_path.resolve()}")


def main():
    ap = argparse.ArgumentParser(description="Batch UVL -> KR facts transformer")
    ap.add_argument("--input", "-i", required=True, help="Input folder containing .uvl files (recursive)")
    ap.add_argument("--output", "-o", required=True, help="Output folder to store .kr.pl files")
    ap.add_argument("--pattern", "-p", default="*.uvl", help="Glob pattern (default: *.uvl)")
    args = ap.parse_args()
    run_batch(args.input, args.output, args.pattern)


if __name__ == "__main__":
    main()
