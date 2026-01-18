from pathlib import Path
import csv
import os
import re
import subprocess
from collections import deque

# =========================
# CONFIG
# =========================
UVL_DIR = Path("uvl_outputs_10models/ISO_DATA")
OUT_DIR = Path("../fig_uvl_c4_fmstyle")
PATTERN = "ISO_DATA__Reduced_c4_M*.uvl"
MAX_DEPTH = 6  # prune for readability

# If dot not in PATH, set one of these:
DOT_CANDIDATES = [
    os.environ.get("GRAPHVIZ_DOT", "").strip().strip('"'),
    r"C:\Program Files\Graphviz\bin\dot.exe",
    r"C:\Program Files (x86)\Graphviz\bin\dot.exe",
    r"C:\Program Files (x86)\windows_10_cmake_Release_Graphviz-14.1.1-win64\Graphviz-14.1.1-win64\bin\dot.exe",
]
# =========================


# ---------- helpers ----------
def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)

def indent_level(line: str) -> int:
    return (len(line) - len(line.lstrip(" "))) // 4

def is_group_kw(token: str) -> bool:
    return token in ("mandatory", "optional", "alternative", "or")

def clean_feat(token: str) -> str:
    return token.replace("{abstract}", "").strip()

def find_dot() -> str:
    # try PATH first
    for cmd in ("dot.exe", "dot"):
        try:
            subprocess.check_output([cmd, "-V"], stderr=subprocess.STDOUT)
            return cmd
        except Exception:
            pass
    # try candidates
    for p in DOT_CANDIDATES:
        if p and Path(p).exists():
            return p
    raise FileNotFoundError("Graphviz dot not found. Set env GRAPHVIZ_DOT or install Graphviz.")

def short_id(name: str) -> str:
    # keep short names as-is
    if len(name) <= 35 and not name.startswith("item_"):
        return name
    # normalize to safe id
    base = re.sub(r"[^A-Za-z0-9_]+", "_", name).strip("_")
    # compress
    return "F_" + str(abs(hash(base)) % 10**10)

# ---------- parser (captures groups) ----------
def parse_uvl_tree_with_groups(uvl_text: str):
    """
    Build a lightweight AST from 'features' section:
    Each feature node may have a current group mode among its children:
    - mandatory / optional / alternative / or
    Children under that group inherit that relationship.
    """
    lines = uvl_text.splitlines()
    try:
        i0 = next(i for i, l in enumerate(lines) if l.strip().lower() == "features")
    except StopIteration:
        return None

    root = None
    stack = []  # (lvl, node_name)
    current_group = {}  # node_name -> group_kw currently active for its children

    edges = []  # (parent, child, rel) where rel in {mandatory, optional, alternative, or, plain}

    for line in lines[i0+1:]:
        if line.strip().lower() == "constraints":
            break
        if not line.strip():
            continue
        lvl = indent_level(line)
        token = line.strip()

        # group keyword line => set group mode for current parent
        if is_group_kw(token):
            if stack:
                current_group[stack[-1][1]] = token
            continue

        feat = clean_feat(token)
        if not feat:
            continue

        if root is None:
            root = feat

        while stack and stack[-1][0] >= lvl:
            stack.pop()

        if stack:
            parent = stack[-1][1]
            rel = current_group.get(parent, "plain")
            edges.append((parent, feat, rel))

        stack.append((lvl, feat))

    return root, edges


def prune_by_depth(root: str, edges, max_depth: int):
    children = {}
    for p, c, rel in edges:
        children.setdefault(p, []).append((c, rel))

    keep = set()
    q = deque([(root, 0)])
    while q:
        n, d = q.popleft()
        if n in keep:
            continue
        keep.add(n)
        if d >= max_depth:
            continue
        for ch, _ in children.get(n, []):
            q.append((ch, d+1))

    return [(p, c, rel) for (p, c, rel) in edges if p in keep and c in keep]


def build_dot_fmstyle(root: str, edges, out_dot: Path, label_map_rows):
    """
    FM-style graph:
    - No arrows
    - Optional edges dashed
    - Group nodes for OR / ALT
    """
    # Build id mapping
    sid = {}
    def get_id(name):
        if name not in sid:
            sid[name] = short_id(name)
            if sid[name] != name:
                label_map_rows.append((sid[name], name))
        return sid[name]

    dot = []
    dot.append("graph FM {")
    dot.append("  rankdir=TB;")
    dot.append("  splines=ortho;")
    dot.append("  node [shape=box, fontsize=10];")
    dot.append("  edge [dir=none];")

    # declare nodes
    all_nodes = set([root])
    for p, c, rel in edges:
        all_nodes.add(p); all_nodes.add(c)

    for n in all_nodes:
        nid = get_id(n)
        # show short label (id itself)
        dot.append(f'  "{nid}" [label="{nid}"];')

    # group nodes
    gcount = 0
    for p, c, rel in edges:
        pid = get_id(p)
        cid = get_id(c)

        if rel in ("alternative", "or"):
            # create a group node per parent+rel to make it readable
            gcount += 1
            gid = f"G{gcount}_{rel.upper()}"
            shape = "diamond" if rel == "alternative" else "circle"
            label = "XOR" if rel == "alternative" else "OR"
            dot.append(f'  "{gid}" [shape={shape}, label="{label}", fontsize=9, width=0.3, height=0.3];')
            dot.append(f'  "{pid}" -- "{gid}" [style=solid];')
            dot.append(f'  "{gid}" -- "{cid}" [style=solid];')
        else:
            style = "dashed" if rel == "optional" else "solid"
            dot.append(f'  "{pid}" -- "{cid}" [style={style}];')

    dot.append("}")
    out_dot.write_text("\n".join(dot), encoding="utf-8")


def dot_to_svg(dot_exe: str, dot_path: Path, svg_path: Path) -> None:
    subprocess.check_call([dot_exe, "-Tsvg", str(dot_path), "-o", str(svg_path)])


def main():
    if not UVL_DIR.exists():
        raise FileNotFoundError(f"UVL_DIR not found: {UVL_DIR.resolve()}")

    ensure_dir(OUT_DIR)
    files = sorted(UVL_DIR.glob(PATTERN))
    if not files:
        raise FileNotFoundError(f"No UVL files found: {UVL_DIR.resolve()}/{PATTERN}")

    dot_exe = find_dot()
    print(f"🧩 Using Graphviz: {dot_exe}")

    label_map = []  # (short_id, original)

    for f in files:
        text = f.read_text(encoding="utf-8", errors="replace")
        parsed = parse_uvl_tree_with_groups(text)
        if not parsed:
            print(f"⚠️  No features section in: {f.name}")
            continue

        root, edges = parsed
        edges = prune_by_depth(root, edges, MAX_DEPTH)

        out_dot = OUT_DIR / (f.stem + ".fm.dot")
        out_svg = OUT_DIR / (f.stem + ".fm.svg")

        build_dot_fmstyle(root, edges, out_dot, label_map)
        dot_to_svg(dot_exe, out_dot, out_svg)

        print(f"✅ {f.name} -> {out_svg.name}")

    # write mapping table
    map_path = OUT_DIR / "label_map.csv"
    with map_path.open("w", newline="", encoding="utf-8") as fp:
        w = csv.writer(fp)
        w.writerow(["short_id", "original_feature_name"])
        w.writerows(sorted(set(label_map)))

    print("\n✅ Done.")
    print(f"📁 Output folder: {OUT_DIR.resolve()}")
    print(f"🗂  Mapping table: {map_path.resolve()}")


if __name__ == "__main__":
    main()
