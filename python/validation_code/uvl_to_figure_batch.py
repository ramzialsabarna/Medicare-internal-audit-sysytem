from pathlib import Path
import os
import re
import subprocess
from collections import deque

# =========================
# CONFIG (adjust if needed)
# =========================
UVL_DIR = Path("uvl_outputs_10models/ISO_DATA")          # where UVL files are
OUT_DIR = Path("../fig_uvl_c4")                         # output folder (SVG + DOT)
PATTERN = "ISO_DATA__Reduced_c4_M*.uvl"                 # files pattern
MAX_DEPTH = 6                                           # prune depth for readability (5-7 recommended)

# Optional: set env var GRAPHVIZ_DOT to full dot.exe path
# Example (PowerShell):
#   $env:GRAPHVIZ_DOT="C:\...\bin\dot.exe"
# =========================


def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def indent_level(line: str) -> int:
    return (len(line) - len(line.lstrip(" "))) // 4


def clean_feat_name(s: str) -> str:
    s = s.strip()
    return s.replace("{abstract}", "").strip()


def parse_uvl_features_tree(uvl_text: str):
    """
    Parse ONLY the 'features' section of UVL into parent-child edges based on indentation.
    Skips group keywords: mandatory/optional/alternative/or.
    """
    lines = uvl_text.splitlines()

    try:
        i0 = next(i for i, l in enumerate(lines) if l.strip().lower() == "features")
    except StopIteration:
        return [], None

    edges = []
    stack = []  # list[(level, feature)]

    root = None

    for line in lines[i0 + 1 :]:
        if line.strip().lower() == "constraints":
            break
        if not line.strip():
            continue

        lvl = indent_level(line)
        token = line.strip()

        if token in ("mandatory", "optional", "alternative", "or"):
            continue

        feat = clean_feat_name(token)
        if not feat:
            continue

        if root is None:
            root = feat

        while stack and stack[-1][0] >= lvl:
            stack.pop()

        if stack:
            parent = stack[-1][1]
            edges.append((parent, feat))

        stack.append((lvl, feat))

    return edges, root


def prune_edges_by_depth(edges, root, max_depth: int):
    if not root:
        return edges

    children = {}
    for p, c in edges:
        children.setdefault(p, []).append(c)

    keep = set()
    q = deque([(root, 0)])
    while q:
        node, d = q.popleft()
        if node in keep:
            continue
        keep.add(node)
        if d >= max_depth:
            continue
        for ch in children.get(node, []):
            q.append((ch, d + 1))

    return [(p, c) for (p, c) in edges if p in keep and c in keep]


def write_dot(edges, root, out_dot: Path, max_depth: int) -> None:
    pruned_edges = prune_edges_by_depth(edges, root, max_depth)

    dot_lines = [
        "digraph G {",
        "  rankdir=TB;",
        "  node [shape=box];",
    ]
    for p, c in pruned_edges:
        dot_lines.append(f'  "{p}" -> "{c}";')
    dot_lines.append("}")

    out_dot.write_text("\n".join(dot_lines), encoding="utf-8")


def find_dot_exe() -> str:
    """
    Find Graphviz dot executable robustly on Windows.
    Priority:
      1) env var GRAPHVIZ_DOT
      2) 'dot' in PATH
      3) common install locations
    """
    # 1) env var
    env_path = os.environ.get("GRAPHVIZ_DOT", "").strip().strip('"')
    if env_path and Path(env_path).exists():
        return env_path

    # 2) PATH
    for cmd in ("dot.exe", "dot"):
        try:
            subprocess.check_output([cmd, "-V"], stderr=subprocess.STDOUT)
            return cmd
        except Exception:
            pass

    # 3) common locations
    candidates = [
        r"C:\Program Files\Graphviz\bin\dot.exe",
        r"C:\Program Files (x86)\Graphviz\bin\dot.exe",
        # Your case (common when unpacked)
        r"C:\Program Files (x86)\windows_10_cmake_Release_Graphviz-14.1.1-win64\Graphviz-14.1.1-win64\bin\dot.exe",
        r"C:\Program Files\windows_10_cmake_Release_Graphviz-14.1.1-win64\Graphviz-14.1.1-win64\bin\dot.exe",
    ]
    for p in candidates:
        if Path(p).exists():
            return p

    raise FileNotFoundError(
        "Graphviz 'dot' not found.\n"
        "Fix options:\n"
        "1) Add Graphviz bin to PATH, then restart PowerShell.\n"
        "2) Or set env var GRAPHVIZ_DOT to full path of dot.exe, e.g.:\n"
        "   PowerShell:  $env:GRAPHVIZ_DOT='C:\\...\\bin\\dot.exe'\n"
    )


def dot_to_svg(dot_exe: str, dot_path: Path, svg_path: Path) -> None:
    subprocess.check_call([dot_exe, "-Tsvg", str(dot_path), "-o", str(svg_path)])


def main():
    if not UVL_DIR.exists():
        raise FileNotFoundError(f"UVL_DIR not found: {UVL_DIR.resolve()}")

    ensure_dir(OUT_DIR)

    files = sorted(UVL_DIR.glob(PATTERN))
    if not files:
        raise FileNotFoundError(f"No UVL files found: {UVL_DIR.resolve()}/{PATTERN}")

    dot_exe = find_dot_exe()
    print(f"🧩 Using Graphviz: {dot_exe}")

    for f in files:
        text = f.read_text(encoding="utf-8", errors="replace")
        edges, root = parse_uvl_features_tree(text)

        dot_path = OUT_DIR / (f.stem + ".dot")
        svg_path = OUT_DIR / (f.stem + ".svg")

        write_dot(edges, root, dot_path, max_depth=MAX_DEPTH)
        dot_to_svg(dot_exe, dot_path, svg_path)

        print(f"✅ {f.name} -> {svg_path.name}")

    print("\n✅ Done.")
    print(f"📁 Output folder: {OUT_DIR.resolve()}")


if __name__ == "__main__":
    main()
