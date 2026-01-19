import sys
import subprocess
from pathlib import Path

# =========================
# PROJECT ROOT (عدل إذا لزم)
# =========================
BASE_DIR = Path(r"C:\Users\pc\Desktop\phd file draft\phd new\جامعه اشبيليه\برنامج الايزو\vs code\medicareinternalaudit")

# -------------------------
# UVL INPUTS (الحقيقية عندك)
# -------------------------
CLEAN_UVL_DIR = BASE_DIR / "structurecode" / "uvl_outputs_10models" / "ISO_DATA"
INJECTED_UVL_DIR = BASE_DIR / "uvl_scientific_defects_v5"

# -------------------------
# KR OUTPUTS (كما يتوقعها الـ Analyzer)
# -------------------------
CLEAN_KR_DIR = BASE_DIR / "kr_outputs_10models"
INJECTED_KR_DIR = BASE_DIR / "prolog_facts_v5"

# -------------------------
# Path to transformer script
# (موجود داخل structurecode)
# -------------------------
TRANSFORMER_SCRIPT = BASE_DIR / "structurecode" / "batch_uvl_to_kr.py"


def run_transform(input_dir: Path, output_dir: Path, label: str) -> None:
    print(f"\n>>> [{label}] UVL -> KR")
    print(f"    Input : {input_dir}")
    print(f"    Output: {output_dir}")

    if not input_dir.exists():
        raise FileNotFoundError(f"[{label}] Input directory not found: {input_dir}")

    output_dir.mkdir(parents=True, exist_ok=True)

    cmd = [
        sys.executable,
        str(TRANSFORMER_SCRIPT),
        "-i", str(input_dir),
        "-o", str(output_dir),
    ]

    # مهم: نجعل الـ working directory هو structurecode
    # لكي أي مراجع نسبية داخل السكربت تكون آمنة
    subprocess.run(cmd, check=True, cwd=str(TRANSFORMER_SCRIPT.parent))


if __name__ == "__main__":
    # 1) Clean
    run_transform(CLEAN_UVL_DIR, CLEAN_KR_DIR, "CLEAN")

    # 2) Injected
    run_transform(INJECTED_UVL_DIR, INJECTED_KR_DIR, "INJECTED")

    print("\n✅ All transformations completed successfully!")
