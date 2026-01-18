from pathlib import Path
import pandas as pd

# Run this script from inside structurecode folder:
from uvl_builder import build_uvl_from_structure

FILE_PREFIX = "ISO_DATA"
REDUCED_PREFIX = "Reduced_"  # matches Reduced_cX_M01 etc
NAMESPACE = "MedicareAuditStructure"
OUTPUT_ROOT = "uvl_outputs_10models"

def run(folder="."):
    folder_path = Path(folder)
    out_root = folder_path / OUTPUT_ROOT
    out_root.mkdir(parents=True, exist_ok=True)

    files = sorted(folder_path.glob(f"{FILE_PREFIX}*.xlsx"))
    if not files:
        print(f"❌ No files found matching {FILE_PREFIX}*.xlsx in {folder_path.resolve()}")
        return

    for xlsx_path in files:
        print("\n" + "=" * 95)
        print(f"📘 UVL generation for: {xlsx_path.name}")
        print("=" * 95)

        xl = pd.ExcelFile(xlsx_path)
        reduced_sheets = [s for s in xl.sheet_names if str(s).startswith(REDUCED_PREFIX)]

        if not reduced_sheets:
            print("⚠️  No Reduced_* sheets found.")
            continue

        file_out_dir = out_root / xlsx_path.stem
        file_out_dir.mkdir(parents=True, exist_ok=True)

        for sheet in reduced_sheets:
            out_uvl = file_out_dir / f"{xlsx_path.stem}__{sheet}.uvl"

            build_uvl_from_structure(
                input_xlsx=str(xlsx_path),
                sheet_name=sheet,
                uvl_out_path=str(out_uvl),
                namespace=NAMESPACE,
                report_to_terminal=True,
                require_answers=True,
            )

            print(f"✅ {sheet} -> {out_uvl.name}")

    print("\n✅ All done.")

if __name__ == "__main__":
    run(".")
