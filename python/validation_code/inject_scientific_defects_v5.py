import os
import random
import re
import pandas as pd

# ============================================================
# SCIENTIFIC DEFECT INJECTION (V5)
# - Reads clean UVL files
# - Injects: DF1 (Dead Feature), FO (False Optional), RE (Redundancy-like)
# - Writes injected UVL files with SAME stem + "_injected" suffix
#   so the analyzer can pair clean vs injected correctly.
# - Saves an oracle key (Excel + CSV)
# ============================================================

# --- إعدادات المسارات ---
BASE_DIR = r"C:\Users\pc\Desktop\phd file draft\phd new\جامعه اشبيليه\برنامج الايزو\vs code\medicareinternalaudit"

# IMPORTANT: ضع هنا مسار UVL النظيفة الحقيقي لديك
SOURCE_DIR = os.path.join(BASE_DIR, "structurecode", "uvl_outputs_10models", "ISO_DATA")

# Output injected UVLs
OUTPUT_DIR = os.path.join(BASE_DIR, "uvl_scientific_defects_v5")
os.makedirs(OUTPUT_DIR, exist_ok=True)

ROOT = "InternalAuditSystem"

_RESERVED = {
    "features", "constraints", "mandatory", "optional", "alternative", "or", "namespace"
}


def _read_lines_any_encoding(file_path: str):
    for enc in ("utf-8", "utf-8-sig", "latin-1"):
        try:
            with open(file_path, "r", encoding=enc) as f:
                return f.readlines()
        except Exception:
            continue
    raise RuntimeError(f"Cannot read file with supported encodings: {file_path}")


def _extract_features_uvl(all_text: str):
    # Extract feature tokens at any indentation level in "features" tree.
    # This keeps it simple and robust for your UVL style.
    feats = re.findall(r"^\s+([A-Za-z_][A-Za-z0-9_]*)\b", all_text, re.MULTILINE)
    feats = [f for f in feats if f.lower() not in _RESERVED]
    return sorted(set(feats))


def inject_and_track_defects(file_path, out_path):
    lines = _read_lines_any_encoding(file_path)
    all_text = "".join(lines)

    potential_targets = _extract_features_uvl(all_text)
    if len(potential_targets) < 3:
        return None, "Not enough features to inject 3 constraints"

    # اختيار 3 ميزات عشوائية للحقن
    df_feat, fo_feat, re_feat = random.sample(potential_targets, 3)

    # قيود UVL نقية (بدون تعليقات)
    injected_constraints = [
        f"    {ROOT} => !{df_feat}\n",  # DF1: Dead Feature
        f"    {ROOT} => {fo_feat}\n",   # FO: False Optional
        f"    {re_feat} => {ROOT}\n"    # RE: Redundancy-like (implication to root)
    ]

    new_content = []
    added = False

    for line in lines:
        new_content.append(line)
        if line.strip().lower() == "constraints":
            new_content.extend(injected_constraints)
            added = True

    if not added:
        # if constraints section is missing, append it at end
        if not new_content[-1].endswith("\n"):
            new_content.append("\n")
        new_content.append("\nconstraints\n")
        new_content.extend(injected_constraints)

    with open(out_path, "w", encoding="utf-8") as f:
        f.writelines(new_content)

    return {
        "File_Name": os.path.basename(out_path),
        "Source_File": os.path.basename(file_path),
        "DF1_DeadFeature": df_feat,
        "FO_FalseOptional": fo_feat,
        "RE_Redundancy": re_feat
    }, "Success"


# --- التنفيذ الرئيسي ---
print(f"{'Source File Name':<55} | {'Status'}")
print("-" * 120)

tracking_list = []

for filename in sorted(os.listdir(SOURCE_DIR)):
    if not filename.endswith(".uvl"):
        continue

    src_p = os.path.join(SOURCE_DIR, filename)

    # ✅ naming FIX: keep same stem and add "_injected"
    stem = filename[:-4]  # remove ".uvl"
    out_n = f"{stem}_injected.uvl"
    target_p = os.path.join(OUTPUT_DIR, out_n)

    defect_data, status = inject_and_track_defects(src_p, target_p)

    if defect_data:
        tracking_list.append(defect_data)
        print(f"{filename:<55} | ✅ Injected -> {out_n}")
    else:
        print(f"{filename:<55} | ❌ {status}")

# --- حفظ مفتاح العيوب (The Oracle Key) ---
if tracking_list:
    df_key = pd.DataFrame(tracking_list)
    df_key.to_excel(os.path.join(OUTPUT_DIR, "Defect_Injected_Key.xlsx"), index=False)
    df_key.to_csv(os.path.join(OUTPUT_DIR, "Defect_Injected_Key.csv"), index=False, encoding="utf-8-sig")

    print("\n" + "=" * 60)
    print("🚀 DONE! Injected UVL files generated in:")
    print(f"   {OUTPUT_DIR}")
    print("📄 Oracle key saved:")
    print("   Defect_Injected_Key.xlsx / Defect_Injected_Key.csv")
    print("=" * 60)
else:
    print("\n⚠️ No files injected. Check SOURCE_DIR and UVL contents.")
