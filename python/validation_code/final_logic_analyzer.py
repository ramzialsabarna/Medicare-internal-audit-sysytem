import re
import csv
import os
import time
from pathlib import Path

# --- إعدادات المسارات النهائية ---
BASE_DIR = Path(r"C:\Users\pc\Desktop\phd file draft\phd new\جامعه اشبيليه\برنامج الايزو\vs code\medicareinternalaudit")
# المجلد السليم (يحتوي على 50 ملف تقريباً c1-c5)
KR_CLEAN_DIR = BASE_DIR / "kr_outputs_10models"
# مجلد العيوب (الذي يحتوي على 50 ملف DEFECTIVE_*)
KR_DEFECTIVE_DIR = BASE_DIR / "structurecode" / "kr_outputs_defective"
# مخرجات الجدول النهائي الشامل
OUTPUT_CSV = BASE_DIR / "structurecode" / "phd_comprehensive_results.csv"

def analyze_logic_defects(kr_content):
    """المحرك الاستدلالي لاكتشاف الميزة الميتة (DF1)."""
    dead_features = []
    
    # 1. الميزات الإجبارية
    mandatories = ["InternalAuditSystem"] 
    m_matches = re.findall(r"group\(.*?,mandatory,\[(.*?)\]\)\.", kr_content)
    for match in m_matches:
        mandatories.extend([f.strip() for f in match.split(',')])

    # 2. الميزات الاختيارية
    optionals = []
    o_matches = re.findall(r"group\(.*?,optional,\[(.*?)\]\)\.", kr_content)
    for match in o_matches:
        optionals.extend([f.strip() for f in match.split(',')])

    # 3. اكتشاف قيود الاستبعاد (imp)
    exclusions = re.findall(r"imp\(\s*([^,]+?)\s*,\s*not\(\s*([^)]+?)\s*\)\s*\)\.", kr_content)
    
    for feat_a, feat_b in exclusions:
        if feat_a in mandatories and feat_b in optionals:
            dead_features.append(feat_b)
        elif feat_b in mandatories and feat_a in optionals:
            dead_features.append(feat_a)
            
    return list(set(dead_features))

def run_comprehensive_analysis():
    all_results = []
    targets = [
        {"path": KR_CLEAN_DIR, "type": "Clean (Original)"},
        {"path": KR_DEFECTIVE_DIR, "type": "Defective (Injected)"}
    ]

    print(f"\n{'Group':<20} | {'Model Name':<45} | {'NF':<5} | {'NDF':<5} | {'Time (s)'}")
    print("-" * 105)

    total_files = 0
    start_all = time.perf_counter()

    for target in targets:
        dir_path = target["path"]
        if not dir_path.exists():
            print(f"⚠️ Warning: Folder not found: {dir_path}")
            continue

        # البحث عن ملفات .kr.pl (باستخدام rglob للبحث في المجلدات الفرعية أيضاً)
        for kr_file in sorted(dir_path.rglob("*.kr.pl")):
            try:
                # --- بدء قياس الوقت للموديل الواحد ---
                start_model = time.perf_counter()
                
                content = kr_file.read_text(encoding="utf-8")
                # حساب عدد الميزات (NF)
                nf_count = len(re.findall(r"feature\(.*?\)\.", content))
                # اكتشاف العيوب
                dead_list = analyze_logic_defects(content)
                
                # --- نهاية قياس الوقت للموديل الواحد ---
                model_duration = time.perf_counter() - start_model
                
                all_results.append({
                    "Folder": target["type"],
                    "Model": kr_file.name,
                    "NF": nf_count,
                    "NDF": len(dead_list),
                    "Execution_Time": model_duration, # العمود الجديد
                    "Detected": ", ".join(dead_list) if dead_list else "None"
                })
                
                print(f"{target['type']:<20} | {kr_file.name:<45} | {nf_count:<5} | {len(dead_list):<5} | {model_duration:.4f}")
                total_files += 1
            except Exception as e:
                print(f"❌ Error processing {kr_file.name}: {e}")

    # حفظ النتائج النهائية
    if all_results:
        try:
            with open(OUTPUT_CSV, 'w', newline='', encoding='utf-8') as f:
                # أضفنا Execution_Time إلى الحقول المحفوظة
                fieldnames = ["Folder", "Model", "NF", "NDF", "Execution_Time", "Detected"]
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(all_results)
            
            total_duration = time.perf_counter() - start_all
            print("-" * 105)
            print(f"✅ SUCCESS! Processed {total_files} models in {total_duration:.2f} seconds.")
            print(f"📄 Master spreadsheet with Time data saved at: {OUTPUT_CSV}")
        except PermissionError:
            print(f"\n❌ CRITICAL: Please close '{OUTPUT_CSV.name}' in Excel and run again!")
    else:
        print("\n❌ No models were found. Check your directories.")

if __name__ == "__main__":
    run_comprehensive_analysis()