import os
import random
import re
import pandas as pd

# --- إعدادات المسارات ---
BASE_DIR = r"C:\Users\pc\Desktop\phd file draft\phd new\جامعه اشبيليه\برنامج الايزو\vs code\medicareinternalaudit"
SOURCE_DIR = os.path.join(BASE_DIR, "structurecode", "uvl_outputs_10models", "ISO_DATA")
OUTPUT_DIR = os.path.join(BASE_DIR, "uvl_scientific_defects_v5")

if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)

def inject_and_track_defects(file_path, out_path):
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()
    except:
        with open(file_path, 'r', encoding='latin-1') as f:
            lines = f.readlines()
    
    root = "InternalAuditSystem" 
    all_text = "".join(lines)
    
    # استخراج الميزات الحقيقية (الأبناء)
    features = re.findall(r'^\s+(\w+)', all_text, re.MULTILINE)
    reserved = ['features', 'constraints', 'mandatory', 'optional', 'alternative', 'or']
    potential_targets = list(set([f for f in features if f.lower() not in reserved]))

    if len(potential_targets) < 3: 
        return None, "Not enough features"

    # اختيار الميزات للحقن
    df_feat, fo_feat, re_feat = random.sample(potential_targets, 3)
    
    # صياغة القيود بصيغة UVL نقية (بدون تعليقات لضمان قبول الموقع)
    injected_constraints = [
        f"    {root} => !{df_feat}\n", # DF1: Dead Feature
        f"    {root} => {fo_feat}\n",  # FO: False Optional
        f"    {re_feat} => {root}\n"   # RE: Redundancy
    ]
    
    new_content = []
    added = False
    for line in lines:
        new_content.append(line)
        if line.strip().lower() == "constraints":
            new_content.extend(injected_constraints)
            added = True
            
    if not added:
        new_content.append("\nconstraints\n")
        new_content.extend(injected_constraints)

    with open(out_path, 'w', encoding='utf-8') as f:
        f.writelines(new_content)
    
    # إرجاع بيانات العيوب للتتبع
    return {
        "File_Name": os.path.basename(out_path),
        "DF1_DeadFeature": df_feat,
        "FO_FalseOptional": fo_feat,
        "RE_Redundancy": re_feat
    }, "Success"

# --- التنفيذ الرئيسي ---
print(f"{'Source File Name':<45} | {'Status'}")
print("-" * 110)

tracking_list = []

for filename in os.listdir(SOURCE_DIR):
    if filename.endswith(".uvl"):
        src_p = os.path.join(SOURCE_DIR, filename)
        out_n = f"SCIENTIFIC_V5_{filename}"
        target_p = os.path.join(OUTPUT_DIR, out_n)
        
        defect_data, status = inject_and_track_defects(src_p, target_p)
        
        if defect_data:
            tracking_list.append(defect_data)
            print(f"{filename:<45} | ✅ Injected: DF1, FO, RE")
        else:
            print(f"{filename:<45} | ❌ {status}")

# --- حفظ مفتاح العيوب (The Oracle Key) ---
if tracking_list:
    df_key = pd.DataFrame(tracking_list)
    # حفظ كـ Excel للتوثيق
    df_key.to_excel(os.path.join(OUTPUT_DIR, "Defect_Injected_Key.xlsx"), index=False)
    # حفظ كـ CSV لسهولة القراءة البرمجية
    df_key.to_csv(os.path.join(OUTPUT_DIR, "Defect_Injected_Key.csv"), index=False)
    print("\n" + "="*50)
    print("🚀 DONE! All files generated in: uvl_scientific_defects_v5")
    print("📄 Defect key saved as 'Defect_Injected_Key.xlsx'")
    print("="*50)