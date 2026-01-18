import os
import random
import re
from pathlib import Path

# --- إعدادات المسارات ---
BASE_DIR = r"C:\Users\pc\Desktop\phd file draft\phd new\جامعه اشبيليه\برنامج الايزو\vs code\medicareinternalaudit"
SOURCE_DIR = os.path.join(BASE_DIR, "structurecode", "uvl_outputs_10models", "ISO_DATA")
OUTPUT_DIR = os.path.join(BASE_DIR, "uvl_defective_models")

if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)

def inject_high_impact_defect(file_path, out_path):
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()
    except UnicodeDecodeError:
        with open(file_path, 'r', encoding='latin-1') as f:
            lines = f.readlines()
    
    # استهداف الجذر لضمان ميزة ميتة حتمية (DF1) كما في منهجية الورقة
    high_level_mandatory = "InternalAuditSystem" 
    
    all_text = "".join(lines)
    # البحث عن الميزات الاختيارية (تنتهي بـ _active)
    optional_targets = re.findall(r'\b(\w+_active)\b', all_text)
    
    if not optional_targets:
        return False, "No active features found"
    
    o_feat = random.choice(list(set(optional_targets)))
    
    # قيد التعارض: Root => !OptionalFeature
    defect_line = f"    {high_level_mandatory} => !{o_feat}\n"
    
    new_content = []
    added = False
    for line in lines:
        new_content.append(line)
        if line.strip().lower() == "constraints":
            new_content.append(defect_line)
            added = True
            
    if not added:
        new_content.append("\nconstraints\n")
        new_content.append(defect_line)

    with open(out_path, 'w', encoding='utf-8') as f:
        f.writelines(new_content)
    return True, f"DEAD: {o_feat}"

# --- التنفيذ الشامل ---
print(f"{'Source File Name':<45} | {'Injection Status'}")
print("-" * 70)

injected_count = 0
# مسح المجلد بالكامل للبحث عن كل ملفات الـ UVL
for filename in os.listdir(SOURCE_DIR):
    if filename.endswith(".uvl"):
        src_path = os.path.join(SOURCE_DIR, filename)
        # سنحافظ على اسم الملف الأصلي مع إضافة بادئة DEFECTIVE لتمييزه
        out_name = f"DEFECTIVE_{filename}"
        target_path = os.path.join(OUTPUT_DIR, out_name)
        
        success, msg = inject_high_impact_defect(src_path, target_path)
        if success:
            injected_count += 1
            print(f"{filename:<45} | ✅ {msg}")
        else:
            print(f"{filename:<45} | ❌ {msg}")

print("-" * 70)
print(f"🚀 تم بنجاح حقن العيوب في ({injected_count}) موديلاً من كافة المستويات (c1-c5).")