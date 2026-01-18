import os
import re
import xml.etree.ElementTree as ET
from xml.dom import minidom
from pathlib import Path

# --- الإعدادات ---
SOURCE_DIR = r"C:\Users\pc\Desktop\phd file draft\phd new\جامعه اشبيليه\برنامج الايزو\vs code\medicareinternalaudit\structurecode\uvl_outputs_10models\ISO_DATA"
OUTPUT_DIR = r"C:\Users\pc\Desktop\phd file draft\phd new\جامعه اشبيليه\برنامج الايزو\vs code\medicareinternalaudit\featureide_xml"

if not os.path.exists(OUTPUT_DIR): 
    os.makedirs(OUTPUT_DIR)

def prettify(elem):
    """إضافة تنسيق جمالي لملف الـ XML (إزاحة وتدرج)"""
    rough_string = ET.tostring(elem, 'utf-8')
    reparsed = minidom.parseString(rough_string)
    return reparsed.toprettyxml(indent="  ")

def convert_uvl_to_featureide(uvl_path, xml_path):
    with open(uvl_path, 'r', encoding='utf-8') as f:
        lines = f.readlines()

    root = ET.Element("featureModel")
    struct = ET.SubElement(root, "struct")
    
    # القاموس لتتبع نوع العلاقة الحالية لكل مستوى إزاحة
    stack = [(-1, struct)] 
    current_rel = "and" # الافتراضي لـ Mandatory/Optional

    for line in lines:
        raw_content = line.rstrip()
        stripped = raw_content.strip()
        
        if not stripped or stripped.lower() in ['features', 'constraints']:
            continue
            
        # تحديد نوع المجموعة وتحويلها لمصطلحات FeatureIDE
        if stripped.lower() in ['mandatory', 'optional', 'alternative', 'or']:
            # FeatureIDE يستخدم: and (للإلزامي)، alt (للمثلث المفرغ)، or (للمثلث المظلل)
            if stripped.lower() == 'alternative': current_rel = "alt"
            elif stripped.lower() == 'or': current_rel = "or"
            else: current_rel = "and"
            continue

        indent = len(raw_content) - len(raw_content.lstrip())
        feature_name = re.sub(r'\{.*\}', '', stripped).strip()
        
        # إدارة الشجرة
        while stack and stack[-1][0] >= indent:
            stack.pop()
            
        parent_element = stack[-1][1]
        
        # إنشاء عقدة الفيتشر
        # إذا كانت العلاقة "and" (إلزامي/اختياري) نستخدم التاج المباشر
        # إذا كانت "alt" أو "or" ننشئ حاوية للمجموعة
        
        tag_name = "and" if current_rel == "and" else current_rel
        
        # تمييز الاختياري (Mandatory vs Optional)
        is_abstract = "abstract" in raw_content.lower()
        mandatory = "true" if "mandatory" in line.lower() or current_rel == "and" else "false"

        new_feat = ET.SubElement(parent_element, tag_name, {
            "name": feature_name,
            "mandatory": "true" # سيتم تعديلها حسب الحاجة في FeatureIDE
        })
        
        stack.append((indent, new_feat))

    # حفظ الملف بتنسيق جميل
    with open(xml_path, "w", encoding="utf-8") as f:
        f.write(prettify(root))

# --- حلقة التشغيل الفعلي ---
print("🚀 Starting XML Conversion for FeatureIDE...")
files_processed = 0

for root_dir, dirs, files in os.walk(SOURCE_DIR):
    for filename in files:
        if filename.endswith(".uvl"):
            uvl_file = os.path.join(root_dir, filename)
            xml_file = os.path.join(OUTPUT_DIR, f"{Path(filename).stem}.xml")
            try:
                convert_uvl_to_featureide(uvl_file, xml_file)
                print(f"✅ Converted: {filename}")
                files_processed += 1
            except Exception as e:
                print(f"❌ Error in {filename}: {e}")

print(f"\n🎉 Process Finished! {files_processed} XML files are ready in: {OUTPUT_DIR}")