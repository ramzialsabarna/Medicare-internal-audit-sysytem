import subprocess

# تعريف المسارات
ORIGINAL_PATH = r"C:\Users\pc\Desktop\phd file draft\phd new\جامعه اشبيليه\برنامج الايزو\vs code\medicareinternalaudit\uvl_models"
DEFECTIVE_PATH = r"C:\Users\pc\Desktop\phd file draft\phd new\جامعه اشبيليه\برنامج الايزو\vs code\medicareinternalaudit\uvl_defective_models"

def run_transformation(input_path, output_label):
    print(f"\n>>> Processing: {output_label}...")
    subprocess.run([
        "python", "batch_uvl_to_kr.py",
        "-i", input_path,
        "-o", f"./kr_outputs_{output_label}"
    ])

if __name__ == "__main__":
    # تشغيل التحويل للمجلد الأول
    run_transformation(ORIGINAL_PATH, "original")
    
    # تشغيل التحويل للمجلد الثاني (بعد الحقن)
    run_transformation(DEFECTIVE_PATH, "defective")
    
    print("\n✅ All transformations completed!")