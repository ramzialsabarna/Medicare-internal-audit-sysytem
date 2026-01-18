import pandas as pd
import matplotlib.pyplot as plt
import os

# 1. البحث عن ملف الإكسل الأصلي
def get_iso_file():
    files = [f for f in os.listdir('.') if f.startswith('ISO_DATA') and f.endswith('.xlsx')]
    if not files:
        print("❌ Error: No file starting with 'ISO_DATA' found in this folder.")
        return None
    return files[0]

# 2. وظيفة التحليل الشامل والرسم الاحترافي
def perform_full_analysis(df, sheet_name):
    print(f"🔄 Processing Sheet: {sheet_name} ...")
    
    # أولاً: حساب كافة الإحصائيات المطلوبة للدكتورة
    stats = {
        'Sheet': sheet_name,
        'Categories_Count': df['CATEGORY_CODE'].nunique(),
        'Unique_Items': df['ITEM_FEATURE_NAME'].nunique(),
        'Unique_Answers': df['ANSWER_FEATURE_NAME'].nunique(),
        'Audit_Plans': df['AUDIT_PLAN_CODE'].nunique(),
        'Audit_Types': df['AUDIT_TYPE_CODE'].nunique(),
        'Branches': df['BRANCH_FEATURE_CODE'].nunique()
    }

    # ثانياً: تجهيز بيانات الرسم (عدد البنود لكل فئة) - ترتيب تصاعدي للرسم الأفقي
    cat_details = df.groupby('CATEGORY_CODE')['ITEM_FEATURE_NAME'].nunique().sort_values(ascending=True).reset_index()
    cat_details.columns = ['Category', 'Items_Count']

    # ثالثاً: الرسم الأفقي الاحترافي (حل مشكلة تداخل الأسماء)
    plt.figure(figsize=(14, 10)) # مساحة واسعة للوضوح
    bars = plt.barh(cat_details['Category'], cat_details['Items_Count'], color='#2c3e50', edgecolor='black')
    
    plt.title(f'Feature Distribution per Audit Category\nDataset: {sheet_name}', fontsize=16, fontweight='bold', pad=20)
    plt.xlabel('Number of Unique Audit Items', fontsize=12, fontweight='bold')
    plt.ylabel('Audit Categories (ISO 15189 Clauses)', fontsize=12, fontweight='bold')
    
    # إضافة الأرقام بدقة في نهاية كل بار
    for bar in bars:
        width = bar.get_width()
        plt.text(width + 1, bar.get_y() + bar.get_height()/2, f'{int(width)}', 
                 va='center', fontweight='bold', color='#c0392b')

    plt.grid(axis='x', linestyle='--', alpha=0.3)
    plt.tight_layout()
    
    # حفظ الرسم بجودة عالية جداً للنشر (300 DPI)
    plt.savefig(f'Final_Chart_{sheet_name}.png', dpi=300)
    plt.close()
    
    return stats, cat_details

# 3. دورة التنفيذ وحفظ النتائج
file_name = get_iso_file()
if file_name:
    xls = pd.ExcelFile(file_name)
    target_sheets = [s for s in xls.sheet_names if s.startswith('ISO_Check_cate')]
    
    summary_list = []
    # حفظ كافة النتائج في ملف إكسل واحد بتبويبات مختلفة
    with pd.ExcelWriter('Professional_Audit_Report.xlsx') as writer:
        for sheet in target_sheets:
            data = pd.read_excel(xls, sheet_name=sheet)
            stats, details = perform_full_analysis(data, sheet)
            summary_list.append(stats)
            
            # حفظ تفاصيل كل شيت في تبويب خاص
            details.to_excel(writer, sheet_name=f"Details_{sheet[:20]}", index=False)
            
        # حفظ الملخص الإحصائي العام في التبويب الأول
        pd.DataFrame(summary_list).to_excel(writer, sheet_name='Overall_Statistical_Summary', index=False)

    print("\n" + "="*50)
    print("✅ SUCCESS: Pipeline Completed!")
    print(f"1. Excel Report: 'Professional_Audit_Report.xlsx'")
    print(f"2. High-Res Charts: Saved as 'Final_Chart_ISO_Check_cate...png'")
    print("="*50)