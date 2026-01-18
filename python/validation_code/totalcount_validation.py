import pandas as pd
import matplotlib.pyplot as plt
import os

# 1. البحث عن ملف الإكسل
def get_iso_file():
    files = [f for f in os.listdir('.') if f.startswith('ISO_DATA') and f.endswith('.xlsx')]
    if not files:
        print("❌ Error: No file starting with 'ISO_DATA' found in this folder.")
        return None
    return files[0]

# 2. تحليل البيانات والرسم
def perform_analysis(df, sheet_name):
    print(f"--- Analyzing Sheet: {sheet_name} ---")
    
    # حساب الإحصائيات المطلوبة
    stats = {
        'Sheet': sheet_name,
        'Categories_Count': df['CATEGORY_CODE'].nunique(),
        'Unique_Items': df['ITEM_FEATURE_NAME'].nunique(),
        'Unique_Answers': df['ANSWER_FEATURE_NAME'].nunique(),
        'Audit_Plans': df['AUDIT_PLAN_CODE'].nunique(),
        'Audit_Types': df['AUDIT_TYPE_CODE'].nunique(),
        'Branches': df['BRANCH_FEATURE_CODE'].nunique()
    }

    # تفاصيل الكاتيجوري (عدد البنود لكل منها)
    cat_details = df.groupby('CATEGORY_CODE')['ITEM_FEATURE_NAME'].nunique().sort_values(ascending=False).reset_index()
    cat_details.columns = ['Category', 'Items_Count']

    # الرسم البياني
    plt.figure(figsize=(10, 6))
    plt.bar(cat_details['Category'], cat_details['Items_Count'], color='teal')
    plt.title(f'Items per Category - {sheet_name}')
    plt.xlabel('Category Code')
    plt.ylabel('Number of Items')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(f'Analysis_Chart_{sheet_name}.png')
    plt.close()
    
    return stats, cat_details

# 3. التنفيذ
file_name = get_iso_file()
if file_name:
    xls = pd.ExcelFile(file_name)
    target_sheets = [s for s in xls.sheet_names if s.startswith('ISO_Check_cate')]
    
    summary_list = []
    with pd.ExcelWriter('Audit_Structure_Report.xlsx') as writer:
        for sheet in target_sheets:
            data = pd.read_excel(xls, sheet_name=sheet)
            stats, details = perform_analysis(data, sheet)
            summary_list.append(stats)
            details.to_excel(writer, sheet_name=f"Details_{sheet[:20]}", index=False)
            
        pd.DataFrame(summary_list).to_excel(writer, sheet_name='Overall_Summary', index=False)

    print("\n✅ Success! Check 'Audit_Structure_Report.xlsx' and the generated PNG charts.")