import pandas as pd
import matplotlib.pyplot as plt
import os
import seaborn as sns
from scipy.stats import linregress

# --- إعدادات المسارات ---
BASE_DIR = r"C:\Users\pc\Desktop\phd file draft\phd new\جامعه اشبيليه\برنامج الايزو\vs code\medicareinternalaudit"
RESULTS_FILE = os.path.join(BASE_DIR, "structurecode", "phd_comprehensive_results.csv")
OUTPUT_DIR = os.path.join(BASE_DIR, "structurecode", "phd_final_charts")

if not os.path.exists(OUTPUT_DIR): os.makedirs(OUTPUT_DIR)

# تحميل البيانات وتنسيقها
df = pd.read_csv(RESULTS_FILE)
df['Group'] = df['Folder'].str.replace(' (Original)', '').str.replace(' (Injected)', '')
plt.rcParams.update({'font.size': 10, 'font.family': 'serif'})

def save_academic_plot(filename):
    plt.tight_layout()
    plt.grid(True, linestyle='--', alpha=0.5)
    plt.savefig(os.path.join(OUTPUT_DIR, filename), dpi=300)
    plt.close()

# --- توليد الـ 10 صور المطورة ---

# 1. Scalability (Time vs NF)
plt.figure(figsize=(8, 5))
sns.lineplot(data=df, x="NF", y="Execution_Time", hue="Group", marker='s')
plt.title("Fig 1: Reasoning Scalability (Time vs NF)")
save_academic_plot('01_scalability.png')

# 2. Defect Detection Accuracy (NDF)
plt.figure(figsize=(8, 5))
sns.stripplot(data=df, x="Group", y="NDF", hue="Group", palette="Set1", size=10)
plt.title("Fig 2: Defect Detection Accuracy")
save_academic_plot('02_accuracy.png')

# 3. Efficiency (ms per Feature)
df['ms_feat'] = (df['Execution_Time'] / df['NF']) * 1000
plt.figure(figsize=(8, 5))
sns.scatterplot(data=df, x="NF", y="ms_feat", hue="Group")
plt.title("Fig 3: Reasoning Efficiency (ms/Feature)")
save_academic_plot('03_efficiency.png')

# 4. Distribution of NF across Models
plt.figure(figsize=(8, 5))
sns.histplot(data=df, x="NF", hue="Group", kde=True, bins=15)
plt.title("Fig 4: Distribution of Model Sizes (NF)")
save_academic_plot('04_nf_distribution.png')

# 5. Regression Analysis
plt.figure(figsize=(8, 5))
sns.regplot(data=df[df['Group']=='Clean'], x="NF", y="Execution_Time", label="Clean", scatter_kws={'alpha':0.5})
sns.regplot(data=df[df['Group']=='Defective'], x="NF", y="Execution_Time", label="Defective", scatter_kws={'alpha':0.5})
plt.title("Fig 5: Performance Regression Analysis")
plt.legend()
save_academic_plot('05_regression.png')

# 6. Comparative Boxplot (Time)
plt.figure(figsize=(8, 5))
sns.boxplot(data=df, x="Group", y="Execution_Time", palette="Pastel1")
plt.title("Fig 6: Execution Time Variance")
save_academic_plot('06_time_variance.png')

# 7. Model Levels Complexity (c1-c5)
df['Level'] = df['Model'].str.extract(r'(c\d)')
plt.figure(figsize=(8, 5))
sns.barplot(data=df, x="Level", y="NF", hue="Group")
plt.title("Fig 7: Complexity Levels (c1-c5) NF Analysis")
save_academic_plot('07_levels_complexity.png')

# 8. Cumulative Execution Time
df_sorted = df.sort_values('NF')
plt.figure(figsize=(8, 5))
plt.stackplot(range(len(df_sorted[df_sorted['Group']=='Clean'])), 
              df_sorted[df_sorted['Group']=='Clean']['Execution_Time'], 
              labels=['Total Computational Load'])
plt.title("Fig 8: Cumulative Reasoning Load")
save_academic_plot('08_cumulative_load.png')

# 9. Performance Gap Analysis
plt.figure(figsize=(8, 5))
sns.violinplot(data=df, x="Group", y="ms_feat", inner="quart")
plt.title("Fig 9: Performance Gap Analysis (ms/Feature)")
save_academic_plot('09_performance_gap.png')

# 10. Summary Performance Heatmap
pivot = df.groupby(['Level', 'Group'])['Execution_Time'].mean().unstack()
plt.figure(figsize=(8, 5))
sns.heatmap(pivot, annot=True, cmap="YlGnBu", fmt=".4f")
plt.title("Fig 10: Mean Execution Time Heatmap (Level vs Group)")
save_academic_plot('10_summary_heatmap.png')

print(f"✅ 10 Academic Charts generated in: {OUTPUT_DIR}")