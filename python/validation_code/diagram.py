import os
import re
from graphviz import Digraph
from pathlib import Path

# --- إعداد المسارات (تأكد من مطابقتها لجهازك) ---
GRAPHVIZ_BIN = r"C:\Program Files (x86)\windows_10_cmake_Release_Graphviz-14.1.1-win64\Graphviz-14.1.1-win64\bin"
os.environ["PATH"] += os.pathsep + GRAPHVIZ_BIN

SOURCE_DIR = r"C:\Users\pc\Desktop\phd file draft\phd new\جامعه اشبيليه\برنامج الايزو\vs code\medicareinternalaudit\structurecode\uvl_outputs_10models\ISO_DATA"
OUTPUT_DIR = r"C:\Users\pc\Desktop\phd file draft\phd new\جامعه اشبيليه\برنامج الايزو\vs code\medicareinternalaudit\feature_model_viz"

if not os.path.exists(OUTPUT_DIR): os.makedirs(OUTPUT_DIR)

def draw_final_academic_fm(file_path, output_name):
    dot = Digraph(format='png', engine='dot')
    dot.attr(rankdir='TB', nodesep='0.5', ranksep='0.7')
    dot.attr('node', shape='plaintext', fontname='Arial', fontsize='10')

    with open(file_path, 'r', encoding='utf-8') as f:
        lines = f.readlines()

    stack = [(-1, "Root_None")]
    current_relation = "mandatory"
    group_id = 0
    node_count = 0
    max_nodes = 40 # رفعنا العدد قليلاً لرؤية العلاقات الجماعية

    for line in lines:
        if node_count > max_nodes: break
        raw_content = line.rstrip()
        stripped = raw_content.strip()
        if not stripped or stripped.lower() in ['features', 'constraints']: continue
        
        if stripped.lower() in ['mandatory', 'optional', 'alternative', 'or']:
            current_relation = stripped.lower()
            continue
            
        indent = len(raw_content) - len(raw_content.lstrip())
        feature_name = re.sub(r'\{.*\}', '', stripped).strip()
        
        while stack and stack[-1][0] >= indent:
            stack.pop()
            
        parent_name = stack[-1][1]
        
        if parent_name != "Root_None":
            # 1. العلاقات الفردية (دوائر)
            if current_relation in ['mandatory', 'optional']:
                head = 'dot' if current_relation == 'mandatory' else 'odot'
                dot.edge(parent_name, feature_name, arrowhead=head)
            
            # 2. العلاقات الجماعية (المثلث/القوس المظلل والمفرغ)
            else:
                group_id += 1
                # في الأوراق العلمية: Alternative هو مثلث مفرغ، Or هو مثلث مظلل
                triangle_style = 'filled' if current_relation == 'or' else 'normal'
                fill_color = 'black' if current_relation == 'or' else 'white'
                
                proxy_node = f"p_{group_id}"
                dot.node(proxy_node, "", shape='triangle', width='0.15', height='0.15', 
                         style=triangle_style, fillcolor=fill_color)
                
                dot.edge(parent_name, proxy_node, arrowhead='none')
                dot.edge(proxy_node, feature_name, arrowhead='none')
            
            node_count += 1
            
        stack.append((indent, feature_name))

    dot.render(os.path.join(OUTPUT_DIR, output_name), cleanup=True)

# --- حلقة التشغيل الفعلي (The Execution Loop) ---
print("🚀 Starting Advanced Academic Rendering...")
found_count = 0

for root, dirs, files in os.walk(SOURCE_DIR):
    for filename in files:
        if filename.endswith(".uvl"):
            file_path = os.path.join(root, filename)
            out_name = f"Final_Academic_{Path(filename).stem}"
            print(f"🎨 Processing: {filename}")
            try:
                draw_final_academic_fm(file_path, out_name)
                found_count += 1
            except Exception as e:
                print(f"❌ Error in {filename}: {e}")

if found_count > 0:
    print(f"\n✅ SUCCESS! Generated {found_count} models with Triangles and Dots.")
    print(f"📂 Results: {OUTPUT_DIR}")
else:
    print(f"❌ No files found in: {SOURCE_DIR}")