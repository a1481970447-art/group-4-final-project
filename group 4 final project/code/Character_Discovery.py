import pandas as pd
import jieba.posseg as pseg  # 导入词性标注模块
from collections import Counter
from tqdm import tqdm

print("开始 [阶段二：自动人物发现]...")
print("这将花费几分钟时间，Jieba 正在分析全文...")

# --- 1. 加载数据 ---
# 我们使用段落数据，处理速度更快，且上下文更完整
try:
    df = pd.read_csv("out/fengshen_paragraphs.csv")
except FileNotFoundError:
    print("错误：未在 'out' 文件夹中找到 fengshen_paragraphs.csv")
    exit()

print(f"数据加载完毕，共 {len(df)} 个段落。")

# --- 2. 自动提取人名 (nr) ---
# 'nr' 是 Jieba 词库中“人名”的标记
potential_names = Counter()

# 使用 tqdm 显示进度条
for text in tqdm(df['text'], desc="分析段落"):
    if not isinstance(text, str):
        continue

    # 进行词性标注
    words = pseg.cut(text)

    # 提取所有被标记为 'nr' (人名) 且长度大于1的词
    for word, flag in words:
        if flag == 'nr' and len(word) > 1:
            potential_names[word] += 1

print("人物提取完成。")

# --- 3. 保存为 CSV ---
# 转换为 DataFrame
names_df = pd.DataFrame(potential_names.most_common(),
                        columns=['Potential_Name', 'Frequency'])

# 保存
output_file = 'out/potential_characters_freq.csv'
names_df.to_csv(output_file, index=False, encoding='utf-8-sig')

print(f"--- [阶段二：自动人物发现] 已完成 ---")
print(f"已保存潜在人物列表：{output_file}")
print(f"\n[下一步行动]：请手动打开 {output_file} 文件进行清理。")
