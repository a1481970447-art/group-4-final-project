import pandas as pd
import matplotlib.pyplot as plt
from snownlp import SnowNLP
from opencc import OpenCC
from collections import defaultdict
import itertools
import os
import time

# --- 0. 定义文件路径 ---
# (!!!) 【修改点】: 我们在这里统一定义路径
out_dir = 'out'
sentences_file_path = os.path.join(out_dir, 'fengshen_sentences.csv')
whitelist_file_path = os.path.join(out_dir, 'OPTIMIZED_CHARACTER_WHITELIST.csv')  # <--- 指向 out 文件夹

# --- 检查输入文件 ---
required_files = [sentences_file_path, whitelist_file_path]
if not all(os.path.exists(f) for f in required_files):
    print("错误：缺少必要的输入文件。")
    print(f"请确保 '{sentences_file_path}'")
    print(f"和 '{whitelist_file_path}'")
    print(f"都存在于 '{out_dir}' 文件夹中，且该文件夹与此脚本在同一目录。")
    exit()

# --- 配置 Matplotlib 中文字体 ---
plt.rcParams['font.sans-serif'] = ['Microsoft YaHei', 'SimHei']
plt.rcParams['axes.unicode_minus'] = False

print("--- [项目优化版] ---")

# ==================================================
# PART A: 情感分析 (原阶段一)
# ==================================================
print("\n开始 [Part A: 情感分析]...")

# 初始化繁简转换器
cc = OpenCC('t2s')  # 繁体 -> 简体

print(f"正在加载 {sentences_file_path}...")
# (!!!) 【修改点】: 使用定义好的路径变量
df_sent = pd.read_csv(sentences_file_path)
print(f"已加载 {len(df_sent)} 条句子。")

print("正在将繁体文本转换为简体 (SNOWNLP需要)...")
start_time = time.time()
df_sent['text_simplified'] = df_sent['text'].apply(
    lambda x: cc.convert(x) if isinstance(x, str) else ""
)
print(f"繁简转换完成，耗时: {time.time() - start_time:.2f} 秒")


def get_sentiment(text):
    try:
        if not isinstance(text, str) or not text.strip(): return 0.5
        return SnowNLP(text).sentiments
    except Exception:
        return 0.5


print("正在计算每句话的情感分数...")
start_time = time.time()
df_sent['sentiment'] = df_sent['text_simplified'].apply(get_sentiment)
print(f"情感分数计算完成，耗时: {time.time() - start_time:.2f} 秒")

# 绘图与保存
chapter_sentiment = df_sent.groupby('chapter_no')['sentiment'].mean()
plt.figure(figsize=(15, 7))
chapter_sentiment.plot(kind='line', grid=True, title='《封神演義》逐章情感均值曲線')
plt.xlabel('章節編號 (Chapter No)')
plt.ylabel('情感均值 (0=消極, 1=積極)')
plt.ylim(0, 1)
plt.tight_layout()
plt.savefig('sentiment_per_chapter.png')

print("已保存情感分析图： sentiment_per_chapter.png")
print("--- [Part A] 完成 ---")

# ==================================================
# PART B: 网络数据准备 (修改后的阶段四)
# ==================================================
print("\n开始 [Part B: 人物网络数据准备]...")

# (!!!) 【已修复】: 填入我们检测到的正确参数
whitelist_encoding = 'utf-8'
whitelist_separator = ','

# --- 1. 加载你的人物白名单 ---
print(f"正在加载 {whitelist_file_path} (编码: {whitelist_encoding}, 分隔符: '{whitelist_separator}')...")
try:
    # (!!!) 【修改点】: 使用定义好的路径变量
    df_whitelist = pd.read_csv(
        whitelist_file_path,
        sep=whitelist_separator,
        encoding=whitelist_encoding
    )
    # 从第一列读取所有人物名称
    CHARACTER_LIST = df_whitelist.iloc[:, 0].dropna().astype(str).tolist()
    print(f"成功加载人物白名单，共 {len(CHARACTER_LIST)} 个人物。")
    print(f"名单前5位: {CHARACTER_LIST[:5]}")

except Exception as e:
    print(f"读取白名单时出错: {e}")
    print("请确保文件未被其他程序占用。")
    exit()

# --- 2. 准备 Gephi 节点文件 (Nodes) ---
nodes_df = pd.DataFrame(CHARACTER_LIST, columns=["Id"])
nodes_df["Label"] = nodes_df["Id"]
nodes_df.to_csv("fengshen_nodes.csv", index=False, encoding="utf-8-sig")
print(f"已保存人物节点文件： fengshen_nodes.csv")

# --- 3. 准备 Gephi 边文件 (Edges) ---
edge_weights = defaultdict(int)

print("正在计算人物共现（边）...")
for sentence in df_sent['text']:
    if not isinstance(sentence, str):
        continue

    chars_in_sentence = set()
    for char_name in CHARACTER_LIST:
        if char_name in sentence:  # 繁体 vs 繁体
            chars_in_sentence.add(char_name)

    if len(chars_in_sentence) >= 2:
        for char_a, char_b in itertools.combinations(sorted(list(chars_in_sentence)), 2):
            edge_weights[(char_a, char_b)] += 1

edges_list = []
for (source, target), weight in edge_weights.items():
    edges_list.append([source, target, weight])

edges_df = pd.DataFrame(edges_list, columns=["Source", "Target", "Weight"])
edges_df.to_csv("fengshen_edges.csv", index=False, encoding="utf-8-sig")

print(f"已保存人物关系文件： fengshen_edges.csv (共 {len(edges_df)} 条关系)")
print("--- [Part B] 完成 ---")

print("\n--- [所有 Python 分析已全部完成] ---")
