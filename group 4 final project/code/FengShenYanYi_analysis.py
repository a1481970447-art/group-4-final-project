import pandas as pd
import jieba
import jieba.posseg as pseg
import re
from collections import Counter
import logging


# --- 用户提示 ---
# 运行此代码前，请确保您已安装了所需库：
# pip install pandas jieba
# -----------------

def process_fengshen(csv_file_path, report_file_path, frequency_csv_path):

    jieba.setLogLevel(logging.INFO)

    # 使用 'w' (写入) 模式和 'utf-8' 编码打开报告文件
    # with 语句将确保文件在完成后被正确关闭
    with open(report_file_path, 'w', encoding='utf-8') as report_file:

        report_file.write(f"--- 步骤 1: 开始加载数据 '{csv_file_path}' ---\n")
        try:
            # 1. 读取CSV文件
            df = pd.read_csv(csv_file_path)
            report_file.write("CSV文件加载成功。\n")

            # 2. 检查 'full_text' 列
            if 'full_text' not in df.columns:
                error_msg = "错误：CSV文件中未找到 'full_text' 列。请检查文件。\n"
                report_file.write(error_msg)
                print(error_msg)  # 也在控制台打印错误
                return

            # 3. 合并所有文本
            report_file.write("正在合并所有文本...\n")
            all_text = ' '.join(df['full_text'].dropna().astype(str))

            if not all_text.strip():
                error_msg = "错误：'full_text' 列为空或只包含无效数据。\n"
                report_file.write(error_msg)
                print(error_msg)  # 也在控制台打印错误
                return

            report_file.write(f"数据加载完毕，总字符数 (含空格): {len(all_text)}\n")

        except FileNotFoundError:
            error_msg = f"错误：文件未找到 '{csv_file_path}'。请确保文件路径正确。\n"
            report_file.write(error_msg)
            print(error_msg)
            return
        except Exception as e:
            error_msg = f"加载CSV时出错: {e}\n"
            report_file.write(error_msg)
            print(error_msg)
            return

        # -------------------------------------------------
        # --- 步骤 2: 中文分词与词性标注 (Preprocessing & POS Tagging) 示例 ---
        # -------------------------------------------------
        report_file.write("\n--- 步骤 2: 词性标注 (POS Tagging) 示例 ---\n")
        sample_text = all_text[:200]
        report_file.write(f"示例原文 (Sample Text):\n{sample_text}\n\n")
        report_file.write("词性标注结果 (格式: 词/词性):\n")
        try:
            words_with_pos = pseg.cut(sample_text)
            pos_results = [f"{word}/{flag}" for word, flag in words_with_pos]
            report_file.write(' | '.join(pos_results) + '\n')
        except Exception as e:
            report_file.write(f"Jieba 词性标注出错: {e}\n")

        # -------------------------------------------------
        # --- 步骤 3: 移除标点并统计词频 (Word Frequency) ---
        # -------------------------------------------------
        report_file.write("\n--- 步骤 3: 词频统计 (Word Frequency) ---\n")
        try:
            # 1. 移除标点符号
            report_file.write("正在移除所有标点符号...\n")
            punctuation_pattern = r'[^\u4e00-\u9fa5a-zA-Z0-9]'
            cleaned_text = re.sub(punctuation_pattern, " ", all_text)

            # 2. 使用Jieba分词
            report_file.write("正在使用Jieba分词 (全文本，可能需要一点时间)...\n")
            words = jieba.cut(cleaned_text)

            # 3. 过滤词语
            filtered_words = [word.strip() for word in words if word.strip()]
            report_file.write(f"分词完毕。总词数 (已去标点和空格): {len(filtered_words)}\n")

            # 4. 统计词频
            report_file.write("正在统计词频...\n")
            word_counts = Counter(filtered_words)

            # 5. 将 Top 50 写入报告文件
            report_file.write("\n--- 频率最高的前50个词 (Top 50 Words) ---\n")
            for word, count in word_counts.most_common(50):
                report_file.write(f"{word}: {count}\n")

            # -------------------------------------------------
            # --- 步骤 4: 保存完整的词频列表到 CSV ---
            # -------------------------------------------------
            report_file.write(f"\n--- 步骤 4: 保存完整词频列表 ---\n")
            report_file.write(f"正在将所有词频保存到: {frequency_csv_path}\n")

            # 1. 获取完整的排序列表
            full_sorted_list = word_counts.most_common()

            # 2. 转换为 Pandas DataFrame
            df_freq = pd.DataFrame(full_sorted_list, columns=['Word', 'Frequency'])

            # 3. 保存到 CSV
            # 使用 'utf-8-sig' 编码可以确保 Excel 打开时能正确显示中文
            df_freq.to_csv(frequency_csv_path, index=False, encoding='utf-8-sig')

            report_file.write("完整词频列表保存成功。\n")

        except Exception as e:
            error_msg = f"词频统计或保存过程中出错: {e}\n"
            report_file.write(error_msg)
            print(error_msg)

        report_file.write("\n--- 所有步骤执行完毕 ---")


# --- 主程序入口 ---
if __name__ == "__main__":
    # 1. 输入文件 (使用 r'' 原始字符串来处理 Windows 路径)
    csv_file_path = r'/CBS_5501_Final_project_WuShenyu_25114053g/fengshen_fulltext.csv'

    # 2. 输出文件 (将保存在与脚本相同的目录中)
    report_file_path = 'out/fengshen_analysis_report.txt'
    frequency_csv_path = 'out/fengshen_word_frequency.csv'

    print(f"开始处理: {csv_file_path}...")
    print("分析报告、词性标注示例和Top-50词频将被保存到: " + report_file_path)
    print("完整的词频列表将被保存到: " + frequency_csv_path)

    # 执行主函数
    process_fengshen(csv_file_path, report_file_path, frequency_csv_path)

    print("处理完成。请检查输出文件。")