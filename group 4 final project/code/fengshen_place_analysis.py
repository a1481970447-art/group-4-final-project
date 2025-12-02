"""
封神演义地点出现频率分析工具
功能：导入CSV原文文件，使用jieba分词统计地名出现频率
作者：数据分析师
日期：2024年
"""

import pandas as pd
import jieba
import re
from collections import Counter
import argparse

def create_fengshen_place_dict(dict_path='fengshen_place_dict.txt'):
    """
    创建封神演义地点自定义词典（繁体中文）
    """
    # 封神演义中常见的地点（繁体中文）
    fengshen_places = [
        "商朝 1000 nr", "西周 800 nr", "紂都 600 nr", "西岐 1200 nr", "朝歌 1500 nr",
        "孟津 500 nr", "冀州 400 nr", "陳塘關 350 nr", "九龍島 300 nr", "金鳌島 280 nr",
        "臨潼 250 nr", "黃河 450 nr", "渭水 380 nr", "昆侖山 650 nr", "玉泉山 220 nr",
        "青峰山 200 nr", "五夷山 180 nr", "夾龍山 160 nr", "龍虎山 150 nr", "首陽山 140 nr",
        "燕山 130 nr", "岐山 320 nr", "鄂城 120 nr", "潼關 280 nr", "臨潼關 260 nr",
        "黃河渡口 180 nr", "孟津渡口 160 nr", "西岐城 220 nr", "朝歌城 250 nr", "摘星樓 300 nr",
        "鹿台 350 nr", "太廟 180 nr", "龍德殿 150 nr", "九間殿 140 nr", "女娲宮 280 nr",
        "靈台 220 nr", "肉林酒池 120 nr", "薑太公府 110 nr", "比干府 100 nr", "微子府 90 nr",
        "箕子府 80 nr", "聞太師府 150 nr", "黃飛虎府 180 nr", "蘇護府 120 nr", "崇侯虎府 100 nr",
        "鄂崇禹府 90 nr", "姬昌府 200 nr", "姬發府 220 nr", "哪吒府 150 nr", "楊戩府 140 nr",
        "雷震子府 130 nr", "黃天化府 120 nr", "黃天祿府 110 nr", "黃天祥府 100 nr", "土行孫府 90 nr",
        "鄧九公府 80 nr", "鄔文化府 70 nr", "張桂芳府 60 nr", "魔家四將府 100 nr", "聞仲府 150 nr",
        "申公豹府 120 nr", "姜子牙府 200 nr", "元始天尊宮 250 nr", "通天教主宮 230 nr", "老子宮 200 nr",
        "接引道人廟 180 nr", "准提道人廟 170 nr", "十二金仙府 300 nr", "玉虛宮 400 nr", "碧游宮 380 nr",
        "靈霄殿 280 nr", "南天門 320 nr", "瑤池 250 nr", "蟠桃園 220 nr", "兜率宮 200 nr",
        "朝歌皇宮 300 nr", "西岐王府 280 nr", "商軍營 400 nr", "周軍營 450 nr", "孟津大營 350 nr",
        "黃河大營 320 nr", "潼關大營 300 nr", "臨潼大營 280 nr", "冀州城 250 nr", "陳塘關城 230 nr",
        "西岐城門 200 nr", "朝歌城門 220 nr", "九龍島洞 180 nr", "金鳌島洞 170 nr", "玉泉山洞 150 nr",
        "青峰山東 140 nr", "五夷山東 130 nr", "夾龍山東 120 nr", "龍虎山東 110 nr", "首陽山東 100 nr",
        "燕山東 90 nr", "岐山東 80 nr", "昆侖山頂 250 nr", "昆侖山腳 200 nr", "渭水之濱 220 nr",
        "黃河之畔 200 nr", "孟津之濱 180 nr", "朝歌城外 250 nr", "西岐城外 230 nr", "冀州城外 200 nr",
        "陳塘關外 180 nr", "摘星樓頂 150 nr", "鹿台之上 180 nr", "太廟之內 120 nr", "龍德殿內 100 nr",
        "九間殿內 90 nr", "女娲宮內 150 nr", "靈台之上 120 nr"
    ]
    
    # 写入词典文件
    with open(dict_path, 'w', encoding='utf-8') as f:
        f.write('\n'.join(fengshen_places))
    
    print(f"✅ 成功创建封神演义地点自定义词典: {dict_path}")
    print(f"📚 共包含 {len(fengshen_places)} 个地点词汇")
    
    # 返回地点列表
    place_list = [place.split()[0] for place in fengshen_places]
    return place_list, dict_path

def load_fengshen_data(file_path):
    """
    导入封神演义全文CSV文件
    """
    try:
        df = pd.read_csv(file_path)
        print(f"📖 成功读取文件: {file_path}")
        print(f"📊 数据规模: {df.shape[0]} 章节, {df.shape[1]} 列")
        print(f"🏷️  列名: {df.columns.tolist()}")
        
        # 检查是否包含full_text列
        if 'full_text' not in df.columns:
            raise ValueError("CSV文件中缺少'full_text'列，请检查文件格式")
        
        return df
    except Exception as e:
        print(f"❌ 读取文件时出错: {e}")
        raise

def merge_all_text(df, text_column='full_text'):
    """
    合并所有章节的文本内容
    """
    all_text = ''
    for idx, row in df.iterrows():
        all_text += str(row[text_column]) + '\n'
    
    print(f"📝 成功合并所有文本")
    print(f"📏 文本总长度: {len(all_text):,} 字符")
    print(f"📚 覆盖章节数: {len(df)} 回")
    
    return all_text

def preprocess_text(text):
    """
    文本预处理：去除标点符号、特殊字符等
    """
    # 保留中文字符，去除其他字符
    pattern = re.compile(r'[^一-鿿]')
    cleaned_text = pattern.sub('', text)
    
    # 去除多余的空格
    cleaned_text = re.sub(r'\s+', '', cleaned_text)
    
    print(f"🧹 文本预处理完成")
    print(f"📊 预处理前: {len(text):,} 字符")
    print(f"📊 预处理后: {len(cleaned_text):,} 字符")
    
    return cleaned_text

def segment_and_extract_places(text, place_list, jieba_instance):
    """
    对文本进行分词并筛选出地点词汇
    """
    print(f"🔍 开始分词和地点提取...")
    
    # 分词（精确模式）
    words = jieba_instance.lcut(text, cut_all=False)
    print(f"✂️  总分词数: {len(words):,}")
    
    # 筛选出地点词汇
    place_words = [word for word in words if word in place_list]
    print(f"📍 提取出的地点词汇总数: {len(place_words):,}")
    
    # 去重查看有多少个不同的地点被识别
    unique_places = list(set(place_words))
    print(f"🗺️  识别出的不同地点数量: {len(unique_places)}")
    
    return place_words, unique_places

def count_and_sort_places(place_words):
    """
    统计地点词频并按频率排序
    """
    # 统计词频
    place_counter = Counter(place_words)
    print(f"📈 地点词频统计完成，共统计 {len(place_counter)} 个地点")
    
    # 按频率降序排序
    sorted_places = sorted(place_counter.items(), key=lambda x: x[1], reverse=True)
    
    print(f"🏆 出现频率前10的地点:")
    for i, (place, count) in enumerate(sorted_places[:10], 1):
        print(f"  {i:2d}. {place}: {count:,} 次")
    
    return sorted_places, place_counter

def create_place_statistics_table(sorted_places, total_places_count, output_path='fengshen_place_statistics.csv'):
    """
    创建地点统计表格并保存为CSV文件
    """
    # 准备统计数据
    statistics_data = []
    total_occurrences = sum([count for _, count in sorted_places])
    
    for rank, (place, count) in enumerate(sorted_places, 1):
        frequency_percent = (count / total_occurrences) * 100 if total_occurrences > 0 else 0
        cumulative_percent = (sum([c for _, c in sorted_places[:rank]]) / total_occurrences) * 100 if total_occurrences > 0 else 0
        
        # 确定地点等级
        if rank <= 10:
            level = '主要地点'
        elif rank <= 20:
            level = '重要地点'
        else:
            level = '次要地点'
        
        statistics_data.append({
            '排名': rank,
            '地点名称': place,
            '出现次数': count,
            '出现频率(%)': round(frequency_percent, 2),
            '累计频率(%)': round(cumulative_percent, 2),
            '等级': level
        })
    
    # 创建DataFrame
    df_statistics = pd.DataFrame(statistics_data)
    
    # 保存为CSV文件
    df_statistics.to_csv(output_path, index=False, encoding='utf-8-sig')
    print(f"💾 地点统计表格已保存至: {output_path}")
    
    # 显示统计摘要
    print(f"📊 统计摘要:")
    print(f"- 总地点词汇出现次数: {total_occurrences:,}")
    print(f"- 识别出的不同地点数量: {len(df_statistics)}")
    
    main_places_count = sum(df_statistics[df_statistics['等级']=='主要地点']['出现次数'])
    main_places_ratio = (main_places_count / total_occurrences * 100) if total_occurrences > 0 else 0
    print(f"- 主要地点（前10名）出现次数: {main_places_count:,} ({main_places_ratio:.1f}%)")
    
    important_places_count = sum(df_statistics[df_statistics['等级']=='重要地点']['出现次数'])
    important_places_ratio = (important_places_count / total_occurrences * 100) if total_occurrences > 0 else 0
    print(f"- 重要地点（前20名）出现次数: {important_places_count:,} ({important_places_ratio:.1f}%)")
    
    return df_statistics

def main(input_csv_path, output_csv_path='fengshen_place_statistics.csv'):
    """
    主函数：执行完整的地点统计分析流程
    """
    print("=" * 60)
    print("        封神演义地点出现频率分析工具        ")
    print("=" * 60)
    
    try:
        # 1. 创建自定义词典
        place_list, dict_path = create_fengshen_place_dict()
        
        # 2. 初始化jieba并加载词典
        jieba.load_userdict(dict_path)
        print(f"✅ 成功加载自定义词典到jieba")
        
        # 3. 加载CSV数据
        df = load_fengshen_data(input_csv_path)
        
        # 4. 合并文本
        all_text = merge_all_text(df)
        
        # 5. 文本预处理
        cleaned_text = preprocess_text(all_text)
        
        # 6. 分词和地点提取
        place_words, unique_places = segment_and_extract_places(cleaned_text, place_list, jieba)
        
        # 7. 词频统计和排序
        sorted_places, place_counter = count_and_sort_places(place_words)
        
        # 8. 创建统计表格
        df_statistics = create_place_statistics_table(sorted_places, len(place_words), output_csv_path)
        
        print("\n" + "=" * 60)
        print("        分析完成！所有结果已保存        ")
        print("=" * 60)
        
        return df_statistics
        
    except Exception as e:
        print(f"\n❌ 分析过程中出现错误: {e}")
        print("请检查输入文件格式或联系技术支持")
        return None

if __name__ == "__main__":
    # 解析命令行参数
    parser = argparse.ArgumentParser(description='封神演义地点出现频率分析工具')
    parser.add_argument('input_file', help='输入CSV文件路径（包含full_text列）')
    parser.add_argument('-o', '--output', default='fengshen_place_statistics.csv', 
                        help='输出统计表格路径（默认：fengshen_place_statistics.csv）')
    
    args = parser.parse_args()
    
    # 执行主函数
    main(args.input_file, args.output)
