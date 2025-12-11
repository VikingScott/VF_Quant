"""
目录加载器：从 data_import 文件夹读取所有根级 CSV，并合并为单一 DataFrame
"""
import pandas as pd
import os
import glob

def load_all_catalogs():
    """
    读取 data_import 文件夹中所有根级 CSV 文件（不包括子文件夹）
    返回合并后的 DataFrame
    """
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    IMPORT_DIR = os.path.join(BASE_DIR, 'data_import')
    
    if not os.path.exists(IMPORT_DIR):
        raise FileNotFoundError(f"找不到 data_import 文件夹: {IMPORT_DIR}")
    
    # 获取 data_import 根目录下的所有 CSV 文件（不包括子文件夹）
    csv_files = [
        f for f in glob.glob(os.path.join(IMPORT_DIR, '*.csv'))
        if os.path.isfile(f)  # 确保是文件，不是文件夹
    ]
    
    if not csv_files:
        raise FileNotFoundError(f"data_import 文件夹中没有找到 CSV 文件")
    
    print(f"📁 找到 {len(csv_files)} 个目录文件:")
    for csv_file in csv_files:
        print(f"   - {os.path.basename(csv_file)}")
    
    # 合并所有 CSV
    dfs = []
    for csv_file in csv_files:
        try:
            df = pd.read_csv(csv_file)
            dfs.append(df)
            print(f"   ✓ 已加载: {os.path.basename(csv_file)} ({len(df)} 行)")
        except Exception as e:
            print(f"   ✗ 错误: {os.path.basename(csv_file)} - {e}")
    
    if not dfs:
        raise ValueError("无法读取任何 CSV 文件")
    
    # 合并 DataFrame
    merged_df = pd.concat(dfs, ignore_index=True)
    print(f"\n✅ 合并完成: 共 {len(merged_df)} 行资产")
    
    return merged_df
