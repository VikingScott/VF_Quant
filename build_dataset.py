import pandas as pd
import os
import sys
from catalog_loader import load_all_catalogs

# ================= 配置 =================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, 'data', 'daily_csv')
OUTPUT_DIR = os.path.join(BASE_DIR, 'data', 'processed')

os.makedirs(OUTPUT_DIR, exist_ok=True)

def load_catalog():
    return load_all_catalogs()

def build_matrix(ticker_list, value_col_priority=['adj_close', 'close']):
    """
    输入: ticker 列表
    输出: 一个 DataFrame，Index是日期，Columns是Ticker
    """
    print(f"正在合并 {len(ticker_list)} 个资产的数据...")
    
    series_list = []
    
    for ticker in ticker_list:
        file_path = os.path.join(DATA_DIR, f"{ticker}.csv")
        
        if not os.path.exists(file_path):
            print(f"  [Skip] 缺失文件: {ticker}")
            continue
            
        try:
            # 读取数据
            df = pd.read_csv(file_path, parse_dates=['date'], index_col='date')
            
            # 简单的列名标准化
            df.columns = [c.lower() for c in df.columns]
            
            # 确定取哪一列
            target_col = None
            for col in value_col_priority:
                if col in df.columns:
                    target_col = col
                    break
            
            if target_col:
                # 提取 Series 并重命名为 Ticker
                s = df[target_col]
                s.name = ticker
                # 移除重复索引（以防万一）
                s = s[~s.index.duplicated(keep='last')]
                series_list.append(s)
            else:
                print(f"  [Warning] {ticker} 既没有 adj_close 也没有 close")
                
        except Exception as e:
            print(f"  [Error] 处理 {ticker} 时出错: {e}")

    if not series_list:
        return pd.DataFrame()

    # 核心步骤：外连接合并 (Outer Join)
    # 这样可以保留所有日期，即使某些资产在某天没数据 (NaN)
    print("正在拼接矩阵 (这可能需要一点时间)...")
    matrix = pd.concat(series_list, axis=1, sort=True)
    
    # 按日期排序
    matrix.sort_index(inplace=True)
    
    return matrix

def main():
    # 1. 读取清单
    catalog = load_catalog()
    
    # 只处理活跃的
    active_df = catalog[catalog['is_active'] == 1]
    
    # 2. 分组
    # 定义宏观/指数类别
    macro_types = ['Index', 'Macro']
    
    # 拆分 Ticker
    macro_tickers = active_df[active_df['asset_class'].isin(macro_types)]['ticker'].tolist()
    tradeable_tickers = active_df[~active_df['asset_class'].isin(macro_types)]['ticker'].tolist()
    
    print(f"分类统计: 可交易资产 {len(tradeable_tickers)} 个 | 宏观指数 {len(macro_tickers)} 个")

    # 3. 构建可交易资产矩阵 (使用 Adj Close 计算收益)
    if tradeable_tickers:
        print("\n--- 构建可交易资产矩阵 (Prices) ---")
        price_matrix = build_matrix(tradeable_tickers, value_col_priority=['adj_close', 'close'])
        
        # 保存
        out_path = os.path.join(OUTPUT_DIR, 'universe_prices.csv')
        price_matrix.to_csv(out_path)
        print(f"✅ 保存成功: {out_path}")
        print(f"   维度: {price_matrix.shape} (行=日期, 列=资产)")
        print(f"   时间范围: {price_matrix.index[0].date()} 到 {price_matrix.index[-1].date()}")

    # 4. 构建宏观指数矩阵 (使用 Close/Value)
    if macro_tickers:
        print("\n--- 构建宏观指数矩阵 (Indicators) ---")
        # 宏观数据通常没有 'adj_close'，只有 'close' 或 'value' (FRED)
        # 我们的 update_data.py 把 FRED 数据也标准化为了 close，所以这里优先取 close
        macro_matrix = build_matrix(macro_tickers, value_col_priority=['close', 'value', 'adj_close'])
        
        # 保存
        out_path = os.path.join(OUTPUT_DIR, 'macro_indicators.csv')
        macro_matrix.to_csv(out_path)
        print(f"✅ 保存成功: {out_path}")
        print(f"   维度: {macro_matrix.shape}")

    print("\n=== 全部完成 ===")

if __name__ == "__main__":
    main()