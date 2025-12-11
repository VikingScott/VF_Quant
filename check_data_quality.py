import pandas as pd
import numpy as np
import os
import glob
from datetime import datetime, timedelta
from catalog_loader import load_all_catalogs

# ================= 配置区 =================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, 'data', 'daily_csv')

def analyze_file(file_path):
    """
    对单个CSV文件进行体检
    """
    file_name = os.path.basename(file_path)
    ticker = file_name.replace('.csv', '')
    
    try:
        df = pd.read_csv(file_path)
        
        # 1. 基础检查
        if df.empty:
            return {'ticker': ticker, 'status': 'EMPTY FILE'}
        
        if 'date' not in df.columns:
            return {'ticker': ticker, 'status': 'NO DATE COL'}

        # 转换日期并排序
        df['date'] = pd.to_datetime(df['date'])
        df.sort_values('date', inplace=True)
        df.set_index('date', inplace=True)
        
        # 2. 计算指标
        start_date = df.index[0]
        end_date = df.index[-1]
        total_days = len(df)
        
        # 3. 检查断层 (Gaps)
        # 计算相邻两个交易日的间隔
        days_diff = df.index.to_series().diff().dt.days
        # 正常的周末是间隔3天，长假可能4-5天。超过10天通常意味着缺失
        max_gap = days_diff.max()
        gap_count = (days_diff > 10).sum() # 缺口超过10天的次数
        
        # 4. 检查缺失值 (NA)
        # 统计所有列的空值总数 / (行数 * 列数)
        na_count = df.isna().sum().sum()
        na_pct = (na_count / (total_days * len(df.columns))) * 100
        
        # 5. 检查 Volume (僵尸交易)
        # 宏观数据(FRED)通常没有Volume，需特殊处理
        zero_vol_pct = 0.0
        if 'volume' in df.columns:
             # 有些数据源用 0 表示无交易，有些用 NaN
             zero_vol_count = (df['volume'] == 0).sum() + df['volume'].isna().sum()
             zero_vol_pct = (zero_vol_count / total_days) * 100
        
        # 6. 价格异常检查 (Extreme Returns)
        # 简单计算单日涨跌幅。如果 Close 是 0 会报错，先替换
        safe_close = df['close'].replace(0, np.nan) 
        daily_ret = safe_close.pct_change().abs()
        # 超过 25% 的剧烈波动次数 (非杠杆ETF很少见，除非拆股没处理好)
        extreme_moves = (daily_ret > 0.25).sum()

        return {
            'ticker': ticker,
            'status': 'OK',
            'start_date': start_date.strftime('%Y-%m-%d'),
            'end_date': end_date.strftime('%Y-%m-%d'),
            'total_rows': total_days,
            'max_gap_days': max_gap if not np.isnan(max_gap) else 0,
            'na_pct': round(na_pct, 2),
            'zero_vol_pct': round(zero_vol_pct, 2),
            'extreme_moves': extreme_moves
        }

    except Exception as e:
        return {'ticker': ticker, 'status': f"ERROR: {str(e)}"}

def main():
    print(f"=== 开始数据体检 ===")
    print(f"扫描目录: {DATA_DIR}")
    
    csv_files = glob.glob(os.path.join(DATA_DIR, "*.csv"))
    
    if not csv_files:
        print("未找到任何CSV文件！请先运行 update_data.py")
        return

    results = []
    for f in csv_files:
        res = analyze_file(f)
        results.append(res)
    
    # 转为 DataFrame 展示
    report = pd.DataFrame(results)
    
    # 重新排序列，方便阅读
    cols = ['ticker', 'status', 'start_date', 'end_date', 'total_rows', 
            'max_gap_days', 'zero_vol_pct', 'extreme_moves']
    
    # 确保列存在（防止全报错的情况）
    cols = [c for c in cols if c in report.columns]
    report = report[cols]
    
    # ================= 打印高亮报告 =================
    
    print("\n--- [1] 严重问题文件 (Errors/Empty) ---")
    bad_files = report[report['status'] != 'OK']
    if not bad_files.empty:
        print(bad_files)
    else:
        print("无。")
        
    print("\n--- [2] 可能的数据断层 (Gap > 10 days) ---")
    gaps = report[(report['status']=='OK') & (report['max_gap_days'] > 10)]
    if not gaps.empty:
        print(gaps[['ticker', 'start_date', 'end_date', 'max_gap_days']])
    else:
        print("无明显断层。")

    print("\n--- [3] 僵尸数据 (Zero Volume > 5%) ---")
    # 过滤掉 FRED 数据 (通常 FRED 没有 Volume，或者我们在 update 时填了 0)
    # 我们可以简单通过 ticker 长度判断，或者只看 Equity/ETF
    # 这里简单粗暴：如果 zero_vol_pct 很高且不是宏观数据
    zombies = report[(report['status']=='OK') & (report['zero_vol_pct'] > 5.0)]
    if not zombies.empty:
        print(zombies[['ticker', 'zero_vol_pct', 'total_rows']])
    else:
        print("无僵尸数据。")

    print("\n--- [4] 价格异常 (Extreme Moves > 25%) ---")
    crazy = report[(report['status']=='OK') & (report['extreme_moves'] > 0)]
    if not crazy.empty:
        print(crazy[['ticker', 'extreme_moves', 'start_date']])
    else:
        print("价格波动都在正常范围内。")

    print("\n--- [5] 概览 (前10个) ---")
    print(report.head(10).to_string(index=False))
    
    # 保存报告
    output_path = os.path.join(BASE_DIR, 'data_quality_report.csv')
    report.to_csv(output_path, index=False)
    print(f"\n完整报告已保存至: {output_path}")

if __name__ == "__main__":
    main()