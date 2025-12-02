import pandas as pd
import numpy as np
import os
import glob
import sys

# ================= 配置 =================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, 'data', 'daily_csv')
CATALOG_PATH = os.path.join(BASE_DIR, 'master_catalog.csv')

# 阈值设置
CORR_THRESHOLD_STRICT = 0.995 # 极高相关性，几乎肯定是重复，直接杀
CORR_THRESHOLD_LOOSE = 0.98   # 高相关性，需要检查是否是 Smart Beta

# 豁免关键词 (Smart Beta / 因子 / 风格)
# 如果名字里带这些词，即使相关性在 0.98-0.995 之间，也被视为“不同策略”，予以保留
WHITELIST_KEYWORDS = [
    'VALUE', 'GROWTH', 'QUALITY', 'MOMENTUM', 'LOW VOL', 'MIN VOL', 
    'FACTOR', 'EQUAL WEIGHT', 'DIVIDEND', 'ALPHADEX', 'FUNDAMENTAL'
]

def load_active_universe():
    """
    只读取 master_catalog 中 is_active=1 的资产
    返回: dict {ticker: description} 用于白名单检查
    """
    if not os.path.exists(CATALOG_PATH):
        print("错误: 找不到 master_catalog.csv")
        sys.exit(1)
        
    df = pd.read_csv(CATALOG_PATH)
    
    # 核心逻辑：只取活跃资产
    active_df = df[df['is_active'] == 1].copy()
    
    # 构建字典 {ticker: 'Description String'}
    # 拼合 sub_class 和 description 以便全面检查关键词
    meta_dict = {}
    for _, row in active_df.iterrows():
        desc = str(row.get('description', '')) + " " + str(row.get('sub_class', '')) + " " + str(row.get('sector_style', ''))
        meta_dict[row['ticker']] = desc.upper()
        
    print(f"📋 从 Catalog 中读取到 {len(meta_dict)} 个活跃资产。")
    return meta_dict

def load_data_for_active(active_tickers_list):
    """
    只加载活跃资产的 CSV
    """
    print("正在加载活跃资产的历史数据...")
    price_dict = {}
    stats = {}
    
    for ticker in active_tickers_list:
        file_path = os.path.join(DATA_DIR, f"{ticker}.csv")
        
        if not os.path.exists(file_path):
            # 可能是新加的还没下载，跳过
            continue
            
        try:
            df = pd.read_csv(file_path, parse_dates=['date'], index_col='date')
            if df.empty: continue
            
            # 简单的列名处理
            df.columns = [c.lower() for c in df.columns]
            col = 'adj_close' if 'adj_close' in df.columns else 'close'
            price_dict[ticker] = df[col]
            
            # 统计属性
            recent = df.tail(60) # 最近一季度的流动性
            avg_dollar_vol = 0
            if 'volume' in recent.columns and 'close' in recent.columns:
                avg_dollar_vol = (recent['close'] * recent['volume']).mean()
                
            stats[ticker] = {
                'days': len(df),
                'liquidity': avg_dollar_vol
            }
            
        except Exception as e:
            print(f"Warning: 读取 {ticker} 出错: {e}")

    # 合并
    full_prices = pd.DataFrame(price_dict)
    # 至少要有半年的重叠数据才计算相关性
    full_prices = full_prices.dropna(axis=1, thresh=120)
    
    return full_prices, stats

def is_smart_beta(ticker, meta_dict):
    """检查是否属于因子/聪明贝塔策略"""
    desc = meta_dict.get(ticker, "")
    for kw in WHITELIST_KEYWORDS:
        if kw in desc:
            return True, kw
    return False, None

def find_duplicates(prices, stats, meta_dict):
    print(f"\n正在计算 {len(prices.columns)} 只资产的相关性矩阵...")
    
    returns = prices.pct_change()
    corr_matrix = returns.corr(min_periods=120)
    
    duplicates = []
    
    cols = corr_matrix.columns
    for i in range(len(cols)):
        for j in range(i + 1, len(cols)):
            t_a = cols[i]
            t_b = cols[j]
            score = corr_matrix.iloc[i, j]
            
            # 只有相关性很高才处理
            if score < CORR_THRESHOLD_LOOSE:
                continue
            
            # === 核心判决逻辑 ===
            
            # 1. 检查白名单 (Smart Beta)
            is_sb_a, kw_a = is_smart_beta(t_a, meta_dict)
            is_sb_b, kw_b = is_smart_beta(t_b, meta_dict)
            
            # 如果包含 Smart Beta，且相关性没到 "变态高" (0.995)，则豁免
            if (is_sb_a or is_sb_b) and score < CORR_THRESHOLD_STRICT:
                # 这里我们选择【不报告】，或者报告为【Safe Pair】
                # 为了保持 Kill List 干净，我们直接跳过，意味着系统认为它们“不重复”
                continue
            
            # 2. PK 逻辑
            stat_a = stats.get(t_a, {})
            stat_b = stats.get(t_b, {})
            
            liq_a = stat_a.get('liquidity', 0)
            liq_b = stat_b.get('liquidity', 0)
            days_a = stat_a.get('days', 0)
            days_b = stat_b.get('days', 0)
            
            # 历史长度权重 (如果差很多年)
            year_diff = abs(days_a - days_b) / 252
            
            if year_diff > 5:
                # 历史优先
                if days_a > days_b:
                    winner, loser = t_a, t_b
                    reason = f"历史更长 (+{year_diff:.1f}y)"
                else:
                    winner, loser = t_b, t_a
                    reason = f"历史更长 (+{year_diff:.1f}y)"
            else:
                # 流动性优先
                if liq_a > liq_b:
                    winner, loser = t_a, t_b
                    ratio = liq_a / (liq_b + 1)
                    reason = f"流动性更好 ({ratio:.1f}x)"
                else:
                    winner, loser = t_b, t_a
                    ratio = liq_b / (liq_a + 1)
                    reason = f"流动性更好 ({ratio:.1f}x)"
            
            duplicates.append({
                'Keep': winner,
                'Drop': loser,
                'Correlation': score,
                'Reason': reason,
                'Type': 'Hard Duplicate' if score >= CORR_THRESHOLD_STRICT else 'Soft Duplicate'
            })

    return pd.DataFrame(duplicates)

def main():
    # 1. 获取名单
    meta_dict = load_active_universe()
    active_tickers = list(meta_dict.keys())
    
    if not active_tickers:
        print("没有活跃资产。请检查 master_catalog.csv。")
        return

    # 2. 加载数据
    prices, stats = load_data_for_active(active_tickers)
    
    # 3. 查找重复
    result_df = find_duplicates(prices, stats, meta_dict)
    
    if result_df.empty:
        print("\n✅ 资产池很干净！没有发现需移除的重复项。")
    else:
        # 去重：同一个 Drop 可能出现多次，取相关性最高的那个理由
        result_df = result_df.sort_values('Correlation', ascending=False)
        result_df = result_df.drop_duplicates(subset=['Drop'])
        
        print("\n" + "="*80)
        print(f"🔪 建议移除清单 (共 {len(result_df)} 个)")
        print(f"筛选标准: Active Only | Smart Beta Protected | Strict > {CORR_THRESHOLD_STRICT}")
        print("="*80)
        
        print(result_df[['Keep', 'Drop', 'Correlation', 'Reason']].to_string(index=False))
        
        # 保存
        out_path = os.path.join(BASE_DIR, 'duplicates_report.csv')
        result_df.to_csv(out_path, index=False)
        print(f"\n报告已保存至: {out_path}")
        
        # 打印方便复制的 Kill List
        drops = result_df['Drop'].tolist()
        print("\n[Action Info] 请去 master_catalog.csv 将以下 Ticker 的 is_active 设为 0:")
        print(", ".join(drops))

if __name__ == "__main__":
    main()