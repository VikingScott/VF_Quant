import pandas as pd
import yfinance as yf
import pandas_datareader.data as web
import os
import datetime
import time
import warnings

# 忽略一些 Pandas 的 Future Warnings，保持控制台清爽
warnings.simplefilter(action='ignore', category=FutureWarning)

# ================= 配置区 =================

# 获取当前脚本所在的绝对路径，确保相对路径不出错
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CATALOG_PATH = os.path.join(BASE_DIR, 'master_catalog.csv')
DATA_DIR = os.path.join(BASE_DIR, 'data', 'daily_csv') # 数据存放在 yfdata/data/daily_csv
LOG_DIR = os.path.join(BASE_DIR, 'logs')

# 确保目录存在
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)

# ================= 辅助函数 =================

def load_catalog():
    if not os.path.exists(CATALOG_PATH):
        raise FileNotFoundError(f"找不到资产清单，请检查路径: {CATALOG_PATH}")
    return pd.read_csv(CATALOG_PATH)

def get_last_date_from_csv(file_path):
    """
    读取 CSV 最后一行获取日期。
    """
    if not os.path.exists(file_path):
        return None
    try:
        # 只读最后几行来确定日期，避免读取大文件
        # 如果文件很小，直接读没问题；为了通用性，我们用 pandas 读 tail
        with open(file_path, 'r') as f:
            # 简单的检查文件是否为空
            if len(f.readlines()) < 2: 
                return None
        
        df = pd.read_csv(file_path)
        if df.empty or 'date' not in df.columns:
            return None
        
        # 确保 date 列是 datetime 类型
        last_date_str = df['date'].iloc[-1]
        return pd.to_datetime(last_date_str)
    except Exception as e:
        print(f"    [Warning] 读取 CSV 日期失败: {e}，将重新全量下载。")
        return None

def clean_data(df, asset_class):
    """
    核心清洗逻辑：剔除脏数据
    """
    # 1. 去除空值行
    df.dropna(how='all', inplace=True)
    
    # 2. 针对 ETF/Stock/Crypto 的特殊清洗
    if asset_class in ['Equity', 'Fixed Income', 'Commodity', 'Crypto', 'Alternative']:
        # 必须有 Volume 列
        if 'volume' in df.columns:
            # 剔除 Volume 为 0 的行 (通常是停牌、未上市填充数据、或节假日错误数据)
            # 注意：Crypto 24小时交易，Volume 0 几乎肯定也是错的
            original_len = len(df)
            df = df[df['volume'] > 0]
            dropped_len = original_len - len(df)
            if dropped_len > 0:
                # 只有当剔除量很大时才显示，避免刷屏
                if dropped_len > 100: 
                    print(f"      -> 清洗: 剔除了 {dropped_len} 行 Volume=0 的无效数据")
    
    # 3. 再次去除因清洗产生的头部空缺（比如刚上市那几天可能数据不稳）
    df.sort_index(inplace=True)
    
    return df

def fetch_yahoo_data(ticker, yf_ticker, start_date):
    print(f"    -> 下载中: {ticker} ({yf_ticker})...")
    try:
        # 核心修正：下载时加上 threads=False 防止某些环境报错，加上 timeout
        # auto_adjust=False: 拿到原始 Close 和 Adj Close，方便排查偏差
        df = yf.download(yf_ticker, start=start_date, progress=False, 
                         auto_adjust=False, multi_level_index=False, threads=False)
        
        if df.empty:
            return None

        # 标准化列名
        df.columns = [c.lower().replace(' ', '_') for c in df.columns]
        
        # 必要的列检查
        required = ['open', 'high', 'low', 'close', 'volume']
        for col in required:
            if col not in df.columns:
                print(f"      [Error] 缺少核心字段 {col}，跳过")
                return None
        
        # 如果没有 adj_close，用 close 补齐
        if 'adj_close' not in df.columns:
            df['adj_close'] = df['close']

        # 移除时区
        df.index = df.index.tz_localize(None)
        df.index.name = 'date'
        
        return df
    except Exception as e:
        print(f"      [Error] Yahoo API 报错: {e}")
        return None

def fetch_fred_data(ticker, fred_id, start_date):
    print(f"    -> FRED 下载中: {ticker}...")
    try:
        df = web.DataReader(fred_id, 'fred', start_date)
        if df.empty: return None
        df.columns = ['close'] # 宏观数据通常视为 close
        df['open'] = df['close']
        df['high'] = df['close']
        df['low'] = df['close']
        df['adj_close'] = df['close']
        df['volume'] = 0 # 宏观数据没有成交量
        df.index.name = 'date'
        return df
    except Exception as e:
        print(f"      [Error] FRED API 报错: {e}")
        return None

# ================= 主逻辑 =================

def update_asset(row):
    ticker = row['ticker']
    yf_ticker = row['yf_ticker']
    source = row['source_main']
    asset_class = row['asset_class']
    
    file_path = os.path.join(DATA_DIR, f"{ticker}.csv")
    
    # 1. 确定时间
    last_date = get_last_date_from_csv(file_path)
    
    if last_date is None:
        start_date = "1980-01-01" # 足够早，覆盖大多数 ETF
        mode = "w" # 写入模式
        header = True
    else:
        start_date = last_date + datetime.timedelta(days=1)
        mode = "a" # 追加模式
        header = False
        
        # 简单检查：如果是将来时间，就不跑了
        if start_date > datetime.datetime.now():
            return

    # 2. 下载
    if source == 'yahoo':
        new_data = fetch_yahoo_data(ticker, yf_ticker, start_date)
    elif source == 'fred':
        new_data = fetch_fred_data(ticker, yf_ticker, start_date)
    else:
        print(f"    [Skip] 未知数据源: {source}")
        return

    # 3. 处理与保存
    if new_data is not None and not new_data.empty:
        # 清洗
        new_data = clean_data(new_data, asset_class)
        
        if new_data.empty:
            print(f"      [Info] 清洗后无有效数据。")
            return

        # 僵尸检查 (只在全量下载或很久没更新时检查)
        last_data_date = new_data.index[-1]
        days_diff = (datetime.datetime.now() - last_data_date).days
        if days_diff > 15 and asset_class != 'Macro':
            print(f"      [Warning] ⚠️ 数据停滞警告! 最新日期是 {last_data_date.date()} (距今 {days_diff} 天)。该 ETF 可能已退市。")
        
        # 写入 CSV
        # 注意：CSV 追加时，需要处理 header
        new_data.to_csv(file_path, mode=mode, header=header)
        
        action_str = "初始化" if mode == 'w' else "更新"
        print(f"    [OK] {action_str}完成. 最新日期: {last_data_date.date()}. 条数: {len(new_data)}")
        
    else:
        if mode == 'w':
            print(f"    [Warning] 未获取到任何数据。")

def main():
    print(f"=== yfdata 数据更新启动 ===")
    print(f"数据目录: {DATA_DIR}")
    
    try:
        catalog = load_catalog()
    except Exception as e:
        print(e)
        return

    active_assets = catalog[catalog['is_active'] == 1]
    total = len(active_assets)
    
    print(f"待处理资产数: {total}")
    
    for index, row in active_assets.iterrows():
        print(f"[{index+1}/{total}] {row['ticker']} ({row['asset_class']})")
        update_asset(row)
        time.sleep(0.3) # 稍微快一点，CSV I/O 比较慢

    print("\n=== 全部完成 ===")

if __name__ == "__main__":
    main()