import pandas as pd
import yfinance as yf
import pandas_datareader.data as web
import os
import sys
import datetime
from catalog_loader import load_all_catalogs

# ================= 配置区 =================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_IMPORT_DIR = os.path.join(BASE_DIR, 'data_import')
# 默认保存到 master_catalog.csv（在 data_import 文件夹中）
CATALOG_PATH = os.path.join(DATA_IMPORT_DIR, 'master_catalog.csv')

# ================= 智能推断逻辑 =================

def infer_metadata(ticker):
    """根据 Ticker 去网上抓取信息"""
    print(f"🔍 正在连接 Yahoo Finance 侦测 {ticker} ...")
    
    data = {
        'ticker': ticker,
        'yf_ticker': ticker,
        'asset_class': 'Unknown',
        'sub_class': '',
        'region': 'US',
        'sector_style': '',
        'currency': 'USD',
        'exchange': '',
        'source_main': 'yahoo',
        'source_backup': 'stooq',
        'is_active': 1,
        'description': '',
        'tier': 'Satellite',
        'note': ''
    }

    try:
        t = yf.Ticker(ticker)
        info = t.info
        
        if 'quoteType' not in info and 'shortName' not in info:
            raise ValueError("Yahoo No Data")
            
        name = info.get('longName') or info.get('shortName') or ticker
        data['description'] = name
        data['currency'] = info.get('currency', 'USD')
        data['exchange'] = info.get('exchange', '')
        q_type = info.get('quoteType', '').upper()
        category = info.get('category', '') or info.get('sector', '')

        name_lower = name.lower()
        
        # --- 智能分类 ---
        if q_type == 'CRYPTOCURRENCY':
            data['asset_class'] = 'Crypto'
            data['sub_class'] = 'Cryptocurrency'
            data['region'] = 'Global'
            data['source_backup'] = 'coingecko'
        
        elif q_type == 'ETF':
            data['asset_class'] = 'Equity' 
            
            if 'bond' in name_lower or 'treasury' in name_lower or 'fixed income' in name_lower:
                data['asset_class'] = 'Fixed Income'
                data['sub_class'] = category if category else 'Bond ETF'
            elif 'commodity' in name_lower or 'gold' in name_lower or 'oil' in name_lower:
                data['asset_class'] = 'Commodity'
                data['sub_class'] = 'Commodity ETF'
            elif 'bitcoin' in name_lower or 'ether' in name_lower or 'crypto' in name_lower:
                data['asset_class'] = 'Equity' 
                data['sub_class'] = 'Crypto ETF'
                data['sector_style'] = 'Digital Assets'
            else:
                data['sub_class'] = category if category else 'Equity ETF'

        elif q_type == 'INDEX':
            data['asset_class'] = 'Index'
            data['source_backup'] = ''

        # --- Region ---
        if 'china' in name_lower: data['region'] = 'China'
        elif 'japan' in name_lower: data['region'] = 'Japan'
        elif 'europe' in name_lower: data['region'] = 'Europe'
        elif 'global' in name_lower or 'world' in name_lower: data['region'] = 'Global'
        elif 'emerging' in name_lower: data['region'] = 'Emerging Markets'

        return data

    except Exception as e:
        print(f"⚠️ Yahoo 失败 ({e})。尝试连接 FRED...")
        try:
            start = datetime.datetime.now() - datetime.timedelta(days=30)
            df = web.DataReader(ticker, 'fred', start)
            if not df.empty:
                print("✅ FRED 验证成功！")
                data['asset_class'] = 'Macro'
                data['sub_class'] = 'Economic Indicator'
                data['source_main'] = 'fred'
                data['source_backup'] = ''
                data['description'] = f"FRED Data: {ticker}"
                data['yf_ticker'] = ticker
                return data
        except:
            print(f"❌ 所有数据源均失败。将使用空模版。")
    
    return data

# ================= 交互逻辑 =================

def display_entry(data, title="准备录入"):
    print("\n" + "="*50)
    print(f"   {title}: {data['ticker']}")
    print("="*50)
    df = pd.DataFrame([data]).T
    df.columns = ['Value']
    print(df)
    print("="*50)

def load_catalog():
    if os.path.exists(CATALOG_PATH):
        return pd.read_csv(CATALOG_PATH)
    return pd.DataFrame()

def save_to_catalog(new_row_dict):
    try:
        df = load_catalog()
        # 覆盖逻辑：先删旧的
        if new_row_dict['ticker'] in df['ticker'].values:
            df = df[df['ticker'] != new_row_dict['ticker']]
        
        new_df = pd.DataFrame([new_row_dict])
        
        # 补齐列
        if not df.empty:
            for col in df.columns:
                if col not in new_df.columns: new_df[col] = ''
            new_df = new_df[df.columns]
        
        final_df = pd.concat([df, new_df], ignore_index=True)
        final_df.to_csv(CATALOG_PATH, index=False)
        print(f"✅ 已保存 {new_row_dict['ticker']}。")
    except Exception as e:
        print(f"❌ 保存失败: {e}")

def process_one_ticker(ticker_input):
    ticker_input = ticker_input.strip().upper()
    if not ticker_input: return

    # 1. 检查是否存在
    df = load_catalog()
    existing_row = None
    if not df.empty and ticker_input in df['ticker'].values:
        existing_row = df[df['ticker'] == ticker_input].iloc[0].to_dict()
        print(f"\n⚠️ 提示: {ticker_input} 已经存在于系统中。")
        # 显示现有信息
        display_entry(existing_row, title="现有信息")
        
        choice = input("你需要修改它吗？(y/N/refresh): ").lower()
        if choice == 'refresh':
            print("正在重新联网抓取...")
            # 继续往下走，去 infer
        elif choice == 'y':
            # 使用现有数据作为起点进行修改
            entry = existing_row
            # 跳过 infer，直接进 edit loop
            edit_loop(entry)
            return
        else:
            print("已跳过。")
            return

    # 2. 联网抓取
    entry = infer_metadata(ticker_input)
    
    # 3. 编辑保存
    edit_loop(entry)

def edit_loop(entry):
    """编辑并保存的子循环"""
    while True:
        display_entry(entry)
        print("\n[y] 保存  [n] 放弃  [set key val] 修改")
        cmd = input("指令: ").strip()
        
        if cmd.lower() == 'y':
            save_to_catalog(entry)
            break
        elif cmd.lower() == 'n':
            print("已放弃。")
            break
        elif cmd.startswith('set '):
            try:
                parts = cmd.split(' ', 2)
                if len(parts) == 3:
                    key, val = parts[1], parts[2]
                    if key in entry:
                        entry[key] = val
                        print(f"👌 {key} -> {val}")
                    else:
                        print(f"❌ 字段 {key} 不存在")
            except: pass
        else:
            print("❌ 无效指令")

def main():
    print("=== 资产录入系统 (按 Ctrl+C 退出) ===")
    
    # 支持命令行参数启动一次
    if len(sys.argv) > 1:
        process_one_ticker(sys.argv[1])
        # 处理完命令行参数后，依然进入循环模式，或者退出？
        # 通常命令行模式意味着一次性任务，这里直接退出比较符合直觉
        return

    while True:
        try:
            user_input = input("\n请输入 Ticker (例如 IBIT, DGS10): ").strip()
            if not user_input: continue
            if user_input.lower() in ['exit', 'quit', 'q']: break
            
            process_one_ticker(user_input)
            
        except KeyboardInterrupt:
            print("\n退出程序。")
            break
        except Exception as e:
            print(f"发生错误: {e}")

if __name__ == "__main__":
    main()