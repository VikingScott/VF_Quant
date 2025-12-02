import pandas as pd
import os
import datetime
try:
    import holidays
except ImportError:
    print("错误: 请先安装 holidays 库 -> pip install holidays")
    exit()

# ================= 配置区 =================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, 'data', 'daily_csv')
CATALOG_PATH = os.path.join(BASE_DIR, 'master_catalog.csv')
REPORT_PATH = os.path.join(BASE_DIR, 'gap_report.csv')

def load_catalog_map():
    """
    加载资产目录，返回一个字典: {ticker: {'asset_class': ..., 'region': ...}}
    这样我们可以快速查表。
    """
    if not os.path.exists(CATALOG_PATH):
        print("警告: 找不到 master_catalog.csv，将默认所有资产都使用 NYSE 规则。")
        return {}
    
    df = pd.read_csv(CATALOG_PATH)
    # 建立映射字典
    mapping = {}
    for _, row in df.iterrows():
        mapping[row['ticker']] = {
            'asset_class': row.get('asset_class', 'Equity'),
            'region': row.get('region', 'US')
        }
    return mapping

def get_market_holidays(years, region='US'):
    """根据区域获取假期对象"""
    # 目前主要支持 US/NYSE，未来可以扩展 Global
    if region == 'US':
        try:
            return holidays.NYSE(years=years)
        except AttributeError:
            return holidays.US(years=years)
    else:
        # 暂时默认所有 ETF (即使是 EWU/EWJ) 都在美股交易，所以都遵循 NYSE 规则
        # 除非是真正的原始外盘数据。我们这里假设都是 Yahoo Finance US ticker。
        try:
            return holidays.NYSE(years=years)
        except:
            return holidays.US(years=years)

def check_one_ticker(file_path, ticker, meta_info):
    """检查单个文件的缺口"""
    asset_class = meta_info.get('asset_class', 'Equity')
    
    # 1. 宏观数据跳过 (Macro)
    if asset_class == 'Macro':
        return {
            'ticker': ticker, 'asset_class': asset_class, 'status': 'SKIPPED (Macro)',
            'total_days': 0, 'missing_cnt': 0, 'unexplained_cnt': 0, 'details': 'Macro data irregular'
        }

    try:
        df = pd.read_csv(file_path)
        if df.empty or 'date' not in df.columns:
            return {'ticker': ticker, 'asset_class': asset_class, 'status': 'EMPTY/INVALID', 'missing_cnt': 0}
        
        df['date'] = pd.to_datetime(df['date'])
        df.set_index('date', inplace=True)
        df.sort_index(inplace=True)
        
        start_date = df.index[0]
        end_date = df.index[-1]
        
        # 2. 根据资产类别选择“完美时间轴”
        if asset_class == 'Crypto':
            # Crypto 是 7x24，每一天都必须有
            full_range = pd.date_range(start=start_date, end=end_date, freq='D')
            market_holidays = {} # Crypto 无假期
        else:
            # ETF, Equity, Index, Fixed Income, Commodity -> 默认走美股交易日 (周一至周五)
            full_range = pd.date_range(start=start_date, end=end_date, freq='B')
            years = range(start_date.year, end_date.year + 1)
            market_holidays = get_market_holidays(years)

        # 3. 计算缺失
        missing_days = full_range.difference(df.index)
        
        if len(missing_days) == 0:
            return {
                'ticker': ticker, 'asset_class': asset_class, 'status': 'PERFECT',
                'total_days': len(df), 'missing_cnt': 0, 'unexplained_cnt': 0, 'details': ''
            }
        
        # 4. 分析缺口原因
        unexplained_dates = []
        explained_count = 0
        
        for date in missing_days:
            date_obj = date.date()
            
            # 规则 1: 交易所假期 (Crypto跳过此步)
            if asset_class != 'Crypto' and date_obj in market_holidays:
                explained_count += 1
                continue
                
            # 规则 2: 历史特殊闭市 (911, 飓风, 国丧日)
            is_special = False
            if asset_class != 'Crypto':
                if (date.year == 2001 and date.month == 9 and 11 <= date.day <= 14) or \
                   (date.year == 2012 and date.month == 10 and 29 <= date.day <= 30) or \
                   (date.year == 2018 and date.month == 12 and date.day == 5):
                    is_special = True
            
            if is_special:
                explained_count += 1
                continue
            
            # 如果都无法解释，记录下来
            unexplained_dates.append(date_obj)

        unexplained_cnt = len(unexplained_dates)
        
        # 生成简报
        status = 'OK'
        details = f"Normal Holidays: {explained_count}"
        
        if unexplained_cnt > 0:
            status = 'WARNING' if unexplained_cnt < 10 else 'ERROR'
            # 只列出前3个未知日期
            details = f"UNEXPLAINED: {unexplained_cnt} days (e.g. {unexplained_dates[:3]}...)"

        return {
            'ticker': ticker,
            'asset_class': asset_class,
            'status': status,
            'total_days': len(df),
            'missing_cnt': len(missing_days),
            'unexplained_cnt': unexplained_cnt,
            'details': details
        }

    except Exception as e:
        return {'ticker': ticker, 'asset_class': asset_class, 'status': f'CRASH: {str(e)}', 'missing_cnt': 0}

def main():
    print(f"=== 开始批量 Gap 检查 ===")
    
    # 1. 加载目录映射
    meta_map = load_catalog_map()
    
    # 2. 扫描文件
    csv_files = [f for f in os.listdir(DATA_DIR) if f.endswith('.csv')]
    if not csv_files:
        print("没有找到CSV文件。")
        return

    results = []
    print(f"正在扫描 {len(csv_files)} 个文件... (跳过 Macro, 区分 Crypto/Index)")
    
    for filename in csv_files:
        ticker = filename.replace('.csv', '')
        
        # 从 catalog 获取元数据，如果没有则默认
        meta = meta_map.get(ticker, {})
        
        res = check_one_ticker(os.path.join(DATA_DIR, filename), ticker, meta)
        results.append(res)
    
    # 3. 生成报告 DataFrame
    report_df = pd.DataFrame(results)
    
    # 排序：先看 Error，再看 Warning，最后看 Perfect
    # 为了自定义排序，我们可以映射 status 为数字
    status_order = {'CRASH': 0, 'ERROR': 1, 'WARNING': 2, 'EMPTY/INVALID': 3, 'OK': 4, 'PERFECT': 5, 'SKIPPED (Macro)': 6}
    report_df['sort_key'] = report_df['status'].map(lambda x: status_order.get(x.split(':')[0], 99))
    report_df.sort_values('sort_key', inplace=True)
    report_df.drop(columns=['sort_key'], inplace=True)

    # 4. 保存与展示
    report_df.to_csv(REPORT_PATH, index=False)
    
    print("\n" + "="*40)
    print(f"检查完成！报告已保存至: {REPORT_PATH}")
    print("="*40)
    
    # 打印有问题的行 (Unexplained > 0)
    problems = report_df[report_df['unexplained_cnt'] > 0]
    if not problems.empty:
        print(f"\n发现 {len(problems)} 个资产存在未知缺口 (Unexplained Gaps):")
        # 调整打印格式
        pd.set_option('display.max_rows', 20)
        pd.set_option('display.max_colwidth', 60)
        print(problems[['ticker', 'asset_class', 'unexplained_cnt', 'details']])
    else:
        print("\n太棒了！所有缺口都是正常的假期。")

    # 提醒 Index 情况
    indices = report_df[report_df['asset_class'] == 'Index']
    if not indices.empty:
        print(f"\n[Info] 已检查 {len(indices)} 个 Index (指数)，规则与 ETF 相同。")

if __name__ == "__main__":
    main()