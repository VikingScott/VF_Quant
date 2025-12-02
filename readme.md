# **yfdata: 量化数据中台 (Data Middle Platform)**

这是一个轻量级、本地优先的金融数据管理系统。它负责从开源数据源（Yahoo Finance, FRED）获取数据，进行清洗、验证，并最终输出为量化回测引擎可直接使用的矩阵格式。

## **目录结构**

yfdata/  
├── master\_catalog.csv      \# \[核心\] 资产配置清单（所有资产的身份证）  
├── manager.py              \# \[入口\] 交互式管理控制台  
├── run\_daily.py            \# \[入口\] 每日自动更新脚本  
│  
├── add\_ticker.py           \# 辅助脚本：智能添加新资产  
├── update\_data.py          \# 核心脚本：下载与增量更新  
├── build\_dataset.py        \# 核心脚本：拼接生成最终矩阵  
├── check\_data\_quality.py   \# 质检：基础数据质量检查  
├── run\_gap\_report.py       \# 质检：交易日缺口深度诊断  
├── find\_duplicates.py      \# 分析：相关性分析与资产去重  
│  
└── data/  
    ├── daily\_csv/          \# \[Data Lake\] 存储原始清洗后的单资产 CSV  
    └── processed/          \# \[Output\] 策略层使用的最终数据  
        ├── universe\_prices.csv   \# 可交易资产矩阵 (Adj Close)  
        └── macro\_indicators.csv  \# 宏观/指数矩阵 (Close/Value)

## **快速开始**

### **1\. 初始化环境**

确保安装了必要的 Python 库：  
pip install pandas yfinance pandas\_datareader holidays scipy seaborn matplotlib

### **2\. 日常使用**

* **交互模式：** 运行 python manager.py，通过菜单管理一切。  
* **每日更新：** 运行 python run\_daily.py，一键更新所有数据。  
* **添加资产：** 运行 python add\_ticker.py 或在 Manager 中选择对应功能。

## **数据处理逻辑**

### **1\. 数据清洗 (Sanitization)**

* **去重 (De-duplication):** 针对 adj\_close 和 close 同时保留。  
* **无效数据剔除:** 对于 ETF/股票，强制剔除 Volume \== 0 的行（防止未上市或停牌期间的占位符干扰）。  
* **时区处理:** 统一移除时区信息 (TZ-naive)，避免 Pandas 索引对齐错误。

### **2\. 缺口处理 (Gap Handling)**

* **原则:** **诚实记录 (Honesty)**。  
* **逻辑:**  
  * 如果某天交易所休市（或数据源确实缺失），生成的 CSV 中**不会**有人为填充的行。  
  * **不使用线性插值**，因为这包含未来函数。  
  * **不强制前向填充 (ffill)**，将填充决策权交给策略层（Strategy Layer）。

### **3\. 资产分类与输出**

为了防止回测时的逻辑混淆，输出数据被拆分为两类：

| 文件名 | 内容 | 包含资产类型 | 取值字段 | 用途 |
| :---- | :---- | :---- | :---- | :---- |
| **universe\_prices.csv** | 可投资资产 | Equity, Fixed Income, Commodity, Crypto | adj\_close | 计算 PnL，生成交易信号 |
| **macro\_indicators.csv** | 观察指标 | Index (VIX), Macro (Rates) | close / value | 计算择时因子，判断 Regime |

## **质量保证 (QA)**

系统内置了三层防御机制：

1. **僵尸检测:** 更新时自动检查最新日期，若超过 15 天未更新则报警（可能已退市）。  
2. **缺口诊断 (run\_gap\_report.py):** 结合 NYSE 交易日历，自动区分“正常假期”和“异常缺失”。  
3. **重复分析 (find\_duplicates.py):** 自动识别相关性 \> 0.98 的资产，并根据流动性建议去重，同时设有“Smart Beta 白名单”防止误删风格因子 ETF。

## **注意事项**

1. **Yahoo Finance 限制:** 请勿高频并发运行下载（代码已内置延时），否则可能被封 IP。  
2. **历史数据:** 早期数据（如 2000 年以前的国际 ETF）可能存在流动性缺失，回测时建议配合 Volume 进行过滤。  
3. **Crypto ETF:** 像 IBIT 这样的比特币 ETF 遵循美股交易时间，不要将其 Asset Class 设为 Crypto（后者暗示 7x24 交易）。