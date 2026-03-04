"""
配置模块 - A股数据获取配置
"""
from pathlib import Path

# 项目根目录
PROJECT_ROOT = Path(__file__).parent.parent

# 数据存储目录
DATA_DIR = PROJECT_ROOT / "data"
DATA_DIR.mkdir(exist_ok=True)

# 数据库路径
DATABASE_PATH = DATA_DIR / "stocks.db"

# 日线数据目录
DAILY_DATA_DIR = DATA_DIR / "stock_daily"
DAILY_DATA_DIR.mkdir(exist_ok=True)

# 复权类型: qfq(前复权), hfq(后复权), 空字符串(不复权)
ADJUST = "qfq"

# 数据更新配置
UPDATE_CONFIG = {
    # 股票列表更新间隔(天)
    "stock_list_update_days": 7,
    # 每日更新开始时间(小时)
    "daily_update_start_hour": 16,
}

# Akshare请求间隔(秒), 避免请求过快被限流
REQUEST_INTERVAL = 0.5

# 日期格式
DATE_FORMAT = "%Y%m%d"
DATE_DISPLAY_FORMAT = "%Y-%m-%d"
