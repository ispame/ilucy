"""
股票列表模块 - A股股票列表管理
排除北交所(BJ)
"""
from datetime import datetime, timedelta
from typing import List, Dict, Optional
import time

from . import config
from . import database

# 尝试导入akshare
try:
    import akshare as ak
    AKSHARE_AVAILABLE = True
except ImportError:
    AKSHARE_AVAILABLE = False


def get_shanghai_stocks() -> List[Dict[str, str]]:
    """获取上海A股股票列表"""
    if not AKSHARE_AVAILABLE:
        raise ImportError("akshare未安装，请运行: pip install akshare")

    stocks = []

    # 主板A股
    try:
        df = ak.stock_info_sh_name_code(symbol="主板A股")
        for _, row in df.iterrows():
            stocks.append({
                "code": row["证券代码"],
                "name": row["证券简称"],
                "market": "SH_MAIN",
                "list_date": row.get("上市日期")
            })
    except Exception as e:
        print(f"获取上海主板A股失败: {e}")

    # 科创板
    try:
        df = ak.stock_info_sh_name_code(symbol="科创板")
        for _, row in df.iterrows():
            stocks.append({
                "code": row["证券代码"],
                "name": row["证券简称"],
                "market": "SH_KCB",
                "list_date": row.get("上市日期")
            })
    except Exception as e:
        print(f"获取科创板失败: {e}")

    return stocks


def get_shenzhen_stocks() -> List[Dict[str, str]]:
    """获取深圳A股股票列表"""
    if not AKSHARE_AVAILABLE:
        raise ImportError("akshare未安装，请运行: pip install akshare")

    stocks = []

    try:
        # A股列表(包含主板、中小板、创业板)
        df = ak.stock_info_sz_name_code(symbol="A股列表")
        for _, row in df.iterrows():
            code = row["A股代码"]
            # 排除北交所股票(以830或831开头)
            if str(code).startswith("830") or str(code).startswith("831"):
                continue

            # 判断市场
            market = "SZ_MAIN"  # 默认主板
            if str(code).startswith("000"):
                market = "SZ_MAIN"
            elif str(code).startswith("002"):
                market = "SZ_SME"  # 中小板
            elif str(code).startswith("300"):
                market = "SZ_CHN"  # 创业板

            stocks.append({
                "code": code,
                "name": row["A股简称"],
                "market": market,
                "list_date": row.get("A股上市日期")
            })
    except Exception as e:
        print(f"获取深圳A股列表失败: {e}")

    return stocks


def get_stock_list(force_update: bool = False) -> List[Dict[str, str]]:
    """
    获取A股股票列表(排除北交所)

    Args:
        force_update: 是否强制更新

    Returns:
        股票列表
    """
    # 检查是否需要更新
    if not force_update and not needs_update():
        stocks = database.get_all_stocks()
        if stocks:
            print(f"使用本地股票列表，共 {len(stocks)} 只股票")
            return stocks

    # 从网络获取
    print("正在获取A股股票列表...")

    all_stocks = []

    # 上海A股
    print("获取上海A股...")
    sh_stocks = get_shanghai_stocks()
    all_stocks.extend(sh_stocks)
    print(f"  上海A股: {len(sh_stocks)} 只")

    # 深圳A股
    print("获取深圳A股...")
    sz_stocks = get_shenzhen_stocks()
    all_stocks.extend(sz_stocks)
    print(f"  深圳A股: {len(sz_stocks)} 只")

    # 保存到数据库
    print(f"共获取 {len(all_stocks)} 只股票，保存到数据库...")
    database.insert_stocks(all_stocks)

    # 更新最后更新时间
    today = datetime.now().strftime(config.DATE_FORMAT)
    database.set_stock_list_last_update(today)

    return all_stocks


def save_stock_list(stocks: List[Dict[str, str]]):
    """保存股票列表到本地"""
    database.insert_stocks(stocks)
    today = datetime.now().strftime(config.DATE_FORMAT)
    database.set_stock_list_last_update(today)


def needs_update() -> bool:
    """检查股票列表是否需要更新"""
    last_update = database.get_stock_list_last_update()

    if not last_update:
        return True

    # 解析最后更新时间
    try:
        last_date = datetime.strptime(last_update, config.DATE_FORMAT)
    except ValueError:
        return True

    # 检查是否超过更新间隔
    days_since_update = (datetime.now() - last_date).days

    return days_since_update >= config.UPDATE_CONFIG["stock_list_update_days"]


def get_stock_count() -> int:
    """获取股票数量"""
    return database.get_stock_count()
