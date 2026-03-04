"""
数据获取模块 - 从Akshare获取A股数据
"""
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


def fetch_daily_data(
    symbol: str,
    start_date: str,
    end_date: str,
    adjust: str = None
) -> List[Dict]:
    """
    获取单只股票日线数据

    Args:
        symbol: 股票代码(如: 000001)
        start_date: 开始日期(YYYYMMDD)
        end_date: 结束日期(YYYYMMDD)
        adjust: 复权类型(qfq前复权, hfq后复权, 空字符串不复权)

    Returns:
        日线数据列表
    """
    if not AKSHARE_AVAILABLE:
        raise ImportError("akshare未安装，请运行: pip install akshare")

    if adjust is None:
        adjust = config.ADJUST

    try:
        df = ak.stock_zh_a_hist(
            symbol=symbol,
            period="daily",
            start_date=start_date,
            end_date=end_date,
            adjust=adjust
        )

        if df is None or df.empty:
            return []

        # 转换为字典列表
        data = df.to_dict(orient="records")

        return data

    except Exception as e:
        print(f"获取股票 {symbol} 数据失败: {e}")
        return []


def fetch_and_save_daily_data(
    symbol: str,
    start_date: str,
    end_date: str,
    adjust: str = None
) -> int:
    """
    获取并保存单只股票日线数据

    Args:
        symbol: 股票代码
        start_date: 开始日期
        end_date: 结束日期
        adjust: 复权类型

    Returns:
        获取的数据条数
    """
    data = fetch_daily_data(symbol, start_date, end_date, adjust)

    if data:
        database.insert_daily_data(symbol, data)
        print(f"  股票 {symbol}: 获取 {len(data)} 条数据")

    return len(data)


def fetch_all_daily_data(
    start_date: str,
    end_date: str,
    adjust: str = None,
    interval: float = None
) -> Dict[str, int]:
    """
    批量获取所有股票日线数据

    Args:
        start_date: 开始日期
        end_date: 结束日期
        adjust: 复权类型
        interval: 请求间隔(秒)

    Returns:
        各股票获取的数据条数
    """
    if interval is None:
        interval = config.REQUEST_INTERVAL

    # 获取股票列表
    stocks = database.get_all_stocks()

    if not stocks:
        print("股票列表为空，请先更新股票列表")
        return {}

    print(f"开始批量获取 {len(stocks)} 只股票的数据...")

    results = {}

    for i, stock in enumerate(stocks):
        code = stock["code"]

        # 显示进度
        if (i + 1) % 100 == 0:
            print(f"进度: {i + 1}/{len(stocks)}")

        try:
            count = fetch_and_save_daily_data(code, start_date, end_date, adjust)
            results[code] = count

            # 请求间隔，避免被限流
            time.sleep(interval)

        except Exception as e:
            print(f"  股票 {code} 获取失败: {e}")
            results[code] = 0

    success_count = sum(1 for v in results.values() if v > 0)
    print(f"完成! 成功: {success_count}/{len(stocks)}")

    return results


def fetch_single_stock_latest(symbol: str) -> int:
    """
    获取单只股票最新数据

    Args:
        symbol: 股票代码

    Returns:
        获取的数据条数
    """
    # 获取最新日期
    latest_date = database.get_latest_date(symbol)

    from datetime import datetime, timedelta

    if latest_date:
        # 从最新日期的下一天开始
        latest_dt = datetime.strptime(latest_date, "%Y-%m-%d")
        start_date = (latest_dt + timedelta(days=1)).strftime(config.DATE_FORMAT)
    else:
        # 如果没有数据，获取过去一年的数据
        start_date = (datetime.now() - timedelta(days=365)).strftime(config.DATE_FORMAT)

    end_date = datetime.now().strftime(config.DATE_FORMAT)

    return fetch_and_save_daily_data(symbol, start_date, end_date)
