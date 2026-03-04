"""
增量更新模块 - 数据增量更新
"""
from datetime import datetime, timedelta
from typing import Optional

from . import config
from . import database
from . import stock_list
from . import fetcher


def update_stock_list(force: bool = False):
    """
    更新股票列表

    Args:
        force: 是否强制更新
    """
    print("=" * 50)
    print("更新股票列表")
    print("=" * 50)

    stocks = stock_list.get_stock_list(force_update=force)

    print(f"股票列表更新完成，共 {len(stocks)} 只股票")
    print()


def update_daily_data(symbol: str, start_date: Optional[str] = None, end_date: Optional[str] = None):
    """
    更新单只股票日线数据

    Args:
        symbol: 股票代码
        start_date: 开始日期(YYYYMMDD)，默认从最新日期开始
        end_date: 结束日期(YYYYMMDD)，默认今天
    """
    if start_date is None:
        # 获取数据库中最新日期
        latest = database.get_latest_date(symbol)
        if latest:
            start_date = (datetime.strptime(latest, "%Y-%m-%d") + timedelta(days=1)).strftime(config.DATE_FORMAT)
        else:
            # 如果没有数据，获取过去一年的数据
            start_date = (datetime.now() - timedelta(days=365)).strftime(config.DATE_FORMAT)

    if end_date is None:
        end_date = datetime.now().strftime(config.DATE_FORMAT)

    print(f"更新股票 {symbol} 数据: {start_date} - {end_date}")

    count = fetcher.fetch_and_save_daily_data(symbol, start_date, end_date)

    # 更新最后更新日期
    if count > 0:
        database.set_last_update_date(end_date)

    print(f"更新完成: {count} 条数据")


def update_all_daily_data(start_date: Optional[str] = None, end_date: Optional[str] = None):
    """
    全量更新日线数据

    Args:
        start_date: 开始日期，默认一年前
        end_date: 结束日期，默认今天
    """
    print("=" * 50)
    print("全量更新日线数据")
    print("=" * 50)

    if start_date is None:
        start_date = (datetime.now() - timedelta(days=365)).strftime(config.DATE_FORMAT)

    if end_date is None:
        end_date = datetime.now().strftime(config.DATE_FORMAT)

    print(f"数据范围: {start_date} - {end_date}")
    print()

    # 获取所有股票
    stocks = database.get_all_stocks()

    if not stocks:
        print("股票列表为空，请先更新股票列表")
        return

    print(f"共 {len(stocks)} 只股票")
    print()

    results = fetcher.fetch_all_daily_data(start_date, end_date)

    # 更新最后更新日期
    database.set_last_update_date(end_date)

    print()
    print("全量更新完成!")


def incremental_update():
    """
    增量更新 - 只更新最新数据

    策略:
    1. 获取最后更新日期
    2. 从最后更新日期的下一天开始，到今天为止
    3. 获取增量数据并合并
    4. 更新最后更新日期
    """
    print("=" * 50)
    print("增量更新日线数据")
    print("=" * 50)

    # 获取最后更新日期
    last_update_date = database.get_last_update_date()

    if last_update_date:
        # 从最后更新日期的下一天开始
        last_dt = datetime.strptime(last_update_date, "%Y-%m-%d")
        start_date = (last_dt + timedelta(days=1)).strftime(config.DATE_FORMAT)
    else:
        # 如果没有更新记录，获取过去一年的数据
        start_date = (datetime.now() - timedelta(days=365)).strftime(config.DATE_FORMAT)

    end_date = datetime.now().strftime(config.DATE_FORMAT)

    print(f"数据范围: {start_date} - {end_date}")
    print()

    # 获取所有股票
    stocks = database.get_all_stocks()

    if not stocks:
        print("股票列表为空，请先更新股票列表")
        return

    print(f"共 {len(stocks)} 只股票")
    print()

    # 逐只股票更新
    success_count = 0
    total_count = 0

    for i, stock in enumerate(stocks):
        code = stock["code"]

        # 显示进度
        if (i + 1) % 50 == 0:
            print(f"进度: {i + 1}/{len(stocks)}")

        try:
            count = fetcher.fetch_single_stock_latest(code)
            if count > 0:
                success_count += 1
                total_count += count

            # 请求间隔
            import time
            time.sleep(config.REQUEST_INTERVAL)

        except Exception as e:
            print(f"  股票 {code} 更新失败: {e}")

    # 更新最后更新日期
    database.set_last_update_date(end_date)

    print()
    print(f"增量更新完成! 成功: {success_count}/{len(stocks)}, 共 {total_count} 条数据")


def get_update_status() -> dict:
    """获取更新状态"""
    last_date = database.get_last_update_date()
    stock_count = database.get_stock_count()
    stock_list_update = database.get_stock_list_last_update()

    return {
        "last_update_date": last_date,
        "stock_count": stock_count,
        "stock_list_last_update": stock_list_update,
        "need_stock_list_update": stock_list.needs_update()
    }
