"""
命令行接口 - A股数据获取工具
"""
import argparse
import sys
from datetime import datetime

from . import config
from . import database
from . import stock_list
from . import fetcher
from . import updater


def cmd_update_list(args):
    """更新股票列表"""
    updater.update_stock_list(force=args.force)


def cmd_update(args):
    """增量更新日线数据"""
    if args.all:
        # 全量更新
        updater.update_all_daily_data()
    else:
        # 增量更新
        updater.incremental_update()


def cmd_fetch(args):
    """获取指定股票数据"""
    symbol = args.symbol

    # 验证股票代码格式
    if len(symbol) != 6 or not symbol.isdigit():
        print(f"错误: 股票代码格式不正确: {symbol}")
        return

    start_date = args.start
    end_date = args.end

    # 默认日期范围
    if start_date is None:
        start_date = (datetime.now() - datetime.timedelta(days=365)).strftime(config.DATE_FORMAT)
    if end_date is None:
        end_date = datetime.now().strftime(config.DATE_FORMAT)

    print(f"获取股票 {symbol} 数据: {start_date} - {end_date}")

    count = fetcher.fetch_and_save_daily_data(symbol, start_date, end_date)
    print(f"获取完成: {count} 条数据")


def cmd_status(args):
    """查看更新状态"""
    status = updater.get_update_status()

    print("=" * 50)
    print("更新状态")
    print("=" * 50)
    print(f"股票数量: {status['stock_count']}")
    print(f"股票列表最后更新: {status['stock_list_last_update'] or '未更新'}")
    print(f"日线数据最后更新: {status['last_update_date'] or '未更新'}")
    print(f"需要更新股票列表: {'是' if status['need_stock_list_update'] else '否'}")
    print()


def cmd_init(args):
    """初始化数据库"""
    print("初始化数据库...")
    database.init_database()
    print("数据库初始化完成!")
    print(f"数据库路径: {config.DATABASE_PATH}")


def main():
    parser = argparse.ArgumentParser(
        description="A股股票数据获取工具",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  python -m src.cli init                    初始化数据库
  python -m src.cli update-list             更新股票列表
  python -m src.cli update-list --force     强制更新股票列表
  python -m src.cli update                   增量更新日线数据
  python -m src.cli update --all             全量更新日线数据
  python -m src.cli fetch 000001             获取指定股票数据
  python -m src.cli fetch 000001 --start 20230101 --end 20231231
  python -m src.cli status                    查看更新状态
        """
    )

    subparsers = parser.add_subparsers(dest="command", help="子命令")

    # init - 初始化数据库
    subparsers.add_parser("init", help="初始化数据库")

    # update-list - 更新股票列表
    list_parser = subparsers.add_parser("update-list", help="更新股票列表")
    list_parser.add_argument("--force", "-f", action="store_true", help="强制更新")

    # update - 更新日线数据
    update_parser = subparsers.add_parser("update", help="更新日线数据")
    update_parser.add_argument("--all", "-a", action="store_true", help="全量更新(默认增量)")

    # fetch - 获取股票数据
    fetch_parser = subparsers.add_parser("fetch", help="获取指定股票数据")
    fetch_parser.add_argument("symbol", help="股票代码(如: 000001)")
    fetch_parser.add_argument("--start", "-s", help="开始日期(YYYYMMDD)")
    fetch_parser.add_argument("--end", "-e", help="结束日期(YYYYMMDD)")

    # status - 查看状态
    subparsers.add_parser("status", help="查看更新状态")

    args = parser.parse_args()

    if args.command is None:
        parser.print_help()
        return

    # 执行对应命令
    if args.command == "init":
        cmd_init(args)
    elif args.command == "update-list":
        cmd_update_list(args)
    elif args.command == "update":
        cmd_update(args)
    elif args.command == "fetch":
        cmd_fetch(args)
    elif args.command == "status":
        cmd_status(args)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
