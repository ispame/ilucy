"""
数据库模块 - SQLite数据存储
"""
import sqlite3
from datetime import datetime
from typing import List, Dict, Optional, Tuple

from . import config


def get_connection() -> sqlite3.Connection:
    """获取数据库连接"""
    conn = sqlite3.connect(str(config.DATABASE_PATH))
    conn.row_factory = sqlite3.Row
    return conn


def init_database():
    """初始化数据库表结构"""
    conn = get_connection()
    cursor = conn.cursor()

    # 股票列表表
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS stocks (
            code TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            list_date TEXT,
            market TEXT NOT NULL,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # 日线数据表
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS daily_data (
            date TEXT NOT NULL,
            code TEXT NOT NULL,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            volume REAL,
            amount REAL,
            amplitude REAL,
            change_pct REAL,
            turnover_rate REAL,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (date, code)
        )
    """)

    # 创建索引以加速查询
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_daily_data_code
        ON daily_data(code)
    """)

    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_daily_data_date
        ON daily_data(date)
    """)

    # 元数据表
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS metadata (
            key TEXT PRIMARY KEY,
            value TEXT,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)

    conn.commit()
    conn.close()


def get_metadata(key: str) -> Optional[str]:
    """获取元数据"""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT value FROM metadata WHERE key = ?", (key,))
    row = cursor.fetchone()
    conn.close()
    return row["value"] if row else None


def set_metadata(key: str, value: str):
    """设置元数据"""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        INSERT OR REPLACE INTO metadata (key, value, updated_at)
        VALUES (?, ?, ?)
    """, (key, value, datetime.now().isoformat()))
    conn.commit()
    conn.close()


def get_last_update_date() -> Optional[str]:
    """获取最后更新日期"""
    return get_metadata("last_update_date")


def set_last_update_date(date: str):
    """设置最后更新日期"""
    set_metadata("last_update_date", date)


# ========== 股票列表操作 ==========

def insert_stock(code: str, name: str, market: str, list_date: Optional[str] = None):
    """插入股票"""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        INSERT OR REPLACE INTO stocks (code, name, market, list_date, updated_at)
        VALUES (?, ?, ?, ?, ?)
    """, (code, name, market, list_date, datetime.now().isoformat()))
    conn.commit()
    conn.close()


def insert_stocks(stocks: List[Dict[str, str]]):
    """批量插入股票"""
    conn = get_connection()
    cursor = conn.cursor()
    now = datetime.now().isoformat()
    data = [
        (s["code"], s["name"], s["market"], s.get("list_date"), now)
        for s in stocks
    ]
    cursor.executemany("""
        INSERT OR REPLACE INTO stocks (code, name, market, list_date, updated_at)
        VALUES (?, ?, ?, ?, ?)
    """, data)
    conn.commit()
    conn.close()


def get_all_stocks() -> List[Dict[str, str]]:
    """获取所有股票"""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT code, name, market, list_date FROM stocks")
    rows = cursor.fetchall()
    conn.close()
    return [dict(row) for row in rows]


def get_stock_count() -> int:
    """获取股票数量"""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) as count FROM stocks")
    row = cursor.fetchone()
    conn.close()
    return row["count"] if row else 0


def get_stock_list_last_update() -> Optional[str]:
    """获取股票列表最后更新时间"""
    return get_metadata("stock_list_last_update")


def set_stock_list_last_update(date: str):
    """设置股票列表最后更新时间"""
    set_metadata("stock_list_last_update", date)


# ========== 日线数据操作 ==========

def insert_daily_data(code: str, data: List[Dict]):
    """插入日线数据"""
    if not data:
        return

    conn = get_connection()
    cursor = conn.cursor()
    now = datetime.now().isoformat()

    # 将DataFrame数据转换为元组
    records = []
    for row in data:
        records.append((
            row.get("日期"),
            code,
            row.get("开盘"),
            row.get("最高"),
            row.get("最低"),
            row.get("收盘"),
            row.get("成交量"),
            row.get("成交额"),
            row.get("振幅"),
            row.get("涨跌幅"),
            row.get("换手率"),
            now
        ))

    cursor.executemany("""
        INSERT OR REPLACE INTO daily_data
        (date, code, open, high, low, close, volume, amount, amplitude, change_pct, turnover_rate, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, records)
    conn.commit()
    conn.close()


def get_daily_data(code: str, start_date: Optional[str] = None, end_date: Optional[str] = None) -> List[Dict]:
    """获取日线数据"""
    conn = get_connection()
    cursor = conn.cursor()

    query = "SELECT * FROM daily_data WHERE code = ?"
    params = [code]

    if start_date:
        query += " AND date >= ?"
        params.append(start_date)
    if end_date:
        query += " AND date <= ?"
        params.append(end_date)

    query += " ORDER BY date"

    cursor.execute(query, params)
    rows = cursor.fetchall()
    conn.close()
    return [dict(row) for row in rows]


def get_latest_date(code: str) -> Optional[str]:
    """获取某股票最新数据日期"""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT MAX(date) as latest_date FROM daily_data WHERE code = ?
    """, (code,))
    row = cursor.fetchone()
    conn.close()
    return row["latest_date"] if row and row["latest_date"] else None


def delete_daily_data(code: str, start_date: Optional[str] = None, end_date: Optional[str] = None):
    """删除日线数据"""
    conn = get_connection()
    cursor = conn.cursor()

    query = "DELETE FROM daily_data WHERE code = ?"
    params = [code]

    if start_date:
        query += " AND date >= ?"
        params.append(start_date)
    if end_date:
        query += " AND date <= ?"
        params.append(end_date)

    cursor.execute(query, params)
    conn.commit()
    conn.close()
