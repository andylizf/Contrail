from loguru import logger
import sqlite3
import pandas as pd
import datetime as dt

from name_dict import dict_username


def query_latest_gpu_info(db_path="gpu_history.db"):
    """
    查询最新的 GPU 状态信息（包括 GPU 使用率、内存使用情况等）。

    Args:
        db_path (str): SQLite 数据库路径。

    Returns:
        pd.DataFrame: 最新的 GPU 状态信息。
    """
    logger.trace(f"Querying latest GPU info from {db_path}")
    conn = sqlite3.connect(db_path)

    # 查询最新的 GPU 信息
    query = """
        SELECT *
        FROM gpu_info
        WHERE timestamp = (
            SELECT MAX(timestamp)
            FROM gpu_info
        )
    """
    data = pd.read_sql_query(query, conn)
    conn.close()
    logger.trace("Query latest GPU info completed")

    # 将时间戳转换为 datetime 类型
    data["timestamp"] = (
        pd.to_datetime(data["timestamp"])
        .dt.tz_localize("UTC")
        .dt.tz_convert("Asia/Shanghai")
        .dt.strftime("%Y-%m-%d %H:%M:%S")
    )

    return data


def query_min_max_timestamp(db_path="gpu_history.db"):
    """
    查询最早和最晚的 GPU 数据记录时间。

    Args:
        db_path (str): SQLite 数据库路径。

    Returns:
        min_timestamp (datetime): 最早的 GPU 数据记录时间。
        max_timestamp (datetime): 最晚的 GPU 数据记录时间。
    """
    logger.trace(f"Querying min and max timestamp from {db_path}")
    conn = sqlite3.connect(db_path)

    # 查询最早和最晚的 GPU 数据记录时间
    query = """
        SELECT 
            MIN(timestamp) AS min_timestamp,
            MAX(timestamp) AS max_timestamp
        FROM gpu_history
    """
    data = pd.read_sql_query(query, conn)
    conn.close()
    logger.trace("Query min and max timestamp completed")

    if data.empty:
        return None, None

    min_timestamp = pd.to_datetime(data["min_timestamp"].iloc[0]).tz_localize("UTC").tz_convert("Asia/Shanghai")
    max_timestamp = pd.to_datetime(data["max_timestamp"].iloc[0]).tz_localize("UTC").tz_convert("Asia/Shanghai")

    return min_timestamp, max_timestamp


def query_gpu_realtime_usage(start_time, end_time, db_path="gpu_history.db"):
    """
    查询指定时间范围内的 GPU 使用情况。

    Args:
        start_time (str): 起始时间，格式为 "YYYY-MM-DD HH:MM:SS"。
        end_time (str): 终止时间，格式为 "YYYY-MM-DD HH:MM:SS"。
        db_path (str): SQLite 数据库路径。

    Returns:
        gpu_utilization_df: 每台 GPU 在每个时刻的使用率变化。
    """
    logger.trace(f"Querying GPU realtime usage from {start_time} to {end_time} in {db_path}")
    conn = sqlite3.connect(db_path)

    # 查询 GPU 信息
    query = """
        SELECT gpu_index, gpu_utilization, timestamp
        FROM gpu_info
        WHERE timestamp BETWEEN ? AND ?
        ORDER BY timestamp
    """
    data = pd.read_sql_query(query, conn, params=(start_time, end_time))
    conn.close()
    logger.trace("Query GPU realtime usage completed")

    # 将时间戳转换为 datetime 类型
    data["timestamp"] = pd.to_datetime(data["timestamp"]).dt.tz_localize("UTC").dt.tz_convert("Asia/Shanghai")

    return data


def query_gpu_memory_realtime_usage(start_time, end_time, db_path="gpu_history.db"):
    """
    查询指定时间范围内的 GPU 内存使用情况。

    Args:
        start_time (str): 起始时间，格式为 "YYYY-MM-DD HH:MM:SS"。
        end_time (str): 终止时间，格式为 "YYYY-MM-DD HH:MM:SS"。
        db_path (str): SQLite 数据库路径。

    Returns:
        pd.DataFrame: GPU 内存使用情况。
    """
    logger.trace(f"Querying GPU memory realtime usage from {start_time} to {end_time} in {db_path}")
    conn = sqlite3.connect(db_path)

    # 查询 GPU 信息
    query = """
        SELECT gpu_index, used_memory, timestamp
        FROM gpu_info
        WHERE timestamp BETWEEN ? AND ?
        ORDER BY timestamp
    """
    data = pd.read_sql_query(query, conn, params=(start_time, end_time))
    conn.close()
    logger.trace("Query GPU memory realtime usage completed")

    # 将时间戳转换为 datetime 类型
    data["timestamp"] = pd.to_datetime(data["timestamp"]).dt.tz_localize("UTC").dt.tz_convert("Asia/Shanghai")

    return data


def query_user_gpu_realtime_usage(start_time, end_time, db_path="gpu_history.db"):
    """
    查询指定时间范围内的用户 GPU 使用情况。

    Args:
        start_time (str): 起始时间，格式为 "YYYY-MM-DD HH:MM:SS"。
        end_time (str): 终止时间，格式为 "YYYY-MM-DD HH:MM:SS"。
        db_path (str): SQLite 数据库路径。

    Returns:
        pd.DataFrame: 用户 GPU 使用情况。
    """
    logger.trace(f"Querying user GPU realtime usage from {start_time} to {end_time} in {db_path}")
    conn = sqlite3.connect(db_path)

    # 查询用户 GPU 使用情况
    query = """
        SELECT gpu_index, user, gpu_utilization, timestamp
        FROM gpu_user_info
        WHERE timestamp BETWEEN ? AND ?
        ORDER BY timestamp
    """
    data = pd.read_sql_query(query, conn, params=(start_time, end_time))
    conn.close()
    logger.trace("Query user GPU realtime usage completed")

    # 将时间戳转换为 datetime 类型
    data["timestamp"] = pd.to_datetime(data["timestamp"]).dt.tz_localize("UTC").dt.tz_convert("Asia/Shanghai")

    return data


def query_user_gpu_memory_realtime_usage(start_time, end_time, db_path="gpu_history.db"):
    """
    查询指定时间范围内的用户 GPU 内存使用情况。

    Args:
        start_time (str): 起始时间，格式为 "YYYY-MM-DD HH:MM:SS"。
        end_time (str): 终止时间，格式为 "YYYY-MM-DD HH:MM:SS"。
        db_path (str): SQLite 数据库路径。

    Returns:
        pd.DataFrame: 用户 GPU 内存使用情况。
    """
    logger.trace(f"Querying user GPU memory realtime usage from {start_time} to {end_time} in {db_path}")
    conn = sqlite3.connect(db_path)

    # 查询用户 GPU 使用情况
    query = """
        SELECT gpu_index, user, used_memory, timestamp
        FROM gpu_user_info
        WHERE timestamp BETWEEN ? AND ?
        ORDER BY timestamp
    """
    data = pd.read_sql_query(query, conn, params=(start_time, end_time))
    conn.close()
    logger.trace("Query user GPU memory realtime usage completed")

    # 将时间戳转换为 datetime 类型
    data["timestamp"] = pd.to_datetime(data["timestamp"]).dt.tz_localize("UTC").dt.tz_convert("Asia/Shanghai")

    return data
