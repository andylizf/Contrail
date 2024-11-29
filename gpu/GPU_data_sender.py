import socket
import json
import time
from datetime import datetime
from pynvml import *

from GPU_logger import *


# 发送 GPU 信息的函数
def send_gpu_info(server_ip, server_port):
    # 初始化 Socket
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client_socket.connect((server_ip, server_port))

    DB_PATH = "data/gpu_history_virgo.db"
    DB_REALTIME_PATH = "data/gpu_info_virgo.db"

    initialize_database(db_path=DB_PATH)
    initialize_database(db_path=DB_REALTIME_PATH)
    print("Database initialized.")
    timestamp_last = dt.datetime.now(tz=dt.timezone.utc)
    AGGR_PERIOD = 30  # 聚合周期：30 秒

    try:
        while True:
            # 获取 GPU 信息
            gpu_info = get_gpu_info()
            curr_time = dt.datetime.now(tz=dt.timezone.utc)
            timestamp = datetime.now().isoformat()

            # 组装消息
            data = {
                "magic": 23333,
                "timestamp": timestamp,
                "gpu_info": gpu_info,
            }
            message = json.dumps(data)

            update_database(gpu_info, curr_time.strftime("%Y-%m-%d %H:%M:%S"), db_path=DB_REALTIME_PATH)
            if (curr_time - timestamp_last).seconds >= AGGR_PERIOD - 1:
                timestamp_last = curr_time
                aggregate_data(timestamp_last, period_s=AGGR_PERIOD, db_path=DB_PATH, db_realtime_path=DB_REALTIME_PATH)
                remove_old_data(timestamp_last, period_s=3600, db_path=DB_REALTIME_PATH)

            # 发送数据
            client_socket.sendall(message.encode("utf-8"))

            # 间隔 1 秒发送一次
            time.sleep(1)
    except KeyboardInterrupt:
        print("发送端已停止")
    finally:
        client_socket.close()


# 主程序
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Send GPU information to the server.")
    parser.add_argument("--server_ip", type=str, required=True, help="The IP address of the server.")
    parser.add_argument("--server_port", type=int, required=True, help="The port of the server.")
    SERVER_IP = parser.parse_args().server_ip
    SERVER_PORT = parser.parse_args().server_port

    send_gpu_info(SERVER_IP, SERVER_PORT)
