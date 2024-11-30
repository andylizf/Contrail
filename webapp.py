import os
import argparse
import streamlit as st


parser = argparse.ArgumentParser()
parser.add_argument("--disable_ai4s", action="store_true", help="Disable AI4S monitoring.")
parser.add_argument("--disable_info", action="store_true", help="Disable user information query.")
parser.add_argument("--enable_name_dict", action="store_true", help="Enable name dictionary mapping.")

parser.add_argument("--add_device", type=str, nargs="+", help="List of devices to monitor.", default=["Leo", "Virgo"])
args = parser.parse_args()

pages = {}

for name in args.add_device:
    pages[name] = [
        st.Page(f"webapp/realtime_{name.lower()}.py", title=f"{name}: 实时状态"),
        st.Page(f"webapp/history_{name.lower()}.py", title=f"{name}: 历史信息"),
    ]

if not args.disable_ai4s:
    pages["AI4S"] = [
        st.Page("webapp/ai4s_tasks.py", title="AI4S: 任务列表"),
        st.Page("webapp/fee.py", title="AI4S: 费用记录"),
    ]

if not args.disable_info and args.enable_name_dict:
    pages["Info"] = [
        st.Page("webapp/user_info.py", title="用户信息查询"),
    ]

os.environ["ENABLE_NAME_DICT"] = "1" if args.enable_name_dict else "0"

pg = st.navigation(pages)
pg.run()
