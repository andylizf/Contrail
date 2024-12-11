import datetime as dt
import json
import os

import plotly.express as px
import streamlit as st
from streamlit_autorefresh import st_autorefresh

if os.getenv("ENABLE_NAME_DICT", "0") == "1":
    from name_dict import NAME_DICT_FEE


def read_json_result(file: str = "data/ai4s_data.json") -> dict:
    update_timestamp = os.path.getmtime(file)
    update_time = dt.datetime.fromtimestamp(update_timestamp)
    timedelta = dt.datetime.now() - update_time
    st.write(
        f"更新于 {update_time.strftime("%Y-%m-%d %H:%M:%S")} / {timedelta.total_seconds()//60:.0f} 分钟前"
    )
    if timedelta.total_seconds() > 1200:
        st.warning("过去 20 分钟内没有任务数据更新：AI4S 爬虫程序可能离线。")
    return cached_read_data(update_timestamp, file)


@st.cache_data
def cached_read_data(update_time: float, file: str = "data/ai4s_data.json") -> dict:
    with open(file, "r") as file:
        data = json.loads(file.read())
    return data


def get_data(data: dict, key: str) -> tuple[list[dt.datetime], list[float]]:
    return [dt.datetime.fromtimestamp(x / 1000) for x in data[key]["values"][0]], data[
        key
    ]["values"][1]


st.title("AI4S: 任务列表")

col1, col2, col3 = st.columns([4, 11, 1], vertical_alignment="center")

col1.checkbox("自动刷新", key="autorefresh", value=True)

with col3:
    if st.session_state["autorefresh"]:
        st_autorefresh(interval=60000, key="ai4s_task_monitor")

with col2:
    data = read_json_result()

if not data:
    st.info("没有正在运行的任务。")

for i, task in data.items():
    # st.markdown("---")

    if not task:
        st.warning(f"任务 {i}：读取任务信息失败。")
        continue

    basics, times, resources = st.columns([3, 3, 2], vertical_alignment="bottom")

    with basics:
        if os.getenv("ENABLE_NAME_DICT", "0") == "1":
            user = NAME_DICT_FEE.get(task["user"], task["user"])
        else:
            user = task["user"]
        st.markdown(f"#### {task['task_name']}  \n创建者: **:blue[{user}]**")

    with times:
        st.markdown(f"开始: {task['start_time']}  \n活跃: **{task['active_time']}**")

    with resources:
        st.write(f"{task['cpus']} C / {task['memory']}  \n**GPU: {task['gpu_count']}**")

    if "data" not in task:
        st.warning(f"任务 {task['task_name']}：读取任务数据失败。")
        continue

    gpu, gmem = st.columns(2)

    with gpu:
        timestamps, values = get_data(task["data"], "accelerator_duty_cycle")
        fig = px.line(x=timestamps, y=values)
        fig.update_yaxes(rangemode="tozero")
        fig.update_traces(hovertemplate=None)
        fig.update_layout(
            hovermode="x",
            yaxis_title="GPU 使用率 %",
            xaxis_title=None,
            margin=dict(l=0, r=0, t=0, b=0),
            height=120,
        )
        st.plotly_chart(fig, use_container_width=True, key=f"{task['task_name']}_gpu")
    with gmem:
        timestamps, values = get_data(task["data"], "accelerator_memory_used_bytes")
        values_gb = [x / 1024**3 for x in values]
        fig = px.line(x=timestamps, y=values_gb)
        fig.update_yaxes(rangemode="tozero")
        fig.update_traces(hovertemplate=None)
        fig.update_layout(
            hovermode="x",
            yaxis_title="显存用量 GB",
            xaxis_title=None,
            margin=dict(l=0, r=0, t=0, b=0),
            height=120,
        )
        st.plotly_chart(fig, use_container_width=True, key=f"{task['task_name']}_gmem")
