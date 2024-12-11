import datetime as dt
import os

import altair as alt
import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from GPU_query_db import (
    dict_username,
    query_gpu_history_average_usage,
    query_gpu_history_usage,
    query_gpu_user_history_total_usage,
    query_gpu_user_history_usage,
    query_min_max_timestamp,
)


def gpu_chart_band(df: pd.DataFrame, y_label: str, N_GPU: int = 8):
    df["gpu_index"] = df["gpu_index"].astype(str)
    colors = px.colors.qualitative.Plotly
    tot_colors = len(colors)

    fig = go.Figure()
    for i in range(N_GPU - 1, -1, -1):
        color = ",".join(
            [str(int(colors[i % tot_colors][j : j + 2], 16)) for j in (1, 3, 5)]
        )
        gpu_i = df[df["gpu_index"] == str(i)]
        fig.add_trace(
            go.Scatter(
                name=f"GPU {i}",
                x=gpu_i["timestamp"],
                y=gpu_i[y_label],
                mode="lines",
                line=dict(color=f"rgb({color})"),
                # line_shape="spline",
                legendgroup=f"GPU{i}",
            )
        )
        # add fill between gpu_utilization_min and gpu_utilization_max
        fig.add_trace(
            go.Scatter(
                x=gpu_i["timestamp"],
                y=gpu_i[f"{y_label}_max"],
                mode="lines",
                line=dict(width=1, color=f"rgba({color},0.1)"),
                marker=None,
                showlegend=False,
                hoverinfo="skip",
                legendgroup=f"GPU{i}",
            )
        )
        fig.add_trace(
            go.Scatter(
                x=gpu_i["timestamp"],
                y=gpu_i[f"{y_label}_min"],
                mode="lines",
                line=dict(width=1, color=f"rgba({color},0.1)"),
                marker=None,
                fill="tonexty",
                fillcolor=f"rgba({color},0.05)",
                showlegend=False,
                hoverinfo="skip",
                legendgroup=f"GPU{i}",
            )
        )
    fig.update_layout(
        hovermode="x",
        legend_traceorder="reversed",
        margin=dict(l=0, r=0, t=5, b=5),
        legend=dict(orientation="h", yanchor="top", xanchor="right", y=-0.1, x=1),
    )
    st.plotly_chart(fig, use_container_width=True, key=f"{y_label}_band")


def gpu_chart_user(
    user_usage_grouped: tuple[dict, pd.DatetimeIndex],
    y_label: str,
    name_dict: dict | None = None,
    N_GPU: int = 8,
):
    # stack area chart for y_label
    # df["gpu_index"] = df["gpu_index"].astype(str)
    colors = px.colors.qualitative.Plotly
    tot_colors = len(colors)
    opacities = [0.3 + 0.7 * i / (N_GPU - 1) for i in range(N_GPU)]

    data, base = user_usage_grouped

    fig = go.Figure()
    # add line of zeros as base
    fig.add_trace(
        go.Scatter(
            name="Base",
            x=base,
            y=np.zeros(len(base)),
            mode="lines",
            line=dict(width=0),
            stackgroup="one",
            showlegend=False,
        )
    )

    for i, (user, gpu_data) in enumerate(data.items()):
        color = ",".join(
            [str(int(colors[i % tot_colors][j : j + 2], 16)) for j in (1, 3, 5)]
        )
        username = name_dict.get(user, user) if name_dict is not None else user
        fig.add_trace(
            go.Scatter(
                name=username,
                x=list(gpu_data.values())[0]["timestamp"],
                y=[0],
                mode="lines",
                legendgroup=username,
                line=dict(width=0, color=f"rgb({color})"),
                stackgroup="one",
                showlegend=True,
            )
        )
        for gpu_index, gpu_i in gpu_data.items():
            fig.add_trace(
                go.Scatter(
                    name=f"{username} GPU {gpu_index}",
                    x=gpu_i["timestamp"],
                    y=gpu_i[y_label],
                    mode="lines",
                    line=dict(
                        width=1, color=f"rgba({color},{opacities[int(gpu_index)]})"
                    ),
                    fill="tonexty",
                    fillcolor=f"rgba({color},{opacities[int(gpu_index)] - 0.2})",
                    stackgroup="one",
                    legendgroup=username,
                    showlegend=False,
                )
            )
    fig.update_layout(
        # hovermode="x",
        margin=dict(l=0, r=0, t=5, b=5),
        legend=dict(orientation="h", yanchor="top", xanchor="right", y=-0.1, x=1),
    )
    st.plotly_chart(fig, use_container_width=True, key=f"{y_label}_user")


def gpu_chart_stack(df: pd.DataFrame, y_label: str, y_max: float):
    # stack area chart for y_label
    fig = px.area(
        df,
        x="timestamp",
        y=y_label,
        color="gpu_index",
        line_group="gpu_index",
        labels={y_label: y_label, "timestamp": "Timestamp", "gpu_index": "GPU Index"},
        color_discrete_sequence=px.colors.qualitative.Plotly,
    )
    fig.update_traces(
        line=dict(width=1),
        hovertemplate="%{y:.2f}",
        customdata=df[["gpu_index"]],
    )
    df_sum = df.groupby("timestamp")[y_label].sum().reset_index()
    fig.add_trace(
        go.Scatter(
            x=df_sum["timestamp"],
            y=df_sum[y_label],
            mode="lines",
            line=dict(color="#0083E8"),
            name="Sum",
            hovertemplate="%{y:.2f}",
            stackgaps="interpolate",
            showlegend=True,
        )
    )
    fig.update_xaxes(title=None)
    fig.update_yaxes(title=None, range=[0, y_max])
    fig.update_layout(
        hovermode="x",
        margin=dict(l=0, r=0, t=5, b=5),
        legend=dict(
            orientation="h",
            yanchor="top",
            xanchor="right",
            y=-0.1,
            x=1,
            title_text=None,
        ),
    )
    st.plotly_chart(fig, use_container_width=True, key=f"{y_label}_stack")


def gpu_chart_average(
    df: pd.DataFrame,
    y_label: str,
    y_max: float,
    title: str,
    containers: list,
    N_GPU: int = 8,
):
    containers[0].metric(title, f"{df[y_label].sum():.2f}")
    fig = (
        alt.Chart(df)
        .mark_bar()
        .encode(
            x="gpu_index:N",
            y=alt.Y(f"{y_label}:Q").scale(domain=[0, y_max]),
            color=alt.Color("gpu_index:N").scale(
                domain=range(N_GPU), range=px.colors.qualitative.Plotly
            ),
            tooltip=["gpu_index", f"{y_label}"],
        )
        .configure_axis(labels=False, title=None)
        .configure_legend(disable=True)
        .properties(height=50, padding={"top": 0, "bottom": 0})
    )
    containers[1].altair_chart(fig, use_container_width=True)


def celi_to_quarter(time: dt.datetime) -> dt.datetime:
    minute = (time.minute // 15 + 1) * 15
    if minute == 60:
        time = (time + dt.timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)
    else:
        time = time.replace(minute=minute, second=0, microsecond=0)
    return time


def get_default_time(
    db_path: str,
) -> tuple[dt.datetime, dt.datetime, dt.datetime, dt.datetime, dt.datetime | None]:
    # 默认查询时间范围：pc - 过去 1 天 / mobile - 过去 6 小时
    max_time = default_end_time = dt.datetime.now()
    if st.session_state.is_session_pc:
        min_time = default_start_time = default_end_time - dt.timedelta(days=1)
    else:
        min_time = default_start_time = default_end_time - dt.timedelta(hours=6)

    oldest_timestamp, latest_timestamp = query_min_max_timestamp(db_path)
    if oldest_timestamp is not None:
        default_start_time = celi_to_quarter(
            max(default_start_time, oldest_timestamp.replace(tzinfo=None))
        )
        default_end_time = celi_to_quarter(
            min(default_end_time, latest_timestamp.replace(tzinfo=None))
        )
        min_time = celi_to_quarter(
            min(min_time, oldest_timestamp.replace(tzinfo=None))
            - dt.timedelta(minutes=15)
        )
        max_time = celi_to_quarter(max(max_time, latest_timestamp.replace(tzinfo=None)))

    return default_start_time, default_end_time, min_time, max_time, latest_timestamp


def store_value(key: str) -> None:
    st.session_state["_" + key] = st.session_state[key]


def load_value(key: str) -> None:
    st.session_state[key] = st.session_state.get("_" + key, None)


def webapp_history(
    hostname: str = "Virgo",
    db_path: str = "data/gpu_history_virgo.db",
    config: dict = {},
) -> None:
    DB_PATH = db_path  # 数据库路径

    # if config is not None:
    #     N_GPU = config["N_GPU"]
    #     GMEM = config["GMEM"]
    # else:
    #     N_GPU = 8
    #     GMEM = 80
    N_GPU = config.get("N_GPU", 8)
    GMEM = config.get("GMEM", 80)

    st.title(f"{hostname}: 历史信息")

    # 默认查询时间范围：过去 1 天
    default_start_time, default_end_time, oldest_time, latest_time, latest_timestamp = (
        get_default_time(DB_PATH)
    )

    # st.write(st.session_state)

    def reset_button():
        default_start_time, default_end_time, _, _, _ = get_default_time(DB_PATH)
        st.session_state["start_date"] = default_start_time.date()
        st.session_state["end_date"] = default_end_time.date()
        st.session_state["start_time"] = default_start_time.time()
        st.session_state["end_time"] = default_end_time.time()
        return

    if st.session_state.get(f"_selection_history_{hostname}", None) is None:
        st.session_state[f"_selection_history_{hostname}"] = "**详细信息**"

    # 日期范围选择
    col1, col2, reset = st.columns([5, 5, 2], vertical_alignment="bottom")

    start_date = col1.date_input(
        "开始时间",
        value=default_start_time,
        key="start_date",
        min_value=oldest_time,
        max_value=latest_time,
    )
    start_time = col1.time_input(
        "开始时间",
        value=default_start_time,
        key="start_time",
        label_visibility="collapsed",
    )
    end_date = col2.date_input(
        "结束时间",
        value=default_end_time,
        key="end_date",
        min_value=oldest_time,
        max_value=latest_time,
    )
    end_time = col2.time_input(
        "结束时间", value=default_end_time, key="end_time", label_visibility="collapsed"
    )

    start_time = dt.datetime.combine(start_date, start_time).astimezone(dt.timezone.utc)
    end_time = dt.datetime.combine(end_date, end_time).astimezone(dt.timezone.utc)

    reset.button("重置", use_container_width=True, on_click=reset_button)

    if start_time >= end_time:
        st.error("开始时间必须早于结束时间。")
    else:
        if latest_timestamp is not None:
            st.write(f"数据更新于：{latest_timestamp.strftime('%Y-%m-%d %H:%M:%S')}")
            gpu_avg_fd = query_gpu_history_average_usage(start_time, end_time, DB_PATH)

            fig_u, usage, fig_m, mem = st.columns(
                [3, 2, 3, 2], vertical_alignment="bottom"
            )
            gpu_chart_average(
                gpu_avg_fd,
                "avg_gpu_utilization",
                100,
                "平均使用率 %",
                [usage, fig_u],
                N_GPU,
            )
            gpu_chart_average(
                gpu_avg_fd,
                "avg_used_memory",
                GMEM,
                "平均显存用量 GB",
                [mem, fig_m],
                N_GPU,
            )

            load_value(f"selection_history_{hostname}")
            select = st.pills(
                "信息选择",
                ["**详细信息**", "**用户使用**", "**汇总数据**"],
                label_visibility="collapsed",
                selection_mode="single",
                key=f"selection_history_{hostname}",
                on_change=store_value,
                args=[f"selection_history_{hostname}"],
            )

            if select == "**详细信息**":
                gpu_usage_df = query_gpu_history_usage(
                    start_time, end_time, DB_PATH, True
                )

                st.subheader("使用率 %")
                gpu_chart_band(gpu_usage_df, "gpu_utilization", N_GPU)

                st.subheader("显存用量 GB")
                gpu_chart_band(gpu_usage_df, "used_memory", N_GPU)
            elif select == "**用户使用**":
                user_usage_grouped = query_gpu_user_history_usage(
                    start_time, end_time, DB_PATH, True
                )
                if os.getenv("ENABLE_NAME_DICT", "0") == "1":
                    name_dict = dict_username(DB_PATH)
                else:
                    name_dict = None

                user_total_df = query_gpu_user_history_total_usage(
                    start_time, end_time, DB_PATH
                )
                if name_dict is not None:
                    user_total_df["user"] = user_total_df["user"].apply(
                        lambda x: name_dict.get(x, x)
                    )

                st.subheader("用户使用率 %")
                gpu_chart_user(user_usage_grouped, "gpu_utilization", name_dict, N_GPU)

                st.dataframe(user_total_df)

                st.subheader("用户显存用量 GB")
                gpu_chart_user(user_usage_grouped, "used_memory", name_dict, N_GPU)

            elif select == "**汇总数据**":
                gpu_usage_df = query_gpu_history_usage(start_time, end_time, DB_PATH)

                st.subheader("总使用率 %")
                gpu_chart_stack(gpu_usage_df, "gpu_utilization", 100 * N_GPU)

                st.subheader("总显存用量 GB")
                gpu_chart_stack(gpu_usage_df, "used_memory", GMEM * N_GPU)
        else:
            st.warning("数据库中没有 GPU 数据记录。")
