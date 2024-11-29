import streamlit as st

pages = {
    "Leo": [
        st.Page("webapp/realtime_leo.py", title="Leo: 实时状态"),
        st.Page("webapp/history_leo.py", title="Leo: 历史信息"),
    ],
    "Virgo": [
        st.Page("webapp/realtime_virgo.py", title="Virgo: 实时状态"),
        st.Page("webapp/history_virgo.py", title="Virgo: 历史信息"),
    ],
    "AI4S": [
        st.Page("webapp/ai4s_tasks.py", title="AI4S: 任务列表"),
        st.Page("webapp/fee.py", title="AI4S: 费用记录"),
    ],
    "Info": [
        st.Page("webapp/user_info.py", title="用户信息查询"),
    ],
}

pg = st.navigation(pages)
pg.run()
