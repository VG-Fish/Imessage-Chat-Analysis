import os
import sqlite3
from pathlib import Path

import pandas as pd
import plotly.express as px
from dotenv import load_dotenv
from plotly.graph_objs import Figure

load_dotenv()

CHAT_ID = 150

identifier_to_name: dict[str, str] = {
    os.environ["Leo"]: "Leo",
    os.environ["Abdullah"]: "Abdullah",
    os.environ["Johnathan"]: "Johnathan",
    os.environ["Vishy"]: "Vishy",
    os.environ["Vishy_email"]: "Vishy",
}

db_path: Path = Path.home() / "Library" / "Messages" / "chat.db"
conn: sqlite3.Connection = sqlite3.connect(db_path)

query: str = """
    SELECT
        datetime(m.date / 1000000000 + strftime('%s','2001-01-01'), 'unixepoch', 'localtime') AS timestamp,
        m.text,
        m.is_from_me,
        h.id AS sender_identifier
    FROM chat_message_join cmj
    JOIN message m ON cmj.message_id = m.ROWID
    LEFT JOIN handle h ON m.handle_id = h.ROWID
    WHERE cmj.chat_id = ?
    ORDER BY m.date ASC
"""

df: pd.DataFrame = pd.read_sql_query(query, conn, params=[CHAT_ID])
conn.close()

df["timestamp"] = pd.to_datetime(df["timestamp"])


def label_sender(row: dict) -> str:
    sender_name: str = ""
    if row["is_from_me"]:
        sender_name = "Vishy"
    else:
        ident = row["sender_identifier"]
        sender_name = identifier_to_name.get(ident, "Other Participant")

    if sender_name in ["Me", "Vishy"]:
        return "Vishy"

    return sender_name


df["sender"] = df.apply(label_sender, axis=1)

df_daily: pd.DataFrame = (  # pyright: ignore[reportCallIssue]
    df.groupby(["sender", pd.Grouper(key="timestamp", freq="D")])
    .size()
    .reset_index(name="message_count")
)

df = df.drop(columns=["sender_identifier", "text"])

df_daily["7day_avg"] = df_daily.groupby("sender")["message_count"].transform(
    lambda x: x.rolling(window=7, min_periods=1).mean()
)

fig: Figure = px.line(
    df_daily,
    x="timestamp",
    y="7day_avg",
    color="sender",
    title=f"Chat {CHAT_ID}: Weekly Message Trends (7-Day Rolling Average)",
    labels={
        "timestamp": "Date",
        "7day_avg": "Avg Messages/Day",
        "sender": "Participant",
    },
    render_mode="svg",
)

fig.update_layout(
    hovermode="x unified",
    template="plotly_white",
    xaxis=dict(
        rangeselector=dict(
            buttons=list(
                [
                    dict(count=1, label="1m", step="month", stepmode="backward"),
                    dict(count=6, label="6m", step="month", stepmode="backward"),
                    dict(step="all"),
                ]
            )
        ),
        rangeslider=dict(visible=True),
        type="date",
    ),
)

fig.show()
fig.write_html("chat_analysis.html")
