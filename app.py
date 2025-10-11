
import os
import tempfile
import traceback
import streamlit as st
import sqlite3
from contextlib import closing
from datetime import datetime, time, timezone
from zoneinfo import ZoneInfo
import pandas as pd
from io import BytesIO
from pathlib import Path

APP_TITLE = "Cihaz Arıza Takip — Basit (MVP)"
# HOME first, then /tmp
HOME_DIR = Path.home() / ".lab_downtime"
try:
    HOME_DIR.mkdir(parents=True, exist_ok=True)
    DATA_DIR = HOME_DIR
except Exception:
    DATA_DIR = Path(tempfile.gettempdir())
DB_PATH = str(Path(DATA_DIR) / "downtime.db")
TZ = ZoneInfo("Europe/Istanbul")

def get_conn():
    conn = sqlite3.connect(DB_PATH, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES, timeout=10)
    conn.execute("PRAGMA busy_timeout=5000;")
    conn.execute("PRAGMA foreign_keys=ON;")
    try:
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
    except sqlite3.OperationalError:
        pass
    conn.row_factory = sqlite3.Row
    return conn

SCHEMA = """
CREATE TABLE IF NOT EXISTS devices (
  id         INTEGER PRIMARY KEY AUTOINCREMENT,
  name       TEXT NOT NULL UNIQUE,
  created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS faults (
  id           INTEGER PRIMARY KEY AUTOINCREMENT,
  device_id    INTEGER NOT NULL REFERENCES devices(id),
  reason       TEXT,
  started_utc  TEXT NOT NULL,
  ended_utc    TEXT NOT NULL,
  duration_min INTEGER NOT NULL,
  created_at   TEXT NOT NULL
);
"""

def init_db():
    with closing(get_conn()) as conn:
        conn.executescript(SCHEMA)
        conn.commit()

def normalize_name(name: str) -> str:
    return " ".join((name or "").strip().split())

def device_exists_ci(conn, name_norm: str) -> bool:
    cur = conn.execute("SELECT 1 FROM devices WHERE lower(name)=lower(?) LIMIT 1", (name_norm,))
    return cur.fetchone() is not None

def to_local_datetime(date_val, time_val):
    """Combine date + time into a timezone-aware local datetime."""
    dt = datetime.combine(date_val, time_val)
    return dt.replace(tzinfo=TZ)

def page_devices():
    st.subheader("Cihazlar")
    p = Path(DB_PATH)
    size_info = (str(p.stat().st_size) + " B") if p.exists() else "yok"
    st.caption(f"Veritabanı: `{DB_PATH}` — dosya: {size_info}")
    with st.form("add_dev", clear_on_submit=True):
        raw = st.text_input("Yeni cihaz adı", placeholder="Örn. Cobas t711")
        submit = st.form_submit_button("Cihaz Ekle")
    if submit:
        name = normalize_name(raw)
        if not name:
            st.error("Cihaz adı zorunludur.")
        else:
            try:
                with closing(get_conn()) as conn:
                    if device_exists_ci(conn, name):
                        st.warning("Bu cihaz adı zaten mevcut.")
                    else:
                        conn.execute("INSERT INTO devices(name, created_at) VALUES (?, ?)",
                                     (name, datetime.now(timezone.utc).isoformat()))
                        conn.commit()
                        st.success(f"Eklendi: {name}")
            except Exception as e:
                st.error(f"Ekleme hatası: {e}")
                with st.expander("Ayrıntı"):
                    st.code(traceback.format_exc())

    with closing(get_conn()) as conn:
        df = pd.read_sql_query("SELECT id, name, created_at FROM devices ORDER BY id DESC", conn)
    st.markdown("### Mevcut Cihazlar")
    st.dataframe(df, use_container_width=True)

def page_new_fault():
    st.subheader("Arıza Kaydı (Ekle)")
    with closing(get_conn()) as conn:
        devs = conn.execute("SELECT id, name FROM devices ORDER BY name").fetchall()
    if not devs:
        st.info("Önce **Cihazlar** sayfasından en az bir cihaz ekleyin.")
        return
    device_map = {d["name"]: d["id"] for d in devs}
    dev_label = st.selectbox("Cihaz", list(device_map.keys()))
    reason = st.text_input("Arıza nedeni (opsiyonel)")

    # --- Datetime inputs with separate date + time ---
    now_local = datetime.now(TZ)
    c1, c2 = st.columns(2)
    with c1:
        start_date = st.date_input("Başlangıç tarihi", value=now_local.date(), key="st_date")
        start_time = st.time_input("Başlangıç saati", value=time(hour=now_local.hour, minute=now_local.minute), key="st_time")
    with c2:
        end_date = st.date_input("Bitiş tarihi", value=now_local.date(), key="en_date")
        end_time = st.time_input("Bitiş saati", value=time(hour=now_local.hour, minute=now_local.minute), key="en_time")

    if st.button("Kaydı Oluştur", type="primary"):
        start_local = to_local_datetime(start_date, start_time)
        end_local = to_local_datetime(end_date, end_time)
        if end_local < start_local:
            st.error("Bitiş başlangıçtan önce olamaz.")
            return
        start_utc = start_local.astimezone(timezone.utc).isoformat()
        end_utc = end_local.astimezone(timezone.utc).isoformat()
        dur = max(0, int((end_local - start_local).total_seconds() // 60))
        with closing(get_conn()) as conn:
            conn.execute("""
                INSERT INTO faults(device_id, reason, started_utc, ended_utc, duration_min, created_at)
                VALUES (?,?,?,?,?,?)
            """, (device_map[dev_label], (reason or None), start_utc, end_utc, dur, datetime.now(timezone.utc).isoformat()))
            conn.commit()
        st.success(f"Kayıt eklendi. Süre: {dur} dk")

def page_list_export():
    st.subheader("Kayıtlar, Filtre & Excel")
    today = datetime.now(TZ).date()
    c1, c2 = st.columns(2)
    with c1:
        dfrom = st.date_input("Başlangıç", value=today.replace(day=1))
    with c2:
        dto = st.date_input("Bitiş", value=today)

    start_utc = datetime.combine(dfrom, datetime.min.time(), tzinfo=TZ).astimezone(timezone.utc).isoformat()
    end_utc = datetime.combine(dto, datetime.max.time(), tzinfo=TZ).astimezone(timezone.utc).isoformat()

    sql = """
    SELECT f.id, d.name AS cihaz, f.reason AS neden, f.started_utc, f.ended_utc, f.duration_min
    FROM faults f JOIN devices d ON d.id=f.device_id
    WHERE f.started_utc BETWEEN ? AND ?
    ORDER BY f.started_utc DESC
    """
    with closing(get_conn()) as conn:
        rows = conn.execute(sql, (start_utc, end_utc)).fetchall()
        df = pd.DataFrame(rows, columns=rows[0].keys()) if rows else pd.DataFrame(
            columns=["id","cihaz","neden","started_utc","ended_utc","duration_min"]
        )

    if not df.empty:
        st.dataframe(df, use_container_width=True)
        buf = BytesIO()
        with pd.ExcelWriter(buf, engine="openpyxl") as writer:
            df.to_excel(writer, sheet_name="faults", index=False)
        st.download_button("Excel (XLSX) indir", data=buf.getvalue(),
                           file_name="faults.xlsx",
                           mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    else:
        st.info("Kayıt yok.")

def main():
    st.set_page_config(page_title=APP_TITLE, page_icon="🧪", layout="wide")
    st.title(APP_TITLE)
    init_db()
    page = st.sidebar.radio("Menü", ["Cihazlar", "Arıza Kaydı", "Kayıtlar & Excel"], index=0)
    if page == "Cihazlar":
        page_devices()
    elif page == "Arıza Kaydı":
        page_new_fault()
    else:
        page_list_export()

if __name__ == "__main__":
    main()
