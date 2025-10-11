
import os
import tempfile
import traceback
import streamlit as st
import sqlite3
from contextlib import closing
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
import pandas as pd
from io import BytesIO
from pathlib import Path

APP_TITLE = "Cihaz Arıza Takip — Basit (Safe/Diagnostic)"
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

def device_count(conn) -> int:
    cur = conn.execute("SELECT COUNT(*) AS c FROM devices")
    return int(cur.fetchone()["c"])

def page_devices():
    st.subheader("Cihazlar")
    p = Path(DB_PATH)
    parent = p.parent
    p_exists = p.exists()
    size_info = (str(p.stat().st_size) + " B") if p_exists else "yok"
    st.caption(f"Veritabanı: `{DB_PATH}` — dosya: {size_info} — klasör yazılabilir: {os.access(parent, os.W_OK)}")

    with st.form(key="add_dev_form", clear_on_submit=True):
        raw_name = st.text_input("Yeni cihaz adı", placeholder="Örn. Cobas t711")
        submit = st.form_submit_button("Cihaz Ekle")
    if submit:
        name = normalize_name(raw_name)
        if not name:
            st.error("Cihaz adı zorunludur.")
        else:
            try:
                with closing(get_conn()) as conn:
                    if device_exists_ci(conn, name):
                        st.warning("Bu cihaz adı zaten mevcut.")
                    else:
                        conn.execute("INSERT INTO devices(name, created_at) VALUES (?, ?)", (name, datetime.now(timezone.utc).isoformat()))
                        conn.commit()
                        cnt = device_count(conn)
                        st.success(f"Eklendi: '{name}'. Toplam cihaz: {cnt}")
            except Exception as e:
                st.error(f"Cihaz ekleme hatası: {e}")
                with st.expander("Teknik ayrıntı"):
                    st.code(traceback.format_exc())

    if st.button("🔧 Yazma Testi (TEST cihazı ekle)"):
        test_name = f"TEST-{int(datetime.now().timestamp())}"
        try:
            with closing(get_conn()) as conn:
                conn.execute("INSERT INTO devices(name, created_at) VALUES (?, ?)", (test_name, datetime.now(timezone.utc).isoformat()))
                conn.commit()
                cnt = device_count(conn)
            st.success(f"TEST cihaz eklendi: {test_name} — Toplam cihaz: {cnt}")
        except Exception as e:
            st.error(f"Yazma testi başarısız: {e}")
            with st.expander("Teknik ayrıntı"):
                st.code(traceback.format_exc())

    st.markdown("### Mevcut Cihazlar")
    try:
        with closing(get_conn()) as conn:
            df = pd.read_sql_query("SELECT id, name, created_at FROM devices ORDER BY id DESC", conn)
        st.dataframe(df, use_container_width=True)
    except Exception as e:
        st.error(f"Kayıtları okuma hatası: {e}")
        with st.expander("Teknik ayrıntı"):
            st.code(traceback.format_exc())

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
    now_local = datetime.now(TZ).replace(second=0, microsecond=0)
    c1, c2 = st.columns(2)
    with c1:
        start_local = st.datetime_input("Başlangıç (yerel)", value=now_local)
    with c2:
        end_local = st.datetime_input("Bitiş (yerel)", value=now_local)
    if st.button("Kaydı Oluştur", type="primary"):
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
        from io import BytesIO
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
