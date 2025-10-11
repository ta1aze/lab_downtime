
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

# ===== Admin Auth =====
def get_admin_token():
    # Prefer Streamlit secrets; fallback to environment variable
    token = None
    try:
        token = st.secrets.get("ADMIN_TOKEN", None)  # returns {} if not configured
    except Exception:
        token = None
    if not token:
        token = os.environ.get("ADMIN_TOKEN")
    return token

def ensure_admin_state():
    if "admin_authed" not in st.session_state:
        st.session_state.admin_authed = False

def admin_login_ui():
    ensure_admin_state()
    with st.sidebar.expander("🔑 Admin Girişi", expanded=False):
        if st.session_state.admin_authed:
            st.success("Admin olarak giriş yapıldı.")
            if st.button("Çıkış yap"):
                st.session_state.admin_authed = False
                st.rerun()
        else:
            pwd = st.text_input("Admin şifresi", type="password", help="Streamlit Secrets: ADMIN_TOKEN")
            if st.button("Giriş yap"):
                configured = get_admin_token()
                if configured and pwd == configured:
                    st.session_state.admin_authed = True
                    st.success("Giriş başarılı.")
                    st.rerun()
                else:
                    st.error("Geçersiz şifre.")

def is_admin() -> bool:
    ensure_admin_state()
    return bool(st.session_state.admin_authed)

# ===== DB / Helpers =====
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
  ended_utc    TEXT,
  duration_min INTEGER,
  created_at   TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_faults_device ON faults(device_id);
CREATE INDEX IF NOT EXISTS idx_faults_started ON faults(started_utc);
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
    dt = datetime.combine(date_val, time_val)
    return dt.replace(tzinfo=TZ)

def to_local(iso_utc: str):
    if not iso_utc:
        return None
    dt = datetime.fromisoformat(iso_utc.replace("Z", "+00:00"))
    return dt.astimezone(TZ)

def compute_duration_min(start_iso_utc: str, end_iso_utc: str) -> int | None:
    if not start_iso_utc or not end_iso_utc:
        return None
    s = datetime.fromisoformat(start_iso_utc.replace("Z", "+00:00"))
    e = datetime.fromisoformat(end_iso_utc.replace("Z", "+00:00"))
    return max(0, int((e - s).total_seconds() // 60))

# --- Devices page (admin-gated add) ---
def page_devices():
    st.subheader("Cihazlar")
    p = Path(DB_PATH)
    size_info = (str(p.stat().st_size) + " B") if p.exists() else "yok"
    st.caption(f"Veritabanı: `{DB_PATH}` — dosya: {size_info}")
    if is_admin():
        with st.form("add_dev", clear_on_submit=True):
            raw = st.text_input("Yeni cihaz adı", placeholder="Örn. Cobas t711")
            submit = st.form_submit_button("Cihaz Ekle (admin)")
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
    else:
        st.info("Cihaz ekleme yetkisi yalnızca **admin** kullanıcıdadır.")

    with closing(get_conn()) as conn:
        df = pd.read_sql_query("SELECT id, name, created_at FROM devices ORDER BY id DESC", conn)
    st.markdown("### Mevcut Cihazlar")
    st.dataframe(df, use_container_width=True)

# --- New fault page (supports open-ended) ---
def page_new_fault():
    st.subheader("Arıza Kaydı (Ekle)")
    with closing(get_conn()) as conn:
        devs = conn.execute("SELECT id, name FROM devices ORDER BY name").fetchall()
    if not devs:
        st.info("Önce **Cihazlar** sayfasında admin tarafından en az bir cihaz eklenmelidir.")
        return
    device_map = {d["name"]: d["id"] for d in devs}
    dev_label = st.selectbox("Cihaz", list(device_map.keys()))
    reason = st.text_input("Arıza nedeni (opsiyonel)")

    now_local = datetime.now(TZ)
    c1, c2 = st.columns(2)
    with c1:
        start_date = st.date_input("Başlangıç tarihi", value=now_local.date(), key="st_date")
        start_time = st.time_input("Başlangıç saati", value=time(hour=now_local.hour, minute=now_local.minute), key="st_time")
    with c2:
        end_none = st.checkbox("Bitiş yok (arızaya devam)", value=False, key="end_none")
        end_date = st.date_input("Bitiş tarihi", value=now_local.date(), key="en_date", disabled=end_none)
        end_time = st.time_input("Bitiş saati", value=time(hour=now_local.hour, minute=now_local.minute), key="en_time", disabled=end_none)

    if st.button("Kaydı Oluştur", type="primary"):
        start_local = to_local_datetime(start_date, start_time)
        start_utc = start_local.astimezone(timezone.utc).isoformat()
        if end_none:
            ended_utc = None
            duration = None
        else:
            end_local = to_local_datetime(end_date, end_time)
            if end_local < start_local:
                st.error("Bitiş başlangıçtan önce olamaz.")
                return
            ended_utc = end_local.astimezone(timezone.utc).isoformat()
            duration = compute_duration_min(start_utc, ended_utc)
        with closing(get_conn()) as conn:
            conn.execute("""
                INSERT INTO faults(device_id, reason, started_utc, ended_utc, duration_min, created_at)
                VALUES (?,?,?,?,?,?)
            """, (device_map[dev_label], (reason or None), start_utc, ended_utc, duration, datetime.now(timezone.utc).isoformat()))
            conn.commit()
        st.success("Kayıt eklendi." if end_none else f"Kayıt eklendi. Süre: {duration} dk")

# --- List/export/edit page ---
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
        df_show = df.copy()
        def fmt_local(x):
            if not x:
                return ""
            dt = to_local(x)
            return dt.strftime("%Y-%m-%d %H:%M")
        df_show["Başlangıç (yerel)"] = df_show["started_utc"].apply(fmt_local)
        df_show["Bitiş (yerel)"] = df_show["ended_utc"].apply(fmt_local)
        df_show["Süre (dk)"] = df_show["duration_min"].fillna("")
        st.dataframe(df_show[["id","cihaz","neden","Başlangıç (yerel)","Bitiş (yerel)","Süre (dk)"]], use_container_width=True)
    else:
        st.info("Kayıt yok.")

    # Excel export
    st.markdown("### Excel Dışa Aktarım")
    if not df.empty:
        out = df.copy()
        out.insert(3, "started_local", out["started_utc"].apply(lambda x: to_local(x).strftime("%Y-%m-%d %H:%M") if x else ""))
        out.insert(4, "ended_local", out["ended_utc"].apply(lambda x: to_local(x).strftime("%Y-%m-%d %H:%M") if x else ""))
        buf = BytesIO()
        with pd.ExcelWriter(buf, engine="openpyxl") as writer:
            out.to_excel(writer, sheet_name="faults", index=False)
        st.download_button("Excel (XLSX) indir", data=buf.getvalue(),
                           file_name="faults.xlsx",
                           mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    else:
        st.button("Excel (XLSX) indir", disabled=True)

    st.divider()
    st.markdown("### Mevcut Kayıtları Düzenle")
    if df.empty:
        st.caption("Düzenlenecek kayıt yok.")
    else:
        for _, r in df.iterrows():
            open_state = pd.isna(r["ended_utc"]) or not r["ended_utc"]
            title = f"#{r['id']} — {r['cihaz']} | {'AÇIK' if open_state else 'Kapalı'}"
            with st.expander(title):
                with closing(get_conn()) as conn:
                    devs = conn.execute("SELECT id, name FROM devices ORDER BY name").fetchall()
                dev_map = {d["name"]: d["id"] for d in devs}
                dev_label = st.selectbox("Cihaz", list(dev_map.keys()), index=list(dev_map.keys()).index(r["cihaz"]), key=f"dev_{r['id']}")
                reason = st.text_input("Neden (opsiyonel)", value=r["neden"] or "", key=f"rsn_{r['id']}")

                st_local = to_local(r["started_utc"])
                c1, c2 = st.columns(2)
                with c1:
                    st_date = st.date_input("Başlangıç tarihi", value=st_local.date(), key=f"st_d_{r['id']}")
                with c2:
                    st_time_val = st.time_input("Başlangıç saati", value=time(hour=st_local.hour, minute=st_local.minute), key=f"st_t_{r['id']}")

                en_local = to_local(r["ended_utc"]) if r["ended_utc"] else None
                c3, c4 = st.columns(2)
                end_none = st.checkbox("Bitiş yok (açık)", value=open_state, key=f"end_none_{r['id']}")
                with c3:
                    en_date = st.date_input("Bitiş tarihi", value=(en_local.date() if en_local else st_local.date()), key=f"en_d_{r['id']}", disabled=end_none)
                with c4:
                    en_time_val = st.time_input("Bitiş saati", value=(time(hour=en_local.hour, minute=en_local.minute) if en_local else time(hour=st_local.hour, minute=st_local.minute)), key=f"en_t_{r['id']}", disabled=end_none)

                colu1, colu2 = st.columns(2)
                with colu1:
                    if st.button("Kaydı Güncelle", key=f"upd_{r['id']}", type="primary"):
                        new_start_local = datetime.combine(st_date, st_time_val).replace(tzinfo=TZ)
                        new_start_utc = new_start_local.astimezone(timezone.utc).isoformat()
                        if end_none:
                            new_ended_utc = None
                            new_duration = None
                        else:
                            new_end_local = datetime.combine(en_date, en_time_val).replace(tzinfo=TZ)
                            if new_end_local < new_start_local:
                                st.error("Bitiş başlangıçtan önce olamaz.")
                                st.stop()
                            new_ended_utc = new_end_local.astimezone(timezone.utc).isoformat()
                            new_duration = compute_duration_min(new_start_utc, new_ended_utc)
                        with closing(get_conn()) as conn:
                            conn.execute("""
                                UPDATE faults
                                SET device_id=?, reason=?, started_utc=?, ended_utc=?, duration_min=?
                                WHERE id=?
                            """, (dev_map[dev_label], (reason or None), new_start_utc, new_ended_utc, new_duration, int(r["id"])))
                            conn.commit()
                        st.success("Kayıt güncellendi.")
                        st.experimental_rerun()
                with colu2:
                    if open_state and st.button("Kapat (şimdi)", key=f"close_now_{r['id']}"):
                        now_loc = datetime.now(TZ)
                        now_utc = now_loc.astimezone(timezone.utc).isoformat()
                        new_duration = compute_duration_min(r["started_utc"], now_utc)
                        with closing(get_conn()) as conn:
                            conn.execute("""
                                UPDATE faults
                                SET ended_utc=?, duration_min=?
                                WHERE id=?
                            """, (now_utc, new_duration, int(r["id"])))
                            conn.commit()
                        st.success(f"Kapatıldı. Süre: {new_duration} dk")
                        st.experimental_rerun()

def main():
    st.set_page_config(page_title=APP_TITLE, page_icon="🧪", layout="wide")
    st.title(APP_TITLE)
    init_db()
    admin_login_ui()
    menu_items = ["Arıza Kaydı", "Kayıtlar & Excel"]
    if is_admin():
        menu_items.insert(0, "Cihazlar")  # Only show device add page to admin
    page = st.sidebar.radio("Menü", menu_items, index=0)
    if page == "Cihazlar":
        page_devices()
    elif page == "Kayıtlar & Excel":
        page_list_export()
    else:
        page_new_fault()

if __name__ == "__main__":
    main()
