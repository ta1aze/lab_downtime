import os
import tempfile
import traceback
import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime, time, timezone
from zoneinfo import ZoneInfo
from pathlib import Path
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from contextlib import contextmanager

APP_TITLE = "Cihaz Arıza Takip — Kalıcı (PostgreSQL destekli)"
TZ = ZoneInfo("Europe/Istanbul")

# ---------- DB seçimi: DATABASE_URL varsa Postgres, yoksa HOME altında SQLite ----------
def _get_database_url():
    url = None
    try:
        url = st.secrets.get("DATABASE_URL", None)
    except Exception:
        url = None
    if not url:
        url = os.environ.get("DATABASE_URL")
    return url

def _sqlite_path():
    home_dir = Path.home() / ".lab_downtime"
    try:
        home_dir.mkdir(parents=True, exist_ok=True)
        data_dir = home_dir
    except Exception:
        data_dir = Path(tempfile.gettempdir())
    return data_dir / "downtime.db"

def _mk_sqlite_engine():
    return create_engine(f"sqlite:///{_sqlite_path()}", pool_pre_ping=True)

DB_URL = _get_database_url()
USING_POSTGRES = bool(DB_URL)

# Engine oluşturma (Neon için timeout + fallback)
if USING_POSTGRES:
    if "sslmode=" not in DB_URL:
        DB_URL += ("&" if "?" in DB_URL else "?") + "sslmode=require"
    try:
        engine: Engine = create_engine(
            DB_URL,
            pool_pre_ping=True,
            pool_recycle=300,
            connect_args={"connect_timeout": 10},  # saniye
        )
        # Hızlı ping
        with engine.connect() as conn:
            conn.exec_driver_sql("SELECT 1")
        DB_INFO = f"PostgreSQL: {DB_URL.split('@')[-1]}"
    except Exception as e:
        st.warning(f"PostgreSQL bağlantısı başarısız (timeout/erişim). SQLite'a düşüldü. Ayrıntı: {e}")
        engine: Engine = _mk_sqlite_engine()
        DB_INFO = f"SQLite: {_sqlite_path()}"
        USING_POSTGRES = False
else:
    engine: Engine = _mk_sqlite_engine()
    DB_INFO = f"SQLite: {_sqlite_path()}"

@contextmanager
def connect():
    # Otomatik transaction / commit
    with engine.begin() as conn:
        yield conn

# ---------- Şema ----------
SCHEMA_PG = """
CREATE TABLE IF NOT EXISTS devices (
  id SERIAL PRIMARY KEY,
  name TEXT NOT NULL UNIQUE,
  created_at TIMESTAMPTZ NOT NULL
);
CREATE TABLE IF NOT EXISTS faults (
  id SERIAL PRIMARY KEY,
  device_id INTEGER NOT NULL REFERENCES devices(id) ON DELETE CASCADE,
  reason TEXT,
  started_utc TIMESTAMPTZ NOT NULL,
  ended_utc TIMESTAMPTZ,
  duration_min INTEGER,
  created_at TIMESTAMPTZ NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_faults_device ON faults(device_id);
CREATE INDEX IF NOT EXISTS idx_faults_started ON faults(started_utc);
"""
SCHEMA_SQLITE = SCHEMA_PG.replace("SERIAL", "INTEGER").replace("TIMESTAMPTZ", "TEXT")

def init_db():
    with connect() as conn:
        conn.exec_driver_sql(SCHEMA_PG if USING_POSTGRES else SCHEMA_SQLITE)

# ---------- Admin girişi (cihaz ekleme yetkisi) ----------
def admin_login_ui():
    if "admin_authed" not in st.session_state:
        st.session_state.admin_authed = False
    with st.sidebar.expander("🔑 Admin Girişi", expanded=False):
        if st.session_state.admin_authed:
            st.success("Admin olarak giriş yapıldı.")
            if st.button("Çıkış yap"):
                st.session_state.admin_authed = False
                st.rerun()
        else:
            pwd = st.text_input("Admin şifresi", type="password", help="Secrets: ADMIN_TOKEN")
            if st.button("Giriş yap"):
                token = None
                try:
                    token = st.secrets.get("ADMIN_TOKEN", None)
                except Exception:
                    token = None
                if not token:
                    token = os.environ.get("ADMIN_TOKEN")
                if token and pwd == token:
                    st.session_state.admin_authed = True
                    st.success("Giriş başarılı.")
                    st.rerun()
                else:
                    st.error("Geçersiz şifre.")

# ---------- Yardımcılar ----------
def normalize_name(name: str) -> str:
    return " ".join((name or "").strip().split())

def compute_duration_min(start_iso: str, end_iso: str):
    if not start_iso or not end_iso:
        return None
    s = pd.to_datetime(start_iso, utc=True)
    e = pd.to_datetime(end_iso, utc=True)
    return max(0, int((e - s).total_seconds() // 60))

def to_local_str(iso_ts):
    # Güvenli: NaT/None/boş değerler
    try:
        if iso_ts is None or pd.isna(iso_ts):
            return ""
    except Exception:
        pass
    try:
        dt = pd.to_datetime(iso_ts, utc=True)
        if getattr(dt, 'tzinfo', None) is None:
            dt = dt.tz_localize('UTC')
        return dt.tz_convert(TZ).strftime('%Y-%m-%d %H:%M')
    except Exception:
        return ""

# ---------- Sayfalar ----------
def page_devices(is_admin: bool):
    st.subheader("Cihazlar")
    st.caption(f"Veritabanı: {DB_INFO}")

    if is_admin:
        with st.form("add_dev", clear_on_submit=True):
            raw = st.text_input("Yeni cihaz adı", placeholder="Örn. Cobas t711")
            submitted = st.form_submit_button("Cihaz Ekle (admin)")
        if submitted:
            name = normalize_name(raw)
            if not name:
                st.error("Cihaz adı zorunludur.")
            else:
                try:
                    with connect() as conn:
                        # SQLAlchemy stili parametre bağlama
                        dup = conn.execute(
                            text("SELECT 1 FROM devices WHERE lower(name)=lower(:n) LIMIT 1"),
                            {"n": name}
                        ).first()
                        if dup:
                            st.warning("Bu cihaz adı zaten mevcut.")
                        else:
                            conn.execute(
                                text("INSERT INTO devices(name, created_at) VALUES (:n, :c)"),
                                {"n": name, "c": datetime.now(timezone.utc).isoformat()}
                            )
                            st.success(f"Eklendi: {name}")
                except Exception as e:
                    st.error(f"Ekleme hatası: {e}")
                    with st.expander("Ayrıntı"):
                        st.code(traceback.format_exc())
    else:
        st.info("Cihaz ekleme yetkisi yalnızca **admin** kullanıcıda.")

    with connect() as conn:
        df = pd.read_sql(text("SELECT id, name, created_at FROM devices ORDER BY id DESC"), conn)
    st.dataframe(df, use_container_width=True)

def page_new_fault():
    st.subheader("Arıza Kaydı (Ekle)")
    with connect() as conn:
        devs = pd.read_sql(text("SELECT id, name FROM devices ORDER BY name"), conn)
    if devs.empty:
        st.info("Önce admin tarafından cihaz eklenmelidir.")
        return

    device_map = {row["name"]: int(row["id"]) for _, row in devs.iterrows()}
    dev_label = st.selectbox("Cihaz", list(device_map.keys()))
    reason = st.text_input("Arıza nedeni (opsiyonel)")

    now_local = pd.Timestamp.now(TZ).to_pydatetime()
    c1, c2 = st.columns(2)
    with c1:
        st_date = st.date_input("Başlangıç tarihi", value=now_local.date(), key="st_date")
        st_time_val = st.time_input("Başlangıç saati", value=time(hour=now_local.hour, minute=now_local.minute), key="st_time")
    with c2:
        end_none = st.checkbox("Bitiş yok (arızaya devam)", value=False, key="end_none")
        en_date = st.date_input("Bitiş tarihi", value=now_local.date(), key="en_date", disabled=end_none)
        en_time_val = st.time_input("Bitiş saati", value=time(hour=now_local.hour, minute=now_local.minute), key="en_time", disabled=end_none)

    if st.button("Kaydı Oluştur", type="primary"):
        st_local = datetime.combine(st_date, st_time_val).replace(tzinfo=TZ)
        start_iso = st_local.astimezone(timezone.utc).isoformat()
        if end_none:
            end_iso = None
            dur = None
        else:
            en_local = datetime.combine(en_date, en_time_val).replace(tzinfo=TZ)
            if en_local < st_local:
                st.error("Bitiş başlangıçtan önce olamaz.")
                return
            end_iso = en_local.astimezone(timezone.utc).isoformat()
            dur = compute_duration_min(start_iso, end_iso)
        with connect() as conn:
            conn.execute(text("""
                INSERT INTO faults(device_id, reason, started_utc, ended_utc, duration_min, created_at)
                VALUES (:d, :r, :s, :e, :m, :c)
            """), {"d": device_map[dev_label], "r": (reason or None), "s": start_iso,
                   "e": end_iso, "m": dur, "c": datetime.now(timezone.utc).isoformat()})
        st.success("Kayıt eklendi." if end_none else f"Kayıt eklendi. Süre: {dur} dk")

def page_list_export():
    st.subheader("Kayıtlar, Filtre & Excel")
    today = pd.Timestamp.now(TZ).date()
    c1, c2 = st.columns(2)
    with c1:
        dfrom = st.date_input("Başlangıç", value=today.replace(day=1))
    with c2:
        dto = st.date_input("Bitiş", value=today)

    start_iso = pd.Timestamp.combine(dfrom, pd.Timestamp.min.time()).tz_localize(TZ).tz_convert("UTC").isoformat()
    end_iso   = pd.Timestamp.combine(dto,   pd.Timestamp.max.time()).tz_localize(TZ).tz_convert("UTC").isoformat()

    with connect() as conn:
        df = pd.read_sql(
            text("""
                SELECT f.id, d.name AS cihaz, f.reason AS neden, f.started_utc, f.ended_utc, f.duration_min, f.created_at
                FROM faults f JOIN devices d ON d.id=f.device_id
                WHERE f.started_utc BETWEEN :a AND :b
                ORDER BY f.started_utc DESC
            """),
            conn, params={"a": start_iso, "b": end_iso}
        )

    if not df.empty:
        # Durum kolonu (Açık/Kapalı)
        df["durum"] = np.where(df["ended_utc"].isna(), "Açık", "Kapalı")

        # Görüntüleme tablosu
        df_show = df.copy()
        df_show["Başlangıç (yerel)"] = df_show["started_utc"].apply(to_local_str)
        df_show["Bitiş (yerel)"]     = df_show["ended_utc"].apply(to_local_str)
        df_show["Süre (dk)"]         = df_show["duration_min"].fillna("")
        st.dataframe(
            df_show[["id","cihaz","durum","neden","Başlangıç (yerel)","Bitiş (yerel)","Süre (dk)"]],
            use_container_width=True
        )

        # --- Hızlı seçim: listeden tıkla → detay formu aşağıda açılır
        st.markdown("#### Detaya git")
        labels = {
            int(r.id): f"#{int(r.id)} | {r.cihaz} | {('Açık' if pd.isna(r.ended_utc) else 'Kapalı')} | {to_local_str(r.started_utc)}"
            for _, r in df.iterrows()
        }
        if "edit_id" not in st.session_state:
            st.session_state.edit_id = int(df.iloc[0]["id"])

        selected_id = st.selectbox(
            "Kayıt seçin",
            options=list(labels.keys()),
            index=list(labels.keys()).index(st.session_state.edit_id) if st.session_state.get("edit_id") in labels else 0,
            format_func=lambda x: labels[x],
        )
        if selected_id != st.session_state.edit_id:
            st.session_state.edit_id = selected_id
            st.rerun()

        # === Kayıt düzenleme paneli ===
        st.markdown("### Kayıt düzenle")
        # Cihaz listesi
        with connect() as conn2:
            devs = pd.read_sql(text("SELECT id, name FROM devices ORDER BY name"), conn2)
        device_map_name2id = {row["name"]: int(row["id"]) for _, row in devs.iterrows()}
        device_choices = list(device_map_name2id.keys())

        edit_id = int(st.session_state.edit_id)
        row = df.loc[df["id"] == edit_id].iloc[0]

        def _to_local_dt(utc_val):
            if pd.isna(utc_val) or utc_val is None:
                return None
            return pd.to_datetime(utc_val, utc=True).tz_convert(TZ)

        started_local = _to_local_dt(row["started_utc"])
        ended_local   = _to_local_dt(row["ended_utc"])

        with st.form("edit_fault_form", clear_on_submit=False):
            cur_device_name = row["cihaz"] if row.get("cihaz", None) else (device_choices[0] if device_choices else "")
            idx = device_choices.index(cur_device_name) if cur_device_name in device_choices else 0
            new_device_name = st.selectbox("Cihaz", device_choices, index=idx)

            new_reason = st.text_input("Arıza nedeni (opsiyonel)", value=row.get("neden", "") or "")

            c3, c4 = st.columns(2)
            with c3:
                st_date = st.date_input("Başlangıç tarihi", value=started_local.date())
                st_time_val = st.time_input("Başlangıç saati", value=time(started_local.hour, started_local.minute))
            with c4:
                end_none = st.checkbox("Bitiş yok (açık arıza)", value=pd.isna(row["ended_utc"]))
                if ended_local is None:
                    ended_local = started_local
                en_date = st.date_input("Bitiş tarihi", value=ended_local.date(), disabled=end_none)
                en_time_val = st.time_input("Bitiş saati", value=time(ended_local.hour, ended_local.minute), disabled=end_none)

            saved = st.form_submit_button("Değişiklikleri Kaydet", type="primary")

        if saved:
            st_local = datetime.combine(st_date, st_time_val).replace(tzinfo=TZ)
            start_iso2 = st_local.astimezone(timezone.utc).isoformat()
            if end_none:
                end_iso2 = None
                dur2 = None
            else:
                en_local = datetime.combine(en_date, en_time_val).replace(tzinfo=TZ)
                if en_local < st_local:
                    st.error("Bitiş başlangıçtan önce olamaz.")
                    st.stop()
                end_iso2 = en_local.astimezone(timezone.utc).isoformat()
                dur2 = max(0, int((pd.to_datetime(end_iso2, utc=True) - pd.to_datetime(start_iso2, utc=True)).total_seconds() // 60))
            try:
                with connect() as conn3:
                    conn3.execute(text("""
                        UPDATE faults
                           SET device_id    = :d,
                               reason       = :r,
                               started_utc  = :s,
                               ended_utc    = :e,
                               duration_min = :m
                         WHERE id = :id
                    """), {
                        "d": device_map_name2id[new_device_name],
                        "r": (new_reason or None),
                        "s": start_iso2,
                        "e": end_iso2,
                        "m": dur2,
                        "id": int(edit_id),
                    })
                st.success(f"#{edit_id} güncellendi.")
                st.rerun()
            except Exception as e:
                st.error(f"Güncelleme hatası: {e}")

        # === Excel için tz-aware -> tz-naive dönüşüm (UTC) ===
        df_x = df.copy()
        for col in ["started_utc", "ended_utc", "created_at"]:
            if col in df_x.columns:
                s = pd.to_datetime(df_x[col], errors="coerce", utc=True)
                s = s.dt.tz_convert("UTC").dt.tz_localize(None)
                df_x[col] = s

        from io import BytesIO
        buf = BytesIO()
        with pd.ExcelWriter(buf, engine="openpyxl") as w:
            df_x.to_excel(w, sheet_name="faults", index=False)
        st.download_button("Excel (XLSX) indir", data=buf.getvalue(),
                           file_name="faults.xlsx",
                           mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    else:
        st.info("Kayıt yok.")
        st.button("Excel (XLSX) indir", disabled=True)

def main():
    st.set_page_config(page_title=APP_TITLE, page_icon="🧪", layout="wide")
    st.title(APP_TITLE)
    st.caption(f"Veritabanı: {DB_INFO}")
    admin_login_ui()

    # Bakım yardımcıları (opsiyonel)
    with st.sidebar.expander("⚙️ Bakım"):
        if st.button("Önbelleği Temizle"):
            st.cache_data.clear(); st.cache_resource.clear()
            st.success("Önbellek temizlendi."); st.rerun()

    is_admin = bool(st.session_state.get("admin_authed", False))
    menu = ["Arıza Kaydı", "Kayıtlar & Excel"]
    if is_admin:
        menu.insert(0, "Cihazlar")
    page = st.sidebar.radio("Menü", menu, index=0)

    if page == "Cihazlar":
        page_devices(is_admin=True)
    elif page == "Kayıtlar & Excel":
        page_list_export()
    else:
        page_new_fault()

if __name__ == "__main__":
    try:
        init_db()
    except Exception as e:
        st.error(f"DB init error: {e}")
        st.stop()
    main()
