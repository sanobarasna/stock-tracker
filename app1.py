import os
import socket
from urllib.parse import urlparse, unquote
from datetime import date

import pandas as pd
import streamlit as st
import psycopg2
from psycopg2.extras import RealDictCursor, execute_values


# -----------------------------
# Config
# -----------------------------
st.set_page_config(page_title="Pack Split Tracker (Supabase)", layout="wide")


# -----------------------------
# DB Connection (IPv4-safe)
# -----------------------------
def _get_database_url() -> str:
    if "DATABASE_URL" in st.secrets:
        return st.secrets["DATABASE_URL"]
    env = os.getenv("DATABASE_URL", "")
    return env


@st.cache_resource(show_spinner=False)
def get_conn():
    db_url = _get_database_url()
    if not db_url:
        raise RuntimeError("DATABASE_URL is not set in Streamlit secrets or environment.")

    u = urlparse(db_url)

    if u.scheme not in ("postgres", "postgresql"):
        raise RuntimeError("DATABASE_URL must start with postgresql:// or postgres://")

    host = u.hostname
    port = u.port or 5432
    user = unquote(u.username or "")
    password = unquote(u.password or "")
    dbname = (u.path or "").lstrip("/") or "postgres"

    if not host or not user:
        raise RuntimeError("DATABASE_URL is missing host or user.")

    # Force IPv4 to avoid Streamlit Cloud IPv6 routing issues.
    try:
        hostaddr = socket.gethostbyname(host)  # returns IPv4 A record
    except Exception as e:
        raise RuntimeError(f"Failed to resolve IPv4 for host '{host}': {e}")

    # NOTE: host is still passed for TLS/SNI; hostaddr is used for actual network connection.
    conn = psycopg2.connect(
        dbname=dbname,
        user=user,
        password=password,
        host=host,
        hostaddr=hostaddr,
        port=port,
        sslmode="require",
        connect_timeout=10,
        cursor_factory=RealDictCursor,
    )
    return conn


def execute(sql: str, params=None, fetchone=False, fetchall=False):
    params = params or ()
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute(sql, params)
        if fetchone:
            row = cur.fetchone()
            conn.commit()
            return row
        if fetchall:
            rows = cur.fetchall()
            conn.commit()
            return rows
    conn.commit()
    return None


def read_df(sql: str, params=None) -> pd.DataFrame:
    params = params or ()
    conn = get_conn()
    return pd.read_sql_query(sql, conn, params=params)


def init_db():
    # Safe to run every time
    execute(
        """
        create table if not exists products (
          barcode text primary key,
          description text not null,
          pack_size int,
          split_mode text not null check (split_mode in ('AUTO','MANUAL','NONE')),
          auto_singles_per_box int not null default 0,
          auto_sixpk_per_box int not null default 0,
          created_at timestamptz not null default now()
        );
        """
    )
    execute(
        """
        create table if not exists stock (
          barcode text primary key references products(barcode) on delete cascade,
          closed_boxes int not null default 0,
          singles int not null default 0,
          sixpk int not null default 0,
          updated_at timestamptz not null default now()
        );
        """
    )
    execute(
        """
        create table if not exists open_log (
          id bigserial primary key,
          log_date date not null,
          barcode text not null references products(barcode) on delete cascade,
          boxes_opened int not null check (boxes_opened >= 0),
          singles_made int not null default 0,
          sixpk_made int not null default 0,
          note text,
          created_at timestamptz not null default now()
        );
        """
    )
    execute("create index if not exists idx_open_log_date on open_log(log_date);")
    execute("create index if not exists idx_open_log_barcode on open_log(barcode);")


def ensure_stock_row(barcode: str):
    execute(
        """
        insert into stock (barcode, closed_boxes, singles, sixpk)
        values (%s, 0, 0, 0)
        on conflict (barcode) do nothing;
        """,
        (barcode,),
    )


def upsert_product(barcode: str, description: str, pack_size, split_mode: str, auto_singles: int, auto_sixpk: int):
    execute(
        """
        insert into products (barcode, description, pack_size, split_mode, auto_singles_per_box, auto_sixpk_per_box)
        values (%s, %s, %s, %s, %s, %s)
        on conflict (barcode) do update set
          description = excluded.description,
          pack_size = excluded.pack_size,
          split_mode = excluded.split_mode,
          auto_singles_per_box = excluded.auto_singles_per_box,
          auto_sixpk_per_box = excluded.auto_sixpk_per_box;
        """,
        (barcode, description, pack_size, split_mode, auto_singles, auto_sixpk),
    )
    ensure_stock_row(barcode)


def get_product(barcode: str) -> dict:
    row = execute("select * from products where barcode=%s;", (barcode,), fetchone=True)
    if not row:
        raise ValueError("Product not found. Add it in Add / Edit Products.")
    return dict(row)


def get_stock(barcode: str) -> dict:
    ensure_stock_row(barcode)
    row = execute("select * from stock where barcode=%s;", (barcode,), fetchone=True)
    return dict(row)


def set_stock(barcode: str, closed_boxes: int, singles: int, sixpk: int):
    execute(
        """
        insert into stock (barcode, closed_boxes, singles, sixpk, updated_at)
        values (%s, %s, %s, %s, now())
        on conflict (barcode) do update set
          closed_boxes = excluded.closed_boxes,
          singles = excluded.singles,
          sixpk = excluded.sixpk,
          updated_at = now();
        """,
        (barcode, closed_boxes, singles, sixpk),
    )


def apply_opening(log_date: date, barcode: str, boxes_opened: int, singles_made: int, sixpk_made: int, note: str = ""):
    prod = get_product(barcode)
    stk = get_stock(barcode)

    if boxes_opened < 0:
        raise ValueError("Boxes opened cannot be negative.")

    split_mode = prod["split_mode"]

    if split_mode == "AUTO":
        derived_singles = boxes_opened * int(prod.get("auto_singles_per_box") or 0)
        derived_sixpk = boxes_opened * int(prod.get("auto_sixpk_per_box") or 0)
        singles_to_store = 0
        sixpk_to_store = 0
    elif split_mode == "MANUAL":
        derived_singles = int(singles_made or 0)
        derived_sixpk = int(sixpk_made or 0)
        singles_to_store = derived_singles
        sixpk_to_store = derived_sixpk
    else:
        derived_singles = 0
        derived_sixpk = 0
        singles_to_store = 0
        sixpk_to_store = 0

    new_closed = int(stk["closed_boxes"]) - int(boxes_opened)
    new_singles = int(stk["singles"]) + int(derived_singles)
    new_sixpk = int(stk["sixpk"]) + int(derived_sixpk)

    execute(
        """
        insert into open_log (log_date, barcode, boxes_opened, singles_made, sixpk_made, note)
        values (%s, %s, %s, %s, %s, %s);
        """,
        (log_date, barcode, boxes_opened, singles_to_store, sixpk_to_store, note or ""),
    )

    set_stock(barcode, new_closed, new_singles, new_sixpk)

    return {
        "description": prod["description"],
        "split_mode": split_mode,
        "derived_singles": derived_singles,
        "derived_sixpk": derived_sixpk,
        "new_closed_boxes": new_closed,
        "new_singles": new_singles,
        "new_sixpk": new_sixpk,
    }


def undo_last_entry():
    last = execute("select * from open_log order by id desc limit 1;", fetchone=True)
    if not last:
        return False, None

    row = dict(last)
    barcode = row["barcode"]

    prod = get_product(barcode)
    stk = get_stock(barcode)

    boxes_opened = int(row["boxes_opened"])
    singles_made = int(row["singles_made"])
    sixpk_made = int(row["sixpk_made"])

    if prod["split_mode"] == "AUTO":
        derived_singles = boxes_opened * int(prod.get("auto_singles_per_box") or 0)
        derived_sixpk = boxes_opened * int(prod.get("auto_sixpk_per_box") or 0)
    elif prod["split_mode"] == "MANUAL":
        derived_singles = singles_made
        derived_sixpk = sixpk_made
    else:
        derived_singles = 0
        derived_sixpk = 0

    new_closed = int(stk["closed_boxes"]) + boxes_opened
    new_singles = int(stk["singles"]) - derived_singles
    new_sixpk = int(stk["sixpk"]) - derived_sixpk

    execute("delete from open_log where id=%s;", (row["id"],))
    set_stock(barcode, new_closed, new_singles, new_sixpk)

    return True, {
        "barcode": barcode,
        "description": prod["description"],
        "new_closed_boxes": new_closed,
        "new_singles": new_singles,
        "new_sixpk": new_sixpk,
    }


# -----------------------------
# UI
# -----------------------------
st.title("📦 Pack Split Tracker (Supabase/Postgres)")

try:
    init_db()
except Exception as e:
    st.error(f"Database init/connect failed: {e}")
    st.stop()

tab1, tab2, tab3 = st.tabs(["✅ Daily Entry", "📊 Dashboard", "➕ Add / Edit Products"])


with tab3:
    st.subheader("Add / Edit a product")

    cA, cB, cC = st.columns([1.3, 2.2, 1.2])
    with cA:
        barcode_in = st.text_input("Barcode (unique)", "").strip()
    with cB:
        desc_in = st.text_input("Description", "").strip()
    with cC:
        pack_size_in = st.number_input("Pack size (optional)", min_value=0, value=0, step=1)

    split_mode_in = st.selectbox("Split mode", ["MANUAL", "AUTO", "NONE"], index=0)

    auto_singles_in = 0
    auto_sixpk_in = 0
    if split_mode_in == "AUTO":
        st.info("AUTO = app calculates singles/6pk every time you open a box.")
        c1, c2 = st.columns(2)
        with c1:
            auto_singles_in = st.number_input("Auto singles per opened box", min_value=0, value=40, step=1)
        with c2:
            auto_sixpk_in = st.number_input("Auto 6pk per opened box", min_value=0, value=0, step=1)

    if st.button("Save Product", type="primary"):
        if not barcode_in or not desc_in:
            st.error("Barcode and Description are required.")
        else:
            upsert_product(
                barcode=barcode_in,
                description=desc_in,
                pack_size=int(pack_size_in) if pack_size_in else None,
                split_mode=split_mode_in,
                auto_singles=int(auto_singles_in),
                auto_sixpk=int(auto_sixpk_in),
            )
            st.success("Saved product.")

    st.divider()
    st.subheader("Set / correct current stock snapshot")

    products_df = read_df("select barcode, description from products order by description;")
    if products_df.empty:
        st.info("No products yet. Add products above.")
    else:
        picked_barcode = st.selectbox(
            "Pick product",
            products_df["barcode"].tolist(),
            format_func=lambda b: f"{products_df.loc[products_df['barcode']==b,'description'].iloc[0]} ({b})",
        )

        cur = get_stock(picked_barcode)
        s1, s2, s3 = st.columns(3)
        with s1:
            closed_edit = st.number_input("Unopened (closed) boxes", value=int(cur["closed_boxes"]), step=1)
        with s2:
            singles_edit = st.number_input("Singles", value=int(cur["singles"]), step=1)
        with s3:
            sixpk_edit = st.number_input("6-packs", value=int(cur["sixpk"]), step=1)

        if st.button("Update Stock Snapshot"):
            set_stock(picked_barcode, int(closed_edit), int(singles_edit), int(sixpk_edit))
            st.success("Stock updated.")


with tab1:
    st.subheader("Log daily openings")

    products = read_df(
        "select barcode, description, split_mode, pack_size from products order by description;"
    )
    if products.empty:
        st.info("Add products first in ‘Add / Edit Products’.")
    else:
        col1, col2, col3 = st.columns([1.2, 2.6, 1.2])
        with col1:
            log_date = st.date_input("Date", value=date.today())
        with col2:
            barcode = st.selectbox(
                "Product",
                products["barcode"].tolist(),
                format_func=lambda b: f"{products.loc[products['barcode']==b,'description'].iloc[0]} ({b})",
            )
        with col3:
            boxes_opened = st.number_input("Boxes opened", min_value=0, value=0, step=1)

        picked = products[products["barcode"] == barcode].iloc[0]
        split_mode = picked["split_mode"]
        pack_size = int(picked["pack_size"]) if not pd.isna(picked["pack_size"]) else 0

        cur = get_stock(barcode)
        st.caption("Current stock snapshot (before saving):")
        m1, m2, m3 = st.columns(3)
        m1.metric("Unopened boxes", int(cur["closed_boxes"]))
        m2.metric("Singles", int(cur["singles"]))
        m3.metric("6-packs", int(cur["sixpk"]))

        singles_made = 0
        sixpk_made = 0
        if split_mode == "MANUAL":
            st.info("Manual mode: enter singles and 6-packs made for this entry.")
            c1, c2 = st.columns(2)
            with c1:
                singles_made = st.number_input("Singles made (manual)", min_value=0, value=0, step=1)
            with c2:
                sixpk_made = st.number_input("6-packs made (manual)", min_value=0, value=0, step=1)
        elif split_mode == "AUTO":
            st.success("Auto mode: singles/6pk are calculated automatically.")
        else:
            st.warning("No-split mode: only unopened boxes will be reduced.")

        note = st.text_input("Note (optional)", "")

        validate = st.checkbox("Validate manual split (requires pack size)", value=False)
        if validate and split_mode == "MANUAL":
            if pack_size <= 0:
                st.error("Pack size missing for this product. Add it in Add/Edit Products.")
            else:
                max_units = pack_size * int(boxes_opened)
                used_units = int(singles_made) + int(sixpk_made) * 6
                if used_units > max_units:
                    st.error(f"You entered {used_units} units but {boxes_opened} box(es) max is {max_units}.")
                else:
                    st.caption(f"Manual units used: {used_units} / {max_units}")

        if st.button("Save Daily Entry", type="primary"):
            res = apply_opening(log_date, barcode, int(boxes_opened), int(singles_made), int(sixpk_made), note)
            st.success(
                f"Saved ✅ Unopened boxes still in stock for **{res['description']}**: **{res['new_closed_boxes']}**"
            )
            a1, a2, a3 = st.columns(3)
            a1.metric("Unopened boxes (after save)", int(res["new_closed_boxes"]))
            a2.metric("Singles (after save)", int(res["new_singles"]))
            a3.metric("6-packs (after save)", int(res["new_sixpk"]))
            st.caption(
                f"Added from this entry → Singles: {int(res['derived_singles'])}, "
                f"6-packs: {int(res['derived_sixpk'])} (mode: {res['split_mode']})"
            )

        if st.button("Undo last entry"):
            ok, meta = undo_last_entry()
            if ok:
                st.success(f"Undid last entry ✅ {meta['description']} unopened boxes now: {meta['new_closed_boxes']}")
            else:
                st.info("No entries to undo.")

        st.divider()
        st.subheader("Entries for selected date")
        day_df = read_df(
            """
            select l.id, l.log_date, p.description, l.barcode, l.boxes_opened, l.singles_made, l.sixpk_made, l.note, l.created_at
            from open_log l
            join products p on p.barcode = l.barcode
            where l.log_date = %s
            order by l.id desc;
            """,
            (log_date,),
        )
        st.dataframe(day_df, use_container_width=True)


with tab2:
    st.subheader("Current stock position (live)")

    pos = read_df(
        """
        select
          p.description,
          p.barcode,
          p.split_mode,
          coalesce(p.pack_size, 0) as pack_size,
          coalesce(s.closed_boxes, 0) as unopened_boxes,
          coalesce(s.singles, 0) as singles,
          coalesce(s.sixpk, 0) as sixpk,
          (coalesce(s.singles, 0) + coalesce(s.sixpk, 0)*6 + coalesce(s.closed_boxes, 0)*coalesce(p.pack_size, 0)) as total_units_equiv,
          coalesce(s.updated_at, now()) as updated_at
        from products p
        left join stock s on s.barcode = p.barcode
        order by p.description;
        """
    )

    if pos.empty:
        st.info("No data yet.")
    else:
        k1, k2, k3, k4 = st.columns(4)
        k1.metric("Total unopened boxes", int(pos["unopened_boxes"].sum()))
        k2.metric("Total singles", int(pos["singles"].sum()))
        k3.metric("Total 6-packs", int(pos["sixpk"].sum()))
        k4.metric("Total units (equiv)", int(pos["total_units_equiv"].sum()))

        st.dataframe(pos, use_container_width=True)

        st.divider()
        st.subheader("Low stock alerts (unopened boxes)")
        threshold = st.number_input("Low threshold (unopened boxes)", min_value=0, value=2, step=1)
        low = pos[pos["unopened_boxes"] <= threshold][["description", "barcode", "unopened_boxes", "split_mode"]]
        if low.empty:
            st.success("No low-stock items at this threshold.")
        else:
            st.warning("Items at/below threshold:")
            st.dataframe(low, use_container_width=True)

        st.divider()
        st.subheader("Export")
        c1, c2 = st.columns(2)
        with c1:
            csv_pos = pos.to_csv(index=False).encode("utf-8")
            st.download_button("Download stock_position.csv", data=csv_pos, file_name="stock_position.csv", mime="text/csv")
        with c2:
            logs = read_df(
                """
                select l.id, l.log_date, p.description, l.barcode, l.boxes_opened, l.singles_made, l.sixpk_made, l.note, l.created_at
                from open_log l
                join products p on p.barcode = l.barcode
                order by l.id desc;
                """
            )
            csv_logs = logs.to_csv(index=False).encode("utf-8")
            st.download_button("Download open_log.csv", data=csv_logs, file_name="open_log.csv", mime="text/csv")
