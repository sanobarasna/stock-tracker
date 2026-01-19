import os
from datetime import date

import pandas as pd
import streamlit as st
import psycopg2
from psycopg2.extras import RealDictCursor

st.set_page_config(page_title="Pack Split Tracker (Supabase)", layout="wide")


# -----------------------------
# DB helpers (Pooler-friendly)
# -----------------------------
def _get_database_url() -> str:
    if "DATABASE_URL" in st.secrets:
        return st.secrets["DATABASE_URL"]
    return os.getenv("DATABASE_URL", "")


def _redact_db_url(url: str) -> str:
    # show host/port/db, hide password
    try:
        # postgresql://user:pass@host:port/db
        left, right = url.split("://", 1)
        creds, rest = right.split("@", 1)
        user = creds.split(":", 1)[0]
        return f"{left}://{user}:***@{rest}"
    except Exception:
        return "Could not parse DATABASE_URL (but it is set)."


def _connect():
    db_url = _get_database_url()
    if not db_url:
        raise RuntimeError("DATABASE_URL is not set in Streamlit Secrets.")

    # Force search_path to public so we always read/write the same schema
    return psycopg2.connect(
        db_url,
        sslmode="require",
        connect_timeout=10,
        cursor_factory=RealDictCursor,
        options="-c search_path=public",
    )


def execute(sql: str, params=None, fetchone=False, fetchall=False):
    params = params or ()
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            if fetchone:
                return cur.fetchone()
            if fetchall:
                return cur.fetchall()
        conn.commit()
    return None


def read_df(sql: str, params=None) -> pd.DataFrame:
    params = params or ()
    with _connect() as conn:
        return pd.read_sql_query(sql, conn, params=params)


def init_db():
    execute(
        """
        create table if not exists public.products (
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
        create table if not exists public.stock (
          barcode text primary key references public.products(barcode) on delete cascade,
          closed_boxes int not null default 0,
          singles int not null default 0,
          sixpk int not null default 0,
          updated_at timestamptz not null default now()
        );
        """
    )

    execute(
        """
        create table if not exists public.open_log (
          id bigserial primary key,
          log_date date not null,
          barcode text not null references public.products(barcode) on delete cascade,
          boxes_opened int not null check (boxes_opened >= 0),
          singles_made int not null default 0,
          sixpk_made int not null default 0,
          note text,
          created_at timestamptz not null default now()
        );
        """
    )

    execute("create index if not exists idx_open_log_date on public.open_log(log_date);")
    execute("create index if not exists idx_open_log_barcode on public.open_log(barcode);")


# -----------------------------
# Cleanup + repair (the fix)
# -----------------------------
def cleanup_bad_rows_and_repair_stock():
    # Deletes header/blank rows + rebuild missing stock rows for real products
    execute(
        """
        delete from public.open_log
        where barcode is null or trim(barcode) = '' or lower(trim(barcode)) = 'barcode';
        """
    )
    execute(
        """
        delete from public.stock
        where barcode is null or trim(barcode) = '' or lower(trim(barcode)) = 'barcode';
        """
    )
    execute(
        """
        delete from public.products
        where barcode is null or description is null
           or trim(barcode) = '' or trim(description) = ''
           or lower(trim(barcode)) = 'barcode'
           or lower(trim(description)) = 'description';
        """
    )
    execute(
        """
        update public.products
        set barcode = trim(barcode),
            description = trim(description);
        """
    )
    execute(
        """
        insert into public.stock (barcode, closed_boxes, singles, sixpk)
        select p.barcode, 0, 0, 0
        from public.products p
        left join public.stock s on s.barcode = p.barcode
        where s.barcode is null;
        """
    )


def ensure_stock_row(barcode: str):
    execute(
        """
        insert into public.stock (barcode, closed_boxes, singles, sixpk)
        select p.barcode, 0, 0, 0
        from public.products p
        where p.barcode = %s
        on conflict (barcode) do nothing;
        """,
        (barcode,),
    )


def product_exists(barcode: str) -> bool:
    row = execute("select 1 as ok from public.products where barcode=%s;", (barcode,), fetchone=True)
    return bool(row)


def upsert_product(barcode: str, description: str, pack_size, split_mode: str, auto_singles: int, auto_sixpk: int):
    barcode = (barcode or "").strip()
    description = (description or "").strip()

    if not barcode or not description:
        raise ValueError("Barcode and Description are required.")
    if barcode.lower() == "barcode" or description.lower() == "description":
        raise ValueError("Invalid barcode/description (looks like a header row).")

    execute(
        """
        insert into public.products (barcode, description, pack_size, split_mode, auto_singles_per_box, auto_sixpk_per_box)
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


def get_stock(barcode: str) -> dict:
    if not product_exists(barcode):
        raise ValueError(f"Unknown product barcode: {barcode}")
    ensure_stock_row(barcode)
    row = execute("select * from public.stock where barcode=%s;", (barcode,), fetchone=True)
    return dict(row) if row else {"barcode": barcode, "closed_boxes": 0, "singles": 0, "sixpk": 0}


def set_stock(barcode: str, closed_boxes: int, singles: int, sixpk: int):
    if not product_exists(barcode):
        raise ValueError(f"Cannot set stock: product not found for barcode {barcode}")

    execute(
        """
        insert into public.stock (barcode, closed_boxes, singles, sixpk, updated_at)
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
    prod = execute("select * from public.products where barcode=%s;", (barcode,), fetchone=True)
    if not prod:
        raise ValueError("Product not found.")
    prod = dict(prod)

    stk = get_stock(barcode)

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
        derived_singles = derived_sixpk = singles_to_store = sixpk_to_store = 0

    new_closed = int(stk["closed_boxes"]) - int(boxes_opened)
    new_singles = int(stk["singles"]) + int(derived_singles)
    new_sixpk = int(stk["sixpk"]) + int(derived_sixpk)

    execute(
        """
        insert into public.open_log (log_date, barcode, boxes_opened, singles_made, sixpk_made, note)
        values (%s, %s, %s, %s, %s, %s);
        """,
        (log_date, barcode, boxes_opened, singles_to_store, sixpk_to_store, note or ""),
    )

    set_stock(barcode, new_closed, new_singles, new_sixpk)

    return prod["description"], new_closed, new_singles, new_sixpk, derived_singles, derived_sixpk


def _safe_int(x) -> int:
    try:
        if x is None or (isinstance(x, float) and pd.isna(x)):
            return 0
        return int(x)
    except Exception:
        return 0


# -----------------------------
# App start
# -----------------------------
st.title("📦 Pack Split Tracker (Supabase/Postgres)")

try:
    init_db()
except Exception as e:
    st.error(f"Database init/connect failed: {e}")
    st.stop()

tab1, tab2, tab3, tab4 = st.tabs(["✅ Daily Entry", "📊 Dashboard", "➕ Add / Edit Products", "🛠 Diagnostics / Fix"])


# -----------------------------
# Diagnostics tab (THIS will reveal the real issue)
# -----------------------------
with tab4:
    st.subheader("Diagnostics")
    db_url = _get_database_url()
    st.code(_redact_db_url(db_url), language="text")

    st.write("Row counts (what THIS app is connected to):")
    c1, c2, c3 = st.columns(3)
    with c1:
        st.metric("products", _safe_int(execute("select count(*) as c from public.products;", fetchone=True)["c"]))
    with c2:
        st.metric("stock", _safe_int(execute("select count(*) as c from public.stock;", fetchone=True)["c"]))
    with c3:
        st.metric("open_log", _safe_int(execute("select count(*) as c from public.open_log;", fetchone=True)["c"]))

    st.write("Sample products (first 20):")
    st.dataframe(read_df("select barcode, description, split_mode, pack_size from public.products order by description limit 20;"),
                 use_container_width=True)

    st.divider()
    st.subheader("One-click cleanup (removes header rows like 'barcode/description')")
    if st.button("RUN CLEANUP + REPAIR STOCK (safe)", type="primary"):
        cleanup_bad_rows_and_repair_stock()
        st.success("Cleanup done. Refresh the page now (R) or rerun the app.")

    st.caption(
        "If you click cleanup here and the dashboard still shows header rows, "
        "then the app is connected to a different DB than you think (wrong DATABASE_URL)."
    )


# -----------------------------
# Products tab
# -----------------------------
with tab3:
    st.subheader("Add / Edit a product")

    cA, cB, cC = st.columns([1.2, 2.2, 1.2])
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
        cc1, cc2 = st.columns(2)
        with cc1:
            auto_singles_in = st.number_input("Auto singles per opened box", min_value=0, value=40, step=1)
        with cc2:
            auto_sixpk_in = st.number_input("Auto 6pk per opened box", min_value=0, value=0, step=1)

    if st.button("Save Product", type="primary"):
        try:
            upsert_product(
                barcode_in,
                desc_in,
                int(pack_size_in) if pack_size_in else None,
                split_mode_in,
                int(auto_singles_in),
                int(auto_sixpk_in),
            )
            st.success("Saved.")
        except Exception as e:
            st.error(str(e))

    st.divider()
    st.subheader("Set / correct current stock snapshot")

    products_df = read_df("select barcode, description from public.products order by description;")
    # filter junk rows at UI level too
    products_df["barcode"] = products_df["barcode"].astype(str).str.strip()
    products_df["description"] = products_df["description"].astype(str).str.strip()
    products_df = products_df[
        (products_df["barcode"] != "")
        & (products_df["description"] != "")
        & (products_df["barcode"].str.lower() != "barcode")
        & (products_df["description"].str.lower() != "description")
    ]

    if products_df.empty:
        st.warning("No products found. Go to Diagnostics tab and run cleanup.")
    else:
        label_map = {r["barcode"]: f"{r['description']} ({r['barcode']})" for _, r in products_df.iterrows()}
        picked_barcode = st.selectbox("Pick product", products_df["barcode"].tolist(), format_func=lambda b: label_map[b])
        cur = get_stock(picked_barcode)

        s1, s2, s3 = st.columns(3)
        with s1:
            closed_edit = st.number_input("Unopened (closed) boxes", value=_safe_int(cur["closed_boxes"]), step=1)
        with s2:
            singles_edit = st.number_input("Singles", value=_safe_int(cur["singles"]), step=1)
        with s3:
            sixpk_edit = st.number_input("6-packs", value=_safe_int(cur["sixpk"]), step=1)

        if st.button("Update Stock Snapshot"):
            try:
                set_stock(picked_barcode, int(closed_edit), int(singles_edit), int(sixpk_edit))
                st.success("Stock updated.")
            except Exception as e:
                st.error(str(e))


# -----------------------------
# Daily entry tab
# -----------------------------
with tab1:
    st.subheader("Log daily openings")

    products = read_df("select barcode, description, split_mode, pack_size from public.products order by description;")
    products["barcode"] = products["barcode"].astype(str).str.strip()
    products["description"] = products["description"].astype(str).str.strip()
    products = products[
        (products["barcode"] != "")
        & (products["description"] != "")
        & (products["barcode"].str.lower() != "barcode")
        & (products["description"].str.lower() != "description")
    ]

    if products.empty:
        st.info("No valid products found. Go to Diagnostics tab → RUN CLEANUP, then refresh.")
    else:
        label_map = {r["barcode"]: f"{r['description']} ({r['barcode']})" for _, r in products.iterrows()}
        col1, col2, col3 = st.columns([1.2, 2.6, 1.2])
        with col1:
            log_date = st.date_input("Date", value=date.today())
        with col2:
            barcode = st.selectbox("Product", products["barcode"].tolist(), format_func=lambda b: label_map[b])
        with col3:
            boxes_opened = st.number_input("Boxes opened", min_value=0, value=0, step=1)

        picked = products[products["barcode"] == barcode].iloc[0]
        split_mode = picked["split_mode"]

        cur = get_stock(barcode)
        m1, m2, m3 = st.columns(3)
        m1.metric("Unopened boxes", _safe_int(cur["closed_boxes"]))
        m2.metric("Singles", _safe_int(cur["singles"]))
        m3.metric("6-packs", _safe_int(cur["sixpk"]))

        singles_made = 0
        sixpk_made = 0
        if split_mode == "MANUAL":
            c1, c2 = st.columns(2)
            with c1:
                singles_made = st.number_input("Singles made (manual)", min_value=0, value=0, step=1)
            with c2:
                sixpk_made = st.number_input("6-packs made (manual)", min_value=0, value=0, step=1)

        note = st.text_input("Note (optional)", "")

        if st.button("Save Daily Entry", type="primary"):
            try:
                desc, new_closed, new_singles, new_sixpk, add_s, add_6 = apply_opening(
                    log_date, barcode, int(boxes_opened), int(singles_made), int(sixpk_made), note
                )
                st.success(f"Saved ✅ {desc} unopened boxes now: {new_closed}")
            except Exception as e:
                st.error(str(e))


# -----------------------------
# Dashboard tab
# -----------------------------
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
        from public.products p
        left join public.stock s on s.barcode = p.barcode
        where lower(trim(p.barcode)) not in ('barcode','')
          and lower(trim(p.description)) not in ('description','')
        order by p.description;
        """
    )

    if pos.empty:
        st.info("No rows found. Go to Diagnostics tab and check counts.")
    else:
        k1, k2, k3, k4 = st.columns(4)
        k1.metric("Total unopened boxes", _safe_int(pos["unopened_boxes"].sum()))
        k2.metric("Total singles", _safe_int(pos["singles"].sum()))
        k3.metric("Total 6-packs", _safe_int(pos["sixpk"].sum()))
        k4.metric("Total units (equiv)", _safe_int(pos["total_units_equiv"].sum()))
        st.dataframe(pos, use_container_width=True)
