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


def _connect():
    db_url = _get_database_url()
    if not db_url:
        raise RuntimeError("DATABASE_URL is not set in Streamlit Secrets.")
    return psycopg2.connect(
        db_url,
        sslmode="require",
        connect_timeout=10,
        cursor_factory=RealDictCursor,
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
    return None


def read_df(sql: str, params=None) -> pd.DataFrame:
    params = params or ()
    with _connect() as conn:
        return pd.read_sql_query(sql, conn, params=params)


def init_db():
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


# -----------------------------
# Product dataframe cleaning (robust)
# -----------------------------
def _safe_str_series(s: pd.Series) -> pd.Series:
    # Handles ints, floats, None; keeps real values; strips whitespace
    return s.apply(lambda x: "" if x is None else str(x)).str.strip()


def _clean_products_df(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=df.columns if df is not None else [])

    df = df.copy()

    # Ensure columns exist
    if "barcode" not in df.columns or "description" not in df.columns:
        return df.iloc[0:0]

    df["barcode"] = _safe_str_series(df["barcode"])
    df["description"] = _safe_str_series(df["description"])

    # Remove header-like / blank junk
    df = df[
        (df["barcode"] != "")
        & (df["description"] != "")
        & (df["barcode"].str.lower() != "barcode")
        & (df["description"].str.lower() != "description")
    ]

    # Remove duplicates by barcode
    df = df.drop_duplicates(subset=["barcode"], keep="first")

    return df


def load_products_min() -> pd.DataFrame:
    df = read_df("select barcode, description, split_mode, pack_size from products order by description;")
    return _clean_products_df(df)


def load_products_for_picker() -> pd.DataFrame:
    df = read_df("select barcode, description from products order by description;")
    return _clean_products_df(df)


# -----------------------------
# Domain logic
# -----------------------------
def product_exists(barcode: str) -> bool:
    row = execute("select 1 as ok from products where barcode=%s;", (barcode,), fetchone=True)
    return bool(row)


def ensure_stock_row(barcode: str):
    execute(
        """
        insert into stock (barcode, closed_boxes, singles, sixpk)
        select p.barcode, 0, 0, 0
        from products p
        where p.barcode = %s
        on conflict (barcode) do nothing;
        """,
        (barcode,),
    )


def repair_all_stock_rows():
    # Creates missing stock rows for ALL products (one-click repair)
    execute(
        """
        insert into stock (barcode, closed_boxes, singles, sixpk)
        select p.barcode, 0, 0, 0
        from products p
        left join stock s on s.barcode = p.barcode
        where s.barcode is null;
        """
    )


def upsert_product(barcode: str, description: str, pack_size, split_mode: str, auto_singles: int, auto_sixpk: int):
    barcode = (barcode or "").strip()
    description = (description or "").strip()
    if not barcode or not description:
        raise ValueError("Barcode and Description are required.")

    if barcode.lower() == "barcode" or description.lower() == "description":
        raise ValueError("Invalid product (looks like a header row).")

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
        raise ValueError(f"Product not found for barcode: {barcode}")
    return dict(row)


def get_stock(barcode: str) -> dict:
    if not product_exists(barcode):
        raise ValueError(f"Stock requested for unknown product barcode: {barcode}")
    ensure_stock_row(barcode)
    row = execute("select * from stock where barcode=%s;", (barcode,), fetchone=True)
    return dict(row) if row else {"barcode": barcode, "closed_boxes": 0, "singles": 0, "sixpk": 0}


def set_stock(barcode: str, closed_boxes: int, singles: int, sixpk: int):
    if not product_exists(barcode):
        raise ValueError(f"Cannot set stock: product not found for barcode {barcode}")

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

    return True, {"description": prod["description"], "new_closed_boxes": new_closed}


def _safe_int(x) -> int:
    try:
        if x is None:
            return 0
        if pd.isna(x):
            return 0
        return int(x)
    except Exception:
        return 0


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
        c1, c2 = st.columns(2)
        with c1:
            auto_singles_in = st.number_input("Auto singles per opened box", min_value=0, value=40, step=1)
        with c2:
            auto_sixpk_in = st.number_input("Auto 6pk per opened box", min_value=0, value=0, step=1)

    if st.button("Save Product", type="primary"):
        try:
            upsert_product(
                barcode=barcode_in,
                description=desc_in,
                pack_size=int(pack_size_in) if pack_size_in else None,
                split_mode=split_mode_in,
                auto_singles=int(auto_singles_in),
                auto_sixpk=int(auto_sixpk_in),
            )
            st.success("Saved product.")
        except Exception as e:
            st.error(f"Save failed: {e}")

    st.divider()
    st.subheader("Repair / initialize stock rows")
    st.caption("If you migrated products but stock rows are missing, click this once.")
    if st.button("Repair missing stock rows"):
        try:
            repair_all_stock_rows()
            st.success("Repaired stock rows.")
        except Exception as e:
            st.error(f"Repair failed: {e}")

    st.divider()
    st.subheader("Set / correct current stock snapshot")

    products_df = load_products_for_picker()
    if products_df.empty:
        st.info("No valid products found. (Your products exist, so this means they were loaded in a type that got filtered earlier — this code fixes that.)")
    else:
        label_map = {r["barcode"]: f"{r['description']} ({r['barcode']})" for _, r in products_df.iterrows()}
        options = products_df["barcode"].tolist()

        picked_barcode = st.selectbox("Pick product", options, format_func=lambda b: label_map.get(b, b))
        cur = get_stock(picked_barcode)

        s1, s2, s3 = st.columns(3)
        with s1:
            closed_edit = st.number_input("Unopened (closed) boxes", value=_safe_int(cur.get("closed_boxes")), step=1)
        with s2:
            singles_edit = st.number_input("Singles", value=_safe_int(cur.get("singles")), step=1)
        with s3:
            sixpk_edit = st.number_input("6-packs", value=_safe_int(cur.get("sixpk")), step=1)

        if st.button("Update Stock Snapshot"):
            try:
                set_stock(picked_barcode, int(closed_edit), int(singles_edit), int(sixpk_edit))
                st.success("Stock updated.")
            except Exception as e:
                st.error(f"Update failed: {e}")


with tab1:
    st.subheader("Log daily openings")

    products = load_products_min()
    if products.empty:
        st.info("No valid products found. Click **Repair missing stock rows** in Add/Edit tab, then refresh.")
    else:
        label_map = {r["barcode"]: f"{r['description']} ({r['barcode']})" for _, r in products.iterrows()}
        options = products["barcode"].tolist()

        col1, col2, col3 = st.columns([1.2, 2.6, 1.2])
        with col1:
            log_date = st.date_input("Date", value=date.today())
        with col2:
            barcode = st.selectbox("Product", options, format_func=lambda b: label_map.get(b, b))
        with col3:
            boxes_opened = st.number_input("Boxes opened", min_value=0, value=0, step=1)

        picked = products[products["barcode"] == barcode].iloc[0]
        split_mode = picked["split_mode"]
        pack_size = _safe_int(picked["pack_size"])

        cur = get_stock(barcode)
        m1, m2, m3 = st.columns(3)
        m1.metric("Unopened boxes", _safe_int(cur.get("closed_boxes")))
        m2.metric("Singles", _safe_int(cur.get("singles")))
        m3.metric("6-packs", _safe_int(cur.get("sixpk")))

        singles_made = 0
        sixpk_made = 0
        if split_mode == "MANUAL":
            c1, c2 = st.columns(2)
            with c1:
                singles_made = st.number_input("Singles made (manual)", min_value=0, value=0, step=1)
            with c2:
                sixpk_made = st.number_input("6-packs made (manual)", min_value=0, value=0, step=1)

        note = st.text_input("Note (optional)", "")

        validate = st.checkbox("Validate manual split (requires pack size)", value=False)
        if validate and split_mode == "MANUAL" and pack_size > 0:
            max_units = pack_size * int(boxes_opened)
            used_units = int(singles_made) + int(sixpk_made) * 6
            if used_units > max_units:
                st.error(f"You entered {used_units} units but {boxes_opened} box(es) max is {max_units}.")

        if st.button("Save Daily Entry", type="primary"):
            try:
                res = apply_opening(log_date, barcode, int(boxes_opened), int(singles_made), int(sixpk_made), note)
                st.success(f"Saved ✅ Unopened boxes still in stock for **{res['description']}**: **{res['new_closed_boxes']}**")
            except Exception as e:
                st.error(f"Save failed: {e}")

        if st.button("Undo last entry"):
            ok, meta = undo_last_entry()
            if ok:
                st.success(f"Undid last entry ✅ {meta['description']} unopened boxes now: {meta['new_closed_boxes']}")
            else:
                st.info("No entries to undo.")

        st.divider()
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
        st.subheader("Entries for selected date")
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
        where lower(trim(p.barcode)) not in ('barcode','')
          and lower(trim(p.description)) not in ('description','')
        order by p.description;
        """
    )

    if pos.empty:
        st.info("No stock rows yet. Go to Add/Edit tab and click **Repair missing stock rows**.")
    else:
        unopened_total = _safe_int(pos["unopened_boxes"].sum())
        singles_total = _safe_int(pos["singles"].sum())
        sixpk_total = _safe_int(pos["sixpk"].sum())
        units_total = _safe_int(pos["total_units_equiv"].sum())

        k1, k2, k3, k4 = st.columns(4)
        k1.metric("Total unopened boxes", unopened_total)
        k2.metric("Total singles", singles_total)
        k3.metric("Total 6-packs", sixpk_total)
        k4.metric("Total units (equiv)", units_total)

        st.dataframe(pos, use_container_width=True)
