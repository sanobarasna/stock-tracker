import sqlite3
from pathlib import Path
from datetime import date
import pandas as pd
import streamlit as st

DB_PATH = Path("pack_tracker.db")

# -----------------------------
# DB helpers
# -----------------------------
def get_conn():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn

def init_db(conn: sqlite3.Connection):
    conn.execute("""
    CREATE TABLE IF NOT EXISTS products (
        barcode TEXT PRIMARY KEY,
        description TEXT NOT NULL,
        pack_size INTEGER,
        split_mode TEXT NOT NULL CHECK (split_mode IN ('AUTO','MANUAL','NONE')),
        auto_singles_per_box INTEGER DEFAULT 0,
        auto_sixpk_per_box INTEGER DEFAULT 0,
        created_at TEXT DEFAULT (datetime('now'))
    );
    """)

    conn.execute("""
    CREATE TABLE IF NOT EXISTS stock (
        barcode TEXT PRIMARY KEY,
        closed_boxes INTEGER NOT NULL DEFAULT 0,
        singles INTEGER NOT NULL DEFAULT 0,
        sixpk INTEGER NOT NULL DEFAULT 0,
        updated_at TEXT DEFAULT (datetime('now')),
        FOREIGN KEY(barcode) REFERENCES products(barcode) ON DELETE CASCADE
    );
    """)

    conn.execute("""
    CREATE TABLE IF NOT EXISTS open_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        log_date TEXT NOT NULL,
        barcode TEXT NOT NULL,
        boxes_opened INTEGER NOT NULL,
        singles_made INTEGER DEFAULT 0,
        sixpk_made INTEGER DEFAULT 0,
        note TEXT,
        created_at TEXT DEFAULT (datetime('now')),
        FOREIGN KEY(barcode) REFERENCES products(barcode) ON DELETE CASCADE
    );
    """)
    conn.commit()

def read_df(conn, query, params=None):
    return pd.read_sql_query(query, conn, params=params or ())

def ensure_stock_row(conn, barcode: str):
    conn.execute("""
    INSERT INTO stock (barcode, closed_boxes, singles, sixpk)
    VALUES (?, 0, 0, 0)
    ON CONFLICT(barcode) DO NOTHING;
    """, (barcode,))
    conn.commit()

def upsert_product(conn, barcode, description, pack_size, split_mode, auto_singles, auto_sixpk):
    conn.execute("""
    INSERT INTO products (barcode, description, pack_size, split_mode, auto_singles_per_box, auto_sixpk_per_box)
    VALUES (?, ?, ?, ?, ?, ?)
    ON CONFLICT(barcode) DO UPDATE SET
        description=excluded.description,
        pack_size=excluded.pack_size,
        split_mode=excluded.split_mode,
        auto_singles_per_box=excluded.auto_singles_per_box,
        auto_sixpk_per_box=excluded.auto_sixpk_per_box;
    """, (barcode, description, pack_size, split_mode, auto_singles, auto_sixpk))
    ensure_stock_row(conn, barcode)
    conn.commit()

def set_stock(conn, barcode, closed_boxes, singles, sixpk):
    conn.execute("""
    INSERT INTO stock (barcode, closed_boxes, singles, sixpk)
    VALUES (?, ?, ?, ?)
    ON CONFLICT(barcode) DO UPDATE SET
        closed_boxes=excluded.closed_boxes,
        singles=excluded.singles,
        sixpk=excluded.sixpk,
        updated_at=datetime('now');
    """, (barcode, closed_boxes, singles, sixpk))
    conn.commit()

def get_product(conn, barcode: str) -> dict:
    df = read_df(conn, "SELECT * FROM products WHERE barcode=?", (barcode,))
    if df.empty:
        raise ValueError("Product not found in master.")
    return df.iloc[0].to_dict()

def get_stock(conn, barcode: str) -> dict:
    ensure_stock_row(conn, barcode)
    df = read_df(conn, "SELECT * FROM stock WHERE barcode=?", (barcode,))
    return df.iloc[0].to_dict()

def apply_opening(conn, log_date, barcode, boxes_opened, singles_made, sixpk_made, note=""):
    prod = get_product(conn, barcode)
    stk = get_stock(conn, barcode)

    if boxes_opened < 0:
        raise ValueError("Boxes opened cannot be negative.")

    # Calculate derived quantities based on split_mode
    if prod["split_mode"] == "AUTO":
        derived_singles = boxes_opened * int(prod["auto_singles_per_box"] or 0)
        derived_sixpk = boxes_opened * int(prod["auto_sixpk_per_box"] or 0)

        # ignore user-entered singles/sixpk for AUTO
        singles_made_to_store = 0
        sixpk_made_to_store = 0

    elif prod["split_mode"] == "MANUAL":
        derived_singles = int(singles_made or 0)
        derived_sixpk = int(sixpk_made or 0)

        singles_made_to_store = int(singles_made or 0)
        sixpk_made_to_store = int(sixpk_made or 0)

    else:  # NONE
        derived_singles = 0
        derived_sixpk = 0
        singles_made_to_store = 0
        sixpk_made_to_store = 0

    # Update stock
    new_closed = int(stk["closed_boxes"]) - int(boxes_opened)
    new_singles = int(stk["singles"]) + int(derived_singles)
    new_sixpk = int(stk["sixpk"]) + int(derived_sixpk)

    # Save log
    conn.execute("""
    INSERT INTO open_log (log_date, barcode, boxes_opened, singles_made, sixpk_made, note)
    VALUES (?, ?, ?, ?, ?, ?)
    """, (str(log_date), barcode, int(boxes_opened), singles_made_to_store, sixpk_made_to_store, note or ""))

    # Save stock snapshot
    set_stock(conn, barcode, new_closed, new_singles, new_sixpk)
    conn.commit()

    return {
        "new_closed_boxes": new_closed,
        "new_singles": new_singles,
        "new_sixpk": new_sixpk,
        "derived_singles": derived_singles,
        "derived_sixpk": derived_sixpk,
        "split_mode": prod["split_mode"],
        "description": prod["description"],
    }

def undo_last_entry(conn):
    last = read_df(conn, "SELECT * FROM open_log ORDER BY id DESC LIMIT 1")
    if last.empty:
        return False, None

    row = last.iloc[0].to_dict()
    barcode = row["barcode"]

    prod = get_product(conn, barcode)
    stk = get_stock(conn, barcode)

    boxes_opened = int(row["boxes_opened"])
    singles_made = int(row["singles_made"] or 0)
    sixpk_made = int(row["sixpk_made"] or 0)

    if prod["split_mode"] == "AUTO":
        derived_singles = boxes_opened * int(prod["auto_singles_per_box"] or 0)
        derived_sixpk = boxes_opened * int(prod["auto_sixpk_per_box"] or 0)
    elif prod["split_mode"] == "MANUAL":
        derived_singles = singles_made
        derived_sixpk = sixpk_made
    else:
        derived_singles = 0
        derived_sixpk = 0

    new_closed = int(stk["closed_boxes"]) + boxes_opened
    new_singles = int(stk["singles"]) - derived_singles
    new_sixpk = int(stk["sixpk"]) - derived_sixpk

    conn.execute("DELETE FROM open_log WHERE id=?", (row["id"],))
    set_stock(conn, barcode, new_closed, new_singles, new_sixpk)
    conn.commit()

    return True, {"barcode": barcode, "new_closed_boxes": new_closed}

# -----------------------------
# UI
# -----------------------------
st.set_page_config(page_title="Pack Split Tracker", layout="wide")

conn = get_conn()
init_db(conn)

st.title("📦 Pack Split Tracker (Daily Boxes → Singles & 6-Packs)")

tab1, tab2, tab3 = st.tabs(["✅ Daily Entry", "📊 Dashboard", "➕ Add / Edit Products"])

# -----------------------------
# Tab 3: Add/Edit Products
# -----------------------------
with tab3:
    st.subheader("Add / Edit a product")

    colA, colB, colC = st.columns([1.2, 2, 1.2])
    with colA:
        barcode = st.text_input("Barcode (unique)", "")
    with colB:
        description = st.text_input("Description", "")
    with colC:
        pack_size = st.number_input("Pack size (optional)", min_value=0, value=0, step=1)

    split_mode = st.selectbox("Split mode", ["MANUAL", "AUTO", "NONE"], index=0)

    auto_singles = 0
    auto_sixpk = 0
    if split_mode == "AUTO":
        st.info("AUTO means the app calculates singles/6pk every time you open a box.")
        c1, c2 = st.columns(2)
        with c1:
            auto_singles = st.number_input("Auto singles per opened box", min_value=0, value=40, step=1)
        with c2:
            auto_sixpk = st.number_input("Auto 6pk per opened box", min_value=0, value=0, step=1)

    if st.button("Save Product", type="primary"):
        if not barcode.strip() or not description.strip():
            st.error("Barcode and Description are required.")
        else:
            upsert_product(
                conn,
                barcode.strip(),
                description.strip(),
                int(pack_size) if pack_size else None,
                split_mode,
                int(auto_singles),
                int(auto_sixpk)
            )
            st.success("Saved product (and ensured stock row exists).")

    st.divider()
    st.subheader("Set / correct current stock (optional but recommended)")
    products = read_df(conn, "SELECT barcode, description FROM products ORDER BY description")
    if products.empty:
        st.warning("No products yet. Add products first.")
    else:
        pick = st.selectbox(
            "Pick product",
            products["barcode"].tolist(),
            format_func=lambda b: f"{products.loc[products['barcode']==b,'description'].iloc[0]} ({b})"
        )

        cur = get_stock(conn, pick)
        c1, c2, c3 = st.columns(3)
        with c1:
            closed = st.number_input("Unopened (closed) boxes", value=int(cur["closed_boxes"]), step=1)
        with c2:
            singles = st.number_input("Singles", value=int(cur["singles"]), step=1)
        with c3:
            sixpk = st.number_input("6-packs", value=int(cur["sixpk"]), step=1)

        if st.button("Update Stock Snapshot"):
            set_stock(conn, pick, int(closed), int(singles), int(sixpk))
            st.success("Stock updated.")

# -----------------------------
# Tab 1: Daily Entry
# -----------------------------
with tab1:
    st.subheader("Log today’s openings")

    products = read_df(conn, """
        SELECT p.barcode, p.description, p.split_mode, p.pack_size
        FROM products p
        ORDER BY p.description
    """)

    if products.empty:
        st.warning("Add products first in the ‘Add / Edit Products’ tab.")
    else:
        col1, col2, col3 = st.columns([1.2, 2.5, 1.2])
        with col1:
            log_date = st.date_input("Date", value=date.today())
        with col2:
            barcode = st.selectbox(
                "Product",
                products["barcode"].tolist(),
                format_func=lambda b: f"{products.loc[products['barcode']==b,'description'].iloc[0]} ({b})"
            )
        with col3:
            boxes_opened = st.number_input("Boxes opened", min_value=0, value=0, step=1)

        picked = products[products["barcode"] == barcode].iloc[0]
        split_mode = picked["split_mode"]
        pack_size = picked["pack_size"]

        # LIVE stock snapshot (before saving)
        cur = get_stock(conn, barcode)
        st.caption("Current stock snapshot (before saving):")
        s1, s2, s3 = st.columns(3)
        s1.metric("Unopened boxes (current)", int(cur["closed_boxes"]))
        s2.metric("Singles (current)", int(cur["singles"]))
        s3.metric("6-packs (current)", int(cur["sixpk"]))

        singles_made = 0
        sixpk_made = 0

        if split_mode == "MANUAL":
            st.info("Manual mode: enter singles/6pk made (for sodas or items you want to split differently).")
            c1, c2 = st.columns(2)
            with c1:
                singles_made = st.number_input("Singles made (manual)", min_value=0, value=0, step=1)
            with c2:
                sixpk_made = st.number_input("6-packs made (manual)", min_value=0, value=0, step=1)
        elif split_mode == "AUTO":
            st.success("Auto mode: the app will calculate singles/6pk for you.")
        else:
            st.warning("No-split mode: only unopened boxes will be reduced.")

        note = st.text_input("Note (optional)", "")

        # Optional validation for MANUAL (helps catch mistakes)
        validate = st.checkbox("Validate manual split (requires pack size)", value=False)
        if validate and split_mode == "MANUAL":
            if not pack_size or int(pack_size) == 0:
                st.error("Pack size is missing for this product. Add it in Add/Edit Products.")
            else:
                max_units = int(pack_size) * int(boxes_opened)
                used_units = int(singles_made) + int(sixpk_made) * 6
                if used_units > max_units:
                    st.error(f"You entered {used_units} units, but {boxes_opened} box(es) max is {max_units}.")
                else:
                    st.caption(f"Manual units used: {used_units} / {max_units}")

        # Save and immediately show unopened boxes still in stock
        if st.button("Save Daily Entry", type="primary"):
            try:
                result = apply_opening(
                    conn,
                    log_date,
                    barcode,
                    int(boxes_opened),
                    int(singles_made),
                    int(sixpk_made),
                    note
                )

                st.success(
                    f"Saved! ✅ Unopened boxes still in stock for "
                    f"**{result['description']}**: **{result['new_closed_boxes']}**"
                )

                # Show after-save snapshot (what changed)
                a1, a2, a3 = st.columns(3)
                a1.metric("Unopened boxes (after save)", int(result["new_closed_boxes"]))
                a2.metric("Singles (after save)", int(result["new_singles"]))
                a3.metric("6-packs (after save)", int(result["new_sixpk"]))

                # Also show what was added (useful confirmation)
                st.caption(
                    f"Added from this entry → Singles: {int(result['derived_singles'])}, "
                    f"6-packs: {int(result['derived_sixpk'])} (mode: {result['split_mode']})"
                )

            except Exception as e:
                st.error(str(e))

        cA, cB = st.columns([1, 1])
        with cA:
            if st.button("Undo last entry"):
                ok, meta = undo_last_entry(conn)
                if ok:
                    st.success(f"Undid last entry. Unopened boxes now: {meta['new_closed_boxes']}")
                else:
                    st.info("No entries to undo.")

        st.divider()
        st.subheader("Entries for selected date")
        day_df = read_df(conn, """
            SELECT l.id, l.log_date, p.description, l.barcode, l.boxes_opened, l.singles_made, l.sixpk_made, l.note
            FROM open_log l
            JOIN products p ON p.barcode = l.barcode
            WHERE l.log_date = ?
            ORDER BY l.id DESC
        """, (str(log_date),))
        st.dataframe(day_df, use_container_width=True)

# -----------------------------
# Tab 2: Dashboard
# -----------------------------
with tab2:
    st.subheader("Current stock position (live)")

    pos = read_df(conn, """
        SELECT
            p.description,
            p.barcode,
            p.split_mode,
            COALESCE(p.pack_size, 0) AS pack_size,
            COALESCE(s.closed_boxes, 0) AS unopened_boxes,
            COALESCE(s.singles, 0) AS singles,
            COALESCE(s.sixpk, 0) AS sixpk,
            (COALESCE(s.singles, 0) + COALESCE(s.sixpk, 0)*6 + COALESCE(s.closed_boxes, 0)*COALESCE(p.pack_size, 0)) AS total_units_equiv
        FROM products p
        LEFT JOIN stock s ON s.barcode = p.barcode
        ORDER BY p.description
    """)

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
        export = pos.copy()
        csv = export.to_csv(index=False).encode("utf-8")
        st.download_button("Download stock_position.csv", data=csv, file_name="stock_position.csv", mime="text/csv")
