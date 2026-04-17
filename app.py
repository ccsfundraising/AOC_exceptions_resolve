
import os
import re
import sqlite3
from datetime import datetime
from pathlib import Path

import pandas as pd
import streamlit as st


# =========================================================
# PATHS
# =========================================================
BASE_DIR = Path(__file__).resolve().parent

# Data file (must be in repo)
DATA_FILE = BASE_DIR / "atleast_one_clma_portid_exceptions_ALL.xlsx"

# Writable directory (IMPORTANT for Posit)
OUTPUT_DIR = Path(os.environ.get("APP_DATA_DIR", "/tmp")) / "parish_portfolio_review"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Outputs
DB_FILE = OUTPUT_DIR / "decisions.db"
PARISH_MATCH_FILE = OUTPUT_DIR / "parish_portfolio_match_decisions.csv"
PAIR_RELATION_FILE = OUTPUT_DIR / "parish_pair_relationship_decisions.csv"


# =========================================================
# PAGE CONFIG
# =========================================================
st.set_page_config(
    page_title="Parish vs Portfolio Review",
    layout="wide",
    initial_sidebar_state="collapsed"
)


# =========================================================
# STYLES
# =========================================================
st.markdown("""
<style>
.block-container {
    padding-top: 1rem;
    padding-bottom: 2rem;
    max-width: 96%;
}
.review-toolbar {
    background: #ffffff;
    border: 1px solid #e5e7eb;
    padding: 12px 16px;
    border-radius: 14px;
    margin-bottom: 14px;
    box-shadow: 0 1px 3px rgba(0,0,0,0.06);
}
.pill {
    display: inline-block;
    border-radius: 999px;
    padding: 4px 10px;
    margin: 2px 6px 2px 0;
    font-size: 0.80rem;
    font-weight: 700;
    border: 1px solid transparent;
}
.pill-yes {
    background: #dcfce7;
    color: #166534;
    border-color: #86efac;
}
.pill-no {
    background: #fee2e2;
    color: #991b1b;
    border-color: #fca5a5;
}
.pill-neutral {
    background: #e2e8f0;
    color: #334155;
    border-color: #cbd5e1;
}
.score-box {
    display: inline-block;
    background: #111827;
    color: white;
    border-radius: 12px;
    padding: 6px 12px;
    font-weight: 800;
    font-size: 0.95rem;
    margin-bottom: 8px;
}
.middle-action-wrap {
    background: #f8fafc;
    border: 1px dashed #cbd5e1;
    border-radius: 14px;
    padding: 10px 12px;
    margin: 0 0 10px 0;
}
.compact-btn .stButton > button {
    min-height: 2.5rem;
}
</style>
""", unsafe_allow_html=True)


# =========================================================
# SQLITE
# =========================================================
def get_conn():
    conn = sqlite3.connect(DB_FILE, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA busy_timeout = 30000;")
    return conn


def init_db():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS parish_portfolio_matches (
            uniqueid TEXT PRIMARY KEY,
            portfolio_id TEXT,
            parish_id TEXT,
            parish_no TEXT,
            match_status TEXT,
            review_notes TEXT,
            reviewer_name TEXT,
            reviewed_at TEXT
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS parish_pair_relationships (
            pair_key TEXT PRIMARY KEY,
            portfolio_id TEXT,
            left_uniqueid TEXT,
            right_uniqueid TEXT,
            relationship_type TEXT,
            reviewer_name TEXT,
            reviewed_at TEXT
        )
    """)

    conn.commit()
    conn.close()


def export_outputs():
    conn = get_conn()
    try:
        parish_match_df = pd.read_sql_query("""
            SELECT
                uniqueid,
                portfolio_id,
                parish_id,
                parish_no,
                match_status,
                review_notes,
                reviewer_name,
                reviewed_at
            FROM parish_portfolio_matches
            ORDER BY reviewed_at, portfolio_id, uniqueid
        """, conn)

        pair_relation_df = pd.read_sql_query("""
            SELECT
                pair_key,
                portfolio_id,
                left_uniqueid,
                right_uniqueid,
                relationship_type,
                reviewer_name,
                reviewed_at
            FROM parish_pair_relationships
            ORDER BY reviewed_at, portfolio_id, pair_key
        """, conn)
    finally:
        conn.close()

    parish_match_df.to_csv(PARISH_MATCH_FILE, index=False)
    pair_relation_df.to_csv(PAIR_RELATION_FILE, index=False)


def save_parish_match(uniqueid, portfolio_id, parish_id, parish_no, reviewer_name="", review_notes=""):
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO parish_portfolio_matches (
                uniqueid,
                portfolio_id,
                parish_id,
                parish_no,
                match_status,
                review_notes,
                reviewer_name,
                reviewed_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(uniqueid) DO UPDATE SET
                portfolio_id = excluded.portfolio_id,
                parish_id = excluded.parish_id,
                parish_no = excluded.parish_no,
                match_status = excluded.match_status,
                review_notes = excluded.review_notes,
                reviewer_name = excluded.reviewer_name,
                reviewed_at = excluded.reviewed_at
        """, (
            str(uniqueid),
            str(portfolio_id),
            str(parish_id),
            str(parish_no),
            "MATCH",
            str(review_notes),
            str(reviewer_name),
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        ))
        conn.commit()
    finally:
        conn.close()

    export_outputs()


def make_pair_key(left_uniqueid, right_uniqueid):
    a, b = sorted([str(left_uniqueid), str(right_uniqueid)])
    return f"{a}__{b}"


def save_pair_relationship(portfolio_id, left_uniqueid, right_uniqueid, relationship_type, reviewer_name=""):
    pair_key = make_pair_key(left_uniqueid, right_uniqueid)

    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO parish_pair_relationships (
                pair_key,
                portfolio_id,
                left_uniqueid,
                right_uniqueid,
                relationship_type,
                reviewer_name,
                reviewed_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(pair_key) DO UPDATE SET
                portfolio_id = excluded.portfolio_id,
                left_uniqueid = excluded.left_uniqueid,
                right_uniqueid = excluded.right_uniqueid,
                relationship_type = excluded.relationship_type,
                reviewer_name = excluded.reviewer_name,
                reviewed_at = excluded.reviewed_at
        """, (
            pair_key,
            str(portfolio_id),
            str(left_uniqueid),
            str(right_uniqueid),
            str(relationship_type),
            str(reviewer_name),
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        ))
        conn.commit()
    finally:
        conn.close()

    export_outputs()


def load_parish_matches():
    conn = get_conn()
    try:
        df = pd.read_sql_query("SELECT * FROM parish_portfolio_matches", conn)
    finally:
        conn.close()

    if df.empty:
        return pd.DataFrame(columns=[
            "uniqueid", "portfolio_id", "parish_id", "parish_no",
            "match_status", "review_notes", "reviewer_name", "reviewed_at"
        ])
    return df.fillna("")


def load_pair_relationships():
    conn = get_conn()
    try:
        df = pd.read_sql_query("SELECT * FROM parish_pair_relationships", conn)
    finally:
        conn.close()

    if df.empty:
        return pd.DataFrame(columns=[
            "pair_key", "portfolio_id", "left_uniqueid", "right_uniqueid",
            "relationship_type", "reviewer_name", "reviewed_at"
        ])
    return df.fillna("")


init_db()


# =========================================================
# HELPERS
# =========================================================
def clean_text(x):
    if pd.isna(x):
        return ""
    s = str(x).strip()
    return "" if s.lower() in {"nan", "none", "null"} else s


def safe_get(row, col):
    return clean_text(row.get(col, ""))


def normalize_name(x):
    s = clean_text(x).lower()
    return re.sub(r"[^a-z0-9]", "", s)


def normalize_email(x):
    return clean_text(x).lower()


def normalize_phone(x):
    s = re.sub(r"\D", "", clean_text(x))
    return s[-10:] if len(s) >= 10 else s


def normalize_address(x):
    s = clean_text(x).lower()
    replacements = {
        r"\bst\b": "street",
        r"\brd\b": "road",
        r"\bdr\b": "drive",
        r"\bln\b": "lane",
        r"\bave\b": "avenue",
        r"\bblvd\b": "boulevard",
        r"\bpkwy\b": "parkway",
        r"\bct\b": "court",
    }
    for pat, repl in replacements.items():
        s = re.sub(pat, repl, s)
    return re.sub(r"[^a-z0-9]", "", s)


def format_phone(x):
    s = normalize_phone(x)
    if len(s) == 10:
        return f"({s[:3]}) {s[3:6]}-{s[6:]}"
    return clean_text(x)


def yes_no(cond):
    return "Yes" if cond else "No"


def badge(label, value):
    value_l = str(value).lower()
    cls = "pill-neutral"
    if value_l in {"yes", "match"}:
        cls = "pill-yes"
    elif value_l == "no":
        cls = "pill-no"
    return f'<span class="pill {cls}">{label}: {value}</span>'


def render_badges(flags):
    html = "".join([badge(k, v) for k, v in flags.items()])
    st.markdown(html, unsafe_allow_html=True)


def info_rows(pairs, height=390):
    df = pd.DataFrame(pairs, columns=["Field", "Value"])
    st.dataframe(df, use_container_width=True, hide_index=True, height=height)


@st.cache_data
def load_data(path):
    ext = Path(path).suffix.lower()

    if ext in {".xlsx", ".xls"}:
        df = pd.read_excel(path, dtype=str).fillna("")
    elif ext == ".csv":
        df = pd.read_csv(path, dtype=str, low_memory=False).fillna("")
    elif ext in {".txt", ".tsv"}:
        df = pd.read_csv(path, sep="\t", dtype=str, low_memory=False).fillna("")
    else:
        raise ValueError("Unsupported file type. Use xlsx, xls, csv, txt, or tsv.")

    return df


def portfolio_display_name(row):
    return " ".join([
        safe_get(row, "first_name_clean_port"),
        safe_get(row, "last_name_clean_port")
    ]).strip()


def portfolio_spouse_name(row):
    return " ".join([
        safe_get(row, "spouse_first_name_clean_port"),
        safe_get(row, "spouse_last_name_clean_port")
    ]).strip()


def build_match_score(row):
    score = 0

    parish_first = normalize_name(row.get("first_name_clean", ""))
    parish_last = normalize_name(row.get("last_name_clean", ""))
    parish_sp_first = normalize_name(row.get("spouse_first", ""))
    parish_sp_last = normalize_name(row.get("spouse_last", ""))

    port_first = normalize_name(row.get("first_name_clean_port", ""))
    port_last = normalize_name(row.get("last_name_clean_port", ""))
    port_sp_first = normalize_name(row.get("spouse_first_name_clean_port", ""))
    port_sp_last = normalize_name(row.get("spouse_last_name_clean_port", ""))

    parish_phone = normalize_phone(row.get("phone1_clean", ""))
    port_phone = normalize_phone(row.get("phone1_clean_port", ""))

    parish_email = normalize_email(row.get("email1_clean", ""))
    port_email = normalize_email(row.get("email1_clean_port", ""))

    parish_addr = normalize_address(row.get("address_full_ncoa_clean", ""))
    port_addr = normalize_address(row.get("address_full_ncoa_clean_port", ""))

    if parish_last and parish_last == port_last:
        score += 25
    if parish_first and parish_first == port_first:
        score += 20
    if parish_first and parish_first == port_sp_first:
        score += 16
    if parish_sp_first and parish_sp_first == port_first:
        score += 14
    if parish_sp_last and parish_sp_last == port_sp_last:
        score += 10
    if parish_phone and parish_phone == port_phone:
        score += 20
    if parish_email and port_email and parish_email == port_email:
        score += 20
    if parish_addr and port_addr and parish_addr == port_addr:
        score += 20

    return score


def compare_flags(row):
    parish_first = normalize_name(row.get("first_name_clean", ""))
    parish_last = normalize_name(row.get("last_name_clean", ""))
    parish_sp_first = normalize_name(row.get("spouse_first", ""))
    port_first = normalize_name(row.get("first_name_clean_port", ""))
    port_last = normalize_name(row.get("last_name_clean_port", ""))
    port_sp_first = normalize_name(row.get("spouse_first_name_clean_port", ""))

    parish_phone = normalize_phone(row.get("phone1_clean", ""))
    port_phone = normalize_phone(row.get("phone1_clean_port", ""))

    parish_email = normalize_email(row.get("email1_clean", ""))
    port_email = normalize_email(row.get("email1_clean_port", ""))

    parish_addr = normalize_address(row.get("address_full_ncoa_clean", ""))
    port_addr = normalize_address(row.get("address_full_ncoa_clean_port", ""))

    return {
        "First": yes_no(bool(parish_first and parish_first == port_first)),
        "Last": yes_no(bool(parish_last and parish_last == port_last)),
        "Sp->Port": yes_no(bool(parish_sp_first and parish_sp_first == port_first)),
        "PortSp": yes_no(bool(parish_first and parish_first == port_sp_first)),
        "Phone": yes_no(bool(parish_phone and parish_phone == port_phone)),
        "Email": yes_no(bool(parish_email and parish_email == port_email)),
        "Address": yes_no(bool(parish_addr and port_addr and parish_addr == port_addr)),
        "Match By": safe_get(row, "match_by") or "Unknown",
        "Code": safe_get(row, "code") or "Unknown",
    }


def render_portfolio_card(row):
    with st.container(border=True):
        st.markdown("### Portfolio Database")
        st.caption(f"Portfolio ID: {safe_get(row, 'portfolio_id')}")
        st.caption(f"Portfolio Name: {portfolio_display_name(row) or '-'}")
        if portfolio_spouse_name(row):
            st.caption(f"Portfolio Spouse: {portfolio_spouse_name(row)}")
        st.markdown("")
        st.markdown("")
        st.markdown("")
        st.markdown("")
        st.markdown("")
        st.markdown("")
        

        info_rows([
            ("First name", safe_get(row, "first_name_clean_port")),
            ("Last name", safe_get(row, "last_name_clean_port")),
            ("Spouse first", safe_get(row, "spouse_first_name_clean_port")),
            ("Spouse last", safe_get(row, "spouse_last_name_clean_port")),
            ("Phone", format_phone(safe_get(row, "phone1_clean_port"))),
            ("Email", safe_get(row, "email1_clean_port")),
            ("Address", safe_get(row, "address_full_ncoa_clean_port")),
            ("City / State / ZIP", f"{safe_get(row, 'city1_ncoa_clean_port')}, {safe_get(row, 'state1_ncoa_clean_port')} {safe_get(row, 'zip1_ncoa_port')}"),
        ], height=385)


def render_parish_card(row, saved_match=False, saved_match_label="MATCH"):
    score = build_match_score(row)
    flags = compare_flags(row)

    with st.container(border=True):
        st.markdown("### Parish Record")
        st.markdown(f'<div class="score-box">Score: {score}</div>', unsafe_allow_html=True)
        st.caption(f"{safe_get(row, 'parish_name')} | Parish No: {safe_get(row, 'parish_no')}")
        st.caption(f"UniqueID: {safe_get(row, 'uniqueid')}")

        render_badges(flags)

        if saved_match:
            st.markdown(badge("Portfolio Match", saved_match_label), unsafe_allow_html=True)

        info_rows([
            ("First name", safe_get(row, "first_name_clean")),
            ("Last name", safe_get(row, "last_name_clean")),
            ("Spouse first", safe_get(row, "spouse_first")),
            ("Spouse last", safe_get(row, "spouse_last")),
            ("Phone", format_phone(safe_get(row, "phone1_clean"))),
            ("Email", safe_get(row, "email1_clean")),
            ("Address", safe_get(row, "address_full_ncoa_clean")),
            ("City / State / ZIP", f"{safe_get(row, 'city1_ncoa_clean')}, {safe_get(row, 'state1_ncoa_clean')} {safe_get(row, 'zip1_ncoa')}"),
            ("Match By", safe_get(row, "match_by")),
            ("Code", safe_get(row, "code")),
            ("Offertory 2023 / 2024 / 2025", f"{safe_get(row, 'offertory_2023')} / {safe_get(row, 'offertory_2024')} / {safe_get(row, 'offertory_2025')}"),
        ], height=430)


# =========================================================
# LOAD
# =========================================================
try:
    df = load_data(DATA_FILE)
except Exception as e:
    st.error(f"Could not load DATA_FILE: {e}")
    st.info("Update DATA_FILE in the PATHS block and rerun.")
    st.stop()

required_cols = {
    "uniqueid", "portfolio_id", "parish_no", "first_name_clean", "last_name_clean",
    "first_name_clean_port", "last_name_clean_port", "offertory_2023", "offertory_2024", "offertory_2025"
}
missing = required_cols - set(df.columns)
if missing:
    st.error(f"Data file is missing required columns: {sorted(missing)}")
    st.stop()

for col in df.columns:
    df[col] = df[col].astype(str).fillna("")

df["portfolio_id"] = df["portfolio_id"].astype(str)
df["uniqueid"] = df["uniqueid"].astype(str)

parish_match_df = load_parish_matches()
pair_relationship_df = load_pair_relationships()

parish_match_map = dict(zip(parish_match_df["uniqueid"].astype(str), parish_match_df["match_status"]))
pair_relationship_map = dict(zip(pair_relationship_df["pair_key"].astype(str), pair_relationship_df["relationship_type"]))

portfolio_ids = list(df["portfolio_id"].dropna().astype(str).unique())

st.title("Parish vs Portfolio Review")

st.markdown(
    f"""
    <div class="review-toolbar">
        <b>Total portfolios:</b> {len(portfolio_ids):,}
    </div>
    """,
    unsafe_allow_html=True
)

reviewer_name = st.text_input("Reviewer name", value="", placeholder="Enter your name")

if "record_idx" not in st.session_state:
    st.session_state.record_idx = 0

max_idx = max(len(portfolio_ids) - 1, 0)

nav1, nav2, nav3 = st.columns([1, 2, 1])
with nav1:
    if st.button("Previous", disabled=st.session_state.record_idx <= 0):
        st.session_state.record_idx -= 1
with nav2:
    st.markdown(f"**Portfolio {st.session_state.record_idx + 1} of {len(portfolio_ids)}**")
with nav3:
    if st.button("Next", disabled=st.session_state.record_idx >= max_idx):
        st.session_state.record_idx += 1

st.session_state.record_idx = max(0, min(st.session_state.record_idx, max_idx))

current_portfolio_id = portfolio_ids[st.session_state.record_idx]
grp = df[df["portfolio_id"].astype(str) == str(current_portfolio_id)].copy()
grp = grp.sort_values(["parish_no", "parish_name", "uniqueid"], na_position="last")

portfolio_row = grp.iloc[0]

st.subheader("Portfolio record vs parish records")

main_left, main_right = st.columns([1.0, 2.4], gap="medium")

with main_left:
    st.markdown("<div style='height:105px'></div>", unsafe_allow_html=True)
    render_portfolio_card(portfolio_row)

with main_right:
    parish_rows = list(grp.iterrows())

    for start in range(0, len(parish_rows), 2):
        row_batch = parish_rows[start:start + 2]
        if len(row_batch) == 2:
            (_, left_row), (_, right_row) = row_batch
            left_uid = safe_get(left_row, "uniqueid")
            right_uid = safe_get(right_row, "uniqueid")
            pair_key = make_pair_key(left_uid, right_uid)
            saved_pair_relationship = pair_relationship_map.get(pair_key, "")

            st.markdown('<div class="middle-action-wrap">', unsafe_allow_html=True)
            top_left, top_mid, top_right = st.columns([1, 1.3, 1], gap="medium")
            with top_mid:
                if saved_pair_relationship:
                    st.markdown(badge("Saved", saved_pair_relationship.replace("_", " ")), unsafe_allow_html=True)
                if st.button("Same Person", key=f"same_person_{pair_key}", use_container_width=True):
                    save_pair_relationship(
                        portfolio_id=current_portfolio_id,
                        left_uniqueid=left_uid,
                        right_uniqueid=right_uid,
                        relationship_type="SAME_PERSON",
                        reviewer_name=reviewer_name
                    )
                    st.success("Saved SAME_PERSON")
                    st.rerun()
            st.markdown('</div>', unsafe_allow_html=True)

            compare_cols = st.columns(2, gap="medium")

            with compare_cols[0]:
                left_saved = parish_match_map.get(left_uid, "") == "MATCH"
                left_label = "MATCH"
                render_parish_card(row=left_row, saved_match=left_saved, saved_match_label=left_label)
                notes_key = f"parish_notes_{left_uid}"
                st.text_area(
                    "Parish notes",
                    key=notes_key,
                    height=80,
                    placeholder="Optional notes about this parish record matching the portfolio..."
                )
                left_b1, left_b2 = st.columns(2, gap="small")
                with left_b1:
                    if st.button("Match", key=f"match_{left_uid}", use_container_width=True):
                        save_parish_match(
                            uniqueid=left_uid,
                            portfolio_id=safe_get(left_row, "portfolio_id"),
                            parish_id=safe_get(left_row, "parish_id"),
                            parish_no=safe_get(left_row, "parish_no"),
                            reviewer_name=reviewer_name,
                            review_notes=st.session_state.get(notes_key, "")
                        )
                        st.success("Saved MATCH")
                        st.rerun()
                with left_b2:
                    if st.button("Match (Spouse)", key=f"match_spouse_{left_uid}", use_container_width=True):
                        save_parish_match(
                            uniqueid=left_uid,
                            portfolio_id=safe_get(left_row, "portfolio_id"),
                            parish_id=safe_get(left_row, "parish_id"),
                            parish_no=safe_get(left_row, "parish_no"),
                            reviewer_name=reviewer_name,
                            review_notes=(st.session_state.get(notes_key, "") + " | MATCH_SPOUSE").strip(" |")
                        )
                        st.success("Saved MATCH (SPOUSE)")
                        st.rerun()

            with compare_cols[1]:
                right_saved = parish_match_map.get(right_uid, "") == "MATCH"
                right_label = "MATCH"
                render_parish_card(row=right_row, saved_match=right_saved, saved_match_label=right_label)
                notes_key = f"parish_notes_{right_uid}"
                st.text_area(
                    "Parish notes",
                    key=notes_key,
                    height=80,
                    placeholder="Optional notes about this parish record matching the portfolio..."
                )
                right_b1, right_b2 = st.columns(2, gap="small")
                with right_b1:
                    if st.button("Match", key=f"match_{right_uid}", use_container_width=True):
                        save_parish_match(
                            uniqueid=right_uid,
                            portfolio_id=safe_get(right_row, "portfolio_id"),
                            parish_id=safe_get(right_row, "parish_id"),
                            parish_no=safe_get(right_row, "parish_no"),
                            reviewer_name=reviewer_name,
                            review_notes=st.session_state.get(notes_key, "")
                        )
                        st.success("Saved MATCH")
                        st.rerun()
                with right_b2:
                    if st.button("Match (Spouse)", key=f"match_spouse_{right_uid}", use_container_width=True):
                        save_parish_match(
                            uniqueid=right_uid,
                            portfolio_id=safe_get(right_row, "portfolio_id"),
                            parish_id=safe_get(right_row, "parish_id"),
                            parish_no=safe_get(right_row, "parish_no"),
                            reviewer_name=reviewer_name,
                            review_notes=(st.session_state.get(notes_key, "") + " | MATCH_SPOUSE").strip(" |")
                        )
                        st.success("Saved MATCH (SPOUSE)")
                        st.rerun()

        else:
            compare_cols = st.columns(2, gap="medium")
            (_, only_row) = row_batch[0]
            only_uid = safe_get(only_row, "uniqueid")

            with compare_cols[0]:
                only_saved = parish_match_map.get(only_uid, "") == "MATCH"
                only_label = "MATCH"
                render_parish_card(row=only_row, saved_match=only_saved, saved_match_label=only_label)
                notes_key = f"parish_notes_{only_uid}"
                st.text_area(
                    "Parish notes",
                    key=notes_key,
                    height=80,
                    placeholder="Optional notes about this parish record matching the portfolio..."
                )
                only_b1, only_b2 = st.columns(2, gap="small")
                with only_b1:
                    if st.button("Match", key=f"match_{only_uid}", use_container_width=True):
                        save_parish_match(
                            uniqueid=only_uid,
                            portfolio_id=safe_get(only_row, "portfolio_id"),
                            parish_id=safe_get(only_row, "parish_id"),
                            parish_no=safe_get(only_row, "parish_no"),
                            reviewer_name=reviewer_name,
                            review_notes=st.session_state.get(notes_key, "")
                        )
                        st.success("Saved MATCH")
                        st.rerun()
                with only_b2:
                    if st.button("Match (Spouse)", key=f"match_spouse_{only_uid}", use_container_width=True):
                        save_parish_match(
                            uniqueid=only_uid,
                            portfolio_id=safe_get(only_row, "portfolio_id"),
                            parish_id=safe_get(only_row, "parish_id"),
                            parish_no=safe_get(only_row, "parish_no"),
                            reviewer_name=reviewer_name,
                            review_notes=(st.session_state.get(notes_key, "") + " | MATCH_SPOUSE").strip(" |")
                        )
                        st.success("Saved MATCH (SPOUSE)")
                        st.rerun()

            with compare_cols[1]:
                st.empty()

st.markdown("---")
st.subheader("Current outputs")

d1, d2 = st.columns(2)

with d1:
    if os.path.exists(PARISH_MATCH_FILE):
        with open(PARISH_MATCH_FILE, "rb") as f:
            st.download_button(
                "Download parish_portfolio_match_decisions.csv",
                f,
                file_name="parish_portfolio_match_decisions.csv",
                mime="text/csv",
                use_container_width=True
            )

with d2:
    if os.path.exists(PAIR_RELATION_FILE):
        with open(PAIR_RELATION_FILE, "rb") as f:
            st.download_button(
                "Download parish_pair_relationship_decisions.csv",
                f,
                file_name="parish_pair_relationship_decisions.csv",
                mime="text/csv",
                use_container_width=True
            )
