import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path

import config
from src.fetcher import HPDFetcher
from src.parser import PDFParser
from src.database import Database
from src.scheduler import Scheduler


st.markdown(
    """
<style>
    .metric-card {
        background: linear-gradient(135deg, #1a1a2e 0%, #16213e 100%);
        padding: 20px;
        border-radius: 12px;
        border: 1px solid #0f3460;
        text-align: center;
    }
    .metric-value {
        font-size: 2.5em;
        font-weight: bold;
        color: #e94560;
    }
    .metric-label {
        color: #a0a0a0;
        font-size: 0.9em;
        text-transform: uppercase;
        letter-spacing: 1px;
    }
    .status-running {
        background: linear-gradient(135deg, #11998e 0%, #38ef7d 100%);
        color: white;
        padding: 8px 16px;
        border-radius: 20px;
        font-weight: bold;
    }
    .status-stopped {
        background: linear-gradient(135deg, #eb3349 0%, #f45c43 100%);
        color: white;
        padding: 8px 16px;
        border-radius: 20px;
        font-weight: bold;
    }
    .section-header {
        background: linear-gradient(135deg, #0f3460 0%, #1a1a2e 100%);
        padding: 15px 20px;
        border-radius: 8px;
        margin-bottom: 20px;
        border-left: 4px solid #e94560;
    }
    div[data-testid="stDataFrame"] {
        border-radius: 8px;
        overflow: hidden;
    }
    .gender-male { background-color: #1e88e5 !important; }
    .gender-female { background-color: #ec407a !important; }
    .gender-unknown { background-color: #757575 !important; }
    
    /* Row highlighting */
    tr:hover {
        background-color: rgba(233, 69, 96, 0.1) !important;
    }
    
    /* Custom scrollbar */
    ::-webkit-scrollbar {
        width: 8px;
        height: 8px;
    }
    ::-webkit-scrollbar-track {
        background: #1a1a2e;
    }
    ::-webkit-scrollbar-thumb {
        background: #e94560;
        border-radius: 4px;
    }
    
    /* Alert boxes */
    .alert-high {
        background: linear-gradient(135deg, #ff416c 0%, #ff4b2b 100%);
        color: white;
        padding: 15px;
        border-radius: 8px;
        margin: 10px 0;
    }
    .alert-medium {
        background: linear-gradient(135deg, #f093fb 0%, #f5576c 100%);
        color: white;
        padding: 15px;
        border-radius: 8px;
        margin: 10px 0;
    }
    .alert-low {
        background: linear-gradient(135deg, #4facfe 0%, #00f2fe 100%);
        color: white;
        padding: 15px;
        border-radius: 8px;
        margin: 10px 0;
    }
</style>
""",
    unsafe_allow_html=True,
)


def init_session_state():
    if "db" not in st.session_state:
        st.session_state.db = Database()

    if "scheduler" not in st.session_state:
        st.session_state.scheduler = Scheduler()
        st.session_state.scheduler.set_sync_callback(run_full_sync)

    if "sync_status" not in st.session_state:
        st.session_state.sync_status = "idle"


def get_bail_severity(bail_amount):
    if pd.isna(bail_amount) or bail_amount == 0:
        return "low"
    elif bail_amount < 10000:
        return "medium"
    else:
        return "high"


def get_bail_color(bail_amount):
    severity = get_bail_severity(bail_amount)
    colors = {
        "high": "color: #ff4b4b; font-weight: bold;",
        "medium": "color: #ffa726;",
        "low": "color: #66bb6a;",
    }
    return colors.get(severity, "")


def get_gender_color(gender):
    colors = {
        "Male": "color: #42a5f5;",
        "Female": "color: #ec407a;",
    }
    return colors.get(gender, "color: #9e9e9e;")


def get_release_code_color(code):
    colors = {
        "RBL": "color: #ffa726;",
        "RNC": "color: #66bb6a;",
        "RPC": "color: #ef5350;",
        "RPI": "color: #ab47bc;",
        "ROR": "color: #26a69a;",
        "ISC": "color: #5c6bc0;",
        "DCT": "color: #7e57c2;",
        "CCT": "color: #7e57c2;",
        "FCT": "color: #7e57c2;",
        "OTH": "color: #78909c;",
    }
    return colors.get(code, "color: #9e9e9e;")


def get_race_color(race):
    colors = {
        "White": "color: #e3f2fd;",
        "Black": "color: #212121; background: #424242; padding: 2px 6px; border-radius: 4px;",
        "Hawaiian": "color: #00695c;",
        "Filipino": "color: #1565c0;",
        "Japanese": "color: #c62828;",
        "Chinese": "color: #d32f2f;",
        "Samoan": "color: #0277bd;",
        "Micronesian": "color: #00838f;",
        "Laotian": "color: #558b2f;",
        "Hispanic": "color: #ff8f00;",
    }
    return colors.get(race, "color: #9e9e9e;")


def style_dataframe(df, display_cols):
    if df.empty:
        return df

    styled = df[display_cols].copy()

    for col in display_cols:
        if col == "gender":
            styled[col] = styled[col].apply(
                lambda x: f'<span style="{get_gender_color(x)}">{x}</span>'
            )
        elif col == "race_ethnicity":
            styled[col] = styled[col].apply(
                lambda x: f'<span style="{get_race_color(x)}">{x}</span>'
            )
        elif col == "bail_amount":
            styled[col] = styled[col].apply(
                lambda x: (
                    f'<span style="{get_bail_color(x)}">${x:,.0f}</span>'
                    if pd.notna(x) and x > 0
                    else "—"
                )
            )
        elif col == "release_code":
            styled[col] = styled[col].apply(
                lambda x: (
                    f'<span style="{get_release_code_color(x)}">{x}</span>'
                    if pd.notna(x)
                    else "—"
                )
            )

    return styled


def run_full_sync():
    fetcher = HPDFetcher()
    parser = PDFParser()
    db = st.session_state.db

    with st.status("Fetching PDFs from HPD...", expanded=True) as status:
        results = fetcher.fetch_all_current()
        status.update(label=f"Downloaded {len(results)} PDFs", state="complete")

    archived = fetcher.get_archived_pdfs()

    if archived:
        latest_pdfs = db.get_latest_pdf_per_day()

        with st.status("Parsing PDFs...", expanded=True) as status:
            all_records = []
            skipped = 0
            for pdf_path in archived[:20]:
                pdf_name = str(pdf_path.name)
                records, _ = parser.parse_pdf(pdf_path)

                pdf_date = pdf_path.stem[:10] if pdf_path.stem else None
                is_latest = pdf_date and latest_pdfs.get(pdf_date) == str(pdf_path)

                if not is_latest:
                    skipped += len(records)
                    status.update(
                        label=f"Skipping {pdf_name} (not latest for {pdf_date})",
                        state="running",
                    )
                    continue

                all_records.extend(records)
                db.log_sync(pdf_name, True, "", len(records))
                status.update(label=f"Parsed {pdf_name}", state="running")

            if all_records:
                inserted, errors, dup_skipped = db.insert_batch(all_records)
                status.update(
                    label=f"Inserted {inserted} records ({errors} errors, {dup_skipped} duplicates skipped)",
                    state="complete",
                )

                removed, _ = db.remove_duplicates(dry_run=False)
                if removed > 0:
                    st.success(f"Auto-cleaned {removed} duplicate records")
            else:
                status.update(label="No records to insert", state="complete")
    else:
        st.warning("No new PDFs to parse")


def import_historical_pdfs(directory: Path):
    parser = PDFParser()
    db = st.session_state.db

    if not directory.exists():
        st.error(f"Directory not found: {directory}")
        return

    pdf_files = list(directory.rglob("*.pdf"))

    if not pdf_files:
        st.warning(f"No PDF files found in {directory}")
        return

    progress_bar = st.progress(0)
    status_text = st.empty()

    all_records = []
    latest_pdfs = db.get_latest_pdf_per_day(str(directory))

    for i, pdf_path in enumerate(pdf_files):
        pdf_date = pdf_path.stem[:10] if pdf_path.stem else None
        is_latest = pdf_date and latest_pdfs.get(pdf_date) == str(pdf_path)

        if not is_latest:
            progress_bar.progress((i + 1) / len(pdf_files))
            continue

        status_text.text(f"Parsing {pdf_path.name}...")
        try:
            records, row_count = parser.parse_pdf(pdf_path)
            all_records.extend(records)
            db.log_sync(str(pdf_path.name), True, "", len(records))
        except Exception as e:
            db.log_sync(str(pdf_path.name), False, str(e), 0)
        progress_bar.progress((i + 1) / len(pdf_files))

    progress_bar.empty()
    status_text.empty()

    if all_records:
        inserted, errors, dup_skipped = db.insert_batch(all_records)
        st.success(
            f"Imported {inserted} records ({errors} errors, {dup_skipped} duplicates skipped) from {len(pdf_files)} PDFs"
        )
    else:
        st.warning("No records found in PDFs")


def main():
    st.set_page_config(
        page_title="HPD Arrest Log Archival System", page_icon="🚔", layout="wide"
    )

    init_session_state()

    st.title("🚔 HPD Arrest Log Archival System")
    st.markdown("---")

    tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs(
        [
            "📊 Dashboard",
            "⚙️ Controls",
            "🔍 Data Explorer",
            "📁 Import Historical",
            "🔔 Search & Favorites",
            "🧹 Maintenance",
        ]
    )

    with tab1:
        col1, col2, col3, col4 = st.columns(4)

        db_stats = st.session_state.db.get_stats()
        fetcher = HPDFetcher()
        fetcher_stats = fetcher.get_stats()

        with col1:
            st.markdown('<div class="metric-card">', unsafe_allow_html=True)
            st.markdown(
                f'<div class="metric-value">{db_stats["total_arrests"]}</div>',
                unsafe_allow_html=True,
            )
            st.markdown(
                '<div class="metric-label">Total Arrests</div>', unsafe_allow_html=True
            )
            st.markdown("</div>", unsafe_allow_html=True)

        with col2:
            st.markdown('<div class="metric-card">', unsafe_allow_html=True)
            st.markdown(
                f'<div class="metric-value">{db_stats["unique_persons"]}</div>',
                unsafe_allow_html=True,
            )
            st.markdown(
                '<div class="metric-label">Unique Persons</div>', unsafe_allow_html=True
            )
            st.markdown("</div>", unsafe_allow_html=True)

        with col3:
            st.markdown('<div class="metric-card">', unsafe_allow_html=True)
            st.markdown(
                f'<div class="metric-value">{db_stats["total_charges"]}</div>',
                unsafe_allow_html=True,
            )
            st.markdown(
                '<div class="metric-label">Total Charges</div>', unsafe_allow_html=True
            )
            st.markdown("</div>", unsafe_allow_html=True)

        with col4:
            dup_count = db_stats.get("duplicate_records", 0)
            dup_color = "#ff4b4b" if dup_count > 0 else "#66bb6a"
            st.markdown('<div class="metric-card">', unsafe_allow_html=True)
            st.markdown(
                f'<div class="metric-value" style="color: {dup_color}">{dup_count}</div>',
                unsafe_allow_html=True,
            )
            st.markdown(
                '<div class="metric-label">Duplicates Found</div>',
                unsafe_allow_html=True,
            )
            st.markdown("</div>", unsafe_allow_html=True)

        st.markdown("---")

        scheduler_status = st.session_state.scheduler.get_status()

        status_html = (
            "status-running" if scheduler_status["is_running"] else "status-stopped"
        )
        status_text = "Running" if scheduler_status["is_running"] else "Stopped"
        st.markdown(
            f'<div class="{status_html}">Scheduler: {status_text}</div>',
            unsafe_allow_html=True,
        )

        if scheduler_status["next_run"]:
            st.info(f"Next scheduled sync: {scheduler_status['next_run']}")

        st.markdown("---")
        st.markdown("#### 📋 Recent Arrests")

        recent_df = st.session_state.db.get_recent_arrests(20)

        if not recent_df.empty:
            display_cols = [
                "full_name",
                "age_at_arrest",
                "gender",
                "race_ethnicity",
                "arrest_timestamp",
                "location",
                "offense_description",
                "bail_amount",
            ]
            display_df = style_dataframe(recent_df, display_cols)

            st.write(
                display_df.to_html(escape=False, index=False), unsafe_allow_html=True
            )
        else:
            st.info("No arrest records yet. Click 'Fetch & Sync Now' to get started.")

        favorites_df = st.session_state.db.get_favorites()
        if not favorites_df.empty:
            st.markdown("---")
            st.markdown("#### 🔔 Favorite Alerts")
            alerts = st.session_state.db.check_favorite_alerts()
            if not alerts.empty:
                st.markdown(
                    f'<div class="alert-high">🚨 {len(alerts)} new arrest(s) for favorited individuals in the last 24 hours!</div>',
                    unsafe_allow_html=True,
                )
                for _, alert in alerts.iterrows():
                    st.markdown(
                        f'<div class="alert-medium">**{alert["full_name"]}** was arrested on '
                        f"{alert['arrest_timestamp'].strftime('%Y-%m-%d %H:%M')} at "
                        f"{alert['location']} for *{alert['offense_description']}*</div>",
                        unsafe_allow_html=True,
                    )
            else:
                st.markdown(
                    '<div class="alert-low">✓ No recent arrests for your favorited individuals.</div>',
                    unsafe_allow_html=True,
                )

    with tab2:
        st.markdown(
            '<div class="section-header"><h3>Scheduler Controls</h3></div>',
            unsafe_allow_html=True,
        )

        col1, col2 = st.columns(2)

        if col1.button("▶️ Start Scheduler", type="primary", use_container_width=True):
            st.session_state.scheduler.start()
            st.rerun()

        if col2.button("⏹️ Stop Scheduler", type="secondary", use_container_width=True):
            st.session_state.scheduler.stop()
            st.rerun()

        st.markdown("---")

        if st.button("🔄 Fetch & Sync Now", type="primary", use_container_width=True):
            with st.spinner("Running sync..."):
                run_full_sync()
            st.rerun()

        st.markdown("#### ⏰ Scheduled Times")
        for time_str in config.SCHEDULE_TIMES:
            st.write(f"• **{time_str}** HST")

    with tab3:
        st.markdown(
            '<div class="section-header"><h3>Data Explorer</h3></div>',
            unsafe_allow_html=True,
        )

        col1, col2 = st.columns(2)

        min_date = datetime.now() - timedelta(days=30)
        start_date = col1.date_input("Start Date", min_date)
        end_date = col2.date_input("End Date", datetime.now())

        if st.button("🔍 Search", use_container_width=True, type="primary"):
            start_dt = datetime.combine(start_date, datetime.min.time())
            end_dt = datetime.combine(end_date, datetime.max.time())

            results = st.session_state.db.get_arrests_by_date_range(start_dt, end_dt)

            if not results.empty:
                st.dataframe(results, use_container_width=True)
                st.success(f"Found {len(results)} records")
            else:
                st.info("No records found in date range")

        st.markdown("---")
        st.markdown("#### 📈 Statistics")

        stats = st.session_state.db.get_stats()

        stat_cols = st.columns(3)
        for i, (key, value) in enumerate(stats.items()):
            if key == "last_sync":
                continue
            with stat_cols[i % 3]:
                st.metric(key.replace("_", " ").title(), value)

    with tab4:
        st.markdown(
            '<div class="section-header"><h3>Import Historical PDFs</h3></div>',
            unsafe_allow_html=True,
        )
        st.write(
            "Import PDF files from a local directory for parsing and archival. Only the latest PDF per day will be imported."
        )

        import_path = st.text_input(
            "Directory Path",
            value="./data/raw_pdfs",
            help="Path to directory containing HPD arrest log PDFs",
        )

        if st.button("📥 Import PDFs", type="primary", use_container_width=True):
            import_historical_pdfs(Path(import_path))
            st.rerun()

        st.markdown("---")
        st.markdown("#### 📊 Archive Statistics")
        fetcher = HPDFetcher()
        stats = fetcher.get_stats()

        stats_cols = st.columns(3)
        with stats_cols[0]:
            st.metric("Total PDFs", stats["total_pdfs"])
        with stats_cols[1]:
            st.metric("Total Size", f"{stats['total_size_mb']} MB")
        with stats_cols[2]:
            st.metric("Latest Date", stats.get("latest_date", "N/A"))

    with tab5:
        st.markdown(
            '<div class="section-header"><h3>Search & Favorites</h3></div>',
            unsafe_allow_html=True,
        )

        subtab1, subtab2 = st.tabs(["🔎 Search by Name", "📌 Pinned Individuals"])

        with subtab1:
            st.markdown("#### Search Individuals")
            st.write("Enter any part of a name to search")

            col1, col2 = st.columns(2)
            search_first = col1.text_input(
                "First Name (partial)", "", placeholder="e.g., John"
            )
            search_last = col2.text_input(
                "Last Name (partial)", "", placeholder="e.g., Smith"
            )

            if st.button("🔍 Search", use_container_width=True, type="primary"):
                if search_first or search_last:
                    results = st.session_state.db.search_by_name(
                        search_first, search_last
                    )
                    if not results.empty:
                        favorites = st.session_state.db.get_favorites()
                        fav_ids = (
                            set(favorites["person_id"].astype(str).tolist())
                            if not favorites.empty
                            else set()
                        )

                        for idx, row in results.iterrows():
                            col1, col2 = st.columns([4, 1])
                            is_fav = str(row["person_id"]) in fav_ids
                            fav_icon = "📌" if is_fav else ""

                            with col1:
                                gender_color = get_gender_color(
                                    row.get("gender", "Unknown")
                                )
                                race_color = get_race_color(
                                    row.get("race_ethnicity", "Unknown")
                                )
                                st.markdown(f"**{row['full_name']}** {fav_icon}")
                                st.markdown(
                                    f'<span style="{gender_color}">Gender: {row.get("gender", "N/A")}</span> | '
                                    f"Age: {row.get('age_at_arrest', 'N/A')} | "
                                    f'<span style="{race_color}">Race: {row.get("race_ethnicity", "N/A")}</span> | '
                                    f"Arrests: **{row.get('arrest_count', 0)}**"
                                )
                                if pd.notna(row.get("last_arrest")):
                                    st.caption(
                                        f"Last arrest: {row['last_arrest'].strftime('%Y-%m-%d')}"
                                    )

                            with col2:
                                if is_fav:
                                    if st.button(
                                        "Unpin",
                                        key=f"unfav_{row['person_id']}",
                                        type="secondary",
                                    ):
                                        st.session_state.db.remove_favorite(
                                            str(row["person_id"])
                                        )
                                        st.rerun()
                                else:
                                    if st.button(
                                        "Pin",
                                        key=f"fav_{row['person_id']}",
                                        type="primary",
                                    ):
                                        st.session_state.db.add_favorite(
                                            str(row["person_id"])
                                        )
                                        st.rerun()

                            st.divider()

                        st.success(f"Found {len(results)} individual(s)")
                    else:
                        st.info("No individuals found matching your search")
                else:
                    st.warning("Please enter at least a first or last name")

        with subtab2:
            st.markdown("#### Your Pinned Individuals")
            favorites_df = st.session_state.db.get_favorites()

            if favorites_df.empty:
                st.info(
                    "No pinned individuals yet. Use the search to find and pin people."
                )
            else:
                for idx, row in favorites_df.iterrows():
                    col1, col2 = st.columns([4, 1])

                    with col1:
                        gender_color = get_gender_color(row.get("gender", "Unknown"))
                        race_color = get_race_color(
                            row.get("race_ethnicity", "Unknown")
                        )
                        st.markdown(f"**{row['full_name']}** 📌")
                        st.markdown(
                            f'<span style="{gender_color}">Gender: {row.get("gender", "N/A")}</span> | '
                            f"Age: {row.get('age_at_arrest', 'N/A')} | "
                            f'<span style="{race_color}">Race: {row.get("race_ethnicity", "N/A")}</span> | '
                            f"Total Arrests: **{row.get('arrest_count', 0)}**"
                        )
                        if pd.notna(row.get("last_arrest")):
                            st.caption(
                                f"Last arrest: {row['last_arrest'].strftime('%Y-%m-%d')}"
                            )
                        st.caption(
                            f"Pinned on: {row['favorited_at'].strftime('%Y-%m-%d %H:%M')}"
                        )

                    with col2:
                        if st.button(
                            "Unpin", key=f"remove_{row['person_id']}", type="secondary"
                        ):
                            st.session_state.db.remove_favorite(str(row["person_id"]))
                            st.rerun()

                    st.divider()

                st.success(f"You have {len(favorites_df)} pinned individual(s)")

    with tab6:
        st.markdown(
            '<div class="section-header"><h3>Maintenance & Cleanup</h3></div>',
            unsafe_allow_html=True,
        )
        st.write("Manage duplicates and clean up old PDF files.")

        db = st.session_state.db

        st.markdown("#### 🔄 Database Deduplication")

        col1, col2, col3 = st.columns([2, 1, 1])

        with col1:
            stats = db.get_stats()
            dup_count = stats.get("duplicate_records", 0)
            dup_groups = stats.get("duplicate_groups", 0)

            if dup_count > 0:
                st.warning(
                    f"Found **{dup_count}** duplicate records in **{dup_groups}** groups"
                )

                with st.expander("View Duplicate Records", expanded=False):
                    duplicates = db.find_duplicates()
                    if not duplicates.empty:
                        st.dataframe(duplicates, use_container_width=True)
            else:
                st.success("✓ No duplicates found in the database")

        with col2:
            if st.button(
                "🧹 Clean Duplicates Now", type="primary", use_container_width=True
            ):
                if dup_count > 0:
                    removed, names = db.remove_duplicates(dry_run=False)
                    st.success(f"Removed {removed} duplicate records")
                    st.rerun()
                else:
                    st.info("No duplicates to remove")

        with col3:
            if st.button(
                "🔄 Refresh Stats", type="secondary", use_container_width=True
            ):
                st.rerun()

        st.markdown("---")
        st.markdown("#### 🗑️ PDF Cleanup")

        fetcher = HPDFetcher()
        stats = fetcher.get_stats()

        pdf_dir = config.HPD_ARCHIVE_DIR
        cleanup_info = db.cleanup_old_pdfs(pdf_dir, dry_run=True)

        col_pdf1, col_pdf2 = st.columns(2)

        with col_pdf1:
            if cleanup_info["removed"]:
                st.warning(
                    f"Found **{len(cleanup_info['removed'])}** old PDFs that are not the latest for their day"
                )

                with st.expander(f"View {len(cleanup_info['removed'])} PDFs to Remove"):
                    for pdf in cleanup_info["removed"][:10]:
                        st.text(pdf)
                    if len(cleanup_info["removed"]) > 10:
                        st.caption(f"... and {len(cleanup_info['removed']) - 10} more")

                if st.button(
                    "🗑️ Delete Old PDFs", type="secondary", use_container_width=True
                ):
                    db.cleanup_old_pdfs(pdf_dir, dry_run=False)
                    st.success(f"Deleted {len(cleanup_info['removed'])} old PDFs")
                    st.rerun()
            else:
                st.success("✓ All PDFs are the latest for their respective days")

        with col_pdf2:
            st.metric("Total PDFs", stats["total_pdfs"])
            st.metric("Kept (latest)", len(cleanup_info["kept"]))
            st.metric("Would Remove", len(cleanup_info["removed"]))


if __name__ == "__main__":
    main()
