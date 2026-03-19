import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path

import config
from src.fetcher import HPDFetcher
from src.parser import PDFParser
from src.database import Database
from src.scheduler import Scheduler


def init_session_state():
    if "db" not in st.session_state:
        st.session_state.db = Database()

    if "scheduler" not in st.session_state:
        st.session_state.scheduler = Scheduler()
        st.session_state.scheduler.set_sync_callback(run_full_sync)

    if "sync_status" not in st.session_state:
        st.session_state.sync_status = "idle"


def run_full_sync():
    fetcher = HPDFetcher()
    parser = PDFParser()
    db = st.session_state.db

    with st.status("Fetching PDFs from HPD...") as status:
        results = fetcher.fetch_all_current()
        status.update(label=f"Downloaded {len(results)} PDFs")

    archived = fetcher.get_archived_pdfs()

    if archived:
        with st.status("Parsing PDFs...") as status:
            all_records = []
            for pdf_path in archived[:20]:
                records, _ = parser.parse_pdf(pdf_path)
                all_records.extend(records)
                db.log_sync(str(pdf_path.name), True, "", len(records))

            if all_records:
                inserted, errors = db.insert_batch(all_records)
                status.update(label=f"Inserted {inserted} records ({errors} errors)")
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
    for i, pdf_path in enumerate(pdf_files):
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
        inserted, errors = db.insert_batch(all_records)
        st.success(
            f"Imported {inserted} records ({errors} errors) from {len(pdf_files)} PDFs"
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

    tab1, tab2, tab3, tab4, tab5 = st.tabs(
        [
            "Dashboard",
            "Controls",
            "Data Explorer",
            "Import Historical",
            "Search & Favorites",
        ]
    )

    with tab1:
        col1, col2, col3, col4 = st.columns(4)

        db_stats = st.session_state.db.get_stats()
        fetcher = HPDFetcher()
        fetcher_stats = fetcher.get_stats()

        col1.metric("Total Arrests", db_stats["total_arrests"])
        col2.metric("Unique Persons", db_stats["unique_persons"])
        col3.metric("Total Charges", db_stats["total_charges"])
        col4.metric("PDFs Archived", fetcher_stats["total_pdfs"])

        st.markdown("---")

        scheduler_status = st.session_state.scheduler.get_status()

        if scheduler_status["is_running"]:
            st.success("🟢 Scheduler is running")
        else:
            st.error("🔴 Scheduler is stopped")

        if scheduler_status["next_run"]:
            st.info(f"Next scheduled sync: {scheduler_status['next_run']}")

        st.markdown("#### Recent Arrests")
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
            display_df = recent_df[[c for c in display_cols if c in recent_df.columns]]
            st.dataframe(display_df, use_container_width=True)
        else:
            st.info("No arrest records yet. Click 'Fetch & Sync Now' to get started.")

        favorites_df = st.session_state.db.get_favorites()
        if not favorites_df.empty:
            st.markdown("---")
            st.markdown("#### :bell: Favorite Alerts")
            alerts = st.session_state.db.check_favorite_alerts()
            if not alerts.empty:
                st.error(
                    f"**{len(alerts)} new arrest(s)** for favorited individuals in the last 24 hours!"
                )
                for _, alert in alerts.iterrows():
                    st.warning(
                        f"**{alert['full_name']}** was arrested on "
                        f"{alert['arrest_timestamp'].strftime('%Y-%m-%d %H:%M')} at "
                        f"{alert['location']} for *{alert['offense_description']}*"
                    )
            else:
                st.success("No recent arrests for your favorited individuals.")

    with tab2:
        st.header("Scheduler Controls")

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

        st.markdown("#### Scheduled Times")
        for time_str in config.SCHEDULE_TIMES:
            st.write(f"• {time_str} HST")

    with tab3:
        st.header("Data Explorer")

        col1, col2 = st.columns(2)

        min_date = datetime.now() - timedelta(days=30)
        start_date = col1.date_input("Start Date", min_date)
        end_date = col2.date_input("End Date", datetime.now())

        if st.button("Search", use_container_width=True):
            start_dt = datetime.combine(start_date, datetime.min.time())
            end_dt = datetime.combine(end_date, datetime.max.time())

            results = st.session_state.db.get_arrests_by_date_range(start_dt, end_dt)

            if not results.empty:
                st.dataframe(results, use_container_width=True)
                st.success(f"Found {len(results)} records")
            else:
                st.info("No records found in date range")

        st.markdown("#### Statistics")
        stats = st.session_state.db.get_stats()
        for key, value in stats.items():
            st.write(f"**{key.replace('_', ' ').title()}:** {value}")

    with tab4:
        st.header("Import Historical PDFs")
        st.write("Import PDF files from a local directory for parsing and archival.")

        import_path = st.text_input(
            "Directory Path",
            value="./data/raw_pdfs",
            help="Path to directory containing HPD arrest log PDFs",
        )

        if st.button("Import PDFs", type="primary", use_container_width=True):
            import_historical_pdfs(Path(import_path))
            st.rerun()

        st.markdown("---")
        st.markdown("#### Archive Statistics")
        fetcher = HPDFetcher()
        stats = fetcher.get_stats()
        st.write(f"**Total PDFs:** {stats['total_pdfs']}")
        st.write(f"**Total Size:** {stats['total_size_mb']} MB")
        st.write(f"**Date Range:** {stats.get('latest_date', 'N/A')}")

    with tab5:
        st.header("Search & Favorites")

        subtab1, subtab2 = st.tabs(["Search by Name", "Favorited Individuals"])

        with subtab1:
            st.markdown("#### Search Individuals")
            st.write(
                "Enter any part of a name to search (no minimum characters required)"
            )

            col1, col2 = st.columns(2)
            search_first = col1.text_input("First Name (partial)", "")
            search_last = col2.text_input("Last Name (partial)", "")

            if st.button("Search", use_container_width=True, type="primary"):
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

                            with col1:
                                st.markdown(f"**{row['full_name']}**")
                                st.caption(
                                    f"Age: {row.get('age_at_arrest', 'N/A')} | "
                                    f"Gender: {row.get('gender', 'N/A')} | "
                                    f"Race: {row.get('race_ethnicity', 'N/A')} | "
                                    f"Arrests: {row.get('arrest_count', 0)}"
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
                        st.markdown(f"**{row['full_name']}** :pushpin:")
                        st.caption(
                            f"Age: {row.get('age_at_arrest', 'N/A')} | "
                            f"Gender: {row.get('gender', 'N/A')} | "
                            f"Race: {row.get('race_ethnicity', 'N/A')} | "
                            f"Total Arrests: {row.get('arrest_count', 0)}"
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


if __name__ == "__main__":
    main()
