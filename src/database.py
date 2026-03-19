import os
import re
import duckdb
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple
from pathlib import Path
from uuid import uuid4
import pandas as pd

import config


class Database:
    def __init__(self, db_path: str = config.DATABASE_PATH):
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        self.db_path = db_path
        self.conn = duckdb.connect(db_path)
        self._init_schema()

    def _init_schema(self):
        self.conn.execute("""
            CREATE SEQUENCE IF NOT EXISTS sync_log_id_seq START 1;
        """)

        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS arrestees (
                person_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                full_name VARCHAR NOT NULL,
                age_at_arrest INTEGER,
                gender VARCHAR,
                race_ethnicity VARCHAR
            )
        """)

        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS officers (
                officer_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                officer_name VARCHAR NOT NULL UNIQUE
            )
        """)

        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS arrest_incidents (
                incident_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                person_id UUID REFERENCES arrestees(person_id),
                officer_id UUID REFERENCES officers(officer_id),
                arrest_timestamp TIMESTAMP,
                location VARCHAR,
                source_pdf VARCHAR,
                pdf_timestamp TIMESTAMP,
                UNIQUE(person_id, arrest_timestamp, location)
            )
        """)

        existing_cols = self.conn.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_name = 'arrest_incidents'"
        ).fetchall()
        existing_col_names = [r[0] for r in existing_cols]

        if "source_pdf" not in existing_col_names:
            self.conn.execute(
                "ALTER TABLE arrest_incidents ADD COLUMN source_pdf VARCHAR"
            )
        if "pdf_timestamp" not in existing_col_names:
            self.conn.execute(
                "ALTER TABLE arrest_incidents ADD COLUMN pdf_timestamp TIMESTAMP"
            )

        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS charges (
                charge_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                incident_id UUID REFERENCES arrest_incidents(incident_id),
                offense_description VARCHAR,
                statute_code VARCHAR,
                report_number VARCHAR,
                bail_amount DOUBLE,
                court_location VARCHAR,
                release_code VARCHAR,
                release_timestamp TIMESTAMP
            )
        """)

        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS sync_log (
                id INTEGER DEFAULT nextval('sync_log_id_seq'),
                filename VARCHAR,
                downloaded_at TIMESTAMP,
                parsed BOOLEAN DEFAULT FALSE,
                parse_error TEXT,
                record_count INTEGER DEFAULT 0
            )
        """)

        for code, definition in config.RELEASE_CODES.items():
            existing = self.conn.execute(
                "SELECT 1 FROM information_schema.tables WHERE table_name = 'release_codes'"
            ).fetchone()
            if not existing:
                self.conn.execute("""
                    CREATE TABLE release_codes (
                        code VARCHAR PRIMARY KEY,
                        definition VARCHAR
                    )
                """)
            self.conn.execute(
                "INSERT OR IGNORE INTO release_codes (code, definition) VALUES (?, ?)",
                [code, definition],
            )

        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS favorites (
                person_id UUID PRIMARY KEY REFERENCES arrestees(person_id),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

    def get_or_create_person_id(
        self, full_name: str, age: int, gender: str, race: str
    ) -> str:
        result = self.conn.execute(
            """
            SELECT person_id FROM arrestees 
            WHERE full_name = ? AND age_at_arrest = ? AND gender = ? AND race_ethnicity = ?
        """,
            [full_name, age, gender, race],
        ).fetchone()

        if result:
            return str(result[0])

        person_id = str(uuid4())
        self.conn.execute(
            """
            INSERT INTO arrestees (person_id, full_name, age_at_arrest, gender, race_ethnicity)
            VALUES (?, ?, ?, ?, ?)
        """,
            [person_id, full_name, age, gender, race],
        )
        return person_id

    def get_or_create_officer_id(self, officer_name: str) -> str:
        result = self.conn.execute(
            """
            SELECT officer_id FROM officers WHERE officer_name = ?
        """,
            [officer_name],
        ).fetchone()

        if result:
            return str(result[0])

        officer_id = str(uuid4())
        self.conn.execute(
            """
            INSERT INTO officers (officer_id, officer_name) VALUES (?, ?)
        """,
            [officer_id, officer_name],
        )
        return officer_id

    def insert_arrest(self, record: Dict, source_pdf: str = None) -> Tuple[bool, str]:
        try:
            full_name = record["full_name"]
            age = record["age_at_arrest"]
            gender = record["gender"]
            race = record["race_ethnicity"]
            arrest_ts = record["arrest_timestamp"]
            location = record["location"]

            existing_id = self.is_duplicate(
                full_name, age, gender, race, arrest_ts, location
            )
            if existing_id:
                if source_pdf:
                    self.conn.execute(
                        """
                        UPDATE arrest_incidents 
                        SET source_pdf = ?, pdf_timestamp = ?
                        WHERE incident_id = ?
                    """,
                        [
                            source_pdf,
                            self._parse_pdf_timestamp(source_pdf),
                            existing_id,
                        ],
                    )
                return False, f"Duplicate: {existing_id}"

            person_id = self.get_or_create_person_id(
                full_name,
                age,
                gender,
                race,
            )

            officer_id = self.get_or_create_officer_id(record["officer_name"])

            incident_id = str(uuid4())
            pdf_ts = self._parse_pdf_timestamp(source_pdf) if source_pdf else None

            self.conn.execute(
                """
                INSERT INTO arrest_incidents (incident_id, person_id, officer_id, arrest_timestamp, location, source_pdf, pdf_timestamp)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
                [
                    incident_id,
                    person_id,
                    officer_id,
                    arrest_ts,
                    location,
                    source_pdf,
                    pdf_ts,
                ],
            )

            charge_id = str(uuid4())
            self.conn.execute(
                """
                INSERT INTO charges (charge_id, incident_id, offense_description, statute_code, 
                                    report_number, bail_amount, court_location, release_code, release_timestamp)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
                [
                    charge_id,
                    incident_id,
                    record["offense_description"],
                    record["statute_code"],
                    record["report_number"],
                    record["bail_amount"],
                    record["court_location"],
                    record["release_code"],
                    record.get("release_timestamp"),
                ],
            )

            return True, incident_id

        except Exception as e:
            return False, str(e)

    def insert_batch(
        self, records: List[Dict], source_pdf: str = None
    ) -> Tuple[int, int, int]:
        success_count = 0
        error_count = 0
        skipped_count = 0

        for record in records:
            success, msg = self.insert_arrest(record, source_pdf)
            if success:
                success_count += 1
            elif "Duplicate" in msg:
                skipped_count += 1
            else:
                error_count += 1

        return success_count, error_count, skipped_count

    def log_sync(
        self,
        filename: str,
        success: bool = True,
        error: str = "",
        record_count: int = 0,
    ):
        self.conn.execute(
            """
            INSERT INTO sync_log (filename, downloaded_at, parsed, parse_error, record_count)
            VALUES (?, ?, ?, ?, ?)
        """,
            [filename, datetime.now(), success, error, record_count],
        )

    def get_stats(self) -> Dict:
        stats = {}

        stats["total_arrests"] = self.conn.execute(
            "SELECT COUNT(*) FROM arrest_incidents"
        ).fetchone()[0]

        stats["unique_persons"] = self.conn.execute(
            "SELECT COUNT(*) FROM arrestees"
        ).fetchone()[0]

        stats["total_charges"] = self.conn.execute(
            "SELECT COUNT(*) FROM charges"
        ).fetchone()[0]

        stats["unique_officers"] = self.conn.execute(
            "SELECT COUNT(*) FROM officers"
        ).fetchone()[0]

        last_sync = self.conn.execute(
            "SELECT downloaded_at FROM sync_log ORDER BY id DESC LIMIT 1"
        ).fetchone()
        stats["last_sync"] = last_sync[0] if last_sync else None

        duplicates_df = self.find_duplicates()
        stats["duplicate_records"] = len(duplicates_df)

        if not duplicates_df.empty:
            stats["duplicate_groups"] = duplicates_df.groupby(
                [
                    "full_name",
                    "age_at_arrest",
                    "gender",
                    "race_ethnicity",
                    "arrest_date",
                    "location",
                ]
            ).ngroups
        else:
            stats["duplicate_groups"] = 0

        return stats

    def get_recent_arrests(self, limit: int = 50) -> pd.DataFrame:
        query = """
            SELECT 
                ai.incident_id,
                a.full_name,
                a.age_at_arrest,
                a.gender,
                a.race_ethnicity,
                ai.arrest_timestamp,
                ai.location,
                o.officer_name,
                c.offense_description,
                c.statute_code,
                c.report_number,
                c.bail_amount,
                c.court_location,
                c.release_code,
                rc.definition as release_definition
            FROM arrest_incidents ai
            JOIN arrestees a ON ai.person_id = a.person_id
            JOIN officers o ON ai.officer_id = o.officer_id
            JOIN charges c ON ai.incident_id = c.incident_id
            LEFT JOIN release_codes rc ON c.release_code = rc.code
            ORDER BY ai.arrest_timestamp DESC
            LIMIT ?
        """
        return self.conn.execute(query, [limit]).df()

    def get_arrests_by_date_range(
        self, start_date: datetime, end_date: datetime
    ) -> pd.DataFrame:
        query = """
            SELECT 
                ai.incident_id,
                a.full_name,
                ai.arrest_timestamp,
                ai.location,
                c.offense_description,
                c.statute_code
            FROM arrest_incidents ai
            JOIN arrestees a ON ai.person_id = a.person_id
            JOIN charges c ON ai.incident_id = c.incident_id
            WHERE ai.arrest_timestamp >= ? AND ai.arrest_timestamp <= ?
            ORDER BY ai.arrest_timestamp DESC
        """
        return self.conn.execute(query, [start_date, end_date]).df()

    def search_by_name(self, first_name: str = "", last_name: str = "") -> pd.DataFrame:
        query = """
            SELECT DISTINCT
                a.person_id,
                a.full_name,
                a.age_at_arrest,
                a.gender,
                a.race_ethnicity,
                (SELECT COUNT(*) FROM arrest_incidents ai2 WHERE ai2.person_id = a.person_id) as arrest_count,
                (SELECT MAX(ai3.arrest_timestamp) FROM arrest_incidents ai3 WHERE ai3.person_id = a.person_id) as last_arrest
            FROM arrestees a
            WHERE 1=1
        """
        params = []

        if last_name:
            query += " AND LOWER(a.full_name) LIKE ?"
            params.append(f"%{last_name.lower()}%")

        if first_name:
            query += " AND LOWER(a.full_name) LIKE ?"
            params.append(f"%{first_name.lower()}%")

        query += " ORDER BY a.full_name LIMIT 100"

        return self.conn.execute(query, params).df()

    def add_favorite(self, person_id: str) -> bool:
        try:
            self.conn.execute(
                "INSERT OR IGNORE INTO favorites (person_id) VALUES (?)", [person_id]
            )
            return True
        except Exception:
            return False

    def remove_favorite(self, person_id: str) -> bool:
        try:
            self.conn.execute("DELETE FROM favorites WHERE person_id = ?", [person_id])
            return True
        except Exception:
            return False

    def is_favorite(self, person_id: str) -> bool:
        result = self.conn.execute(
            "SELECT 1 FROM favorites WHERE person_id = ?", [person_id]
        ).fetchone()
        return result is not None

    def get_favorites(self) -> pd.DataFrame:
        query = """
            SELECT 
                a.person_id,
                a.full_name,
                a.age_at_arrest,
                a.gender,
                a.race_ethnicity,
                f.created_at as favorited_at,
                (SELECT COUNT(*) FROM arrest_incidents ai2 WHERE ai2.person_id = a.person_id) as arrest_count,
                (SELECT MAX(ai3.arrest_timestamp) FROM arrest_incidents ai3 WHERE ai3.person_id = a.person_id) as last_arrest
            FROM favorites f
            JOIN arrestees a ON f.person_id = a.person_id
            ORDER BY f.created_at DESC
        """
        return self.conn.execute(query).df()

    def check_favorite_alerts(self, since_timestamp: datetime = None) -> pd.DataFrame:
        if since_timestamp is None:
            since_timestamp = datetime.now() - timedelta(hours=24)

        query = """
            SELECT 
                a.person_id,
                a.full_name,
                ai.arrest_timestamp,
                ai.location,
                c.offense_description,
                c.statute_code,
                c.bail_amount
            FROM favorites f
            JOIN arrestees a ON f.person_id = a.person_id
            JOIN arrest_incidents ai ON ai.person_id = a.person_id
            JOIN charges c ON c.incident_id = ai.incident_id
            WHERE ai.arrest_timestamp >= ?
            ORDER BY ai.arrest_timestamp DESC
        """
        return self.conn.execute(query, [since_timestamp]).df()

    def close(self):
        self.conn.close()

    def _parse_pdf_timestamp(self, pdf_filename: str) -> Optional[datetime]:
        match = re.search(r"(\d{4}-\d{2}-\d{2}-\d{2}-\d{2}-\d{2})", pdf_filename)
        if match:
            try:
                return datetime.strptime(match.group(1), "%Y-%m-%d-%H-%M-%S")
            except ValueError:
                return None
        return None

    def get_latest_pdf_per_day(
        self, pdf_dir: str = config.HPD_ARCHIVE_DIR
    ) -> Dict[str, str]:
        latest_pdfs = {}
        pdf_path = Path(pdf_dir)

        if not pdf_path.exists():
            return latest_pdfs

        for pdf_file in pdf_path.rglob("*.pdf"):
            ts = self._parse_pdf_timestamp(pdf_file.name)
            if ts:
                date_key = ts.date().isoformat()
                if date_key not in latest_pdfs or ts > self._parse_pdf_timestamp(
                    latest_pdfs[date_key]
                ):
                    latest_pdfs[date_key] = str(pdf_file)

        return latest_pdfs

    def is_duplicate(
        self,
        full_name: str,
        age: int,
        gender: str,
        race: str,
        arrest_timestamp: datetime,
        location: str,
    ) -> Optional[str]:
        result = self.conn.execute(
            """
            SELECT ai.incident_id FROM arrest_incidents ai
            JOIN arrestees a ON ai.person_id = a.person_id
            WHERE a.full_name = ? 
              AND a.age_at_arrest = ? 
              AND a.gender = ? 
              AND a.race_ethnicity = ?
              AND DATE(ai.arrest_timestamp) = DATE(?)
              AND ai.location = ?
        """,
            [full_name, age, gender, race, arrest_timestamp, location],
        ).fetchone()

        return str(result[0]) if result else None

    def find_duplicates(self) -> pd.DataFrame:
        query = """
            WITH record_signature AS (
                SELECT 
                    ai.incident_id,
                    a.full_name,
                    a.age_at_arrest,
                    a.gender,
                    a.race_ethnicity,
                    DATE(ai.arrest_timestamp) as arrest_date,
                    ai.location,
                    ai.source_pdf,
                    ai.pdf_timestamp,
                    ROW_NUMBER() OVER (
                        PARTITION BY a.full_name, a.age_at_arrest, a.gender, 
                                     a.race_ethnicity, DATE(ai.arrest_timestamp), ai.location
                        ORDER BY COALESCE(ai.pdf_timestamp, ai.arrest_timestamp) DESC
                    ) as rn,
                    COUNT(*) OVER (
                        PARTITION BY a.full_name, a.age_at_arrest, a.gender, 
                                     a.race_ethnicity, DATE(ai.arrest_timestamp), ai.location
                    ) as dup_count
                FROM arrest_incidents ai
                JOIN arrestees a ON ai.person_id = a.person_id
            )
            SELECT incident_id, full_name, age_at_arrest, gender, race_ethnicity,
                   arrest_date, location, source_pdf, pdf_timestamp, dup_count
            FROM record_signature
            WHERE rn > 1
            ORDER BY full_name, arrest_date
        """
        return self.conn.execute(query).df()

    def remove_duplicates(self, dry_run: bool = True) -> Tuple[int, List[str]]:
        duplicates = self.find_duplicates()
        if duplicates.empty:
            return 0, []

        incident_ids = duplicates["incident_id"].tolist()
        affected_names = duplicates["full_name"].unique().tolist()

        if not dry_run:
            for inc_id in incident_ids:
                self.conn.execute("DELETE FROM charges WHERE incident_id = ?", [inc_id])
            for inc_id in incident_ids:
                self.conn.execute(
                    "DELETE FROM arrest_incidents WHERE incident_id = ?", [inc_id]
                )

        return len(incident_ids), affected_names

    def cleanup_old_pdfs(
        self, pdf_dir: str = config.HPD_ARCHIVE_DIR, dry_run: bool = True
    ) -> Dict[str, List[str]]:
        latest_pdfs = self.get_latest_pdf_per_day(pdf_dir)

        to_remove = {"kept": [], "removed": []}
        pdf_path = Path(pdf_dir)

        if not pdf_path.exists():
            return to_remove

        for pdf_file in pdf_path.rglob("*.pdf"):
            ts = self._parse_pdf_timestamp(pdf_file.name)
            if ts:
                date_key = ts.date().isoformat()
                if date_key in latest_pdfs and str(pdf_file) != latest_pdfs[date_key]:
                    to_remove["removed"].append(str(pdf_file))
                    if not dry_run:
                        pdf_file.unlink()
                else:
                    to_remove["kept"].append(str(pdf_file))

        return to_remove
