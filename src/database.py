import os
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
                location VARCHAR
            )
        """)

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

    def insert_arrest(self, record: Dict) -> Tuple[bool, str]:
        try:
            person_id = self.get_or_create_person_id(
                record["full_name"],
                record["age_at_arrest"],
                record["gender"],
                record["race_ethnicity"],
            )

            officer_id = self.get_or_create_officer_id(record["officer_name"])

            incident_id = str(uuid4())
            self.conn.execute(
                """
                INSERT INTO arrest_incidents (incident_id, person_id, officer_id, arrest_timestamp, location)
                VALUES (?, ?, ?, ?, ?)
            """,
                [
                    incident_id,
                    person_id,
                    officer_id,
                    record["arrest_timestamp"],
                    record["location"],
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

    def insert_batch(self, records: List[Dict]) -> Tuple[int, int]:
        success_count = 0
        error_count = 0

        for record in records:
            success, _ = self.insert_arrest(record)
            if success:
                success_count += 1
            else:
                error_count += 1

        return success_count, error_count

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
