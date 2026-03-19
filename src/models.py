from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional
from uuid import UUID, uuid4


@dataclass
class Arrestee:
    person_id: UUID = field(default_factory=uuid4)
    full_name: str = ""
    age_at_arrest: int = 0
    gender: str = ""
    race_ethnicity: str = ""

    def to_dict(self):
        return {
            "person_id": str(self.person_id),
            "full_name": self.full_name,
            "age_at_arrest": self.age_at_arrest,
            "gender": self.gender,
            "race_ethnicity": self.race_ethnicity,
        }


@dataclass
class Officer:
    officer_id: UUID = field(default_factory=uuid4)
    officer_name: str = ""

    def to_dict(self):
        return {
            "officer_id": str(self.officer_id),
            "officer_name": self.officer_name,
        }


@dataclass
class ArrestIncident:
    incident_id: UUID = field(default_factory=uuid4)
    person_id: UUID = None
    officer_id: UUID = None
    arrest_timestamp: datetime = None
    location: str = ""

    def to_dict(self):
        return {
            "incident_id": str(self.incident_id),
            "person_id": str(self.person_id) if self.person_id else None,
            "officer_id": str(self.officer_id) if self.officer_id else None,
            "arrest_timestamp": self.arrest_timestamp.isoformat()
            if self.arrest_timestamp
            else None,
            "location": self.location,
        }


@dataclass
class Charge:
    charge_id: UUID = field(default_factory=uuid4)
    incident_id: UUID = None
    offense_description: str = ""
    statute_code: str = ""
    report_number: str = ""
    bail_amount: Optional[float] = None
    court_location: str = ""
    release_code: str = ""
    release_timestamp: Optional[datetime] = None

    def to_dict(self):
        return {
            "charge_id": str(self.charge_id),
            "incident_id": str(self.incident_id) if self.incident_id else None,
            "offense_description": self.offense_description,
            "statute_code": self.statute_code,
            "report_number": self.report_number,
            "bail_amount": self.bail_amount,
            "court_location": self.court_location,
            "release_code": self.release_code,
            "release_timestamp": self.release_timestamp.isoformat()
            if self.release_timestamp
            else None,
        }


@dataclass
class ReleaseCode:
    code: str
    definition: str

    def to_dict(self):
        return {
            "code": self.code,
            "definition": self.definition,
        }


@dataclass
class SyncLogEntry:
    id: int = 0
    filename: str = ""
    downloaded_at: datetime = None
    parsed: bool = False
    parse_error: str = ""
    record_count: int = 0
