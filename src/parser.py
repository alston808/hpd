import re
import io
import fitz
from PIL import Image
import pytesseract
from datetime import datetime
from typing import List, Dict, Optional, Tuple
from pathlib import Path

import config


class PDFParser:
    def __init__(self):
        self.release_codes = config.RELEASE_CODES

    def _clean_text(self, text: str) -> str:
        if not text:
            return ""
        text = re.sub(r"\s+", " ", text)
        return text.strip()

    def _parse_timestamp(
        self, date_str: str, time_str: str = None
    ) -> Optional[datetime]:
        if not date_str:
            return None

        date_str = self._clean_text(date_str)

        try:
            if time_str:
                dt_str = f"{date_str} {time_str}"
                return datetime.strptime(dt_str, "%m/%d/%Y %H:%M")
            return datetime.strptime(date_str, "%m/%d/%Y")
        except ValueError:
            pass

        try:
            return datetime.strptime(date_str, "%Y-%m-%d")
        except ValueError:
            return None

    def _parse_bail(self, bail_str: str) -> Optional[float]:
        if not bail_str:
            return None
        bail_str = self._clean_text(bail_str)
        numbers = re.findall(r"[\d,]+", bail_str)
        if numbers:
            return float(numbers[0].replace(",", ""))
        return None

    def _parse_offense(self, offense_str: str) -> Tuple[str, str]:
        offense = self._clean_text(offense_str)

        hrs_match = re.search(r"HRS\s+[\d\-\.]+", offense)
        roh_match = re.search(r"RO\s+[\d\-\.]+", offense)

        statute = ""
        if hrs_match:
            statute = hrs_match.group(0)
            offense = offense.replace(hrs_match.group(0), "").strip()
        elif roh_match:
            statute = roh_match.group(0)
            offense = offense.replace(roh_match.group(0), "").strip()

        return offense, statute

    def _parse_demographics(self, demo_str: str) -> Tuple[str, str, int]:
        gender = "Unknown"
        age = 0
        race = "Unknown"

        gender_match = re.search(r"\b(Male|Female|M|F)\b", demo_str, re.IGNORECASE)
        if gender_match:
            g = gender_match.group(1).upper()
            gender = (
                "Male"
                if g in ("M", "MALE")
                else "Female"
                if g in ("F", "FEMALE")
                else g
            )

        age_match = re.search(r"/(\d+)", demo_str)
        if age_match:
            age = int(age_match.group(1))

        for race_opt in config.RACE_ETHNICITY_OPTIONS:
            if race_opt.lower() in demo_str.lower():
                race = race_opt
                break

        return race, gender, age

    def _extract_items(self, page: fitz.Page, zoom: int = 3) -> List[Dict]:
        mat = fitz.Matrix(zoom, zoom)
        pix = page.get_pixmap(matrix=mat)
        img = Image.open(io.BytesIO(pix.tobytes("png")))

        data = pytesseract.image_to_data(img, output_type=pytesseract.Output.DICT)

        skip_words = {
            "Sorted",
            "by:",
            "Arr-Date",
            "Time",
            "Race",
            "S/",
            "Age",
            "Name",
            "Report-",
            "Offense",
            "Offense #",
            "Date:",
            "to",
            "Loc-Of-Arrest",
            "Arrest-Officer",
            "Court-Information",
            "Rel-Date/Time",
            "How-Rel",
            "Disclaimer",
            "call",
            "confirm",
            "if",
            "someone",
            "is",
            "still",
            "in",
            "custody.",
            "HONOLULU",
            "POLICE",
            "DEPARTMENT",
            "Adult",
            "Arrest",
            "Log",
            "Updated",
            "PROB",
            "CONDUCT",
            "VIOLATION",
            "PM",
            "PROCESE",
        }

        items = []
        n_boxes = len(data["text"])
        for i in range(n_boxes):
            text = data["text"][i].strip()
            if text and text not in skip_words:
                items.append(
                    {
                        "text": text,
                        "x": data["left"][i],
                        "y": data["top"][i],
                    }
                )

        return items

    def parse_pdf(self, pdf_path: Path) -> Tuple[List[Dict], int]:
        records = []
        total_rows = 0

        try:
            doc = fitz.open(pdf_path)

            for page_num, page in enumerate(doc):
                items = self._extract_items(page)

                if not items:
                    continue

                sorted_items = sorted(items, key=lambda x: (x["y"], x["x"]))

                i = 0
                while i < len(sorted_items):
                    item = sorted_items[i]
                    text = item["text"]
                    y = item["y"]
                    x = item["x"]

                    if re.match(r"\d{2}/\d{2}/\d{4}", text) and x < 150:
                        record = {
                            "arrest_date": text,
                            "arrest_time": "",
                            "name_parts": [],
                            "race": "",
                            "gender_age": "",
                            "report_number": "",
                            "offense_parts": [],
                            "location_parts": [],
                            "officer_parts": [],
                            "court_parts": [],
                            "bail": "",
                            "release_code": "",
                            "release_date": "",
                            "release_time": "",
                            "end_y": y,
                        }

                        i += 1
                        while i < len(sorted_items):
                            item = sorted_items[i]
                            text = item["text"]
                            y = item["y"]
                            x = item["x"]

                            if y > record["end_y"] + 150:
                                break

                            if re.match(r"\d{2}/\d{2}/\d{4}", text) and x < 150:
                                break

                            if re.match(r"\d{2}:\d{2}", text) and x < 100:
                                record["arrest_time"] = text

                            elif re.match(r"\d{8,}-\d{3}", text):
                                record["report_number"] = text

                            elif 350 <= x < 680:
                                record["name_parts"].append(text)

                            elif text in [
                                "White",
                                "Black",
                                "Hawaiian",
                                "Filipino",
                                "Japanese",
                                "Samoan",
                                "Chinese",
                                "Micronesian",
                                "Laotian",
                                "Hispanic",
                                "Native American",
                                "Indian",
                                "Other Asian",
                                "Other Pac. Isl",
                            ]:
                                record["race"] = text

                            elif re.match(r"[MF]/", text):
                                record["gender_age"] = text

                            elif x >= 680 and x < 1050:
                                record["offense_parts"].append(text)

                            elif "," in text and x >= 1200:
                                record["officer_parts"].append(text)

                            elif x >= 1200 and x < 1900:
                                record["location_parts"].append(text)

                            elif x >= 1900:
                                if re.match(r"\d{2}/\d{2}/\d{4}", text):
                                    if not record["release_date"]:
                                        record["release_date"] = text
                                    else:
                                        record["court_parts"].append(text)
                                elif re.match(r"\d{2}:\d{2}", text):
                                    record["release_time"] = text
                                elif (
                                    "RBL" in text
                                    or "DCT" in text
                                    or "RNC" in text
                                    or "ROR" in text
                                    or "OTH" in text
                                    or "ISC" in text
                                    or "CCT" in text
                                    or "FCT" in text
                                    or "RPC" in text
                                    or "RPI" in text
                                ):
                                    for code in self.release_codes.keys():
                                        if code in text:
                                            record["release_code"] = code
                                            break
                                    bail_match = re.search(r"[\d,]+", text)
                                    if bail_match:
                                        record["bail"] = bail_match.group(0)
                                else:
                                    record["court_parts"].append(text)

                            elif re.match(r"\d{2}:\d{2}", text) and x > 1800:
                                record["release_time"] = text

                            i += 1

                        normalized = self._normalize_record(record)
                        if normalized:
                            records.append(normalized)
                            total_rows += 1
                    else:
                        i += 1

            doc.close()

        except Exception as e:
            print(f"Error parsing {pdf_path}: {e}")

        return records, total_rows

    def _normalize_record(self, record: Dict) -> Optional[Dict]:
        name_parts = record.get("name_parts", [])

        if not name_parts:
            return None

        full_name = " ".join(name_parts)
        if "," in full_name:
            parts = full_name.split(",")
            if len(parts) == 2:
                full_name = f"{parts[0].strip()}, {parts[1].strip()}"

        gender = "Unknown"
        age = 0
        race = "Unknown"

        gender_age_str = record.get("gender_age", "")

        gender_match = re.search(
            r"\b(Male|Female|M|F)\b", gender_age_str, re.IGNORECASE
        )
        if gender_match:
            g = gender_match.group(1).upper()
            gender = (
                "Male"
                if g in ("M", "MALE")
                else "Female"
                if g in ("F", "FEMALE")
                else g
            )

        age_match = re.search(r"/(\d+)", gender_age_str)
        if age_match:
            age = int(age_match.group(1))

        race_str = record.get("race", "")
        for race_opt in config.RACE_ETHNICITY_OPTIONS:
            if race_opt.lower() in race_str.lower():
                race = race_opt
                break

        arrest_ts = self._parse_timestamp(
            record.get("arrest_date", ""), record.get("arrest_time", "")
        )

        offense_parts = record.get("offense_parts", [])
        offense_str = " ".join(offense_parts)
        offense_desc, statute = self._parse_offense(offense_str)
        bail_amount = self._parse_bail(record.get("bail", ""))

        release_ts = None
        release_date = record.get("release_date", "")
        release_time = record.get("release_time", "")
        if release_date:
            release_ts = self._parse_timestamp(
                release_date, release_time if release_time else None
            )

        location = " ".join(record.get("location_parts", []))
        officer_name = " ".join(record.get("officer_parts", []))
        court_location = " ".join(record.get("court_parts", []))

        return {
            "full_name": full_name,
            "age_at_arrest": age,
            "gender": gender,
            "race_ethnicity": race,
            "arrest_timestamp": arrest_ts,
            "location": location,
            "officer_name": officer_name,
            "report_number": record.get("report_number", ""),
            "offense_description": offense_desc,
            "statute_code": statute,
            "bail_amount": bail_amount,
            "court_location": court_location,
            "release_code": record.get("release_code", ""),
            "release_timestamp": release_ts,
        }

    def parse_directory(self, directory: Path) -> Tuple[List[Dict], Dict]:
        all_records = []
        stats = {"files": 0, "records": 0, "errors": 0}

        pdf_files = (
            list(directory.rglob("*.pdf")) if directory.is_dir() else [directory]
        )

        for pdf_path in sorted(pdf_files):
            stats["files"] += 1
            try:
                records, row_count = self.parse_pdf(pdf_path)
                all_records.extend(records)
                stats["records"] += len(records)
            except Exception as e:
                stats["errors"] += 1
                print(f"Error parsing {pdf_path}: {e}")

        return all_records, stats
