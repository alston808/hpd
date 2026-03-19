HPD_BASE_URL = "https://www.honolulupd.org/information/arrest-logs/"
HPD_ARCHIVE_DIR = "./data/raw_pdfs"
DATABASE_PATH = "./data/hpd_arrests.db"

SCHEDULE_TIMES = ["05:15", "11:15", "17:15", "23:15"]
TIMEZONE = "Pacific/Honolulu"

USER_AGENT = "Mozilla/5.0 (compatible; HPD-Archival-Project/1.0; +https://github.com/hpd-archival)"

PDF_FILENAME_PATTERN = r"(\d{4}-\d{2}-\d{2}-\d{2}-\d{2}-\d{2})_Arrest_Log\.pdf"

RELEASE_CODES = {
    "RBL": "Released, Bail and/or Bond posted",
    "RNC": "Released, No charge",
    "RPC": "Released, Prosecution declined",
    "RPI": "Released, Pending further investigation",
    "ROR": "Released, Own Recognizance",
    "ISC": "Intake Service Center (transferred to OCCC)",
    "DCT": "Taken directly to District Court",
    "CCT": "Taken directly to Circuit Court",
    "FCT": "Taken directly to Family Court",
    "OTH": "Other (e.g., Identification Process under HRS 286-102)",
}

RACE_ETHNICITY_OPTIONS = [
    "Hawaiian",
    "White",
    "Micronesian",
    "Filipino",
    "Black",
    "Japanese",
    "Chinese",
    "Samoan",
    "Laotian",
    "Hispanic",
    "Native American",
    "Indian",
    "Other Asian",
    "Other Pac. Isl",
    "Unknown",
]
