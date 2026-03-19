import requests
import re
import pandas as pd
from functools import lru_cache
from typing import Optional
import streamlit as st

HRS_BASE_URL = "https://www.capitol.hawaii.gov/hrscurrent"
RO_BASE_URL = "https://www.capitol.hawaii.gov"

CACHE_TTL = 86400


def _parse_statute_code(
    code: str,
) -> tuple[Optional[str], Optional[str], Optional[str]]:
    """Parse statute code into (type, chapter, section).

    Returns (type, chapter, section) where type is 'HRS' or 'RO'.
    """
    if not code:
        return None, None, None

    code = code.strip()

    hrs_match = re.match(r"HRS\s*(\d+)[-_]?(\d+(?:\.\d+)?)", code, re.IGNORECASE)
    if hrs_match:
        chapter = hrs_match.group(1)
        section = hrs_match.group(2)
        return "HRS", chapter, section

    ro_match = re.match(r"RO\s*(\d+)[-_]?(\d+(?:\.\d+)?)", code, re.IGNORECASE)
    if ro_match:
        chapter = ro_match.group(1)
        section = ro_match.group(2)
        return "RO", chapter, section

    return None, None, None


def _build_hrs_url(chapter: str, section: str) -> str:
    """Build URL for HRS statute."""
    chapter = int(chapter)

    if 1 <= chapter <= 42:
        volume = "Vol01_Ch0001-0042"
    elif 46 <= chapter <= 115:
        volume = "Vol02_Ch0046-0115"
    elif 121 <= chapter <= 200:
        volume = "Vol03_Ch0121-0200D"
    elif 201 <= chapter <= 257:
        volume = "Vol04_Ch0201-0257"
    elif 261 <= chapter <= 319:
        volume = "Vol05_Ch0261-0319"
    elif 321 <= chapter <= 344:
        volume = "Vol06_Ch0321-0344"
    elif 346 <= chapter <= 398:
        volume = "Vol07_Ch0346-0398A"
    elif 401 <= chapter <= 429:
        volume = "Vol08_Ch0401-0429"
    elif 431 <= chapter <= 435:
        volume = "Vol09_Ch0431-0435H"
    elif 436 <= chapter <= 474:
        volume = "Vol10_Ch0436-0474"
    elif 476 <= chapter <= 490:
        volume = "Vol11_Ch0476-0490"
    else:
        volume = f"Vol{((chapter - 1) // 100) + 1:02d}_Ch{chapter:04d}"

    chapter_str = f"HRS_{chapter:04d}"
    section_padded = section.replace(".", "-")
    return (
        f"{HRS_BASE_URL}/{volume}/{chapter_str}/HRS_{chapter_str}_{section_padded}.htm"
    )


def _extract_section_text(html: str, section: str) -> Optional[str]:
    """Extract the text of a specific section from HRS HTML."""
    section_pattern = section.replace(".", r"\.")
    pattern = rf"<SECTION[^>]*>\s*{section_pattern}\s*</SECTION>\s*<SECTION_TEXT>(.*?)</SECTION_TEXT>"
    match = re.search(pattern, html, re.DOTALL | re.IGNORECASE)

    if not match:
        pattern = rf'<p[^>]*class=["\']?sect["\']?[^>]*>\s*{section_pattern}[^<]*</p>\s*<p[^>]*>(.*?)</p>'
        match = re.search(pattern, html, re.DOTALL | re.IGNORECASE)

    if match:
        text = match.group(1)
        text = re.sub(r"<[^>]+>", " ", text)
        text = re.sub(r"\s+", " ", text)
        text = text.strip()
        return text

    return None


@st.cache_data(ttl=CACHE_TTL, show_spinner=False)
def get_statute_text(code: str) -> Optional[str]:
    """Fetch the actual law text for a statute code."""
    if not code or pd.isna(code):
        return None

    code = str(code).strip()
    if not code:
        return None

    stat_type, chapter, section = _parse_statute_code(code)

    if not stat_type or not chapter or not section:
        return None

    if stat_type == "HRS":
        url = _build_hrs_url(chapter, section)
    else:
        return None

    try:
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            text = _extract_section_text(response.text, section)
            if text:
                text = text[:500] + "..." if len(text) > 500 else text
                return text
    except Exception:
        pass

    return None


def get_statute_info(codes: list) -> dict:
    """Fetch statute text for multiple codes."""
    result = {}
    for code in codes:
        if code and not pd.isna(code):
            text = get_statute_text(code)
            if text:
                result[code] = text
    return result
