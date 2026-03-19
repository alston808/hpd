from typing import Optional
import urllib.parse


VINELINK_BASE = "https://vinelink.vineapps.com/search/HI/Person"
JAILEXCHANGE_BASE = "https://www.jailexchange.com/inmate-search"


def build_vinelink_url(name: Optional[str]) -> str:
    """Build VineLink search URL for a person's name."""
    if not name or not isinstance(name, str):
        return VINELINK_BASE

    parts = name.strip().split()
    if len(parts) >= 2:
        fname = parts[0]
        lname = parts[-1]
    elif len(parts) == 1:
        fname = parts[0]
        lname = ""
    else:
        return VINELINK_BASE

    params = {}
    if fname:
        params["fname"] = fname
    if lname:
        params["lname"] = lname

    query = urllib.parse.urlencode(params)
    return f"{VINELINK_BASE}?{query}" if query else VINELINK_BASE


def build_jailexchange_url(name: Optional[str]) -> str:
    """Build Jailexchange search URL for a person's name."""
    if not name or not isinstance(name, str):
        return f"{JAILEXCHANGE_BASE}?state=HI"

    parts = name.strip().split()
    if len(parts) >= 2:
        fname = parts[0]
        lname = parts[-1]
    elif len(parts) == 1:
        fname = parts[0]
        lname = ""
    else:
        return f"{JAILEXCHANGE_BASE}?state=HI"

    params = {}
    if fname:
        params["fname"] = fname
    if lname:
        params["lname"] = lname

    query = urllib.parse.urlencode(params)
    return (
        f"{JAILEXCHANGE_BASE}?state=HI&{query}"
        if query
        else f"{JAILEXCHANGE_BASE}?state=HI"
    )


def get_lookup_buttons(name: Optional[str]) -> str:
    """Generate HTML for VineLink and Jailexchange search buttons."""
    if not name:
        return ""

    vl_url = build_vinelink_url(name)
    je_url = build_jailexchange_url(name)

    return f'''<div class="lookup-buttons">
        <a href="{vl_url}" target="_blank" class="lookup-btn vinelink-btn" title="Search on VineLink">
            <svg width="14" height="14" viewBox="0 0 24 24" fill="currentColor"><path d="M12 2C6.48 2 2 6.48 2 12s4.48 10 10 10 10-4.48 10-10S17.52 2 12 2zm-2 15l-5-5 1.41-1.41L10 14.17l7.59-7.59L19 8l-9 9z"/></svg>
            VineLink
        </a>
        <a href="{je_url}" target="_blank" class="lookup-btn jailexchange-btn" title="Search on Jailexchange">
            <svg width="14" height="14" viewBox="0 0 24 24" fill="currentColor"><path d="M12 2C6.48 2 2 6.48 2 12s4.48 10 10 10 10-4.48 10-10S17.52 2 12 2zm-2 15l-5-5 1.41-1.41L10 14.17l7.59-7.59L19 8l-9 9z"/></svg>
            JailExchange
        </a>
    </div>'''
