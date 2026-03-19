import os
import re
import hashlib
import requests
from datetime import datetime
from typing import List, Optional, Tuple
from bs4 import BeautifulSoup
from pathlib import Path

import config


class HPDFetcher:
    def __init__(self, archive_dir: str = config.HPD_ARCHIVE_DIR):
        self.base_url = config.HPD_BASE_URL
        self.archive_dir = Path(archive_dir)
        self.pattern = re.compile(config.PDF_FILENAME_PATTERN)
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.5",
                "Accept-Encoding": "gzip, deflate, br",
                "Connection": "keep-alive",
                "Upgrade-Insecure-Requests": "1",
            }
        )

    def _get_page_content(self) -> Optional[BeautifulSoup]:
        try:
            response = self.session.get(self.base_url, timeout=30)
            response.raise_for_status()
            return BeautifulSoup(response.content, "lxml")
        except requests.RequestException as e:
            print(f"Error fetching page: {e}")
            return None

    def find_pdf_links(self) -> List[Tuple[str, str]]:
        soup = self._get_page_content()
        if not soup:
            return []

        links = []
        for link in soup.find_all("a", href=True):
            href = link["href"]
            if href.endswith(".pdf") and self.pattern.search(href):
                full_url = (
                    href
                    if href.startswith("http")
                    else f"https://www.honolulupd.org{href}"
                )
                filename = os.path.basename(href)
                links.append((full_url, filename))
        return links

    def _get_file_hash(self, content: bytes) -> str:
        return hashlib.md5(content).hexdigest()

    def _get_existing_hashes(self) -> set:
        hash_file = self.archive_dir / ".downloaded_hashes"
        if hash_file.exists():
            with open(hash_file, "r") as f:
                return set(line.strip() for line in f)
        return set()

    def _save_hash(self, file_hash: str):
        hash_file = self.archive_dir / ".downloaded_hashes"
        with open(hash_file, "a") as f:
            f.write(f"{file_hash}\n")

    def download_pdf(self, url: str, filename: str) -> Tuple[bool, str]:
        self.archive_dir.mkdir(parents=True, exist_ok=True)

        existing_hashes = self._get_existing_hashes()

        try:
            response = self.session.get(url, timeout=60, stream=True)
            response.raise_for_status()

            content = b"".join(response.iter_content(chunk_size=8192))
            file_hash = self._get_file_hash(content)

            if file_hash in existing_hashes:
                return True, "Already downloaded"

            match = self.pattern.search(filename)
            if match:
                date_str = match.group(1)
                date_dir = self.archive_dir / date_str[:10]
                date_dir.mkdir(parents=True, exist_ok=True)
                filepath = date_dir / filename
            else:
                filepath = self.archive_dir / filename

            with open(filepath, "wb") as f:
                f.write(content)

            self._save_hash(file_hash)
            return True, str(filepath)

        except requests.RequestException as e:
            return False, str(e)

    def fetch_all_current(self) -> List[Tuple[str, str, str]]:
        links = self.find_pdf_links()
        results = []

        for url, filename in links:
            success, message = self.download_pdf(url, filename)
            status = "Downloaded" if success else f"Failed: {message}"
            results.append((filename, url, status))

        return results

    def get_archived_pdfs(self) -> List[Path]:
        pdfs = []
        for pdf_path in self.archive_dir.rglob("*.pdf"):
            pdfs.append(pdf_path)
        return sorted(pdfs, key=lambda p: p.stat().st_mtime, reverse=True)

    def get_stats(self) -> dict:
        pdfs = self.get_archived_pdfs()
        total_size = sum(p.stat().st_size for p in pdfs)

        date_dirs = set()
        for p in pdfs:
            if p.parent.name and p.parent.name.count("-") == 2:
                date_dirs.add(p.parent.name)

        return {
            "total_pdfs": len(pdfs),
            "total_size_mb": round(total_size / (1024 * 1024), 2),
            "unique_dates": len(date_dirs),
            "latest_date": max(date_dirs) if date_dirs else None,
        }
