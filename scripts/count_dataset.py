"""
Fetch the latest Starrydata dataset from Figshare and count data entries.
Outputs json/counts.json for use on starrydata.github.io.
"""

import io
import json
import os
import zipfile
from datetime import datetime, timezone

import requests

FIGSHARE_PROJECT_ID = 155129
FIGSHARE_API = "https://api.figshare.com/v2"
OUTPUT_PATH = os.path.join(os.path.dirname(__file__), "..", "json", "counts.json")


def get_latest_article():
    """Get the most recent article from the Figshare project."""
    url = f"{FIGSHARE_API}/projects/{FIGSHARE_PROJECT_ID}/articles"
    resp = requests.get(url, params={"page_size": 1}, timeout=30)
    resp.raise_for_status()
    articles = resp.json()
    if not articles:
        raise RuntimeError("No articles found in project")
    return articles[0]


def get_download_url(article_id):
    """Get the ZIP file download URL for an article."""
    url = f"{FIGSHARE_API}/articles/{article_id}/files"
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    files = resp.json()
    for f in files:
        if f["name"].endswith(".zip"):
            return f["download_url"], f["name"]
    raise RuntimeError("No ZIP file found in article")


def count_csv_rows(zip_bytes, filename_keyword):
    """Count data rows (excluding header) in a CSV inside the ZIP."""
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        for name in zf.namelist():
            if filename_keyword in name and name.endswith(".csv"):
                with zf.open(name) as f:
                    lines = f.read().decode("utf-8", errors="replace").splitlines()
                    # Subtract 1 for header row
                    return max(0, len(lines) - 1)
    return 0


def main():
    print("Fetching latest article from Figshare...")
    article = get_latest_article()
    article_id = article["id"]
    title = article.get("title", "")
    print(f"  Article: {title} (ID: {article_id})")

    print("Getting download URL...")
    download_url, zip_name = get_download_url(article_id)
    print(f"  File: {zip_name}")

    print("Downloading ZIP...")
    resp = requests.get(download_url, timeout=300)
    resp.raise_for_status()
    zip_bytes = resp.content
    print(f"  Size: {len(zip_bytes) / 1024 / 1024:.1f} MB")

    print("Counting rows...")
    papers = count_csv_rows(zip_bytes, "papers")
    samples = count_csv_rows(zip_bytes, "samples")
    curves = count_csv_rows(zip_bytes, "curves")

    counts = {
        "papers": papers,
        "samples": samples,
        "curves": curves,
        "dataset_title": title,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }

    print(f"  Papers:  {papers:,}")
    print(f"  Samples: {samples:,}")
    print(f"  Curves:  {curves:,}")

    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    with open(OUTPUT_PATH, "w") as f:
        json.dump(counts, f, indent=2)

    print(f"Written to {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
