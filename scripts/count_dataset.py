"""
Fetch the latest Starrydata dataset from Figshare and count data entries.
Outputs json/counts.json for use on starrydata.github.io.
Includes both total counts and per-project breakdowns.
"""

import csv
import io
import json
import os
import zipfile
from collections import Counter
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


def read_csv_from_zip(zip_bytes, filename_keyword):
    """Read a CSV file from the ZIP and return rows as list of dicts."""
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        for name in zf.namelist():
            if filename_keyword in name and name.endswith(".csv"):
                with zf.open(name) as f:
                    text = f.read().decode("utf-8-sig", errors="replace")
                    return list(csv.DictReader(io.StringIO(text)))
    return []


def count_by_project(rows):
    """Count rows per project from project_names column (JSON array strings)."""
    counter = Counter()
    for row in rows:
        raw = row.get("project_names", "").strip()
        if raw:
            try:
                plist = json.loads(raw)
                for p in plist:
                    counter[p] += 1
            except (json.JSONDecodeError, TypeError):
                counter[raw] += 1
    return dict(counter.most_common())


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

    print("Reading CSVs...")
    papers_rows = read_csv_from_zip(zip_bytes, "papers")
    samples_rows = read_csv_from_zip(zip_bytes, "samples")
    curves_rows = read_csv_from_zip(zip_bytes, "curves")

    print("Counting totals...")
    total_papers = len(papers_rows)
    total_samples = len(samples_rows)
    total_curves = len(curves_rows)

    print("Counting per project...")
    papers_by_project = count_by_project(papers_rows)
    curves_by_project = count_by_project(curves_rows)

    # Build per-project summary
    all_projects = sorted(set(papers_by_project) | set(curves_by_project))
    projects = {}
    for p in all_projects:
        projects[p] = {
            "papers": papers_by_project.get(p, 0),
            "curves": curves_by_project.get(p, 0),
        }

    counts = {
        "papers": total_papers,
        "samples": total_samples,
        "curves": total_curves,
        "projects": projects,
        "dataset_title": title,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }

    print(f"\n  Total Papers:  {total_papers:,}")
    print(f"  Total Samples: {total_samples:,}")
    print(f"  Total Curves:  {total_curves:,}")
    print(f"\n  Projects ({len(projects)}):")
    for name, data in sorted(projects.items(), key=lambda x: -x[1]["curves"]):
        print(f"    {name}: {data['papers']:,} papers, {data['curves']:,} curves")

    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    with open(OUTPUT_PATH, "w") as f:
        json.dump(counts, f, indent=2)

    print(f"\nWritten to {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
