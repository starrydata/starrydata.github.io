"""
Fetch the latest Starrydata dataset from Figshare and count data entries.
Outputs json/counts.json for use on starrydata.github.io.
Includes totals and per-project breakdowns for papers, figures, samples, curves.
"""

import csv
import io
import json
import os
import zipfile
from collections import Counter, defaultdict
from datetime import datetime, timezone

import requests

FIGSHARE_PROJECT_ID = 155129
FIGSHARE_API = "https://api.figshare.com/v2"
OUTPUT_PATH = os.path.join(os.path.dirname(__file__), "..", "json", "counts.json")


def get_latest_article():
    url = f"{FIGSHARE_API}/projects/{FIGSHARE_PROJECT_ID}/articles"
    resp = requests.get(url, params={"page_size": 1}, timeout=30)
    resp.raise_for_status()
    articles = resp.json()
    if not articles:
        raise RuntimeError("No articles found in project")
    return articles[0]


def get_download_url(article_id):
    url = f"{FIGSHARE_API}/articles/{article_id}/files"
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    files = resp.json()
    for f in files:
        if f["name"].endswith(".zip"):
            return f["download_url"], f["name"]
    raise RuntimeError("No ZIP file found in article")


def read_csv_from_zip(zip_bytes, filename_keyword):
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        for name in zf.namelist():
            if filename_keyword in name and name.endswith(".csv"):
                with zf.open(name) as f:
                    text = f.read().decode("utf-8-sig", errors="replace")
                    return list(csv.DictReader(io.StringIO(text)))
    return []


def parse_projects(raw):
    if not raw:
        return []
    try:
        plist = json.loads(raw)
        return plist if isinstance(plist, list) else [raw]
    except (json.JSONDecodeError, TypeError):
        return [raw]


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
    samples_rows = read_csv_from_zip(zip_bytes, "samples")
    curves_rows = read_csv_from_zip(zip_bytes, "curves")

    total_samples = len(samples_rows)

    # Aggregate from curves
    total_curves = 0
    all_sids = set()
    all_figures = set()
    all_sample_ids = set()

    proj_papers = defaultdict(set)
    proj_figures = defaultdict(set)
    proj_samples = defaultdict(set)
    proj_curves = Counter()

    for row in curves_rows:
        total_curves += 1
        sid = row.get("SID", "").strip()
        fig_id = row.get("figure_id", "").strip()
        sample_id = row.get("sample_id", "").strip()
        projects = parse_projects(row.get("project_names", "").strip())

        if sid:
            all_sids.add(sid)
        if fig_id:
            all_figures.add(fig_id)
        if sample_id:
            all_sample_ids.add(sample_id)

        for p in projects:
            proj_curves[p] += 1
            if sid:
                proj_papers[p].add(sid)
            if fig_id:
                proj_figures[p].add(fig_id)
            if sample_id:
                proj_samples[p].add(sample_id)

    total_papers_with_data = len(all_sids)
    total_figures = len(all_figures)

    # Build per-project summary
    all_project_names = sorted(
        set(proj_curves.keys()),
        key=lambda x: -proj_curves[x],
    )
    projects = {}
    for p in all_project_names:
        projects[p] = {
            "papers": len(proj_papers[p]),
            "figures": len(proj_figures[p]),
            "samples": len(proj_samples[p]),
            "curves": proj_curves[p],
        }

    counts = {
        "papers": total_papers_with_data,
        "figures": total_figures,
        "samples": total_samples,
        "curves": total_curves,
        "projects": projects,
        "dataset_title": title,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }

    print(f"\n  Papers with data: {total_papers_with_data:,}")
    print(f"  Figures:          {total_figures:,}")
    print(f"  Samples:          {total_samples:,}")
    print(f"  Curves:           {total_curves:,}")
    print(f"\n  Projects ({len(projects)}):")
    for name, d in projects.items():
        print(
            f"    {name}: {d['papers']:,} papers, "
            f"{d['figures']:,} figures, {d['samples']:,} samples, "
            f"{d['curves']:,} curves"
        )

    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    with open(OUTPUT_PATH, "w") as f:
        json.dump(counts, f, indent=2)

    print(f"\nWritten to {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
