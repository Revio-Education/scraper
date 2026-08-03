#!/usr/bin/env python3
"""Extract the NEET Question Bank from dictors-eye-view-academix.org.

The site is a client-side React app with no server-rendered data. All content
is served live from its Supabase backend (PostgREST) using the public "anon"
key embedded in the site's own JS bundle -- the same call the frontend makes
when you browse the Question Bank. No login is required: the "quick sign up"
modal shown in the UI is a lead-capture prompt only, not an access control --
the underlying API call already fires (and returns data) before that form is
ever submitted.

Pulled tables:
    subjects                      taxonomy: NEET subjects
    chapters                      taxonomy: chapters per subject
    topics                        taxonomy: topics per chapter
    test_portal_questions         question rows, filtered to source_type=question_bank

Writes one raw JSON file per dataset. No normalization, no database, no
image downloads yet -- just a reliable raw pull.
"""

import argparse
import gzip
import json
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

SUPABASE_URL = "https://bfonufxlgxbwjitfmbat.supabase.co"
SUPABASE_ANON_KEY = (
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9."
    "eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImJmb251ZnhsZ3hid2ppdGZtYmF0Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3NTg4ODI0NDMsImV4cCI6MjA3NDQ1ODQ0M30."
    "Sf-ttbE6Si41g42rYHMm3gUeVl4mkInA35nXgKzV4dI"
)
REST = f"{SUPABASE_URL}/rest/v1"
AUTH_HEADERS = {
    "apikey": SUPABASE_ANON_KEY,
    "Authorization": f"Bearer {SUPABASE_ANON_KEY}",
    "Accept-Encoding": "gzip",
    "User-Agent": "eyeview-qbank-scraper/1.0 (raw data pull, python-urllib)",
}

PAGE_SIZE = 1000  # PostgREST caps responses at this regardless of what's requested
RETRYABLE_STATUS = {429, 500, 502, 503, 504}

DATASETS = [
    {"name": "subjects", "table": "subjects", "filters": {}},
    {"name": "chapters", "table": "chapters", "filters": {}},
    {"name": "topics", "table": "topics", "filters": {}},
    {
        "name": "question_bank",
        "table": "test_portal_questions",
        "filters": {"source_type": "eq.question_bank"},
    },
]


def request_with_retry(url, headers, retries=4, backoff=2.0, timeout=30):
    """GET a URL and return (json_body, content_range_header).

    Retries transient failures (timeouts, connection errors, 429/5xx) with
    exponential backoff. Non-retryable errors (e.g. 400/404) raise immediately.
    """
    for attempt in range(1, retries + 1):
        try:
            req = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                raw = resp.read()
                if resp.headers.get("Content-Encoding") == "gzip":
                    raw = gzip.decompress(raw)
                return json.loads(raw.decode("utf-8")), resp.headers.get("Content-Range")
        except urllib.error.HTTPError as exc:
            if exc.code not in RETRYABLE_STATUS or attempt == retries:
                raise
            wait = backoff**attempt
            print(f"    HTTP {exc.code}, retrying in {wait:.1f}s ({attempt}/{retries})", file=sys.stderr)
            time.sleep(wait)
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            if attempt == retries:
                raise
            wait = backoff**attempt
            print(f"    {exc}, retrying in {wait:.1f}s ({attempt}/{retries})", file=sys.stderr)
            time.sleep(wait)


def fetch_all(table, filters, delay=0.3):
    """Fetch every row from a PostgREST table, paging past the 1000-row cap."""
    rows = []
    start = 0
    total = None

    while total is None or start < total:
        end = start + PAGE_SIZE - 1
        query = urllib.parse.urlencode({"select": "*", **filters})
        url = f"{REST}/{table}?{query}"
        headers = {**AUTH_HEADERS, "Range": f"{start}-{end}", "Prefer": "count=exact"}

        page, content_range = request_with_retry(url, headers)
        rows.extend(page)

        if content_range and "/" in content_range:
            total = int(content_range.rsplit("/", 1)[-1])

        if not page or len(page) < PAGE_SIZE:
            break
        start += PAGE_SIZE
        time.sleep(delay)

    return rows


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("-o", "--out", type=Path, default=Path("eyeview_out"), help="output directory (default: eyeview_out)")
    ap.add_argument("--delay", type=float, default=0.3, help="seconds to wait between pages (default: 0.3)")
    args = ap.parse_args()

    args.out.mkdir(parents=True, exist_ok=True)

    ok = 0
    for dataset in DATASETS:
        print(f"{dataset['name']}:")
        try:
            rows = fetch_all(dataset["table"], dataset["filters"], delay=args.delay)
        except (urllib.error.URLError, urllib.error.HTTPError, OSError) as exc:
            print(f"  FAILED: {exc}", file=sys.stderr)
            continue

        out_path = args.out / f"{dataset['name']}.json"
        out_path.write_text(json.dumps(rows, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        print(f"  {len(rows)} rows -> {out_path}")
        ok += 1

    if ok == 0:
        print("nothing extracted", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
