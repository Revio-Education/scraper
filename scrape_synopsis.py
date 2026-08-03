#!/usr/bin/env python3
"""Extract the question banks from synopsis-neet-pg-special.pages.dev.

The site ships its whole corpus as static JS files, so this is a download and a
parse rather than a crawl. Two banks are pulled:

    data.js           5140 past-year questions (AIIMS / INI-CET / NEET)
    core_btr_data.js  7065 CoreBTR qbank questions

Writes SQLite and JSONL. Images are opt-in via --images because the CoreBTR half
resolves to that company's own production S3 bucket rather than to the site.
"""

import argparse
import gzip
import json
import re
import sqlite3
import sys
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

BASE = "https://synopsis-neet-pg-special.pages.dev"
UA = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/150.0 Safari/537.36"

QUESTION_BANKS = [
    {"file": "data.js", "var": "mcqData", "source": "pyq"},
    {"file": "core_btr_data.js", "var": "coreBtrData", "source": "corebtr"},
]

MARROW_RR = {"file": "marrow_rr_data.js", "var": "marrowRRData", "source": "marrow_rr"}

DECODER = json.JSONDecoder()

SCHEMA = """
CREATE TABLE IF NOT EXISTS questions (
    id                TEXT PRIMARY KEY,
    source            TEXT NOT NULL,
    num               TEXT,
    question          TEXT NOT NULL,
    option_a          TEXT,
    option_b          TEXT,
    option_c          TEXT,
    option_d          TEXT,
    answer            TEXT,
    explanation       TEXT,
    subject           TEXT,
    chapter           TEXT,
    year              TEXT,
    tags              TEXT,
    image             TEXT,
    explanation_image TEXT
);
CREATE INDEX IF NOT EXISTS idx_subject ON questions(subject);
CREATE INDEX IF NOT EXISTS idx_source  ON questions(source);
CREATE INDEX IF NOT EXISTS idx_year    ON questions(year);

CREATE TABLE IF NOT EXISTS marrow_notes (
    subject TEXT NOT NULL,
    chapter TEXT NOT NULL,
    type    TEXT,
    PRIMARY KEY (subject, chapter)
);
CREATE TABLE IF NOT EXISTS marrow_pages (
    subject   TEXT NOT NULL,
    chapter   TEXT NOT NULL,
    page      TEXT NOT NULL PRIMARY KEY,
    page_index INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_marrow_pages_note ON marrow_pages(subject, chapter, page_index);

-- Deduped on stem+options, not stem alone: many image-based questions share a
-- generic stem ("Match the following:") and are otherwise unrelated, so keying
-- on the stem would collapse distinct questions. Rows whose stem is an unresolved
-- template artifact ($1a, $19, ...) are corrupt upstream and excluded.
CREATE VIEW IF NOT EXISTS questions_clean AS
SELECT * FROM questions
WHERE question NOT GLOB '$[0-9a-f]*'
GROUP BY question, option_a, option_b, option_c, option_d;
"""


def fetch(url, timeout=120):
    req = urllib.request.Request(url, headers={"User-Agent": UA, "Accept-Encoding": "gzip"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        raw = r.read()
        if r.headers.get("Content-Encoding") == "gzip":
            raw = gzip.decompress(raw)
        return raw


def parse_assignment(text, var):
    """Pull the JSON value assigned to `var` out of the JS source.

    raw_decode rather than a regex: the payload contains newlines and escaped
    quotes that a non-greedy pattern silently truncates.
    """
    match = re.search(rf"(?:const|let|var)\s+{re.escape(var)}\s*=\s*", text)
    if not match:
        raise ValueError(f"could not locate `{var}` assignment")
    obj, _ = DECODER.raw_decode(text, match.end())
    return obj


def normalize(row, source):
    opts = row.get("options") or {}
    return (
        row.get("id"),
        source,
        row.get("num"),
        (row.get("question") or "").strip(),
        opts.get("a"),
        opts.get("b"),
        opts.get("c"),
        opts.get("d"),
        (row.get("answer") or "").strip().lower() or None,
        row.get("explanation"),
        row.get("subject"),
        row.get("chapter"),
        row.get("year"),
        json.dumps(row.get("tags") or [], ensure_ascii=False),
        row.get("image"),
        row.get("explanation_image"),
    )


def image_urls(rows):
    """Map remote URL -> local filename for every image referenced.

    data.js stores bare filenames served from /images/; core_btr_data.js stores
    absolute URLs into CoreBTR's S3 bucket.
    """
    out = {}
    for row in rows:
        for field in ("image", "explanation_image"):
            ref = row.get(field)
            if not ref:
                continue
            if ref.startswith(("http://", "https://")):
                out[ref] = ref.rsplit("/", 1)[-1]
            else:
                out[f"{BASE}/images/{ref}"] = ref
    return out


def marrow_image_urls(notes):
    """Map every Marrow RR note page to its public image URL."""
    return {f"{BASE}/images/{page}": page for note in notes for page in note.get("pages", [])}


def download_images(mapping, dest, workers):
    dest.mkdir(parents=True, exist_ok=True)
    pending = {u: n for u, n in mapping.items() if not (dest / n).exists()}
    skipped = len(mapping) - len(pending)
    if not pending:
        print(f"  images: all {len(mapping)} already present")
        return
    print(f"  images: {len(pending)} to fetch ({skipped} already on disk)")

    failures = []

    def grab(item):
        url, name = item
        try:
            data = fetch(url, timeout=90)
        except (urllib.error.URLError, urllib.error.HTTPError, OSError) as exc:
            failures.append((url, str(exc)))
            return 0
        # A missing asset on Pages returns the SPA shell with a 200, so reject HTML.
        if data[:15].lstrip().lower().startswith(b"<!doctype"):
            failures.append((url, "SPA fallback (asset missing)"))
            return 0
        (dest / name).write_bytes(data)
        return 1

    with ThreadPoolExecutor(max_workers=workers) as ex:
        done = 0
        for got in ex.map(grab, pending.items()):
            done += got
            if done and done % 250 == 0:
                print(f"    {done}/{len(pending)}")
    print(f"  images: {done} saved, {len(failures)} failed")
    if failures:
        log = dest.parent / "image_failures.txt"
        log.write_text("\n".join(f"{u}\t{e}" for u, e in failures), encoding="utf-8")
        print(f"  failures logged to {log}")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("-o", "--out", type=Path, default=Path("out"), help="output directory (default: out)")
    ap.add_argument("--images", action="store_true", help="also download question images")
    ap.add_argument("--workers", type=int, default=8, help="concurrent image downloads (default: 8)")
    args = ap.parse_args()

    args.out.mkdir(parents=True, exist_ok=True)
    db_path = args.out / "questions.db"
    conn = sqlite3.connect(db_path)
    conn.executescript(SCHEMA)

    all_rows = []
    for bank in QUESTION_BANKS:
        url = f"{BASE}/{bank['file']}"
        print(f"{bank['file']}:")
        try:
            text = fetch(url).decode("utf-8", "replace")
        except (urllib.error.URLError, urllib.error.HTTPError, OSError) as exc:
            print(f"  FAILED to download: {exc}", file=sys.stderr)
            continue
        try:
            rows = parse_assignment(text, bank["var"])
        except ValueError as exc:
            print(f"  FAILED to parse: {exc}", file=sys.stderr)
            continue
        if not isinstance(rows, list):
            print(f"  FAILED to parse: `{bank['var']}` is {type(rows).__name__}, expected list", file=sys.stderr)
            continue
        print(f"  {len(text) / 1_048_576:.1f} MB -> {len(rows)} questions")

        conn.executemany(
            "INSERT OR REPLACE INTO questions VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            [normalize(r, bank["source"]) for r in rows],
        )
        conn.commit()

        jsonl = args.out / f"{bank['source']}.jsonl"
        with jsonl.open("w", encoding="utf-8") as fh:
            for r in rows:
                fh.write(json.dumps(r, ensure_ascii=False) + "\n")
        print(f"  wrote {jsonl}")
        all_rows.extend(rows)

    # Marrow RR is a separate note/page corpus, rather than a question bank.
    marrow_notes = []
    print(f"{MARROW_RR['file']}:")
    try:
        marrow_text = fetch(f"{BASE}/{MARROW_RR['file']}").decode("utf-8", "replace")
        marrow = parse_assignment(marrow_text, MARROW_RR["var"])
        marrow_notes = marrow.get("notes", []) if isinstance(marrow, dict) else []
        if not isinstance(marrow_notes, list):
            raise ValueError("`notes` is not a list")
    except (urllib.error.URLError, urllib.error.HTTPError, OSError, ValueError) as exc:
        print(f"  FAILED to download or parse: {exc}", file=sys.stderr)
    else:
        (args.out / "marrow_rr.json").write_text(json.dumps(marrow, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        conn.execute("DELETE FROM marrow_pages")
        conn.execute("DELETE FROM marrow_notes")
        conn.executemany(
            "INSERT INTO marrow_notes VALUES (?,?,?)",
            [(n.get("subject"), n.get("chapter"), n.get("type")) for n in marrow_notes],
        )
        conn.executemany(
            "INSERT INTO marrow_pages VALUES (?,?,?,?)",
            [
                (n.get("subject"), n.get("chapter"), page, index)
                for n in marrow_notes
                for index, page in enumerate(n.get("pages", []), start=1)
            ],
        )
        conn.commit()
        page_count = sum(len(n.get("pages", [])) for n in marrow_notes)
        print(f"  {len(marrow_notes)} notes, {page_count} pages -> wrote {args.out / 'marrow_rr.json'}")

    if not all_rows and not marrow_notes:
        print("nothing extracted", file=sys.stderr)
        return 1

    total = conn.execute("SELECT COUNT(*) FROM questions").fetchone()[0]
    with_ans = conn.execute("SELECT COUNT(*) FROM questions WHERE answer IS NOT NULL").fetchone()[0]
    with_exp = conn.execute("SELECT COUNT(*) FROM questions WHERE explanation <> ''").fetchone()[0]
    clean = conn.execute("SELECT COUNT(*) FROM questions_clean").fetchone()[0]
    print(f"\n{db_path}: {total} questions, {with_ans} with answers, {with_exp} with explanations")
    for src, n in conn.execute("SELECT source, COUNT(*) FROM questions GROUP BY source"):
        print(f"  {src}: {n}")
    print(f"  questions_clean view: {clean} unique ({total - clean} duplicate/corrupt rows filtered)")
    conn.close()

    mapping = image_urls(all_rows)
    mapping.update(marrow_image_urls(marrow_notes))
    if args.images:
        download_images(mapping, args.out / "images", args.workers)
    else:
        local = sum(1 for u in mapping if u.startswith(BASE))
        print(f"\n{len(mapping)} images referenced ({local} on-site, {len(mapping) - local} on CoreBTR's S3)")
        print("re-run with --images to download them")
    return 0


if __name__ == "__main__":
    sys.exit(main())
