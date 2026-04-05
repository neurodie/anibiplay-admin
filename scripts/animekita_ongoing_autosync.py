#!/usr/bin/env python3
"""
AnimeKita ongoing auto-sync bot.

Flow:
1) Fetch ongoing list from AnimeKita API
2) Check slug in DB table series (source_platform=animekita)
3) If missing: auto-insert series + genres
4) Sync episodes + streams for each slug (single-slug logic)

Usage:
  python3 scripts/animekita_ongoing_autosync.py
  python3 scripts/animekita_ongoing_autosync.py --slug niwatori-fighter
  python3 scripts/animekita_ongoing_autosync.py --limit 20
  python3 scripts/animekita_ongoing_autosync.py --dry-run
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional, Set, Tuple
from urllib.parse import quote

try:
    import requests
except Exception as exc:  # pragma: no cover
    print("ERROR: requests not installed. Install with: pip install requests", file=sys.stderr)
    raise

try:
    import pymysql
    from pymysql.cursors import DictCursor
except Exception as exc:  # pragma: no cover
    print("ERROR: pymysql not installed. Install with: pip install pymysql", file=sys.stderr)
    raise


DEFAULT_ONGOING_URL = "https://apps.animekita.org/api/v1.2.5/home/ongoing.php"
DEFAULT_SERIES_URL_TEMPLATE = "https://apps.animekita.org/api/v1.2.5/series.php?url={slug}"
DEFAULT_EPISODE_URL_TEMPLATE = "https://apps.animekita.org/api/v1.2.5/series/episode/data.php?url={chapter_slug}"


def load_dotenv(path: str = ".env") -> None:
    if not os.path.exists(path):
        return
    with open(path, "r", encoding="utf-8") as fh:
        for line in fh:
            s = line.strip()
            if not s or s.startswith("#") or "=" not in s:
                continue
            key, val = s.split("=", 1)
            key = key.strip()
            val = val.strip()
            if val.startswith('"') and val.endswith('"') and len(val) >= 2:
                val = val[1:-1]
            if key and key not in os.environ:
                os.environ[key] = val


def clean_slug(value: object) -> str:
    s = str(value or "").strip()
    return re.sub(r"^/+|/+$", "", s)


def first_non_empty(*values: object) -> str:
    for v in values:
        s = str(v or "").strip()
        if s:
            return s
    return ""


def clean_synopsis(text: object) -> str:
    s = str(text or "")
    s = re.sub(r"Nonton Anime tanpa iklan di Aplikasi AnimeLovers V3\s*", "", s, flags=re.IGNORECASE)
    return s.strip()


def parse_genres(raw: object) -> List[str]:
    out: List[str] = []
    seen: Set[str] = set()
    if isinstance(raw, list):
        values = [str(x or "").strip() for x in raw]
    elif isinstance(raw, str):
        values = [x.strip() for x in raw.split(",")]
    else:
        values = []
    for item in values:
        if not item:
            continue
        key = item.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(item)
    return out


def fetch_json(url: str, timeout_sec: int = 25, max_attempts: int = 4) -> dict:
    last_err: Optional[Exception] = None
    for i in range(1, max_attempts + 1):
        try:
            resp = requests.get(
                url,
                timeout=timeout_sec,
                headers={
                    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
                    "Accept": "application/json,text/plain,*/*",
                },
            )
            resp.raise_for_status()
            return resp.json()
        except Exception as exc:
            last_err = exc
            if i < max_attempts:
                time.sleep(0.35 * i)
            else:
                break
    raise RuntimeError(f"fetch failed ({url}): {last_err}")


def extract_ongoing_slugs(ongoing_json: object) -> List[str]:
    items: List[dict] = []
    if isinstance(ongoing_json, dict):
        data = ongoing_json.get("data")
        if isinstance(data, list):
            items = [x for x in data if isinstance(x, dict)]
    elif isinstance(ongoing_json, list):
        items = [x for x in ongoing_json if isinstance(x, dict)]

    out: List[str] = []
    seen: Set[str] = set()
    for row in items:
        slug = clean_slug(first_non_empty(row.get("series_id"), row.get("url"), row.get("slug")))
        if not slug or slug in seen:
            continue
        seen.add(slug)
        out.append(slug)
    return out


@dataclass
class SeriesPayload:
    source_platform: str
    source_series_id: int
    content_origin: str
    title: str
    title2: str
    series_slug: str
    cover_url: str
    type: str
    status: str
    rating: Optional[float]
    published_text: str
    author: str
    synopsis: str
    genres: List[str]


def map_series_payload(remote: dict, slug: str) -> SeriesPayload:
    source_series_id = int(float(first_non_empty(remote.get("id"), 0) or 0))
    if source_series_id <= 0:
        raise ValueError(f"source_series_id tidak valid untuk slug={slug}")

    rating_val: Optional[float] = None
    rating_raw = first_non_empty(remote.get("rating"), remote.get("score"))
    if rating_raw:
        try:
            rating_val = float(rating_raw)
        except Exception:
            rating_val = None

    return SeriesPayload(
        source_platform="animekita",
        source_series_id=source_series_id,
        content_origin="anime",
        title=first_non_empty(remote.get("judul"), remote.get("title"), remote.get("name")),
        title2=first_non_empty(
            remote.get("title2"),
            remote.get("title_alt"),
            remote.get("judul2"),
            remote.get("english_title"),
            remote.get("en_title"),
            remote.get("alt_title"),
        ),
        series_slug=clean_slug(first_non_empty(remote.get("series_id"), remote.get("url"), slug)),
        cover_url=first_non_empty(remote.get("cover"), remote.get("cover_url"), remote.get("image"), remote.get("poster")),
        type=first_non_empty(remote.get("type"), remote.get("format")),
        status=first_non_empty(remote.get("status")),
        rating=rating_val,
        published_text=first_non_empty(remote.get("published"), remote.get("published_text"), remote.get("date")),
        author=first_non_empty(remote.get("author"), remote.get("studio"), remote.get("producer")),
        synopsis="-",
        genres=parse_genres(remote.get("genre") or remote.get("genres")),
    )


def get_mysql_conn():
    host = os.getenv("DB_HOST", "127.0.0.1")
    port = int(os.getenv("DB_PORT", "3306"))
    user = os.getenv("DB_USER", "root")
    password = os.getenv("DB_PASS", "")
    database = os.getenv("DB_NAME", "anime")
    return pymysql.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        database=database,
        charset="utf8mb4",
        autocommit=False,
        cursorclass=DictCursor,
    )


def get_series_by_slug(cur, slug: str) -> Optional[dict]:
    cur.execute(
        """
        SELECT id, source_series_id, series_slug, title
        FROM series
        WHERE source_platform = 'animekita' AND series_slug = %s
        LIMIT 1
        """,
        (slug,),
    )
    return cur.fetchone()


def insert_series(cur, payload: SeriesPayload) -> int:
    cur.execute(
        """
        INSERT INTO series (
          source_platform, source_series_id, content_origin, title, title2, series_slug,
          cover_url, type, status, rating, published_text, author, synopsis
        ) VALUES (%s, %s, %s, %s, NULLIF(%s, ''), %s, NULLIF(%s, ''), NULLIF(%s, ''), NULLIF(%s, ''), %s, NULLIF(%s, ''), NULLIF(%s, ''), NULLIF(%s, ''))
        """,
        (
            payload.source_platform,
            payload.source_series_id,
            payload.content_origin,
            payload.title,
            payload.title2,
            payload.series_slug,
            payload.cover_url,
            payload.type,
            payload.status,
            payload.rating,
            payload.published_text,
            payload.author,
            payload.synopsis,
        ),
    )
    series_id = int(cur.lastrowid)

    for genre_name in payload.genres:
        cur.execute(
            "INSERT INTO genres (name) VALUES (%s) ON DUPLICATE KEY UPDATE id = LAST_INSERT_ID(id)",
            (genre_name,),
        )
        genre_id = int(cur.lastrowid)
        cur.execute(
            "INSERT IGNORE INTO series_genres (series_id, genre_id) VALUES (%s, %s)",
            (series_id, genre_id),
        )
    return series_id


def get_existing_episodes(cur, series_id: int) -> Tuple[Dict[int, int], Dict[str, int]]:
    cur.execute(
        "SELECT id, source_episode_id, chapter_slug FROM episodes WHERE series_id = %s",
        (series_id,),
    )
    rows = cur.fetchall() or []
    by_source: Dict[int, int] = {}
    by_slug: Dict[str, int] = {}
    for r in rows:
        eid = int(r["id"])
        source_id = r.get("source_episode_id")
        if source_id is not None:
            try:
                by_source[int(source_id)] = eid
            except Exception:
                pass
        slug = clean_slug(r.get("chapter_slug"))
        if slug:
            by_slug[slug] = eid
    return by_source, by_slug


def get_episode_ids_with_streams(cur, series_id: int) -> Set[int]:
    cur.execute(
        """
        SELECT DISTINCT e.id
        FROM episodes e
        JOIN episode_streams es ON es.episode_id = e.id
        WHERE e.series_id = %s
        """,
        (series_id,),
    )
    rows = cur.fetchall() or []
    return {int(r["id"]) for r in rows if r.get("id") is not None}


def map_stream_rows(episode_id: int, streams_map: object) -> List[Tuple[int, str, Optional[int], str, Optional[int]]]:
    out: List[Tuple[int, str, Optional[int], str, Optional[int]]] = []
    if not isinstance(streams_map, dict):
        return out
    for reso_key, rows in streams_map.items():
        if not isinstance(rows, list):
            continue
        for stream in rows:
            if not isinstance(stream, dict):
                continue
            stream_url = str(stream.get("link") or "").strip()
            if not stream_url:
                continue
            resolution = str(stream.get("reso") or reso_key or "unknown").strip() or "unknown"
            source_id_raw = stream.get("id")
            stream_source_id: Optional[int] = None
            if source_id_raw is not None:
                try:
                    n = int(source_id_raw)
                    if n > 0:
                        stream_source_id = n
                except Exception:
                    stream_source_id = None
            size_kb: Optional[int] = None
            try:
                size_raw = stream.get("size_kb")
                if size_raw is not None:
                    s = int(float(size_raw))
                    if s >= 0:
                        size_kb = s
            except Exception:
                size_kb = None
            out.append((episode_id, resolution, stream_source_id, stream_url, size_kb))
    return out


def insert_streams(cur, stream_rows: List[Tuple[int, str, Optional[int], str, Optional[int]]]) -> int:
    if not stream_rows:
        return 0
    cur.executemany(
        """
        INSERT INTO episode_streams (
          episode_id, resolution, stream_source_id, stream_url, size_kb
        ) VALUES (%s, %s, %s, %s, %s)
        """,
        stream_rows,
    )
    return len(stream_rows)


def sync_single_slug(
    conn,
    slug: str,
    series_id: int,
    series_url_tmpl: str,
    episode_url_tmpl: str,
    dry_run: bool = False,
) -> dict:
    report = {
        "slug": slug,
        "series_id": series_id,
        "episodes_candidate": 0,
        "episodes_new": 0,
        "episodes_inserted": 0,
        "streams_inserted": 0,
        "errors": [],
    }

    series_url = series_url_tmpl.format(slug=quote(slug))
    series_json = fetch_json(series_url)
    series_data = ((series_json or {}).get("data") or [None])[0]
    chapters = (series_data or {}).get("chapter") or []
    if not isinstance(chapters, list):
        chapters = []
    report["episodes_candidate"] = len(chapters)

    with conn.cursor() as cur:
        by_source, by_slug = get_existing_episodes(cur, series_id)
        with_streams = get_episode_ids_with_streams(cur, series_id) if not dry_run else set()

        for ch in chapters:
            try:
                source_episode_id = int(ch.get("id"))
            except Exception:
                continue
            if source_episode_id <= 0:
                continue
            chapter_slug = clean_slug(ch.get("url"))
            if not chapter_slug:
                continue

            episode_id = by_source.get(source_episode_id) or by_slug.get(chapter_slug)
            if episode_id:
                if dry_run:
                    continue
                if episode_id in with_streams:
                    continue
                try:
                    ep_url = episode_url_tmpl.format(chapter_slug=quote(chapter_slug))
                    ep_json = fetch_json(ep_url)
                    ep_data = ((ep_json or {}).get("data") or [None])[0]
                    streams_map = (ep_data or {}).get("streams") or {}
                    to_insert = map_stream_rows(episode_id, streams_map)
                    inserted = insert_streams(cur, to_insert)
                    report["streams_inserted"] += inserted
                    if inserted > 0:
                        with_streams.add(episode_id)
                except Exception as exc:
                    report["errors"].append(f"chapter {chapter_slug} backfill: {exc}")
                continue

            report["episodes_new"] += 1
            if dry_run:
                continue

            try:
                cur.execute(
                    """
                    INSERT INTO episodes (
                      source_platform, source_episode_id, series_id,
                      chapter_label, chapter_slug, release_date_text
                    ) VALUES (%s, %s, %s, %s, %s, %s)
                    """,
                    (
                        "animekita",
                        source_episode_id,
                        series_id,
                        str(ch.get("ch") or ""),
                        chapter_slug,
                        str(ch.get("date") or ""),
                    ),
                )
                episode_id = int(cur.lastrowid)
                report["episodes_inserted"] += 1
                by_source[source_episode_id] = episode_id
                by_slug[chapter_slug] = episode_id
            except pymysql.err.IntegrityError:
                cur.execute(
                    """
                    SELECT id
                    FROM episodes
                    WHERE source_platform='animekita' AND source_episode_id=%s
                    LIMIT 1
                    """,
                    (source_episode_id,),
                )
                row = cur.fetchone()
                if not row:
                    continue
                episode_id = int(row["id"])

            try:
                ep_url = episode_url_tmpl.format(chapter_slug=quote(chapter_slug))
                ep_json = fetch_json(ep_url)
                ep_data = ((ep_json or {}).get("data") or [None])[0]
                streams_map = (ep_data or {}).get("streams") or {}
                to_insert = map_stream_rows(episode_id, streams_map)
                inserted = insert_streams(cur, to_insert)
                report["streams_inserted"] += inserted
                if inserted > 0:
                    with_streams.add(episode_id)
            except Exception as exc:
                report["errors"].append(f"chapter {chapter_slug}: {exc}")

        if not dry_run and report["episodes_inserted"] > 0:
            cur.execute(
                "UPDATE series SET updated_at = CURRENT_TIMESTAMP WHERE id = %s LIMIT 1",
                (series_id,),
            )

    return report


def main() -> int:
    parser = argparse.ArgumentParser(description="AnimeKita ongoing auto-insert + single-slug sync bot")
    parser.add_argument("--slug", action="append", default=[], help="Process one slug only (can repeat)")
    parser.add_argument("--limit", type=int, default=0, help="Limit total slug from ongoing list")
    parser.add_argument("--dry-run", action="store_true", help="No write to DB")
    parser.add_argument("--ongoing-url", default=DEFAULT_ONGOING_URL)
    parser.add_argument("--series-url-template", default=DEFAULT_SERIES_URL_TEMPLATE)
    parser.add_argument("--episode-url-template", default=DEFAULT_EPISODE_URL_TEMPLATE)
    args = parser.parse_args()

    load_dotenv(".env")

    slugs: List[str]
    if args.slug:
        slugs = [clean_slug(s) for s in args.slug if clean_slug(s)]
    else:
        ongoing_json = fetch_json(args.ongoing_url)
        slugs = extract_ongoing_slugs(ongoing_json)

    if args.limit and args.limit > 0:
        slugs = slugs[: args.limit]

    if not slugs:
        print("No slug to process.")
        return 0

    print(f"Target slug count: {len(slugs)}")
    conn = get_mysql_conn()
    summary = {
        "total": len(slugs),
        "series_inserted": 0,
        "series_existing": 0,
        "episodes_inserted": 0,
        "streams_inserted": 0,
        "errors": 0,
        "items": [],
    }

    try:
        for slug in slugs:
            item = {"slug": slug, "series_id": None, "inserted_series": False, "sync": None, "error": None}
            try:
                with conn.cursor() as cur:
                    row = get_series_by_slug(cur, slug)
                    if row:
                        series_id = int(row["id"])
                        summary["series_existing"] += 1
                    else:
                        series_json = fetch_json(args.series_url_template.format(slug=quote(slug)))
                        series_data = ((series_json or {}).get("data") or [None])[0]
                        if not isinstance(series_data, dict):
                            raise RuntimeError(f"series data kosong untuk slug={slug}")
                        payload = map_series_payload(series_data, slug)
                        if not payload.title or not payload.series_slug:
                            raise RuntimeError(f"payload series invalid untuk slug={slug}")
                        if args.dry_run:
                            series_id = -1
                        else:
                            series_id = insert_series(cur, payload)
                            summary["series_inserted"] += 1
                            item["inserted_series"] = True

                    item["series_id"] = series_id

                    if args.dry_run:
                        item["sync"] = {
                            "slug": slug,
                            "series_id": series_id,
                            "episodes_candidate": 0,
                            "episodes_new": 0,
                            "episodes_inserted": 0,
                            "streams_inserted": 0,
                            "errors": [],
                        }
                    else:
                        sync_report = sync_single_slug(
                            conn=conn,
                            slug=slug,
                            series_id=series_id,
                            series_url_tmpl=args.series_url_template,
                            episode_url_tmpl=args.episode_url_template,
                            dry_run=False,
                        )
                        item["sync"] = sync_report
                        summary["episodes_inserted"] += int(sync_report["episodes_inserted"])
                        summary["streams_inserted"] += int(sync_report["streams_inserted"])
                        summary["errors"] += len(sync_report["errors"])

                if args.dry_run:
                    conn.rollback()
                else:
                    conn.commit()
            except Exception as exc:
                conn.rollback()
                item["error"] = str(exc)
                summary["errors"] += 1
            summary["items"].append(item)
            status = "OK" if not item["error"] else "ERR"
            print(f"[{status}] slug={slug} series_id={item['series_id']} err={item['error'] or '-'}")
    finally:
        conn.close()

    print("\n=== SUMMARY ===")
    print(json.dumps(summary, indent=2, ensure_ascii=False))
    return 0 if summary["errors"] == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
