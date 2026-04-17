#!/usr/bin/env python3
"""
Bot Anibi:
- Auto detect ongoing AnimeKita
- Auto insert series if missing
- For newly inserted series: auto enrich title2/synopsis/rating (MAL/Otakudesu logic)
- Sync episode + stream per slug

Usage:
  python3 scripts/bot-anibi.py
  python3 scripts/bot-anibi.py --slug niwatori-fighter
  python3 scripts/bot-anibi.py --limit 20
  python3 scripts/bot-anibi.py --dry-run
  python3 scripts/bot-anibi.py --no-translate-synopsis
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
import uuid
from dataclasses import dataclass
from typing import Dict, List, Optional, Set, Tuple
from urllib.parse import quote, quote_plus, urlparse

import requests
from bs4 import BeautifulSoup

try:
    import pymysql
    from pymysql.cursors import DictCursor
except Exception:  # pragma: no cover
    print("ERROR: pymysql belum terinstall. Install: pip install pymysql", file=sys.stderr)
    raise


DEFAULT_ONGOING_URL = "https://apps.animekita.org/api/v1.2.5/home/ongoing.php"
DEFAULT_SERIES_URL_TEMPLATE = "https://apps.animekita.org/api/v1.2.5/series.php?url={slug}"
DEFAULT_EPISODE_URL_TEMPLATE = "https://apps.animekita.org/api/v1.2.5/series/episode/data.php?url={chapter_slug}"

OTAKU_DETAIL_URL_TEMPLATE = "https://otakudesu.blog/anime/{slug}/"
MAL_SEARCH_URL_TEMPLATE = "https://myanimelist.net/anime.php?q={query}&cat=anime"


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


def normalize_text(text: object) -> str:
    return re.sub(r"\s+", " ", str(text or "")).strip()


def clean_slug(value: object) -> str:
    s = normalize_text(value)
    return re.sub(r"^/+|/+$", "", s)


def first_non_empty(*values: object) -> str:
    for v in values:
        s = normalize_text(v)
        if s:
            return s
    return ""


def is_blank_text(value: object) -> bool:
    s = normalize_text(value).lower()
    return s in {"", "-", "null", "none", "n/a", "na", "unknown"}


def to_nonneg_int(value: object, fallback: int = 0) -> int:
    try:
        n = int(float(value))
        if n < 0:
            return fallback
        return n
    except Exception:
        return fallback


def to_float_or_none(value: object) -> Optional[float]:
    s = normalize_text(value)
    if not s:
        return None
    try:
        return float(s)
    except Exception:
        return None


def parse_genres(raw: object) -> List[str]:
    out: List[str] = []
    seen: Set[str] = set()
    if isinstance(raw, list):
        values = [normalize_text(x) for x in raw]
    elif isinstance(raw, str):
        values = [normalize_text(x) for x in raw.split(",")]
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
                    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
                    "Accept": "application/json,text/plain,*/*",
                },
            )
            resp.raise_for_status()
            return resp.json()
        except Exception as exc:
            last_err = exc
            if i < max_attempts:
                time.sleep(0.35 * i)
    raise RuntimeError(f"fetch failed ({url}): {last_err}")


def fetch_html(url: str, timeout_sec: int = 25, max_attempts: int = 4) -> str:
    last_err: Optional[Exception] = None
    for i in range(1, max_attempts + 1):
        try:
            resp = requests.get(
                url,
                timeout=timeout_sec,
                headers={
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
                    "Accept-Language": "id-ID,id;q=0.9,en-US;q=0.8,en;q=0.7",
                },
            )
            resp.raise_for_status()
            return resp.text
        except Exception as exc:
            last_err = exc
            if i < max_attempts:
                time.sleep(0.35 * i)
    raise RuntimeError(f"fetch html failed ({url}): {last_err}")


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

    rating_val = to_float_or_none(first_non_empty(remote.get("rating"), remote.get("score")))
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
        synopsis="baru",
        genres=parse_genres(remote.get("genre") or remote.get("genres")),
    )


def extract_anime_slug_from_url(url: str) -> str:
    path = urlparse(url).path.strip("/")
    parts = path.split("/")
    if len(parts) >= 2 and parts[0] == "anime":
        return clean_slug(parts[1])
    return clean_slug(parts[-1] if parts else "")


def parse_otakudesu_detail(html: str) -> dict:
    soup = BeautifulSoup(html, "html.parser")
    title_el = soup.select_one(".venser .jdlrx h1")
    info: Dict[str, str] = {}
    for p in soup.select(".infozingle p"):
        text = normalize_text(p.get_text(" ", strip=True))
        if ":" not in text:
            continue
        key, value = text.split(":", 1)
        info[normalize_text(key)] = normalize_text(value)
    return {
        "title": normalize_text(title_el.get_text(" ", strip=True)) if title_el else "",
        "info": info,
    }


def build_mal_query(base_title: str, slug: str) -> str:
    detail_url = OTAKU_DETAIL_URL_TEMPLATE.format(slug=quote_plus(slug))
    try:
        detail_html = fetch_html(detail_url)
        otaku = parse_otakudesu_detail(detail_html)
        title_from_info = normalize_text((otaku.get("info") or {}).get("Judul"))
        if title_from_info:
            return title_from_info
        page_title = normalize_text(otaku.get("title"))
        if page_title:
            return re.sub(r"\s+Sub\s+Indo$", "", page_title, flags=re.IGNORECASE).strip()
    except Exception:
        pass

    if normalize_text(base_title):
        return normalize_text(base_title)
    return slug.replace("-", " ").strip()


def find_first_mal_anime_url(search_html: str) -> str:
    soup = BeautifulSoup(search_html, "html.parser")
    for a in soup.select("a.hoverinfo_trigger[href*='/anime/']"):
        href = normalize_text(a.get("href"))
        if "/anime/" in href:
            return href
    return ""


def extract_labeled_text(soup: BeautifulSoup, label: str) -> str:
    target = label.rstrip(":").lower()
    for lb in soup.select("span.dark_text"):
        raw = normalize_text(lb.get_text(" ", strip=True)).rstrip(":").lower()
        if raw != target:
            continue
        parent = lb.parent
        if not parent:
            continue
        value = normalize_text(parent.get_text(" ", strip=True))
        value = value.replace(normalize_text(lb.get_text(" ", strip=True)), "", 1)
        return normalize_text(value)
    return ""


def parse_mal_detail(html: str) -> dict:
    soup = BeautifulSoup(html, "html.parser")
    title_el = soup.select_one("h1.title-name")
    score_el = soup.select_one("div.score-label") or soup.select_one("span[itemprop='ratingValue']")
    synopsis_el = soup.select_one("p[itemprop='description']") or soup.select_one("[itemprop='description']")

    synopsis = normalize_text(synopsis_el.get_text(" ", strip=True)) if synopsis_el else ""
    if synopsis.lower().startswith("no synopsis information"):
        synopsis = ""

    return {
        "title": normalize_text(title_el.get_text(" ", strip=True)) if title_el else "",
        "english_title": extract_labeled_text(soup, "English"),
        "japanese_title": extract_labeled_text(soup, "Japanese"),
        "score": normalize_text(score_el.get_text(" ", strip=True)) if score_el else "",
        "synopsis": synopsis,
    }


def scrape_mal_by_query(query: str) -> dict:
    search_url = MAL_SEARCH_URL_TEMPLATE.format(query=quote_plus(query))
    search_html = fetch_html(search_url)
    anime_url = find_first_mal_anime_url(search_html)
    if not anime_url:
        return {"url": "", "english_title": "", "score": "", "synopsis": ""}
    detail_html = fetch_html(anime_url)
    data = parse_mal_detail(detail_html)
    data["url"] = anime_url
    return data


def split_text_for_translation(text: str, max_len: int = 450) -> List[str]:
    src = normalize_text(text)
    if not src:
        return []
    if len(src) <= max_len:
        return [src]
    parts: List[str] = []
    current = ""
    sentences = re.split(r"(?<=[.!?])\s+", src)
    for sent in sentences:
        sent = normalize_text(sent)
        if not sent:
            continue
        candidate = f"{current} {sent}".strip() if current else sent
        if len(candidate) <= max_len:
            current = candidate
            continue
        if current:
            parts.append(current)
            current = ""
        if len(sent) <= max_len:
            current = sent
        else:
            for i in range(0, len(sent), max_len):
                parts.append(sent[i : i + max_len])
    if current:
        parts.append(current)
    return parts


def translate_chunk_mymemory(src: str) -> str:
    api_url = "https://api.mymemory.translated.net/get"
    params = {"q": src, "langpair": "en|id"}
    resp = requests.get(api_url, params=params, timeout=25, headers={"User-Agent": "Mozilla/5.0"})
    resp.raise_for_status()
    payload = resp.json()
    return normalize_text((payload or {}).get("responseData", {}).get("translatedText"))


def translate_chunk_google_public(src: str) -> str:
    # Unofficial public endpoint (no key), used as fallback.
    api_url = "https://translate.googleapis.com/translate_a/single"
    params = {
        "client": "gtx",
        "sl": "en",
        "tl": "id",
        "dt": "t",
        "q": src,
    }
    resp = requests.get(api_url, params=params, timeout=25, headers={"User-Agent": "Mozilla/5.0"})
    resp.raise_for_status()
    payload = resp.json()
    if not isinstance(payload, list) or not payload:
        return ""
    first = payload[0]
    if not isinstance(first, list):
        return ""
    parts: List[str] = []
    for row in first:
        if isinstance(row, list) and row:
            parts.append(normalize_text(row[0]))
    return normalize_text(" ".join(parts))


def translate_to_indonesian(text: str, cache: Dict[str, str]) -> str:
    src = normalize_text(text)
    if not src:
        return ""
    if src in cache:
        return cache[src]
    chunks = split_text_for_translation(src, max_len=450)
    if not chunks:
        cache[src] = ""
        return ""
    translated_chunks: List[str] = []
    try:
        for ch in chunks:
            t = translate_chunk_mymemory(ch)
            if not t:
                raise RuntimeError("empty translation")
            translated_chunks.append(t)
        merged = normalize_text(" ".join(translated_chunks))
        cache[src] = merged if merged else ""
        return cache[src]
    except Exception:
        translated_chunks = []
    try:
        for ch in chunks:
            t = translate_chunk_google_public(ch)
            if not t:
                raise RuntimeError("empty translation")
            translated_chunks.append(t)
        merged = normalize_text(" ".join(translated_chunks))
        cache[src] = merged if merged else ""
        return cache[src]
    except Exception:
        cache[src] = ""
        return ""


def get_mysql_conn():
    host = os.getenv("DB_HOST", "103.16.116.244")
    port = int(os.getenv("DB_PORT", "3306"))
    user = os.getenv("DB_USER", "hxcuser_remote")
    password = os.getenv("DB_PASS", "@Hudaxcode21")
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
        SELECT id, source_series_id, series_slug, title, title2, synopsis, rating
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


def enrich_new_series_detail(cur, series_id: int, slug: str, base_title: str, translate_synopsis: bool, cache: Dict[str, str]) -> Dict[str, object]:
    cur.execute("SELECT title2, synopsis, rating FROM series WHERE id = %s LIMIT 1", (series_id,))
    row = cur.fetchone() or {}

    need_title2 = is_blank_text(row.get("title2"))
    need_synopsis = is_blank_text(row.get("synopsis")) or normalize_text(row.get("synopsis")) == "-"
    need_rating = to_float_or_none(row.get("rating")) in (None, 0.0)

    if not (need_title2 or need_synopsis or need_rating):
        return {"updated": False, "fields": [], "mal_url": ""}

    query = build_mal_query(base_title, slug)
    mal = scrape_mal_by_query(query)
    updates: Dict[str, object] = {}

    if need_title2 and not is_blank_text(mal.get("english_title")):
        updates["title2"] = normalize_text(mal.get("english_title"))
    if need_rating:
        rv = to_float_or_none(mal.get("score"))
        if rv is not None and rv > 0:
            updates["rating"] = rv
    if need_synopsis:
        syn = normalize_text(mal.get("synopsis"))
        if not is_blank_text(syn):
            if translate_synopsis:
                syn_id = normalize_text(translate_to_indonesian(syn, cache))
                # Prefer Indonesian translation; if unavailable, keep MAL synopsis.
                updates["synopsis"] = syn_id if syn_id else syn
            else:
                updates["synopsis"] = syn
        else:
            updates["synopsis"] = "baru"

    if not updates:
        return {"updated": False, "fields": [], "mal_url": mal.get("url", "")}

    cols = []
    vals: List[object] = []
    for k, v in updates.items():
        cols.append(f"{k} = %s")
        vals.append(v)
    vals.append(series_id)
    cur.execute(
        f"UPDATE series SET {', '.join(cols)}, updated_at = CURRENT_TIMESTAMP WHERE id = %s LIMIT 1",
        tuple(vals),
    )
    return {"updated": True, "fields": list(updates.keys()), "mal_url": mal.get("url", "")}


def get_existing_episodes(cur, series_id: int) -> Tuple[Dict[int, int], Dict[str, int]]:
    cur.execute("SELECT id, source_episode_id, chapter_slug FROM episodes WHERE series_id = %s", (series_id,))
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
            stream_url = normalize_text(stream.get("link"))
            if not stream_url:
                continue
            resolution = normalize_text(stream.get("reso") or reso_key or "unknown") or "unknown"
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


def build_idempotency_key(prefix: str, slug: str, suffix: str = "") -> str:
    ts = int(time.time() * 1000)
    rand = uuid.uuid4().hex[:10]
    safe_slug = clean_slug(slug) or "unknown"
    safe_suffix = clean_slug(suffix) if suffix else ""
    if safe_suffix:
        return f"{prefix}-{safe_slug}-{safe_suffix}-{ts}-{rand}"
    return f"{prefix}-{safe_slug}-{ts}-{rand}"


def post_notification(
    api_url: str,
    admin_secret: str,
    payload: Dict[str, object],
    idempotency_key: str,
    timeout_sec: int = 25,
) -> None:
    resp = requests.post(
        api_url,
        timeout=timeout_sec,
        headers={
            "x-admin-secret": admin_secret,
            "Idempotency-Key": idempotency_key,
            "Content-Type": "application/json",
        },
        json=payload,
    )
    if resp.status_code >= 400:
        raise RuntimeError(f"notif http {resp.status_code}: {normalize_text(resp.text)[:300]}")


def normalize_episode_label(label: object) -> str:
    raw = normalize_text(label)
    if not raw:
        return ""
    if re.match(r"(?i)^ep(?:isode)?\b", raw):
        return raw
    return f"Episode {raw}"


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
        "series_title": "",
        "series_cover": "",
        "episodes_candidate": 0,
        "episodes_new": 0,
        "episodes_inserted": 0,
        "streams_inserted": 0,
        "inserted_episodes": [],
        "errors": [],
    }

    series_url = series_url_tmpl.format(slug=quote(slug))
    series_json = fetch_json(series_url)
    series_data = ((series_json or {}).get("data") or [None])[0]
    if isinstance(series_data, dict):
        report["series_title"] = first_non_empty(series_data.get("judul"), series_data.get("title"), series_data.get("name"))
        report["series_cover"] = first_non_empty(
            series_data.get("cover"), series_data.get("cover_url"), series_data.get("image"), series_data.get("poster")
        )
    chapters = (series_data or {}).get("chapter") or []
    if not isinstance(chapters, list):
        chapters = []
    report["episodes_candidate"] = len(chapters)

    with conn.cursor() as cur:
        by_source, by_slug = get_existing_episodes(cur, series_id)

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
                # Do not touch existing episode/series data.
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
                        normalize_text(ch.get("ch")),
                        chapter_slug,
                        normalize_text(ch.get("date")),
                    ),
                )
                episode_id = int(cur.lastrowid)
                report["episodes_inserted"] += 1
                by_source[source_episode_id] = episode_id
                by_slug[chapter_slug] = episode_id
                report["inserted_episodes"].append(
                    {
                        "source_episode_id": source_episode_id,
                        "chapter_slug": chapter_slug,
                        "chapter_label": normalize_text(ch.get("ch")),
                        "release_date_text": normalize_text(ch.get("date")),
                    }
                )
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
            except Exception as exc:
                report["errors"].append(f"chapter {chapter_slug}: {exc}")

        if not dry_run and report["episodes_inserted"] > 0:
            # Keep series "fresh" for feeds that sort by created_at.
            cur.execute(
                """
                UPDATE series
                SET created_at = CURRENT_TIMESTAMP,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = %s
                LIMIT 1
                """,
                (series_id,),
            )

    return report


def main() -> int:
    parser = argparse.ArgumentParser(description="Bot Anibi: ongoing autosync + enrich detail for new series")
    parser.add_argument("--slug", action="append", default=[], help="Process one slug only (can repeat)")
    parser.add_argument("--limit", type=int, default=0, help="Limit total slug from ongoing list")
    parser.add_argument("--dry-run", action="store_true", help="No write to DB")
    parser.add_argument("--no-translate-synopsis", action="store_true", help="Do not translate MAL synopsis to Indonesian")
    parser.add_argument("--ongoing-url", default=DEFAULT_ONGOING_URL)
    parser.add_argument("--series-url-template", default=DEFAULT_SERIES_URL_TEMPLATE)
    parser.add_argument("--episode-url-template", default=DEFAULT_EPISODE_URL_TEMPLATE)
    parser.add_argument("--notify", action="store_true", help="Send broadcast notification on new series/episode")
    parser.add_argument("--notify-url", default=os.getenv("NOTIFY_API_URL", ""))
    parser.add_argument("--notify-secret", default=os.getenv("NOTIFICATIONS_ADMIN_SECRET", os.getenv("NOTIFY_ADMIN_SECRET", "")))
    parser.add_argument("--notify-topic", default=os.getenv("NOTIFY_TOPIC", "anime-update"))
    args = parser.parse_args()

    load_dotenv(".env")
    translate_synopsis = not args.no_translate_synopsis
    if not normalize_text(args.notify_url):
        args.notify_url = normalize_text(os.getenv("NOTIFY_API_URL"))
    if not normalize_text(args.notify_secret):
        args.notify_secret = normalize_text(os.getenv("NOTIFICATIONS_ADMIN_SECRET", os.getenv("NOTIFY_ADMIN_SECRET", "")))

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
    translate_cache: Dict[str, str] = {}
    summary = {
        "total": len(slugs),
        "series_inserted": 0,
        "series_existing": 0,
        "series_detail_enriched": 0,
        "episodes_inserted": 0,
        "streams_inserted": 0,
        "notifications_sent": 0,
        "notification_errors": 0,
        "errors": 0,
        "items": [],
    }

    notify_enabled = bool(args.notify and not args.dry_run and normalize_text(args.notify_url) and normalize_text(args.notify_secret))
    if args.notify and not notify_enabled:
        print("[WARN] notify aktif tapi NOTIFY URL/SECRET kosong, notifikasi dilewati.")

    try:
        for slug in slugs:
            item = {
                "slug": slug,
                "series_id": None,
                "inserted_series": False,
                "series_slug": slug,
                "series_title": "",
                "series_cover": "",
                "detail_enriched": False,
                "detail_fields": [],
                "detail_mal_url": "",
                "sync": None,
                "notifications": [],
                "notify_errors": [],
                "error": None,
            }
            try:
                with conn.cursor() as cur:
                    row = get_series_by_slug(cur, slug)
                    if row:
                        series_id = int(row["id"])
                        base_title = normalize_text(row.get("title"))
                        item["series_title"] = base_title
                        summary["series_existing"] += 1
                        if not args.dry_run:
                            enrich = enrich_new_series_detail(
                                cur=cur,
                                series_id=series_id,
                                slug=slug,
                                base_title=base_title,
                                translate_synopsis=translate_synopsis,
                                cache=translate_cache,
                            )
                            item["detail_enriched"] = bool(enrich.get("updated"))
                            item["detail_fields"] = list(enrich.get("fields") or [])
                            item["detail_mal_url"] = normalize_text(enrich.get("mal_url"))
                            if item["detail_enriched"]:
                                summary["series_detail_enriched"] += 1
                    else:
                        series_json = fetch_json(args.series_url_template.format(slug=quote(slug)))
                        series_data = ((series_json or {}).get("data") or [None])[0]
                        if not isinstance(series_data, dict):
                            raise RuntimeError(f"series data kosong untuk slug={slug}")
                        payload = map_series_payload(series_data, slug)
                        if not payload.title or not payload.series_slug:
                            raise RuntimeError(f"payload series invalid untuk slug={slug}")
                        base_title = payload.title
                        item["series_slug"] = payload.series_slug
                        item["series_title"] = payload.title
                        item["series_cover"] = payload.cover_url
                        if args.dry_run:
                            series_id = -1
                        else:
                            series_id = insert_series(cur, payload)
                            summary["series_inserted"] += 1
                            item["inserted_series"] = True

                            enrich = enrich_new_series_detail(
                                cur=cur,
                                series_id=series_id,
                                slug=slug,
                                base_title=base_title,
                                translate_synopsis=translate_synopsis,
                                cache=translate_cache,
                            )
                            item["detail_enriched"] = bool(enrich.get("updated"))
                            item["detail_fields"] = list(enrich.get("fields") or [])
                            item["detail_mal_url"] = normalize_text(enrich.get("mal_url"))
                            if item["detail_enriched"]:
                                summary["series_detail_enriched"] += 1

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
                        if normalize_text(sync_report.get("series_title")):
                            item["series_title"] = normalize_text(sync_report.get("series_title"))
                        if normalize_text(sync_report.get("series_cover")):
                            item["series_cover"] = normalize_text(sync_report.get("series_cover"))
                        summary["episodes_inserted"] += int(sync_report["episodes_inserted"])
                        summary["streams_inserted"] += int(sync_report["streams_inserted"])
                        summary["errors"] += len(sync_report["errors"])

                if args.dry_run:
                    conn.rollback()
                else:
                    conn.commit()

                if notify_enabled:
                    sync = item.get("sync") or {}
                    series_slug = normalize_text(item.get("series_slug")) or slug
                    series_title = normalize_text(item.get("series_title")) or series_slug.replace("-", " ").title()
                    series_cover = normalize_text(item.get("series_cover"))

                    if item.get("inserted_series"):
                        payload = {
                            "type": "anime_update",
                            "title": "Anime Update",
                            "message": f"{series_title} baru ditambahkan",
                            "imageUrl": series_cover,
                            "actionType": "open_anime",
                            "actionValue": series_slug,
                            "actionLabel": "Lihat Anime",
                            "dedupeKey": f"anime-update:{series_slug}",
                            "topic": normalize_text(args.notify_topic) or "anime-update",
                        }
                        key = build_idempotency_key("anime-new", series_slug)
                        try:
                            post_notification(
                                api_url=normalize_text(args.notify_url),
                                admin_secret=normalize_text(args.notify_secret),
                                payload=payload,
                                idempotency_key=key,
                            )
                            item["notifications"].append({"kind": "anime_new", "idempotency_key": key})
                            summary["notifications_sent"] += 1
                        except Exception as notif_exc:
                            item["notify_errors"].append(str(notif_exc))
                            summary["notification_errors"] += 1

                    for ep in (sync.get("inserted_episodes") or []):
                        chapter_slug = clean_slug(ep.get("chapter_slug"))
                        if not chapter_slug:
                            continue
                        chapter_label = normalize_episode_label(ep.get("chapter_label")) or "Episode Baru"
                        payload = {
                            "type": "episode_update",
                            "title": "Episode Baru Tersedia",
                            "message": f"{series_title} {chapter_label} sudah rilis",
                            "imageUrl": series_cover,
                            "actionType": "open_episode",
                            "actionValue": chapter_slug,
                            "actionLabel": "Tonton",
                            "dedupeKey": f"episode-update:{series_slug}:{chapter_slug}",
                            "topic": normalize_text(args.notify_topic) or "anime-update",
                        }
                        key = build_idempotency_key("episode-new", series_slug, chapter_slug)
                        try:
                            post_notification(
                                api_url=normalize_text(args.notify_url),
                                admin_secret=normalize_text(args.notify_secret),
                                payload=payload,
                                idempotency_key=key,
                            )
                            item["notifications"].append({"kind": "episode_new", "chapter_slug": chapter_slug, "idempotency_key": key})
                            summary["notifications_sent"] += 1
                        except Exception as notif_exc:
                            item["notify_errors"].append(f"{chapter_slug}: {notif_exc}")
                            summary["notification_errors"] += 1
            except Exception as exc:
                conn.rollback()
                item["error"] = str(exc)
                summary["errors"] += 1
            summary["items"].append(item)
            if item["error"]:
                status = "ERR"
            else:
                sync = item.get("sync") or {}
                has_insert = bool(item.get("inserted_series")) or int(sync.get("episodes_inserted", 0)) > 0
                status = "INSERT" if has_insert else "SKIP"
            print(
                f"[{status}] slug={slug} series_id={item['series_id']} "
                f"inserted={item['inserted_series']} detail={item['detail_fields']} err={item['error'] or '-'}"
            )
    finally:
        conn.close()

    print("\n=== SUMMARY ===")
    print(json.dumps(summary, indent=2, ensure_ascii=False))
    return 0 if summary["errors"] == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
