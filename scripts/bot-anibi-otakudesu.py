#!/usr/bin/env python3
"""
Bot Anibi Otakudesu:
- Auto detect ongoing from Otakudesu scrape
- Auto insert series if missing
- For newly inserted series: auto enrich title2/synopsis/rating (MAL/Otakudesu logic)
- Sync episode + stream per slug

Usage:
  python3 scripts/bot-anibi-otakudesu.py
  python3 scripts/bot-anibi-otakudesu.py --limit 20
  python3 scripts/bot-anibi-otakudesu.py --otakudesu-max-pages 5
  python3 scripts/bot-anibi-otakudesu.py --dry-run
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
import uuid
import zlib
from dataclasses import dataclass
from typing import Dict, List, Optional, Set, Tuple
from urllib.parse import quote_plus, urljoin, urlparse

import requests
from bs4 import BeautifulSoup

try:
    import pymysql
    from pymysql.cursors import DictCursor
except Exception:  # pragma: no cover
    print("ERROR: pymysql belum terinstall. Install: pip install pymysql", file=sys.stderr)
    raise


DEFAULT_ONGOING_URL = "https://otakudesu.blog/ongoing-anime/"
DEFAULT_OTAKUDESU_ONGOING_URL = "https://otakudesu.blog/ongoing-anime/"
DEFAULT_SKIP_SLUGS = {
    "1piece-sub-indo",
}
LOCK_RETRY_ATTEMPTS = 1
DEFAULT_NOTIFY_AUTO = True
DEFAULT_NOTIFY_URL = "https://panel.hudaxcode.cloud/api/notifications/send"
DEFAULT_NOTIFY_SECRET = "hxc21"
DEFAULT_NOTIFY_TOPIC = "anime-update"
EXCLUDED_STREAM_DOMAINS = {
    "acefile.co",
    "gofile.io",
}

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


def is_excluded_stream_domain(url: str) -> bool:
    host = (urlparse(normalize_text(url)).netloc or "").lower().strip()
    if not host:
        return False
    if host.startswith("www."):
        host = host[4:]
    for blocked in EXCLUDED_STREAM_DOMAINS:
        b = blocked.lower().strip()
        if not b:
            continue
        if host == b or host.endswith("." + b):
            return True
    return False


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


def parse_ongoing_page_otakudesu(html: str, base_url: str = DEFAULT_OTAKUDESU_ONGOING_URL) -> Tuple[List[str], Optional[str]]:
    soup = BeautifulSoup(html, "html.parser")
    slugs: List[str] = []
    seen: Set[str] = set()

    for a in soup.select(".venz ul li .detpost .thumb a[href]"):
        href = a.get("href", "")
        slug = extract_anime_slug_from_url(href)
        if not slug or slug in seen:
            continue
        seen.add(slug)
        slugs.append(slug)

    next_el = soup.select_one(".pagination .next.page-numbers[href]")
    next_url = urljoin(base_url, next_el["href"]) if next_el else None
    return slugs, next_url


def collect_ongoing_slugs_otakudesu(start_url: str, max_pages: int = 5) -> List[str]:
    out: List[str] = []
    seen: Set[str] = set()
    page = 0
    url = start_url
    hard_cap = 30

    while url and page < hard_cap:
        if max_pages > 0 and page >= max_pages:
            break
        html = fetch_html(url)
        page_slugs, next_url = parse_ongoing_page_otakudesu(html, start_url)
        for slug in page_slugs:
            if slug in seen:
                continue
            seen.add(slug)
            out.append(slug)
        page += 1
        url = next_url
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


def map_series_payload_from_otakudesu(detail: dict, slug: str) -> SeriesPayload:
    info = detail.get("info") or {}
    synopsis_text = normalize_text(detail.get("synopsis"))
    title_from_info = first_non_empty(info.get("Judul"), info.get("Title"))
    title_fallback = first_non_empty(detail.get("title"), slug.replace("-", " "))
    return SeriesPayload(
        source_platform="animekita",
        source_series_id=synthetic_source_series_id(slug),
        content_origin="anime",
        title=first_non_empty(title_from_info, title_fallback),
        title2="",
        series_slug=clean_slug(slug),
        cover_url=normalize_text(detail.get("cover_url")),
        type=first_non_empty(info.get("Tipe"), info.get("Type")),
        status=first_non_empty(info.get("Status")),
        rating=to_float_or_none(first_non_empty(info.get("Skor"), info.get("Score"))),
        published_text=first_non_empty(info.get("Tanggal Rilis"), info.get("Dirilis"), info.get("Released")),
        author=first_non_empty(info.get("Studio"), info.get("Produser"), info.get("Producer")),
        synopsis=synopsis_text if synopsis_text else "baru",
        genres=parse_genres(detail.get("genres") or []),
    )


def extract_anime_slug_from_url(url: str) -> str:
    path = urlparse(url).path.strip("/")
    parts = path.split("/")
    if len(parts) >= 2 and parts[0] == "anime":
        return clean_slug(parts[1])
    return clean_slug(parts[-1] if parts else "")


def extract_episode_slug_from_url(url: str) -> str:
    path = urlparse(url).path.strip("/")
    parts = path.split("/")
    if len(parts) >= 2 and parts[0] == "episode":
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


def parse_otakudesu_anime_detail(html: str) -> dict:
    soup = BeautifulSoup(html, "html.parser")
    title_el = soup.select_one(".venser .jdlrx h1")
    synopsis_el = soup.select_one(".sinopc")
    thumb_el = soup.select_one(".fotoanime img")

    info: Dict[str, str] = {}
    for p in soup.select(".infozingle p"):
        text = normalize_text(p.get_text(" ", strip=True))
        if ":" not in text:
            continue
        key, value = text.split(":", 1)
        info[normalize_text(key)] = normalize_text(value)

    genres = [normalize_text(a.get_text(" ", strip=True)) for a in soup.select(".infozingle p a")]
    return {
        "title": normalize_text(title_el.get_text(" ", strip=True)) if title_el else "",
        "synopsis": normalize_text(synopsis_el.get_text(" ", strip=True)) if synopsis_el else "",
        "cover_url": normalize_text(thumb_el.get("src")) if thumb_el else "",
        "info": info,
        "genres": [g for g in genres if g],
    }


def parse_otakudesu_episode_list(html: str) -> List[dict]:
    soup = BeautifulSoup(html, "html.parser")
    target_list = None
    for block in soup.select("div.episodelist"):
        title_tag = block.select_one(".monktit")
        if not title_tag:
            continue
        block_title = normalize_text(title_tag.get_text(" ", strip=True)).lower()
        if "episode list" in block_title:
            target_list = block
            break

    if not target_list:
        return []

    out: List[dict] = []
    for li in target_list.select("ul > li"):
        link_tag = li.select_one("a[href]")
        if not link_tag:
            continue
        href = normalize_text(link_tag.get("href"))
        chapter_slug = extract_episode_slug_from_url(href)
        if not chapter_slug:
            continue
        date_tag = li.select_one(".zeebr")
        out.append(
            {
                "chapter_slug": chapter_slug,
                "chapter_label": normalize_text(link_tag.get_text(" ", strip=True)),
                "release_date_text": normalize_text(date_tag.get_text(" ", strip=True)) if date_tag else "",
                "episode_url": href,
            }
        )
    return out


def parse_otakudesu_episode_streams(html: str) -> List[Tuple[str, str]]:
    soup = BeautifulSoup(html, "html.parser")
    out: List[Tuple[str, str]] = []
    for li in soup.select("div.download ul li"):
        strong = li.select_one("strong")
        if not strong:
            continue
        resolution = normalize_text(strong.get_text(" ", strip=True))
        if not resolution.lower().startswith("mp4"):
            continue
        resolution = re.sub(r"(?i)^mp4\s*", "", resolution).strip() or resolution
        for a in li.select("a[href]"):
            href = normalize_text(a.get("href"))
            if href:
                out.append((resolution or "unknown", href))
    return out


def synthetic_source_series_id(slug: str) -> int:
    return int(zlib.crc32(clean_slug(slug).encode("utf-8")) & 0x7FFFFFFF) or 1


def synthetic_source_episode_id(chapter_slug: str) -> int:
    return int(zlib.crc32(clean_slug(chapter_slug).encode("utf-8")) & 0x7FFFFFFF) or 1


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
    conn = pymysql.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        database=database,
        charset="utf8mb4",
        autocommit=False,
        cursorclass=DictCursor,
    )
    try:
        with conn.cursor() as cur:
            cur.execute("SET SESSION innodb_lock_wait_timeout = 8")
    except Exception:
        pass
    return conn


def acquire_run_lock(cur, lock_name: str, timeout_sec: int = 1) -> bool:
    cur.execute("SELECT GET_LOCK(%s, %s) AS ok", (lock_name, timeout_sec))
    row = cur.fetchone() or {}
    try:
        return int(row.get("ok") or 0) == 1
    except Exception:
        return False


def release_run_lock(cur, lock_name: str) -> None:
    try:
        cur.execute("SELECT RELEASE_LOCK(%s)", (lock_name,))
    except Exception:
        pass


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
    synopsis_now = normalize_text(row.get("synopsis"))
    need_synopsis = is_blank_text(synopsis_now) or synopsis_now in {"-", "baru"}
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


def get_existing_episodes(cur, series_id: int) -> Tuple[Dict[int, int], Dict[str, int], Dict[str, int]]:
    cur.execute("SELECT id, source_episode_id, chapter_slug, chapter_label FROM episodes WHERE series_id = %s", (series_id,))
    rows = cur.fetchall() or []
    by_source: Dict[int, int] = {}
    by_slug: Dict[str, int] = {}
    by_chapter: Dict[str, int] = {}
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
        chapter_no = extract_chapter_number(r.get("chapter_label"), slug)
        if chapter_no:
            by_chapter[chapter_no] = eid
    return by_source, by_slug, by_chapter


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


def map_stream_rows_from_otakudesu(episode_id: int, rows: List[Tuple[str, str]]) -> List[Tuple[int, str, Optional[int], str, Optional[int]]]:
    out: List[Tuple[int, str, Optional[int], str, Optional[int]]] = []
    for resolution, stream_url in rows:
        url = normalize_text(stream_url)
        if not url:
            continue
        if is_excluded_stream_domain(url):
            continue
        out.append((episode_id, normalize_text(resolution) or "unknown", None, url, None))
    return out


def insert_streams(cur, stream_rows: List[Tuple[int, str, Optional[int], str, Optional[int]]]) -> int:
    if not stream_rows:
        return 0
    episode_id = int(stream_rows[0][0])
    cur.execute("SELECT stream_url FROM episode_streams WHERE episode_id = %s", (episode_id,))
    existing_rows = cur.fetchall() or []
    existing: Set[str] = set()
    for r in existing_rows:
        u = normalize_text(r.get("stream_url"))
        if u:
            existing.add(u)

    unique_rows: List[Tuple[int, str, Optional[int], str, Optional[int]]] = []
    seen: Set[str] = set()
    for row in stream_rows:
        u = normalize_text(row[3])
        if not u:
            continue
        if u in existing or u in seen:
            continue
        seen.add(u)
        unique_rows.append((row[0], row[1], row[2], u, row[4]))

    if not unique_rows:
        return 0
    cur.executemany(
        """
        INSERT INTO episode_streams (
          episode_id, resolution, stream_source_id, stream_url, size_kb
        ) VALUES (%s, %s, %s, %s, %s)
        """,
        unique_rows,
    )
    return len(unique_rows)


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


def extract_chapter_number(label: object, chapter_slug: object = "") -> str:
    raw = normalize_text(label)
    slug = clean_slug(chapter_slug)
    if re.fullmatch(r"[0-9]+(?:\.[0-9]+)?", raw):
        return raw
    m = re.search(r"(?i)\bepisode\s*([0-9]+(?:\.[0-9]+)?)\b", raw)
    if m:
        return m.group(1)
    m2 = re.search(r"(?i)\bep\.?\s*([0-9]+(?:\.[0-9]+)?)\b", raw)
    if m2:
        return m2.group(1)
    m3 = re.search(r"(?i)(?:^|-)episode-([0-9]+(?:-[0-9]+)?)", slug)
    if m3:
        return m3.group(1).replace("-", ".")
    m4 = re.search(r"(?i)(?:^|-)ep-([0-9]+(?:-[0-9]+)?)", slug)
    if m4:
        return m4.group(1).replace("-", ".")
    return ""


def build_db_chapter_label(label: object, chapter_slug: object, max_len: int = 100) -> str:
    chapter_no = extract_chapter_number(label, chapter_slug)
    if chapter_no:
        return chapter_no[:max_len]
    out = normalize_text(label) or normalize_text(chapter_slug) or "0"
    return out[:max_len]


def is_lock_wait_timeout_error(exc: Exception) -> bool:
    msg = normalize_text(exc).lower()
    if "lock wait timeout" in msg:
        return True
    args = getattr(exc, "args", ())
    if args and str(args[0]) == "1205":
        return True
    return False


def resolve_desustream_link(url: str, timeout_sec: int = 25) -> str:
    raw = normalize_text(url)
    if not raw:
        return ""
    host = (urlparse(raw).netloc or "").lower()
    if "link.desustream.com" not in host:
        return raw
    try:
        r = requests.get(raw, timeout=timeout_sec, allow_redirects=False, headers={"User-Agent": "Mozilla/5.0"})
        loc = normalize_text(r.headers.get("Location"))
        if loc:
            return urljoin(raw, loc)
    except Exception:
        pass
    try:
        r2 = requests.get(raw, timeout=timeout_sec, allow_redirects=True, headers={"User-Agent": "Mozilla/5.0"})
        final = normalize_text(r2.url)
        if final:
            return final
    except Exception:
        pass
    return raw


def sync_single_slug(
    conn,
    slug: str,
    series_id: int,
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

    detail_url = OTAKU_DETAIL_URL_TEMPLATE.format(slug=quote_plus(slug))
    detail_html = fetch_html(detail_url)
    detail_data = parse_otakudesu_anime_detail(detail_html)
    report["series_title"] = normalize_text(detail_data.get("title"))
    report["series_cover"] = normalize_text(detail_data.get("cover_url"))
    chapters = parse_otakudesu_episode_list(detail_html)
    report["episodes_candidate"] = len(chapters)

    # Step 1: check DB first, so we only fetch stream pages for truly new episodes.
    new_chapters: List[dict] = []
    with conn.cursor() as cur:
        by_source, by_slug, by_chapter = get_existing_episodes(cur, series_id)

        for ch in chapters:
            chapter_slug = clean_slug(ch.get("chapter_slug"))
            if not chapter_slug:
                continue
            source_episode_id = synthetic_source_episode_id(chapter_slug)
            chapter_label = build_db_chapter_label(ch.get("chapter_label"), chapter_slug)
            chapter_no = extract_chapter_number(chapter_label, chapter_slug)

            episode_id = by_source.get(source_episode_id) or by_slug.get(chapter_slug) or (by_chapter.get(chapter_no) if chapter_no else None)
            if episode_id:
                continue

            new_chapters.append(
                {
                    "chapter_slug": chapter_slug,
                    "chapter_label": chapter_label,
                    "release_date_text": normalize_text(ch.get("release_date_text")),
                    "episode_url": normalize_text(ch.get("episode_url")) or f"https://otakudesu.blog/episode/{chapter_slug}/",
                    "source_episode_id": source_episode_id,
                    "chapter_no": chapter_no,
                }
            )

    report["episodes_new"] = len(new_chapters)
    if dry_run or not new_chapters:
        return report

    # Step 2: fetch streams only for new episodes (big speed-up).
    prepared_new: List[dict] = []
    for ch in new_chapters:
        stream_pairs: List[Tuple[str, str]] = []
        try:
            ep_html = fetch_html(ch["episode_url"])
            raw_stream_pairs = parse_otakudesu_episode_streams(ep_html)
            for resolution, stream_url in raw_stream_pairs:
                stream_pairs.append((resolution, resolve_desustream_link(stream_url)))
        except Exception as exc:
            report["errors"].append(f"chapter {ch['chapter_slug']}: {exc}")
        ch["stream_pairs"] = stream_pairs
        prepared_new.append(ch)

    # Step 3: insert only new episodes/streams.
    with conn.cursor() as cur:
        by_source, by_slug, by_chapter = get_existing_episodes(cur, series_id)
        for ch in prepared_new:
            chapter_slug = ch["chapter_slug"]
            source_episode_id = int(ch["source_episode_id"])
            chapter_no = normalize_text(ch.get("chapter_no"))
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
                        ch["chapter_label"],
                        chapter_slug,
                        ch["release_date_text"],
                    ),
                )
                episode_id = int(cur.lastrowid)
                report["episodes_inserted"] += 1
                by_source[source_episode_id] = episode_id
                by_slug[chapter_slug] = episode_id
                if chapter_no:
                    by_chapter[chapter_no] = episode_id
                report["inserted_episodes"].append(
                    {
                        "source_episode_id": source_episode_id,
                        "chapter_slug": chapter_slug,
                        "chapter_label": ch["chapter_label"],
                        "release_date_text": ch["release_date_text"],
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
                to_insert = map_stream_rows_from_otakudesu(episode_id, ch.get("stream_pairs") or [])
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
    parser = argparse.ArgumentParser(description="Bot Anibi Otakudesu: autosync + enrich detail for new series")
    parser.add_argument("--slug", action="append", default=[], help="Process one slug only (can repeat)")
    parser.add_argument("--limit", type=int, default=0, help="Limit total slug from ongoing list")
    parser.add_argument("--dry-run", action="store_true", help="No write to DB")
    parser.add_argument("--no-translate-synopsis", action="store_true", help="Do not translate MAL synopsis to Indonesian")
    parser.add_argument("--ongoing-url", default=DEFAULT_ONGOING_URL)
    parser.add_argument("--otakudesu-max-pages", type=int, default=5, help="Max pages ongoing Otakudesu")
    parser.add_argument("--notify", action="store_true", help="Send broadcast notification on new series/episode")
    parser.add_argument("--no-notify", action="store_true", help="Disable notification for this run")
    parser.add_argument("--notify-url", default=DEFAULT_NOTIFY_URL)
    parser.add_argument("--notify-secret", default=DEFAULT_NOTIFY_SECRET)
    parser.add_argument("--notify-topic", default=DEFAULT_NOTIFY_TOPIC)
    parser.add_argument("--enrich-existing", action="store_true", help="Enrich title2/rating/synopsis for existing series too")
    parser.add_argument("--skip-slug", action="append", default=[], help="Slug to skip (can repeat)")
    args = parser.parse_args()

    load_dotenv(".env")
    translate_synopsis = not args.no_translate_synopsis
    env_notify_auto = normalize_text(os.getenv("NOTIFY_AUTO")).lower() in {"1", "true", "yes", "on"}
    notify_requested = bool(args.notify or env_notify_auto)
    if DEFAULT_NOTIFY_AUTO:
        notify_requested = True
    if args.no_notify:
        notify_requested = False
    if not normalize_text(args.notify_url):
        args.notify_url = DEFAULT_NOTIFY_URL
    if not normalize_text(args.notify_secret):
        args.notify_secret = DEFAULT_NOTIFY_SECRET

    if args.slug:
        slugs = [clean_slug(s) for s in args.slug if clean_slug(s)]
    else:
        ongoing_url = normalize_text(args.ongoing_url) or DEFAULT_OTAKUDESU_ONGOING_URL
        max_pages = max(1, min(int(args.otakudesu_max_pages or 5), 30))
        slugs = collect_ongoing_slugs_otakudesu(ongoing_url, max_pages=max_pages)
    if args.limit and args.limit > 0:
        slugs = slugs[: args.limit]
    if not slugs:
        print("No slug to process.")
        return 0

    skip_slugs = {clean_slug(s) for s in DEFAULT_SKIP_SLUGS}
    for x in (args.skip_slug or []):
        s = clean_slug(x)
        if s:
            skip_slugs.add(s)

    print(f"Target slug count: {len(slugs)}")
    conn = get_mysql_conn()
    run_lock_name = "bot_anibi_otakudesu_single_run"
    with conn.cursor() as cur:
        if not acquire_run_lock(cur, run_lock_name, timeout_sec=1):
            print("[SKIP] proses lain masih jalan (run lock aktif).")
            conn.close()
            return 0
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

    notify_enabled = bool(notify_requested and not args.dry_run and normalize_text(args.notify_url) and normalize_text(args.notify_secret))
    if notify_requested and not notify_enabled:
        print("[WARN] notify aktif tapi NOTIFY URL/SECRET kosong, notifikasi dilewati.")

    try:
        for slug in slugs:
            if slug in skip_slugs:
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
                    "skipped": True,
                    "skip_reason": "slug_blocklist",
                }
                summary["items"].append(item)
                print(f"[SKIP] slug={slug} reason=slug_blocklist")
                continue

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
            slug_done = False
            for attempt in range(1, LOCK_RETRY_ATTEMPTS + 1):
                try:
                    existed_series = False
                    with conn.cursor() as cur:
                        row = get_series_by_slug(cur, slug)
                        if row:
                            existed_series = True
                            series_id = int(row["id"])
                            base_title = normalize_text(row.get("title"))
                            item["series_title"] = base_title
                            if not args.dry_run and args.enrich_existing:
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
                        else:
                            detail_url = OTAKU_DETAIL_URL_TEMPLATE.format(slug=quote_plus(slug))
                            detail_html = fetch_html(detail_url)
                            detail_data = parse_otakudesu_anime_detail(detail_html)
                            payload = map_series_payload_from_otakudesu(detail_data, slug)
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
                                dry_run=False,
                            )
                            item["sync"] = sync_report
                            if normalize_text(sync_report.get("series_title")):
                                item["series_title"] = normalize_text(sync_report.get("series_title"))
                            if normalize_text(sync_report.get("series_cover")):
                                item["series_cover"] = normalize_text(sync_report.get("series_cover"))

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
                    if existed_series:
                        summary["series_existing"] += 1
                    if item.get("inserted_series"):
                        summary["series_inserted"] += 1
                    if item.get("detail_enriched"):
                        summary["series_detail_enriched"] += 1
                    sync_final = item.get("sync") or {}
                    summary["episodes_inserted"] += int(sync_final.get("episodes_inserted", 0))
                    summary["streams_inserted"] += int(sync_final.get("streams_inserted", 0))
                    summary["errors"] += len(sync_final.get("errors") or [])
                    slug_done = True
                    break
                except Exception as exc:
                    conn.rollback()
                    if is_lock_wait_timeout_error(exc) and attempt < LOCK_RETRY_ATTEMPTS:
                        sleep_sec = 0.8 * attempt
                        print(f"[RETRY] slug={slug} lock-timeout attempt={attempt}/{LOCK_RETRY_ATTEMPTS} wait={sleep_sec:.1f}s")
                        time.sleep(sleep_sec)
                        continue
                    if is_lock_wait_timeout_error(exc):
                        item["error"] = None
                        item["skipped"] = True
                        item["skip_reason"] = "lock_timeout"
                        break
                    item["error"] = str(exc)
                    summary["errors"] += 1
                    break

            if not slug_done and not item["error"] and not item.get("skipped"):
                item["error"] = "failed after retries"
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
        try:
            with conn.cursor() as cur:
                release_run_lock(cur, run_lock_name)
        except Exception:
            pass
        conn.close()

#    print("\n=== SUMMARY ===")
#    print(json.dumps(summary, indent=2, ensure_ascii=False))
    return 0 if summary["errors"] == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
