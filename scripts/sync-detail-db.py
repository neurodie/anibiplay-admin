#!/usr/bin/env python3
"""
Sync field kosong series dari MAL berdasarkan daftar anime update terbaru di DB.

Aturan:
- Sumber list dari DB (bukan API ongoing)
- Hanya update field kosong: title2, rating, synopsis
- Tidak mengubah created_at / updated_at / views secara eksplisit
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
from typing import Dict, List, Optional, Tuple
from urllib.parse import quote_plus

import requests
from bs4 import BeautifulSoup

try:
    import pymysql
    from pymysql.cursors import DictCursor
except Exception:  # pragma: no cover
    print("ERROR: pymysql belum terinstall. Install dengan: pip install pymysql", file=sys.stderr)
    raise


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


def is_blank_text(value: object) -> bool:
    s = normalize_text(value).lower()
    return s in {"", "-", "null", "none", "n/a", "na", "unknown"}


def is_bad_translated_synopsis(value: object) -> bool:
    s = normalize_text(value).lower()
    if not s:
        return False
    return (
        "query length limit exceeded" in s
        or "max allowed query" in s
        or s.startswith("mymemory warning")
    )


def fetch_html(url: str, timeout_sec: int = 25, max_attempts: int = 4) -> str:
    last_err: Optional[Exception] = None
    for i in range(1, max_attempts + 1):
        try:
            resp = requests.get(
                url,
                timeout=timeout_sec,
                headers={
                    "User-Agent": (
                        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/124.0.0.0 Safari/537.36"
                    ),
                    "Accept-Language": "id-ID,id;q=0.9,en-US;q=0.8,en;q=0.7",
                },
            )
            resp.raise_for_status()
            return resp.text
        except Exception as exc:
            last_err = exc
            if i < max_attempts:
                time.sleep(0.3 * i)
    raise RuntimeError(f"fetch failed ({url}): {last_err}")


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


def find_first_mal_anime_url(search_html: str) -> str:
    soup = BeautifulSoup(search_html, "html.parser")
    for a in soup.select("a.hoverinfo_trigger[href*='/anime/']"):
        href = normalize_text(a.get("href"))
        if "/anime/" in href:
            return href
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
        return {
            "search_url": search_url,
            "url": "",
            "title": "",
            "english_title": "",
            "japanese_title": "",
            "score": "",
            "synopsis": "",
        }
    detail_html = fetch_html(anime_url)
    data = parse_mal_detail(detail_html)
    data["search_url"] = search_url
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
    resp = requests.get(
        api_url,
        params=params,
        timeout=25,
        headers={"User-Agent": "Mozilla/5.0"},
    )
    resp.raise_for_status()
    payload = resp.json()
    translated = normalize_text((payload or {}).get("responseData", {}).get("translatedText"))
    if is_bad_translated_synopsis(translated):
        return ""
    return translated


def translate_to_indonesian(text: str, cache: Dict[str, str]) -> Tuple[str, str]:
    src = normalize_text(text)
    if not src:
        return "", "empty"
    if src in cache:
        return cache[src], "cache"

    chunks = split_text_for_translation(src, max_len=450)
    if not chunks:
        cache[src] = src
        return src, "fallback_original"

    translated_chunks: List[str] = []
    try:
        for ch in chunks:
            translated = translate_chunk_mymemory(ch)
            if not translated:
                raise RuntimeError("translation chunk empty or rate-limited")
            translated_chunks.append(translated)
        merged = normalize_text(" ".join(translated_chunks))
        if merged:
            cache[src] = merged
            return merged, "mymemory_chunked"
    except Exception:
        pass

    cache[src] = src
    return src, "fallback_original"


def to_float_or_none(value: object) -> Optional[float]:
    s = normalize_text(value)
    if not s:
        return None
    try:
        return float(s)
    except Exception:
        return None


def should_update_rating(v: object) -> bool:
    fv = to_float_or_none(v)
    return fv is None or fv <= 0.0


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


def get_recent_series(cur, content_origin: str, source_platform: str, limit: int, list_source: str) -> List[dict]:
    if list_source == "series":
        cur.execute(
            """
            SELECT id, source_platform, content_origin, series_slug, title, title2, rating, synopsis
            FROM series
            WHERE (%s = '' OR content_origin = %s)
              AND (%s = '' OR source_platform = %s)
              AND COALESCE(series_slug, '') <> ''
            ORDER BY id DESC
            LIMIT %s
            """,
            (content_origin, content_origin, source_platform, source_platform, limit),
        )
        return cur.fetchall() or []

    cur.execute(
        """
        SELECT s.id, s.source_platform, s.content_origin, s.series_slug, s.title, s.title2, s.rating, s.synopsis
        FROM series s
        JOIN (
          SELECT series_id, MAX(id) AS max_episode_id
          FROM episodes
          GROUP BY series_id
        ) le ON le.series_id = s.id
        WHERE (%s = '' OR s.content_origin = %s)
          AND (%s = '' OR s.source_platform = %s)
          AND COALESCE(s.series_slug, '') <> ''
        ORDER BY le.max_episode_id DESC
        LIMIT %s
        """,
        (content_origin, content_origin, source_platform, source_platform, limit),
    )
    return cur.fetchall() or []


def build_mal_query_from_row(row: dict) -> str:
    slug = clean_slug(row.get("series_slug"))
    detail_url = OTAKU_DETAIL_URL_TEMPLATE.format(slug=quote_plus(slug))
    try:
        detail_html = fetch_html(detail_url)
        otaku_detail = parse_otakudesu_detail(detail_html)
        info = otaku_detail.get("info") or {}
        title = normalize_text(info.get("Judul"))
        if title:
            return title
        page_title = normalize_text(otaku_detail.get("title"))
        if page_title:
            return re.sub(r"\s+Sub\s+Indo$", "", page_title, flags=re.IGNORECASE).strip()
    except Exception:
        pass

    title_db = normalize_text(row.get("title"))
    if title_db:
        return title_db
    return slug.replace("-", " ").strip()


def update_series_fields(cur, series_id: int, updates: Dict[str, object]) -> None:
    if not updates:
        return
    cols = []
    vals: List[object] = []
    for k, v in updates.items():
        cols.append(f"{k} = %s")
        vals.append(v)
    vals.append(series_id)
    # Sengaja tidak menyentuh updated_at/created_at/views.
    sql = f"UPDATE series SET {', '.join(cols)} WHERE id = %s LIMIT 1"
    cur.execute(sql, tuple(vals))


def main() -> int:
    parser = argparse.ArgumentParser(description="Sync field kosong series terbaru dari DB menggunakan MAL")
    parser.add_argument("--content-origin", default="anime", help="Filter content_origin (default: anime)")
    parser.add_argument("--source-platform", default="", help="Filter source_platform opsional (default: semua)")
    parser.add_argument("--limit", type=int, default=100, help="Jumlah anime terbaru dari DB (default: 100)")
    parser.add_argument(
        "--list-source",
        choices=["episodes", "series"],
        default="episodes",
        help="Sumber daftar terbaru: dari update episode atau id series terbaru (default: episodes)",
    )
    parser.add_argument("--slug", action="append", default=[], help="Proses slug tertentu saja (override list terbaru)")
    parser.add_argument(
        "--translate-synopsis",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Terjemahkan synopsis MAL ke Bahasa Indonesia (default: true)",
    )
    parser.add_argument("--dry-run", action="store_true", help="Jangan write ke DB")
    args = parser.parse_args()

    load_dotenv(".env")
    conn = get_mysql_conn()
    translate_cache: Dict[str, str] = {}

    summary = {
        "total": 0,
        "updated": 0,
        "skipped_not_needed": 0,
        "skipped_no_mal_data": 0,
        "errors": 0,
        "items": [],
    }

    try:
        with conn.cursor() as cur:
            if args.slug:
                placeholders = ", ".join(["%s"] * len(args.slug))
                cur.execute(
                    f"""
                    SELECT id, source_platform, content_origin, series_slug, title, title2, rating, synopsis
                    FROM series
                    WHERE COALESCE(series_slug, '') <> ''
                      AND series_slug IN ({placeholders})
                      AND (%s = '' OR content_origin = %s)
                      AND (%s = '' OR source_platform = %s)
                    ORDER BY id DESC
                    """,
                    (
                        *[clean_slug(s) for s in args.slug],
                        args.content_origin,
                        args.content_origin,
                        args.source_platform,
                        args.source_platform,
                    ),
                )
                rows = cur.fetchall() or []
            else:
                rows = get_recent_series(
                    cur,
                    content_origin=args.content_origin,
                    source_platform=args.source_platform,
                    limit=args.limit,
                    list_source=args.list_source,
                )

        summary["total"] = len(rows)
        if not rows:
            print("No series to process.")
            return 0

        print(f"Target series count: {len(rows)}")

        for row in rows:
            item = {
                "slug": row.get("series_slug"),
                "series_id": int(row["id"]),
                "source_platform": row.get("source_platform"),
                "content_origin": row.get("content_origin"),
                "needs": {"title2": False, "rating": False, "synopsis": False},
                "updates": {},
                "mal_url": "",
                "error": None,
            }
            try:
                need_title2 = is_blank_text(row.get("title2"))
                need_rating = should_update_rating(row.get("rating"))
                need_synopsis = is_blank_text(row.get("synopsis")) or is_bad_translated_synopsis(row.get("synopsis"))

                item["needs"] = {
                    "title2": need_title2,
                    "rating": need_rating,
                    "synopsis": need_synopsis,
                }

                if not (need_title2 or need_rating or need_synopsis):
                    summary["skipped_not_needed"] += 1
                    summary["items"].append(item)
                    print(f"[SKIP] slug={item['slug']} series_id={item['series_id']} reason=not_needed")
                    continue

                query = build_mal_query_from_row(row)
                mal_data = scrape_mal_by_query(query)
                item["mal_url"] = mal_data.get("url", "")

                updates: Dict[str, object] = {}
                if need_title2 and not is_blank_text(mal_data.get("english_title")):
                    updates["title2"] = normalize_text(mal_data.get("english_title"))
                if need_rating:
                    rv = to_float_or_none(mal_data.get("score"))
                    if rv is not None and rv > 0:
                        updates["rating"] = rv
                if need_synopsis and not is_blank_text(mal_data.get("synopsis")):
                    syn_en = normalize_text(mal_data.get("synopsis"))
                    if args.translate_synopsis:
                        syn_id, source = translate_to_indonesian(syn_en, translate_cache)
                        item["synopsis_translation_source"] = source
                        updates["synopsis"] = syn_id
                    else:
                        updates["synopsis"] = syn_en

                item["updates"] = updates
                if not updates:
                    summary["skipped_no_mal_data"] += 1
                    summary["items"].append(item)
                    print(f"[SKIP] slug={item['slug']} series_id={item['series_id']} reason=no_mal_fields")
                    continue

                with conn.cursor() as cur:
                    if args.dry_run:
                        conn.rollback()
                    else:
                        update_series_fields(cur, item["series_id"], updates)
                        conn.commit()
                        summary["updated"] += 1

                summary["items"].append(item)
                print(f"[OK] slug={item['slug']} series_id={item['series_id']} updates={list(updates.keys())}")
            except Exception as exc:
                conn.rollback()
                item["error"] = str(exc)
                summary["errors"] += 1
                summary["items"].append(item)
                print(f"[ERR] slug={item['slug']} series_id={item['series_id']} err={exc}")
    finally:
        conn.close()

    print("\n=== SUMMARY ===")
    print(json.dumps(summary, indent=2, ensure_ascii=False, default=str))
    return 0 if summary["errors"] == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
