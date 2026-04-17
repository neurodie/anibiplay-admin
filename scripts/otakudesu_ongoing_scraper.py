#!/usr/bin/env python3
"""Scrape ongoing anime list from otakudesu.blog."""

from __future__ import annotations

import argparse
import json
import re
import sys
from dataclasses import asdict, dataclass
from typing import List, Optional, Tuple
from urllib.parse import quote_plus, urljoin, urlparse

import requests
from bs4 import BeautifulSoup

BASE_URL = "https://otakudesu.blog/ongoing-anime/"
DEFAULT_TIMEOUT = 20


@dataclass
class OngoingAnime:
    title: str
    episode: str
    day: str
    release_date: str
    anime_url: str
    thumbnail_url: str


@dataclass
class AnimeDetail:
    source_url: str
    slug: str
    title: str
    thumbnail_url: str
    synopsis: str
    info: dict
    genres: List[str]
    mal: Optional[dict] = None


def fetch_html(url: str, timeout: int = DEFAULT_TIMEOUT) -> str:
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept-Language": "id-ID,id;q=0.9,en-US;q=0.8,en;q=0.7",
    }
    response = requests.get(url, headers=headers, timeout=timeout)
    response.raise_for_status()
    return response.text


def extract_slug(url: str) -> str:
    path = urlparse(url).path.strip("/")
    parts = path.split("/")
    if not parts:
        return ""
    return parts[-1]


def normalize_text(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def extract_labeled_text(soup: BeautifulSoup, label: str) -> str:
    labels = soup.select("span.dark_text")
    for lb in labels:
        raw = normalize_text(lb.get_text(" ", strip=True))
        if raw.rstrip(":").lower() == label.rstrip(":").lower():
            parent = lb.parent
            if not parent:
                return ""
            value = parent.get_text(" ", strip=True)
            value = value.replace(lb.get_text(" ", strip=True), "", 1)
            return normalize_text(value)
    return ""


def parse_items(html: str) -> Tuple[List[OngoingAnime], Optional[str]]:
    soup = BeautifulSoup(html, "html.parser")
    cards = soup.select(".venz ul li .detpost")

    items: List[OngoingAnime] = []
    for card in cards:
        title_el = card.select_one("h2.jdlflm")
        episode_el = card.select_one(".epz")
        day_el = card.select_one(".epztipe")
        date_el = card.select_one(".newnime")
        link_el = card.select_one(".thumb a")
        img_el = card.select_one(".thumb img")

        anime_link = link_el.get("href", "") if link_el else ""

        item = OngoingAnime(
            title=(title_el.get_text(strip=True) if title_el else ""),
            episode=(episode_el.get_text(" ", strip=True) if episode_el else ""),
            day=(day_el.get_text(" ", strip=True) if day_el else ""),
            release_date=(date_el.get_text(strip=True) if date_el else ""),
            anime_url=extract_slug(anime_link),
            thumbnail_url=(img_el.get("src", "") if img_el else ""),
        )

        if item.title and item.anime_url:
            items.append(item)

    next_page_el = soup.select_one(".pagination .next.page-numbers")
    next_page_url = None
    if next_page_el and next_page_el.get("href"):
        next_page_url = urljoin(BASE_URL, next_page_el["href"])

    return items, next_page_url


def parse_anime_detail(html: str, source_url: str) -> AnimeDetail:
    soup = BeautifulSoup(html, "html.parser")

    title_el = soup.select_one(".venser .jdlrx h1")
    thumbnail_el = soup.select_one(".fotoanime img")
    synopsis_el = soup.select_one(".sinopc")

    info: dict = {}
    for p in soup.select(".infozingle p"):
        text = normalize_text(p.get_text(" ", strip=True))
        if ":" not in text:
            continue
        key, value = text.split(":", 1)
        info[key.strip()] = value.strip()

    genres = [normalize_text(a.get_text(" ", strip=True)) for a in soup.select(".infozingle p a")]
    return AnimeDetail(
        source_url=source_url,
        slug=extract_slug(source_url),
        title=normalize_text(title_el.get_text(" ", strip=True)) if title_el else "",
        thumbnail_url=thumbnail_el.get("src", "") if thumbnail_el else "",
        synopsis=normalize_text(synopsis_el.get_text(" ", strip=True)) if synopsis_el else "",
        info=info,
        genres=[g for g in genres if g],
    )


def find_first_mal_anime_url(search_html: str) -> str:
    soup = BeautifulSoup(search_html, "html.parser")
    for a in soup.select("a.hoverinfo_trigger[href*='/anime/']"):
        href = a.get("href", "")
        if "/anime/" in href:
            return href
    return ""


def parse_mal_detail(html: str) -> dict:
    soup = BeautifulSoup(html, "html.parser")
    title_el = soup.select_one("h1.title-name")
    score_el = soup.select_one("div.score-label")
    synopsis_el = soup.select_one("p[itemprop='description']") or soup.select_one("[itemprop='description']")

    title = normalize_text(title_el.get_text(" ", strip=True)) if title_el else ""
    english_title = extract_labeled_text(soup, "English")
    japanese_title = extract_labeled_text(soup, "Japanese")
    score = normalize_text(score_el.get_text(" ", strip=True)) if score_el else ""
    synopsis = normalize_text(synopsis_el.get_text(" ", strip=True)) if synopsis_el else ""

    if synopsis.lower().startswith("no synopsis information"):
        synopsis = ""

    return {
        "title": title,
        "english_title": english_title,
        "japanese_title": japanese_title,
        "score": score,
        "synopsis": synopsis,
    }


def scrape_mal_from_query(query: str) -> dict:
    search_url = f"https://myanimelist.net/anime.php?q={quote_plus(query)}&cat=anime"
    search_html = fetch_html(search_url)
    anime_url = find_first_mal_anime_url(search_html)
    if not anime_url:
        return {"search_url": search_url, "url": "", "title": "", "english_title": "", "japanese_title": "", "score": "", "synopsis": ""}

    detail_html = fetch_html(anime_url)
    mal_data = parse_mal_detail(detail_html)
    mal_data["search_url"] = search_url
    mal_data["url"] = anime_url
    return mal_data


def scrape_ongoing(max_pages: int = 1, start_url: str = BASE_URL) -> List[OngoingAnime]:
    if max_pages < 1:
        raise ValueError("max_pages must be >= 1")

    url = start_url
    all_items: List[OngoingAnime] = []

    for _ in range(max_pages):
        html = fetch_html(url)
        items, next_page_url = parse_items(html)
        all_items.extend(items)

        if not next_page_url:
            break
        url = next_page_url

    return all_items


def scrape_detail(detail_url: str) -> AnimeDetail:
    html = fetch_html(detail_url)
    return parse_anime_detail(html, detail_url)


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Scrape ongoing or detail anime data from otakudesu.blog")
    parser.add_argument(
        "--mode",
        choices=["ongoing", "detail"],
        default="ongoing",
        help="Scrape mode: ongoing list or anime detail (default: ongoing)",
    )
    parser.add_argument(
        "--pages",
        type=int,
        default=1,
        help="Number of ongoing pages to scrape (default: 1). Only used in ongoing mode.",
    )
    parser.add_argument(
        "--detail-url",
        type=str,
        default="",
        help="Anime detail URL. Required in detail mode.",
    )
    parser.add_argument(
        "--mal-query",
        type=str,
        default="",
        help="Custom query for MAL search. Optional in detail mode.",
    )
    parser.add_argument(
        "--output",
        type=str,
        default="",
        help="Output file path (.json). If empty, prints to stdout.",
    )
    parser.add_argument(
        "--compact",
        action="store_true",
        help="Use compact JSON output (no pretty formatting).",
    )
    return parser


def main() -> int:
    parser = build_arg_parser()
    args = parser.parse_args()

    try:
        if args.mode == "ongoing":
            items = scrape_ongoing(max_pages=args.pages)
            payload = {
                "mode": "ongoing",
                "source": BASE_URL,
                "pages_scraped": args.pages,
                "count": len(items),
                "data": [asdict(item) for item in items],
            }
        else:
            if not args.detail_url:
                parser.error("--detail-url wajib diisi saat --mode detail")
            detail = scrape_detail(args.detail_url)
            query = args.mal_query or detail.info.get("Judul") or detail.title.replace(" Sub Indo", "").strip()
            try:
                mal_data = scrape_mal_from_query(query)
                detail.mal = mal_data

                if not detail.synopsis and mal_data.get("synopsis"):
                    detail.synopsis = mal_data["synopsis"]
                if not detail.info.get("Skor", "").strip() and mal_data.get("score"):
                    detail.info["Skor"] = mal_data["score"]
                if not detail.info.get("English", "").strip() and mal_data.get("english_title"):
                    detail.info["English"] = mal_data["english_title"]
                if not detail.info.get("Japanese", "").strip() and mal_data.get("japanese_title"):
                    detail.info["Japanese"] = mal_data["japanese_title"]
            except Exception as mal_exc:
                detail.mal = {"error": str(mal_exc), "search_url": ""}

            payload = {
                "mode": "detail",
                "data": asdict(detail),
            }
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1

    if args.compact:
        output_text = json.dumps(payload, ensure_ascii=False)
    else:
        output_text = json.dumps(payload, ensure_ascii=False, indent=2)

    if args.output:
        with open(args.output, "w", encoding="utf-8") as f:
            f.write(output_text)
        saved_count = payload["count"] if args.mode == "ongoing" else 1
        print(f"Saved {saved_count} items to {args.output}")
    else:
        print(output_text)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
