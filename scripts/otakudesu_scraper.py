#!/usr/bin/env python3
"""
Simple scraper for https://otakudesu.blog/

Features:
- Scrape homepage sections: ongoing anime, complete anime
- Search anime via query (?s=...&post_type=anime)
- Scrape episode list from anime detail page
- Scrape MP4 download links from episode page
- Output to terminal, JSON, or CSV

Usage examples:
  python otakudesu_scraper.py ongoing --limit 10
  python otakudesu_scraper.py complete --format json --output complete.json
  python otakudesu_scraper.py search "naruto" --limit 20
  python otakudesu_scraper.py detail "https://otakudesu.blog/anime/shunkashuutou-daikousha-sub-indo/"
  python otakudesu_scraper.py episode "https://otakudesu.blog/episode/jgkrk-s2-episode-11-sub-indo/"
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import sys
from dataclasses import asdict, dataclass
from typing import Iterable
from urllib.parse import quote_plus, urljoin, urlparse

import requests
from bs4 import BeautifulSoup, Tag

BASE_URL = "https://otakudesu.blog"
DEFAULT_TIMEOUT = 25


@dataclass
class AnimeItem:
    title: str
    url: str
    episode_info: str = ""
    meta_info: str = ""
    date_info: str = ""
    image_url: str = ""
    source_section: str = ""


@dataclass
class DownloadItem:
    resolution: str
    host: str
    url: str
    resolved_url: str = ""
    source_section: str = "episode_mp4"


class OtakudesuScraper:
    def __init__(self, timeout: int = DEFAULT_TIMEOUT) -> None:
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/125.0.0.0 Safari/537.36"
                )
            }
        )

    def _get_soup(self, url: str) -> BeautifulSoup:
        resp = self.session.get(url, timeout=self.timeout)
        resp.raise_for_status()
        return BeautifulSoup(resp.text, "html.parser")

    @staticmethod
    def _clean(text: str) -> str:
        return re.sub(r"\s+", " ", text).strip()

    def _resolve_short_url(self, url: str) -> str:
        """Resolve short/intermediate URL to final target when possible."""
        host = (urlparse(url).netloc or "").lower()
        if "link.desustream.com" not in host:
            return url

        try:
            # Step 1: inspect redirect Location quickly.
            r = self.session.get(url, timeout=self.timeout, allow_redirects=False)
            loc = (r.headers.get("Location") or "").strip()
            if loc:
                return urljoin(url, loc)

            # Step 2 fallback: follow redirects to final URL.
            r2 = self.session.get(url, timeout=self.timeout, allow_redirects=True)
            if r2.url:
                return r2.url
        except requests.RequestException:
            pass
        return url

    def _parse_cards(self, container: Tag, section_name: str) -> list[AnimeItem]:
        items: list[AnimeItem] = []

        for card in container.select("li .detpost"):
            title_tag = card.select_one("h2.jdlflm")
            link_tag = card.select_one(".thumb a[href]")
            ep_tag = card.select_one(".epz")
            meta_tag = card.select_one(".epztipe")
            date_tag = card.select_one(".newnime")
            img_tag = card.select_one(".thumb img")

            if not title_tag or not link_tag:
                continue

            items.append(
                AnimeItem(
                    title=self._clean(title_tag.get_text(" ", strip=True)),
                    url=urljoin(BASE_URL, link_tag.get("href", "")),
                    episode_info=self._clean(ep_tag.get_text(" ", strip=True)) if ep_tag else "",
                    meta_info=self._clean(meta_tag.get_text(" ", strip=True)) if meta_tag else "",
                    date_info=self._clean(date_tag.get_text(" ", strip=True)) if date_tag else "",
                    image_url=urljoin(BASE_URL, img_tag.get("src", "")) if img_tag else "",
                    source_section=section_name,
                )
            )

        return items

    def scrape_home_sections(self) -> dict[str, list[AnimeItem]]:
        soup = self._get_soup(BASE_URL + "/")
        results: dict[str, list[AnimeItem]] = {"ongoing": [], "complete": []}

        # Section block example:
        # <div class="rseries"> ... <h1>On-going Anime</h1> ... <div class="venz"><ul>...
        for block in soup.select("div.rseries"):
            heading = block.select_one(".rvad h1")
            if not heading:
                continue

            heading_text = self._clean(heading.get_text(" ", strip=True)).lower()
            cards_container = block.select_one(".venz ul")
            if not cards_container:
                continue

            if "on-going" in heading_text or "ongoing" in heading_text:
                results["ongoing"].extend(self._parse_cards(cards_container, "ongoing"))
            elif "complete" in heading_text:
                results["complete"].extend(self._parse_cards(cards_container, "complete"))

        return results

    def scrape_ongoing(self) -> list[AnimeItem]:
        return self.scrape_home_sections()["ongoing"]

    def scrape_complete(self) -> list[AnimeItem]:
        return self.scrape_home_sections()["complete"]

    def search(self, query: str) -> list[AnimeItem]:
        q = quote_plus(query)
        url = f"{BASE_URL}/?s={q}&post_type=anime"
        soup = self._get_soup(url)

        results: list[AnimeItem] = []

        for block in soup.select("div.venz ul"):
            results.extend(self._parse_cards(block, "search"))

        # Fallback if search layout differs
        if not results:
            for a in soup.select("a[href*='/anime/']"):
                href = a.get("href", "")
                title = self._clean(a.get_text(" ", strip=True))
                if not href or not title:
                    continue
                if len(title) < 3:
                    continue

                results.append(
                    AnimeItem(
                        title=title,
                        url=urljoin(BASE_URL, href),
                        source_section="search",
                    )
                )

            unique: dict[str, AnimeItem] = {}
            for item in results:
                unique[item.url] = item
            results = list(unique.values())

        return results

    def scrape_episode_list(self, anime_url: str) -> list[AnimeItem]:
        soup = self._get_soup(anime_url)
        results: list[AnimeItem] = []

        target_list: Tag | None = None
        for block in soup.select("div.episodelist"):
            title_tag = block.select_one(".monktit")
            if not title_tag:
                continue
            block_title = self._clean(title_tag.get_text(" ", strip=True)).lower()
            if "episode list" in block_title:
                target_list = block
                break

        if target_list is None:
            return results

        for li in target_list.select("ul > li"):
            link_tag = li.select_one("a[href]")
            if not link_tag:
                continue

            date_tag = li.select_one(".zeebr")
            title = self._clean(link_tag.get_text(" ", strip=True))
            url = urljoin(BASE_URL, link_tag.get("href", ""))
            date_info = self._clean(date_tag.get_text(" ", strip=True)) if date_tag else ""

            if not title or not url:
                continue

            results.append(
                AnimeItem(
                    title=title,
                    url=url,
                    date_info=date_info,
                    source_section="detail_episode",
                )
            )

        return results

    def scrape_episode_mp4_links(self, episode_url: str, resolve_links: bool = True) -> list[DownloadItem]:
        soup = self._get_soup(episode_url)
        results: list[DownloadItem] = []

        for li in soup.select("div.download ul li"):
            strong = li.select_one("strong")
            if not strong:
                continue

            resolution = self._clean(strong.get_text(" ", strip=True))
            if not resolution.lower().startswith("mp4"):
                continue

            for a in li.select("a[href]"):
                host = self._clean(a.get_text(" ", strip=True))
                href = urljoin(BASE_URL, a.get("href", ""))
                if not host or not href:
                    continue
                resolved = self._resolve_short_url(href) if resolve_links else href
                results.append(
                    DownloadItem(
                        resolution=resolution,
                        host=host,
                        url=href,
                        resolved_url=resolved,
                    )
                )

        return results


def to_json(items: Iterable[AnimeItem], output_path: str | None = None) -> None:
    data = [asdict(item) for item in items]
    text = json.dumps(data, indent=2, ensure_ascii=False)

    if output_path:
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(text)
        print(f"Saved JSON: {output_path}")
    else:
        print(text)


def to_json_download(items: Iterable[DownloadItem], output_path: str | None = None) -> None:
    data = [asdict(item) for item in items]
    text = json.dumps(data, indent=2, ensure_ascii=False)

    if output_path:
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(text)
        print(f"Saved JSON: {output_path}")
    else:
        print(text)


def to_csv(items: Iterable[AnimeItem], output_path: str) -> None:
    rows = [asdict(item) for item in items]
    if not rows:
        with open(output_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["title", "url", "episode_info", "meta_info", "date_info", "image_url", "source_section"])
        print(f"Saved empty CSV with headers: {output_path}")
        return

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)

    print(f"Saved CSV: {output_path}")


def to_csv_download(items: Iterable[DownloadItem], output_path: str) -> None:
    rows = [asdict(item) for item in items]
    if not rows:
        with open(output_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["resolution", "host", "url", "resolved_url", "source_section"])
        print(f"Saved empty CSV with headers: {output_path}")
        return

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)

    print(f"Saved CSV: {output_path}")


def print_text(items: list[AnimeItem]) -> None:
    if not items:
        print("No data found.")
        return

    for i, item in enumerate(items, start=1):
        print(f"{i}. {item.title}")
        print(f"   URL        : {item.url}")
        if item.episode_info:
            print(f"   Episode    : {item.episode_info}")
        if item.meta_info:
            print(f"   Meta       : {item.meta_info}")
        if item.date_info:
            print(f"   Date       : {item.date_info}")
        if item.image_url:
            print(f"   Image      : {item.image_url}")
        print(f"   Section    : {item.source_section}")


def print_download_text(items: list[DownloadItem]) -> None:
    if not items:
        print("No MP4 download links found.")
        return

    for i, item in enumerate(items, start=1):
        print(f"{i}. {item.resolution} - {item.host}")
        print(f"   URL        : {item.url}")
        if item.resolved_url and item.resolved_url != item.url:
            print(f"   Resolved   : {item.resolved_url}")
        print(f"   Section    : {item.source_section}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Scraper untuk otakudesu.blog")
    sub = parser.add_subparsers(dest="command", required=True)

    def add_common(p: argparse.ArgumentParser) -> None:
        p.add_argument("--limit", type=int, default=20, help="Batasi jumlah hasil")
        p.add_argument("--format", choices=["text", "json", "csv"], default="text", help="Format output")
        p.add_argument("--output", help="Path file output (wajib untuk csv, opsional untuk json)")
        p.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT, help="HTTP timeout (detik)")

    p_ongoing = sub.add_parser("ongoing", help="Ambil daftar ongoing dari homepage")
    add_common(p_ongoing)

    p_complete = sub.add_parser("complete", help="Ambil daftar complete dari homepage")
    add_common(p_complete)

    p_search = sub.add_parser("search", help="Cari anime")
    p_search.add_argument("query", help="Keyword pencarian")
    add_common(p_search)

    p_detail = sub.add_parser("detail", help="Ambil episode list dari halaman detail anime")
    p_detail.add_argument("url", help="URL halaman detail anime")
    add_common(p_detail)

    p_episode = sub.add_parser("episode", help="Ambil link download MP4 dari halaman episode")
    p_episode.add_argument("url", help="URL halaman episode")
    p_episode.add_argument(
        "--no-resolve-links",
        action="store_true",
        help="Jangan resolve short link (lebih cepat).",
    )
    add_common(p_episode)

    return parser.parse_args()


def main() -> int:
    args = parse_args()

    scraper = OtakudesuScraper(timeout=args.timeout)

    try:
        if args.command == "ongoing":
            items = scraper.scrape_ongoing()
        elif args.command == "complete":
            items = scraper.scrape_complete()
        elif args.command == "search":
            items = scraper.search(args.query)
        elif args.command == "detail":
            items = scraper.scrape_episode_list(args.url)
        elif args.command == "episode":
            items = scraper.scrape_episode_mp4_links(args.url, resolve_links=not args.no_resolve_links)
        else:
            raise ValueError(f"Unknown command: {args.command}")
    except requests.RequestException as exc:
        print(f"HTTP error: {exc}", file=sys.stderr)
        return 1

    if args.limit > 0:
        items = items[: args.limit]

    if args.format == "json":
        if args.command == "episode":
            to_json_download(items, args.output)
        else:
            to_json(items, args.output)
    elif args.format == "csv":
        if not args.output:
            print("--output wajib untuk format csv", file=sys.stderr)
            return 2
        if args.command == "episode":
            to_csv_download(items, args.output)
        else:
            to_csv(items, args.output)
    else:
        if args.command == "episode":
            print_download_text(items)
        else:
            print_text(items)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
