#!/usr/bin/env python3
"""
Donghub updater (Python version).

Behavior is aligned with Node updater `runDonghubOngoingUpdate`:
- Target ongoing series from DB (platform=donghub, status ongoing/currently airing)
- Fetch episode list from Donghub API
- Insert missing episodes + streams
- Optional mode: only backfill missing streams

Usage examples:
  python3 scripts/donghub_updater.py --dry-run
  python3 scripts/donghub_updater.py --limit 30 --sync-mode full_sync
  python3 scripts/donghub_updater.py --series-id 5205
  python3 scripts/donghub_updater.py --source-series-id 3135
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional, Set, Tuple

import pymysql
import requests
from pymysql.cursors import DictCursor

try:
    from Crypto.Cipher import AES
except Exception as exc:  # pragma: no cover
    print("ERROR: pycryptodome belum terinstall. Install: pip install pycryptodome", file=sys.stderr)
    raise exc


DEFAULT_BASE_URL = "https://restapi-micro.sutejo.com/api/v1"
DEFAULT_AES_KEY_HEX = "1334dd8dd4f713fdc112b539ae77c30b15a5ecc82ec870d8f0b60bc4ef958cb3"


def default_script_env_path() -> str:
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")


def load_dotenv(path: str) -> bool:
    env_path = os.path.abspath(path)
    if not os.path.exists(env_path):
        return False
    with open(env_path, "r", encoding="utf-8") as fh:
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
    return True


def now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def clean_text(v: object) -> str:
    return str(v or "").strip()


def clean_slug(v: object) -> str:
    return clean_text(v).strip("/")


def pad_unpad_pkcs7(data: bytes) -> bytes:
    if not data:
        return data
    pad = data[-1]
    if pad <= 0 or pad > 16:
        return data
    if data[-pad:] != bytes([pad]) * pad:
        return data
    return data[:-pad]


def parse_encrypted_response(text: str) -> dict:
    raw = clean_text(text)
    if not raw:
        raise RuntimeError("Empty response from donghub API")

    try:
        return json.loads(raw)
    except Exception:
        pass

    unquoted = raw
    if unquoted.startswith('"') and unquoted.endswith('"'):
        unquoted = unquoted[1:-1].replace('\\"', '"')

    if not re.fullmatch(r"[0-9a-fA-F]+", unquoted or "") or len(unquoted) < 32:
        preview = unquoted[:240]
        raise RuntimeError(f"Response is not encrypted hex payload. Preview: {preview}")

    key_hex = clean_text(os.getenv("DONGHUB_AES_KEY_HEX", DEFAULT_AES_KEY_HEX))
    key = bytes.fromhex(key_hex)
    data = bytes.fromhex(unquoted)
    if len(data) < 17:
        raise RuntimeError("Encrypted payload invalid length")
    iv = data[:16]
    ciphertext = data[16:]
    cipher = AES.new(key, AES.MODE_CBC, iv=iv)
    decrypted = cipher.decrypt(ciphertext)
    decrypted = pad_unpad_pkcs7(decrypted)
    return json.loads(decrypted.decode("utf-8"))


def make_base_headers() -> Dict[str, str]:
    headers = {
        "User-Agent": clean_text(os.getenv("DONGHUB_USER_AGENT", "Device/14 (Xiaomi) M2101K7BNY/UP1A.230905.011")),
        "Accept-Encoding": "gzip",
    }
    optional = {
        "authorization": os.getenv("DONGHUB_AUTH_BEARER"),
        "x-user": os.getenv("DONGHUB_X_USER"),
        "x-key": os.getenv("DONGHUB_X_KEY"),
        "x-version": os.getenv("DONGHUB_X_VERSION", "5.6"),
        "device-version": os.getenv("DONGHUB_DEVICE_VERSION", "14"),
        "device-model": os.getenv("DONGHUB_DEVICE_MODEL", "M2101K7BNY"),
        "installed-from-playstore": os.getenv("DONGHUB_INSTALLED_FROM_PLAYSTORE", "false"),
        "package-name": os.getenv("DONGHUB_PACKAGE_NAME", "com.anichin.donghub"),
        "android-id": os.getenv("DONGHUB_ANDROID_ID"),
        "installed-package-name": os.getenv("DONGHUB_INSTALLED_PACKAGE_NAME", "com.google.android.packageinstaller"),
        "version-code": os.getenv("DONGHUB_VERSION_CODE", "56"),
        "x-signature": os.getenv("DONGHUB_X_SIGNATURE"),
        "x-premium-license": os.getenv("DONGHUB_X_PREMIUM_LICENSE", ""),
        "x-ads-status": os.getenv("DONGHUB_X_ADS_STATUS", "true"),
        "x-dummy-status": os.getenv("DONGHUB_X_DUMMY_STATUS", "false"),
        "x-device-type": os.getenv("DONGHUB_X_DEVICE_TYPE", "Smartphone"),
    }
    for k, v in optional.items():
        vv = clean_text(v)
        if vv:
            headers[k] = vv
    return headers


def get_env_status() -> Tuple[bool, List[str]]:
    required = [
        "DONGHUB_SESSION_ID",
        "DONGHUB_AUTH_BEARER",
        "DONGHUB_X_USER",
        "DONGHUB_X_KEY",
        "DONGHUB_X_SIGNATURE",
    ]
    missing = [k for k in required if not clean_text(os.getenv(k))]
    return len(missing) == 0, missing


def base_url(path: str) -> str:
    root = clean_text(os.getenv("DONGHUB_BASE_URL", DEFAULT_BASE_URL)).rstrip("/")
    return f"{root}{path}"


def raw_http_request(method: str, url: str, headers: Dict[str, str], body_form: Optional[Dict[str, str]] = None) -> Tuple[int, str]:
    req_headers = dict(headers)
    data = None
    if body_form is not None:
        req_headers["Content-Type"] = "application/x-www-form-urlencoded"
        data = body_form
    resp = requests.request(method=method, url=url, headers=req_headers, data=data, timeout=30)
    return int(resp.status_code), resp.text


def refresh_token() -> str:
    session_id = clean_text(os.getenv("DONGHUB_SESSION_ID"))
    if not session_id:
        raise RuntimeError("Missing DONGHUB_SESSION_ID")

    status, text = raw_http_request(
        method="POST",
        url=base_url("/auth/refresh-token"),
        headers=make_base_headers(),
        body_form={"session": session_id},
    )
    if status >= 400:
        raise RuntimeError(f"Refresh token failed HTTP {status}. Body: {clean_text(text)[:240]}")
    payload = parse_encrypted_response(text)
    token = clean_text(((payload or {}).get("data") or {}).get("token"))
    if not token:
        raise RuntimeError("Failed to refresh donghub token")
    return token


def request_detail(path: str, token: str) -> dict:
    headers = make_base_headers()
    headers["authorization"] = f"Bearer {token}"
    status, text = raw_http_request(method="GET", url=base_url(path), headers=headers)
    if status >= 400:
        raise RuntimeError(f"Request {path} failed HTTP {status}. Body: {clean_text(text)[:240]}")
    return parse_encrypted_response(text)


def build_chapter_slug(source_series_id: int, source_episode_id: int) -> str:
    return f"dh-{source_series_id}-{source_episode_id}"


def to_size_kb(stream: dict) -> Optional[int]:
    try:
        n = float(stream.get("sizeValue"))
        if n >= 0:
            return int(round(n * 1024))
    except Exception:
        pass
    return None


def get_mysql_conn():
    return pymysql.connect(
        host=os.getenv("DB_HOST", "103.16.116.244"),
        port=int(os.getenv("DB_PORT", "3306")),
        user=os.getenv("DB_USER", "hxcuser_remote"),
        password=os.getenv("DB_PASS", "@Hudaxcode21"),
        database=os.getenv("DB_NAME", "anime"),
        charset="utf8mb4",
        autocommit=False,
        cursorclass=DictCursor,
    )


def get_ongoing_donghub_series(cur, limit: int, series_id: int, source_series_id: int, source_series_id_raw: str) -> List[dict]:
    where = ["LOWER(TRIM(source_platform)) = 'donghub'"]
    params: List[object] = []

    if series_id > 0:
        where.append("id = %s")
        params.append(series_id)

    raw = clean_text(source_series_id_raw)
    has_raw = bool(raw)
    if has_raw:
        if source_series_id > 0:
            where.append("(source_series_id = %s OR CAST(source_series_id AS CHAR) = %s)")
            params.extend([source_series_id, raw])
        else:
            where.append("CAST(source_series_id AS CHAR) = %s")
            params.append(raw)

    if series_id <= 0 and not has_raw:
        where.append("status IN ('Ongoing', 'Currently Airing')")

    sql = f"""
      SELECT id, title, source_series_id, source_platform, status
      FROM series
      WHERE {' AND '.join(where)}
      ORDER BY id ASC
      LIMIT %s
    """
    cur.execute(sql, [*params, limit])
    return cur.fetchall() or []


def get_existing_episodes_map(cur, series_id: int) -> Tuple[Dict[int, int], Dict[str, int]]:
    cur.execute(
        """
        SELECT id, source_episode_id, chapter_slug
        FROM episodes
        WHERE series_id = %s
        """,
        (series_id,),
    )
    rows = cur.fetchall() or []
    by_source: Dict[int, int] = {}
    by_slug: Dict[str, int] = {}
    for row in rows:
        episode_id = int(row["id"])
        sid = row.get("source_episode_id")
        if sid is not None:
            try:
                by_source[int(sid)] = episode_id
            except Exception:
                pass
        slug = clean_slug(row.get("chapter_slug"))
        if slug:
            by_slug[slug] = episode_id
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
    out: Set[int] = set()
    for row in rows:
        try:
            out.add(int(row["id"]))
        except Exception:
            pass
    return out


def map_donghub_streams(episode_id: int, streams: List[dict]) -> List[Tuple[int, str, Optional[int], str, Optional[int]]]:
    out: List[Tuple[int, str, Optional[int], str, Optional[int]]] = []
    for stream in streams:
        stream_url = clean_text((stream or {}).get("media"))
        if not stream_url:
            continue
        resolution = clean_text((stream or {}).get("name") or (stream or {}).get("parseName")) or "unknown"
        out.append((episode_id, resolution, None, stream_url, to_size_kb(stream or {})))
    return out


def insert_episode_streams_bulk(cur, rows: List[Tuple[int, str, Optional[int], str, Optional[int]]]) -> int:
    if not rows:
        return 0
    cur.executemany(
        """
        INSERT INTO episode_streams (episode_id, resolution, stream_source_id, stream_url, size_kb)
        VALUES (%s, %s, %s, %s, %s)
        """,
        rows,
    )
    return len(rows)


def run_donghub_ongoing_update(
    conn,
    dry_run: bool,
    identity_mode: str,
    sync_mode: str,
    limit: int,
    series_id: int,
    source_series_id: int,
    source_series_id_raw: str,
) -> dict:
    only_missing_streams = sync_mode == "only_missing_streams"

    with conn.cursor() as cur:
        series_rows = get_ongoing_donghub_series(cur, limit, series_id, source_series_id, source_series_id_raw)

    report = {
        "started_at": now_iso(),
        "dry_run": dry_run,
        "identity_mode": identity_mode,
        "sync_mode": sync_mode,
        "limit": limit,
        "platform": "donghub",
        "total_series_target": len(series_rows),
        "total_series_processed": 0,
        "total_episode_candidates": 0,
        "total_episode_new": 0,
        "total_episode_skipped_by_mode": 0,
        "total_episode_inserted": 0,
        "total_stream_inserted": 0,
        "total_errors": 0,
        "series": [],
    }

    for series in series_rows:
        sid = int(series["id"])
        source_sid = int(series["source_series_id"])
        series_report = {
            "series_id": sid,
            "source_series_id": source_sid,
            "title": series.get("title"),
            "candidates": 0,
            "new_episodes": 0,
            "skipped_new_episodes": 0,
            "inserted_episodes": 0,
            "inserted_streams": 0,
            "errors": [],
        }

        try:
            token = refresh_token()
            detail = request_detail(f"/detail/series-single/{source_sid}", token)
            episodes = ((detail or {}).get("data") or {}).get("episodes") or []
            if not isinstance(episodes, list):
                episodes = []

            with conn.cursor() as cur:
                by_source, by_slug = get_existing_episodes_map(cur, sid)
                episode_ids_with_streams = set() if dry_run else get_episode_ids_with_streams(cur, sid)

                series_report["candidates"] = len(episodes)
                report["total_episode_candidates"] += len(episodes)

                for ep in episodes:
                    try:
                        source_episode_id = int(ep.get("id"))
                    except Exception:
                        continue
                    if source_episode_id <= 0:
                        continue

                    chapter_slug = build_chapter_slug(source_sid, source_episode_id)
                    existing_episode_id = by_slug.get(chapter_slug) if identity_mode == "chapter_slug" else by_source.get(source_episode_id)
                    already_exists = bool(existing_episode_id and int(existing_episode_id) > 0)

                    if already_exists:
                        if dry_run:
                            continue
                        episode_id = int(existing_episode_id)
                        if episode_id in episode_ids_with_streams:
                            continue
                        try:
                            ep_token = refresh_token()
                            ep_detail = request_detail(f"/detail/episode-single/{source_episode_id}", ep_token)
                            streams = ((ep_detail or {}).get("data") or {}).get("stream") or []
                            if not isinstance(streams, list):
                                streams = []
                            rows = map_donghub_streams(episode_id, streams)
                            inserted = insert_episode_streams_bulk(cur, rows)
                            if inserted > 0:
                                episode_ids_with_streams.add(episode_id)
                                series_report["inserted_streams"] += inserted
                                report["total_stream_inserted"] += inserted
                        except Exception as exc:
                            series_report["errors"].append(f"episode {source_episode_id} (backfill): {exc}")
                            report["total_errors"] += 1
                        continue

                    series_report["new_episodes"] += 1
                    report["total_episode_new"] += 1

                    if only_missing_streams:
                        series_report["skipped_new_episodes"] += 1
                        report["total_episode_skipped_by_mode"] += 1
                        continue

                    if dry_run:
                        continue

                    episode_id: Optional[int] = None
                    try:
                        cur.execute(
                            """
                            INSERT INTO episodes (
                              source_platform, source_episode_id, series_id, chapter_label, chapter_slug, release_date_text
                            ) VALUES (%s, %s, %s, %s, %s, %s)
                            """,
                            (
                                "donghub",
                                source_episode_id,
                                sid,
                                clean_text(ep.get("number")),
                                chapter_slug,
                                clean_text(ep.get("created_at")),
                            ),
                        )
                        episode_id = int(cur.lastrowid)
                    except pymysql.err.IntegrityError:
                        cur.execute(
                            """
                            SELECT id
                            FROM episodes
                            WHERE source_platform = 'donghub' AND source_episode_id = %s
                            LIMIT 1
                            """,
                            (source_episode_id,),
                        )
                        row = cur.fetchone()
                        episode_id = int(row["id"]) if row and row.get("id") else None

                    if not episode_id:
                        continue

                    series_report["inserted_episodes"] += 1
                    report["total_episode_inserted"] += 1
                    by_source[source_episode_id] = episode_id
                    by_slug[chapter_slug] = episode_id

                    try:
                        ep_token = refresh_token()
                        ep_detail = request_detail(f"/detail/episode-single/{source_episode_id}", ep_token)
                        streams = ((ep_detail or {}).get("data") or {}).get("stream") or []
                        if not isinstance(streams, list):
                            streams = []
                        rows = map_donghub_streams(episode_id, streams)
                        inserted = insert_episode_streams_bulk(cur, rows)
                        if inserted > 0:
                            episode_ids_with_streams.add(episode_id)
                            series_report["inserted_streams"] += inserted
                            report["total_stream_inserted"] += inserted
                    except Exception as exc:
                        series_report["errors"].append(f"episode {source_episode_id}: {exc}")
                        report["total_errors"] += 1

                if not dry_run and series_report["inserted_episodes"] > 0:
                    cur.execute("UPDATE series SET updated_at = CURRENT_TIMESTAMP WHERE id = %s LIMIT 1", (sid,))

            if dry_run:
                conn.rollback()
            else:
                conn.commit()
        except Exception as exc:
            conn.rollback()
            series_report["errors"].append(str(exc))
            report["total_errors"] += 1

        report["total_series_processed"] += 1
        report["series"].append(series_report)

    report["finished_at"] = now_iso()
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description="Donghub updater (Python)")
    parser.add_argument(
        "--env-file",
        default=default_script_env_path(),
        help="Path env khusus script (default: scripts/.env)",
    )
    parser.add_argument("--dry-run", action="store_true", help="No DB write")
    parser.add_argument("--limit", type=int, default=20, help="Series limit (1-300)")
    parser.add_argument("--identity-mode", choices=["source_episode_id", "chapter_slug"], default="source_episode_id")
    parser.add_argument("--sync-mode", choices=["full_sync", "only_missing_streams"], default="full_sync")
    parser.add_argument("--series-id", type=int, default=0, help="Filter by local series.id")
    parser.add_argument("--source-series-id", type=int, default=0, help="Filter by source_series_id numeric")
    parser.add_argument("--source-series-id-raw", default="", help="Filter by source_series_id raw string")
    args = parser.parse_args()

    env_loaded = load_dotenv(args.env_file)
    if not env_loaded:
        print(f"ERROR: env file tidak ditemukan: {os.path.abspath(args.env_file)}", file=sys.stderr)
        print("Buat dari template: cp scripts/.env.example scripts/.env", file=sys.stderr)
        return 2
    ok, missing = get_env_status()
    if not ok:
        print(f"ERROR: Donghub env belum lengkap: {', '.join(missing)}", file=sys.stderr)
        return 2

    limit = max(1, min(int(args.limit or 20), 300))
    conn = get_mysql_conn()
    try:
        report = run_donghub_ongoing_update(
            conn=conn,
            dry_run=bool(args.dry_run),
            identity_mode=args.identity_mode,
            sync_mode=args.sync_mode,
            limit=limit,
            series_id=max(0, int(args.series_id or 0)),
            source_series_id=max(0, int(args.source_series_id or 0)),
            source_series_id_raw=clean_text(args.source_series_id_raw),
        )
    finally:
        conn.close()

    print(json.dumps(report, indent=2, ensure_ascii=False))
    return 0 if int(report.get("total_errors", 0)) == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
