#!/usr/bin/env python3
"""
Bot Anibi Donghua (simple):
- Ambil series ongoing platform donghub dari DB
- Cek episode baru dari API donghub
- Insert episode + stream (tanpa sentuh views)
- Log per series: SKIP / INSERT / ERR
- Opsional push notif saat ada episode donghua baru

Usage:
  python3 scripts/bot-anibi-donghua.py --dry-run
  python3 scripts/bot-anibi-donghua.py --limit 20
  python3 scripts/bot-anibi-donghua.py --source-series-id 3135 --notify
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import time
import uuid
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

# Hardcoded local config (no .env needed)
HARD_CONFIG = {
    # DB
    "DB_HOST": "103.16.116.244",
    "DB_PORT": "3306",
    "DB_USER": "hxcuser_remote",
    "DB_PASS": "@Hudaxcode21",
    "DB_NAME": "anime",
    # Donghub required
    "DONGHUB_BASE_URL": "https://restapi-micro.sutejo.com/api/v1",
    "DONGHUB_AES_KEY_HEX": DEFAULT_AES_KEY_HEX,
    "DONGHUB_SESSION_ID": "93c1a39a-2307-4d1f-863e-07a7cd4a7429",
    "DONGHUB_AUTH_BEARER": "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJhdWQiOiJyZXN0X2FwaV9taWNyb3NlcnZpY2VfdjIiLCJjcmVhdGVkQXQiOjE3NzIyNTAxMTYsImRldmljZXNJZCI6ImU2YjBkY2E4MDAyZTkwNzIiLCJleHAiOjE3NzIyNTEwMTYsImlhdCI6MTc3MjI1MDExNiwiaXNBZG1pbiI6ZmFsc2UsImlzQmFubmVkIjpmYWxzZSwiaXNQcmVtaXVtIjpmYWxzZSwiaXNzIjoicmVzdF9hcGlfbWljcm9zZXJ2aWNlX3YyIiwianRpIjoiZmI5MjVkNzI5NDAzMzU1ZTg1Zjk3MGFkMzI1NWQ5NWEiLCJzZXNzaW9uS2V5IjoiMzhjMTZhNGQtMzNlMS00MTgwLThhZDYtODMzMTdlNDkyZTI2IiwidXNlckFnZW50IjoiRGV2aWNlLzE0IChYaWFvbWkpIE0yMTAxSzdCTlkvVVAxQS4yMzA5MDUuMDExIiwidXNlckVtYWlsIjoidnBubWVydXNzaWFuQGdtYWlsLmNvbSIsInVzZXJJZCI6Mjc3MDkwMiwidXNlcklwIjoiMTgyLjguMTYyLjE3MSIsInVzZXJOYW1lIjoiTXVoYW1hZCBIdWRhIn0.gW7A4gmNx6PgrqE2diTmjvmyyIpqUaX5fXpdimh50BE",
    "DONGHUB_X_USER": "2eea64f9-2796-4146-a897-f4044ac3b204",
    "DONGHUB_X_KEY": "UwelFpAIGKorbLdVd9pWXoTgFrsDFthi6XFt",
    "DONGHUB_X_SIGNATURE": "85614f123de5d1d631fa6e215fb6098818fd4982bd261e899e42b015cc47e6739827a7ca1929232aac478b33eb2ef6e52210b1f6eb1dbd7ce4e40e363980c388f40d08586554e944ff3a09c96b8006333079fdf77ca62c8c2d42cf7c8ad6c4538d92eef68b4f807e92f8331393f2bbf898bcad2ea727a066e9b42da56b2c2768f9f6e3adf4dfa3faa5ba2f820b1908230052f27faacddea9e482bd93abb0d6a76c484eb84673a9341b807c1a13764d9b6b3df072a4af10a04299b24f175753208647268cfb43265a5137ea43ac1305daa310fcc26a422d49dc949d8637880b0cc913c2f8f9b78fd0fee8ff6158edd375cb614ecfe7118adfbc68e220c3c306455e682376006af53c56c00b3af4b46c894e4fa007f2692e542c511fa4c1c0228bb13aa3eb3f6c1463aa41c425723b9043e505c857baa1c1640f4439c80508c12155aa1a0531c179d94959550edc6e46534bc2a404d3b569dbb49a1835888c650eb9405fcc14922429a270c13a1f0f12a9e26c4887c4b2e46416b5ef62d645cf221f64796ac2f56b16e672e9adfcbb28fd23902dd046c62532295720e68649f9398b7a45837ef55996c4ce08f0d51222c040e0451f9a2ba2306d160f274e0505543a364736a118829b52ba71942592887db7632d1482c79c53cb64b35630906017e4d88419e0561714b383e3c90cfee6834743dbb097d65b3d233ed5552640183c2c996ab4dabeb11d30945572e0ab9796f710bd6ccb6f4dd7f38cd4d04d69517661eec89f3e87f7f2d69df485ba5de7fd7c05c307e141cdbafb359e0363e3a4744d027c8a477abb8dd4c2e1129ac557d7ed501a1c18c0de86c73c14e5bf87b081d2c862c6df014f175176c5923d12992bed73c3d772200af44f1be397eb25c6e6c671cbe5fc6d90518bb3d4ee2fc81b37e2a79e2babad4c352b4cfd704b96c9e500f8a85b26fd7198449fea649299f82c362d5f5213f8ab7469bb6494e921ecde8d51de33b2e3da2d6ef4cb9e7424a5e10c69ebafa8d91045ec238298ccd231d03dab4a012bfa013ec973d4b59b70947c16c8ee8233d76083d621858c7c971dee745316539d981f272e9a60e7fbef7e3a695723d5801aa6abeee0879811b898ec385274af6f662a8c190618e429da1c11a547d178c55f2e36c9c8d50c139835de1b26969379e212dad1cd392665d234ccdb760e0117a62933911dea970db803836e4f4c601409ac4782b8d58e637c49daf4804f7b84d71ef73647ee9b8008365f2fb89e248cf7fd586d8f51b95d3370616f3a6a5a39ecda14f7fe4badc63605320192a61d63a6d7829d2d72a3eea58b866c6b88694352685ddfba3c1bd22ac9339ea0a071ccfb055072b996d0b65416d996593c5880fae386e9b2f39f38737352a34634ef7e2bba3875803f0f4d851caab3078a493989a1c9966edd9727b02681070d56231fe3ae0b7888755da904504677fbca780c250be5df3f6fb056bd70b7ac251a9cb2d895bf5780aa3fd5ee57b0cf184e997dfc87a3f9d235e00d6aa7874d4b5039c759b0df803314da9ab6d1ec08be5278d87277da77e1ae2e7661708e4082912db80f1e2f1ed77913f98b34e28e539bd806203790aa75b5efe802d5de975649f18f19cea9a461c7eea4389c4f4477ffab84e5ab3cfba2086904e479d30c3ccd04b0e0fa4230d909058ff2988e80d91db7016ac97af2a8b22a53c5f3d6e222c8d59fdbe8dda7def1f4d31b1e491285bd2539c748666ec74e5f828c1ad9130728e3b059826c9f7a1f76cbea7af1",
    # Donghub optional headers
    "DONGHUB_USER_AGENT": "Device/14 (Xiaomi) M2101K7BNY/UP1A.230905.011",
    "DONGHUB_X_VERSION": "5.6",
    "DONGHUB_DEVICE_VERSION": "14",
    "DONGHUB_DEVICE_MODEL": "M2101K7BNY",
    "DONGHUB_INSTALLED_FROM_PLAYSTORE": "false",
    "DONGHUB_PACKAGE_NAME": "com.anichin.donghub",
    "DONGHUB_ANDROID_ID": "52c736f1ab01e8aa",
    "DONGHUB_INSTALLED_PACKAGE_NAME": "com.google.android.packageinstaller",
    "DONGHUB_VERSION_CODE": "56",
    "DONGHUB_X_PREMIUM_LICENSE": "",
    "DONGHUB_X_ADS_STATUS": "true",
    "DONGHUB_X_DUMMY_STATUS": "false",
    "DONGHUB_X_DEVICE_TYPE": "Smartphone",
    # Notification defaults
    "NOTIFY_API_URL": "https://panel.hudaxcode.cloud/api/notifications/send",
    "NOTIFICATIONS_ADMIN_SECRET": "hxc21",
    "NOTIFY_TOPIC": "anime-update",
}


def now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def cfg(key: str, default: str = "") -> str:
    return clean_text(HARD_CONFIG.get(key, default))


def clean_text(v: object) -> str:
    return str(v or "").strip()


def clean_slug(v: object) -> str:
    return clean_text(v).strip("/")


def get_env_status() -> Tuple[bool, List[str]]:
    required = [
        "DONGHUB_SESSION_ID",
        "DONGHUB_AUTH_BEARER",
        "DONGHUB_X_USER",
        "DONGHUB_X_KEY",
        "DONGHUB_X_SIGNATURE",
    ]
    missing = [k for k in required if not cfg(k)]
    return len(missing) == 0, missing


def get_mysql_conn():
    return pymysql.connect(
        host=cfg("DB_HOST", "127.0.0.1"),
        port=int(cfg("DB_PORT", "3306")),
        user=cfg("DB_USER", "root"),
        password=cfg("DB_PASS", ""),
        database=cfg("DB_NAME", "anime"),
        charset="utf8mb4",
        autocommit=False,
        cursorclass=DictCursor,
    )


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
        raise RuntimeError(f"Response is not encrypted hex payload. Preview: {unquoted[:240]}")

    key_hex = cfg("DONGHUB_AES_KEY_HEX", DEFAULT_AES_KEY_HEX)
    key = bytes.fromhex(key_hex)
    payload = bytes.fromhex(unquoted)
    if len(payload) < 17:
        raise RuntimeError("Encrypted payload invalid length")
    iv = payload[:16]
    ciphertext = payload[16:]
    cipher = AES.new(key, AES.MODE_CBC, iv=iv)
    decrypted = pad_unpad_pkcs7(cipher.decrypt(ciphertext))
    return json.loads(decrypted.decode("utf-8"))


def make_base_headers() -> Dict[str, str]:
    headers = {
        "User-Agent": cfg("DONGHUB_USER_AGENT", "Device/14 (Xiaomi) M2101K7BNY/UP1A.230905.011"),
        "Accept-Encoding": "gzip",
    }
    optional = {
        "authorization": cfg("DONGHUB_AUTH_BEARER"),
        "x-user": cfg("DONGHUB_X_USER"),
        "x-key": cfg("DONGHUB_X_KEY"),
        "x-version": cfg("DONGHUB_X_VERSION", "5.6"),
        "device-version": cfg("DONGHUB_DEVICE_VERSION", "14"),
        "device-model": cfg("DONGHUB_DEVICE_MODEL", "M2101K7BNY"),
        "installed-from-playstore": cfg("DONGHUB_INSTALLED_FROM_PLAYSTORE", "false"),
        "package-name": cfg("DONGHUB_PACKAGE_NAME", "com.anichin.donghub"),
        "android-id": cfg("DONGHUB_ANDROID_ID"),
        "installed-package-name": cfg("DONGHUB_INSTALLED_PACKAGE_NAME", "com.google.android.packageinstaller"),
        "version-code": cfg("DONGHUB_VERSION_CODE", "56"),
        "x-signature": cfg("DONGHUB_X_SIGNATURE"),
        "x-premium-license": cfg("DONGHUB_X_PREMIUM_LICENSE", ""),
        "x-ads-status": cfg("DONGHUB_X_ADS_STATUS", "true"),
        "x-dummy-status": cfg("DONGHUB_X_DUMMY_STATUS", "false"),
        "x-device-type": cfg("DONGHUB_X_DEVICE_TYPE", "Smartphone"),
    }
    for k, v in optional.items():
        vv = clean_text(v)
        if vv:
            headers[k] = vv
    return headers


def base_url(path: str) -> str:
    root = cfg("DONGHUB_BASE_URL", DEFAULT_BASE_URL).rstrip("/")
    return f"{root}{path}"


def raw_http_request(method: str, url: str, headers: Dict[str, str], form_data: Optional[Dict[str, str]] = None) -> Tuple[int, str]:
    req_headers = dict(headers)
    if form_data is not None:
        req_headers["Content-Type"] = "application/x-www-form-urlencoded"
    resp = requests.request(method=method, url=url, headers=req_headers, data=form_data, timeout=30)
    return int(resp.status_code), resp.text


def refresh_token() -> str:
    session_id = cfg("DONGHUB_SESSION_ID")
    if not session_id:
        raise RuntimeError("Missing DONGHUB_SESSION_ID")
    status, text = raw_http_request(
        method="POST",
        url=base_url("/auth/refresh-token"),
        headers=make_base_headers(),
        form_data={"session": session_id},
    )
    if status >= 400:
        raise RuntimeError(f"Refresh token failed HTTP {status}. Body: {clean_text(text)[:240]}")
    parsed = parse_encrypted_response(text)
    token = clean_text(((parsed or {}).get("data") or {}).get("token"))
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


def get_target_series(cur, limit: int, series_id: int, source_series_id: int, source_series_id_raw: str) -> List[dict]:
    where = ["LOWER(TRIM(source_platform)) = 'donghub'"]
    params: List[object] = []
    if series_id > 0:
        where.append("id = %s")
        params.append(series_id)

    raw = clean_text(source_series_id_raw)
    if raw:
        if source_series_id > 0:
            where.append("(source_series_id = %s OR CAST(source_series_id AS CHAR) = %s)")
            params.extend([source_series_id, raw])
        else:
            where.append("CAST(source_series_id AS CHAR) = %s")
            params.append(raw)
    elif source_series_id > 0:
        where.append("source_series_id = %s")
        params.append(source_series_id)

    if series_id <= 0 and source_series_id <= 0 and not raw:
        where.append("status IN ('Ongoing', 'Currently Airing')")

    cur.execute(
        f"""
        SELECT id, source_series_id, title, series_slug, cover_url
        FROM series
        WHERE {' AND '.join(where)}
        ORDER BY id ASC
        LIMIT %s
        """,
        [*params, limit],
    )
    return cur.fetchall() or []


def get_existing_episodes(cur, series_id: int) -> Tuple[Dict[int, int], Dict[str, int]]:
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
    for r in rows:
        eid = int(r["id"])
        sid = r.get("source_episode_id")
        if sid is not None:
            try:
                by_source[int(sid)] = eid
            except Exception:
                pass
        slug = clean_slug(r.get("chapter_slug"))
        if slug:
            by_slug[slug] = eid
    return by_source, by_slug


def build_chapter_slug(source_series_id: int, source_episode_id: int) -> str:
    return f"dh-{source_series_id}-{source_episode_id}"


def to_size_kb(stream: dict) -> Optional[int]:
    try:
        n = float((stream or {}).get("sizeValue"))
        if n >= 0:
            return int(round(n * 1024))
    except Exception:
        pass
    return None


def map_stream_rows(episode_id: int, streams: List[dict]) -> List[Tuple[int, str, Optional[int], str, Optional[int]]]:
    out: List[Tuple[int, str, Optional[int], str, Optional[int]]] = []
    for stream in streams or []:
        if not isinstance(stream, dict):
            continue
        stream_url = clean_text(stream.get("media"))
        if not stream_url:
            continue
        resolution = clean_text(stream.get("name") or stream.get("parseName")) or "unknown"
        out.append((episode_id, resolution, None, stream_url, to_size_kb(stream)))
    return out


def insert_streams(cur, rows: List[Tuple[int, str, Optional[int], str, Optional[int]]]) -> int:
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


def normalize_episode_label(value: object) -> str:
    raw = clean_text(value)
    if not raw:
        return ""
    if re.match(r"(?i)^ep(?:isode)?\b", raw):
        return raw
    return f"Episode {raw}"


def build_idempotency_key(prefix: str, slug: str, suffix: str = "") -> str:
    ts = int(time.time() * 1000)
    rid = uuid.uuid4().hex[:10]
    slug_part = clean_slug(slug) or "unknown"
    suffix_part = clean_slug(suffix)
    if suffix_part:
        return f"{prefix}-{slug_part}-{suffix_part}-{ts}-{rid}"
    return f"{prefix}-{slug_part}-{ts}-{rid}"


def post_notification(api_url: str, admin_secret: str, payload: Dict[str, object], idempotency_key: str) -> None:
    resp = requests.post(
        api_url,
        timeout=25,
        headers={
            "x-admin-secret": admin_secret,
            "Idempotency-Key": idempotency_key,
            "Content-Type": "application/json",
        },
        json=payload,
    )
    if resp.status_code >= 400:
        raise RuntimeError(f"notif http {resp.status_code}: {clean_text(resp.text)[:300]}")


def sync_single_series(conn, row: dict, dry_run: bool) -> dict:
    series_id = int(row["id"])
    source_series_id = int(row["source_series_id"])
    report = {
        "series_id": series_id,
        "source_series_id": source_series_id,
        "series_slug": clean_text(row.get("series_slug")),
        "series_title": clean_text(row.get("title")),
        "series_cover": clean_text(row.get("cover_url")),
        "episodes_candidate": 0,
        "episodes_new": 0,
        "episodes_inserted": 0,
        "streams_inserted": 0,
        "inserted_episodes": [],
        "errors": [],
    }

    token = refresh_token()
    detail = request_detail(f"/detail/series-single/{source_series_id}", token)
    episodes = ((detail or {}).get("data") or {}).get("episodes") or []
    if not isinstance(episodes, list):
        episodes = []
    report["episodes_candidate"] = len(episodes)

    with conn.cursor() as cur:
        by_source, by_slug = get_existing_episodes(cur, series_id)

        for ep in episodes:
            try:
                source_episode_id = int(ep.get("id"))
            except Exception:
                continue
            if source_episode_id <= 0:
                continue
            chapter_slug = build_chapter_slug(source_series_id, source_episode_id)
            exists_id = by_source.get(source_episode_id) or by_slug.get(chapter_slug)
            if exists_id:
                continue

            report["episodes_new"] += 1
            if dry_run:
                continue

            episode_id = None
            try:
                cur.execute(
                    """
                    INSERT INTO episodes (
                      source_platform, source_episode_id, series_id,
                      chapter_label, chapter_slug, release_date_text
                    ) VALUES (%s, %s, %s, %s, %s, %s)
                    """,
                    (
                        "donghub",
                        source_episode_id,
                        series_id,
                        clean_text(ep.get("number")),
                        chapter_slug,
                        clean_text(ep.get("created_at")),
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
                        "chapter_label": clean_text(ep.get("number")),
                    }
                )
            except pymysql.err.IntegrityError:
                cur.execute(
                    """
                    SELECT id
                    FROM episodes
                    WHERE source_platform='donghub' AND source_episode_id=%s
                    LIMIT 1
                    """,
                    (source_episode_id,),
                )
                found = cur.fetchone()
                episode_id = int(found["id"]) if found and found.get("id") else None

            if not episode_id:
                continue

            try:
                ep_token = refresh_token()
                ep_detail = request_detail(f"/detail/episode-single/{source_episode_id}", ep_token)
                streams = ((ep_detail or {}).get("data") or {}).get("stream") or []
                if not isinstance(streams, list):
                    streams = []
                inserted = insert_streams(cur, map_stream_rows(episode_id, streams))
                report["streams_inserted"] += inserted
            except Exception as exc:
                report["errors"].append(f"episode {source_episode_id}: {exc}")

        if not dry_run and report["episodes_inserted"] > 0:
            cur.execute(
                """
                UPDATE series
                SET updated_at = CURRENT_TIMESTAMP
                WHERE id = %s
                LIMIT 1
                """,
                (series_id,),
            )

    return report


def main() -> int:
    parser = argparse.ArgumentParser(description="Bot Anibi Donghua (simple)")
    parser.add_argument("--dry-run", action="store_true", help="No write DB")
    parser.add_argument("--limit", type=int, default=20, help="Target series max (1-300)")
    parser.add_argument("--series-id", type=int, default=0, help="Filter local series.id")
    parser.add_argument("--source-series-id", type=int, default=0, help="Filter source_series_id numeric")
    parser.add_argument("--source-series-id-raw", default="", help="Filter source_series_id raw string")
    parser.add_argument("--notify", action="store_true", help="Send push notif for each new donghua episode")
    parser.add_argument("--notify-url", default="", help="Override notify endpoint URL")
    parser.add_argument("--notify-secret", default="", help="Override x-admin-secret")
    parser.add_argument("--notify-topic", default="anime-update", help="Notification topic")
    args = parser.parse_args()

    ok, missing = get_env_status()
    if not ok:
        print(f"ERROR: hardcoded config belum lengkap: {', '.join(missing)}", file=sys.stderr)
        print("Isi di blok HARD_CONFIG pada file bot-anibi-donghua.py", file=sys.stderr)
        return 2

    notify_url = clean_text(args.notify_url) or cfg("NOTIFY_API_URL")
    notify_secret = clean_text(args.notify_secret) or cfg("NOTIFICATIONS_ADMIN_SECRET")
    notify_topic = clean_text(args.notify_topic) or cfg("NOTIFY_TOPIC", "anime-update")
    notify_enabled = bool(args.notify and not args.dry_run and notify_url and notify_secret)
    if args.notify and not notify_enabled:
        print("[WARN] notify aktif tapi NOTIFY URL/SECRET kosong, notifikasi dilewati.")

    limit = max(1, min(int(args.limit or 20), 300))
    conn = get_mysql_conn()

    summary = {
        "started_at": now_iso(),
        "platform": "donghub",
        "dry_run": bool(args.dry_run),
        "limit": limit,
        "total": 0,
        "episodes_inserted": 0,
        "streams_inserted": 0,
        "notifications_sent": 0,
        "notification_errors": 0,
        "errors": 0,
        "items": [],
    }

    try:
        with conn.cursor() as cur:
            targets = get_target_series(
                cur=cur,
                limit=limit,
                series_id=max(0, int(args.series_id or 0)),
                source_series_id=max(0, int(args.source_series_id or 0)),
                source_series_id_raw=clean_text(args.source_series_id_raw),
            )
        summary["total"] = len(targets)
        print(f"Target series count: {len(targets)}")

        for row in targets:
            item = {
                "series_id": int(row["id"]),
                "source_series_id": int(row["source_series_id"]),
                "series_slug": clean_text(row.get("series_slug")),
                "series_title": clean_text(row.get("title")),
                "sync": None,
                "notifications": [],
                "notify_errors": [],
                "error": None,
            }
            try:
                sync_report = sync_single_series(conn=conn, row=row, dry_run=bool(args.dry_run))
                item["sync"] = sync_report
                summary["episodes_inserted"] += int(sync_report["episodes_inserted"])
                summary["streams_inserted"] += int(sync_report["streams_inserted"])
                summary["errors"] += len(sync_report["errors"])
                if args.dry_run:
                    conn.rollback()
                else:
                    conn.commit()

                if notify_enabled:
                    series_slug = clean_text(sync_report.get("series_slug")) or f"dh-{item['source_series_id']}"
                    series_title = clean_text(sync_report.get("series_title")) or series_slug.replace("-", " ").title()
                    series_cover = clean_text(sync_report.get("series_cover"))
                    for ep in (sync_report.get("inserted_episodes") or []):
                        chapter_slug = clean_slug(ep.get("chapter_slug"))
                        if not chapter_slug:
                            continue
                        episode_label = normalize_episode_label(ep.get("chapter_label")) or "Episode Baru"
                        payload = {
                            "type": "episode_update",
                            "title": "Episode Baru Tersedia",
                            "message": f"{series_title} {episode_label} sudah rilis",
                            "imageUrl": series_cover,
                            "actionType": "open_episode",
                            "actionValue": chapter_slug,
                            "actionLabel": "Tonton",
                            "dedupeKey": f"episode-update:donghub:{series_slug}:{chapter_slug}",
                            "topic": notify_topic or "anime-update",
                        }
                        idem = build_idempotency_key("donghua-episode", series_slug, chapter_slug)
                        try:
                            post_notification(notify_url, notify_secret, payload, idem)
                            item["notifications"].append({"chapter_slug": chapter_slug, "idempotency_key": idem})
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
                status = "INSERT" if int(sync.get("episodes_inserted", 0)) > 0 else "SKIP"
            sync = item.get("sync") or {}
            print(
                f"[{status}] series_id={item['series_id']} source_series_id={item['source_series_id']} "
                f"new={int(sync.get('episodes_new', 0))} inserted={int(sync.get('episodes_inserted', 0))} "
                f"streams={int(sync.get('streams_inserted', 0))} err={item['error'] or '-'}"
            )
    finally:
        conn.close()

    summary["finished_at"] = now_iso()
    print("\n=== SUMMARY ===")
#    print(json.dumps(summary, indent=2, ensure_ascii=False))
    return 0 if int(summary["errors"]) == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
