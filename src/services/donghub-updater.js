const crypto = require('crypto');
const { pool } = require('../db');
let axiosLib = null;
try {
  // Optional dependency. If not installed, updater falls back to fetch.
  // eslint-disable-next-line global-require
  axiosLib = require('axios');
} catch (_err) {
  axiosLib = null;
}

const DEFAULT_BASE_URL = 'https://restapi-micro.sutejo.com/api/v1';
const DEFAULT_AES_KEY_HEX = '1334dd8dd4f713fdc112b539ae77c30b15a5ecc82ec870d8f0b60bc4ef958cb3';
const DEFAULT_ANIMEKITA_BASE_URL = 'https://apps.animekita.org/api/v1.2.4';

function getEnvStatus() {
  const required = [
    'DONGHUB_SESSION_ID',
    'DONGHUB_AUTH_BEARER',
    'DONGHUB_X_USER',
    'DONGHUB_X_KEY',
    'DONGHUB_X_SIGNATURE'
  ];
  const missing = required.filter((k) => !String(process.env[k] || '').trim());
  return {
    ok: missing.length === 0,
    missing
  };
}

function parseEncryptedResponse(text) {
  const raw = String(text || '').trim();
  if (!raw) throw new Error('Empty response from donghub API');

  try {
    return JSON.parse(raw);
  } catch (_err) {
    // Some gateways wrap encrypted payload in quotes.
    const unquoted = raw.startsWith('"') && raw.endsWith('"')
      ? raw.slice(1, -1).replace(/\\"/g, '"')
      : raw;
    if (!/^[0-9a-fA-F]+$/.test(unquoted) || unquoted.length < 32) {
      const preview = unquoted.slice(0, 240);
      throw new Error(`Response is not encrypted hex payload. Preview: ${preview}`);
    }
    const keyHex = String(process.env.DONGHUB_AES_KEY_HEX || DEFAULT_AES_KEY_HEX).trim();
    const key = Buffer.from(keyHex, 'hex');
    const data = Buffer.from(unquoted, 'hex');
    if (data.length < 17) {
      throw new Error('Encrypted payload invalid length');
    }
    const iv = data.subarray(0, 16);
    const ciphertext = data.subarray(16);
    const decipher = crypto.createDecipheriv('aes-256-cbc', key, iv);
    const decrypted = Buffer.concat([decipher.update(ciphertext), decipher.final()]).toString('utf8');
    return JSON.parse(decrypted);
  }
}

function makeBaseHeaders() {
  const headers = {
    'User-Agent': String(process.env.DONGHUB_USER_AGENT || 'Device/14 (Xiaomi) M2101K7BNY/UP1A.230905.011'),
    'Accept-Encoding': 'gzip'
  };

  const optionalMap = {
    authorization: process.env.DONGHUB_AUTH_BEARER,
    'x-user': process.env.DONGHUB_X_USER,
    'x-key': process.env.DONGHUB_X_KEY,
    'x-version': process.env.DONGHUB_X_VERSION || '5.6',
    'device-version': process.env.DONGHUB_DEVICE_VERSION || '14',
    'device-model': process.env.DONGHUB_DEVICE_MODEL || 'M2101K7BNY',
    'installed-from-playstore': process.env.DONGHUB_INSTALLED_FROM_PLAYSTORE || 'false',
    'package-name': process.env.DONGHUB_PACKAGE_NAME || 'com.anichin.donghub',
    'android-id': process.env.DONGHUB_ANDROID_ID,
    'installed-package-name': process.env.DONGHUB_INSTALLED_PACKAGE_NAME || 'com.google.android.packageinstaller',
    'version-code': process.env.DONGHUB_VERSION_CODE || '56',
    'x-signature': process.env.DONGHUB_X_SIGNATURE,
    'x-premium-license': process.env.DONGHUB_X_PREMIUM_LICENSE || '',
    'x-ads-status': process.env.DONGHUB_X_ADS_STATUS || 'true',
    'x-dummy-status': process.env.DONGHUB_X_DUMMY_STATUS || 'false',
    'x-device-type': process.env.DONGHUB_X_DEVICE_TYPE || 'Smartphone'
  };

  for (const [key, value] of Object.entries(optionalMap)) {
    const v = String(value || '').trim();
    if (v) headers[key] = v;
  }

  return headers;
}

function getHttpClientMode() {
  const mode = String(process.env.DONGHUB_HTTP_CLIENT || '').trim().toLowerCase();
  if (mode === 'axios') return 'axios';
  if (mode === 'fetch') return 'fetch';
  return axiosLib ? 'axios' : 'fetch';
}

async function rawHttpRequest({ method, url, headers, bodyForm }) {
  const mode = getHttpClientMode();
  if (mode === 'axios' && axiosLib) {
    const data = bodyForm ? new URLSearchParams(bodyForm).toString() : undefined;
    const res = await axiosLib.request({
      method,
      url,
      headers: bodyForm
        ? { ...headers, 'Content-Type': 'application/x-www-form-urlencoded' }
        : headers,
      data,
      timeout: 30000,
      responseType: 'text',
      validateStatus: () => true
    });
    const text = typeof res.data === 'string' ? res.data : JSON.stringify(res.data || {});
    return { status: Number(res.status), text };
  }

  const response = await fetch(url, {
    method,
    headers: bodyForm
      ? { ...headers, 'Content-Type': 'application/x-www-form-urlencoded' }
      : headers,
    body: bodyForm ? new URLSearchParams(bodyForm).toString() : undefined
  });
  const text = await response.text();
  return { status: Number(response.status), text };
}

function baseUrl(path) {
  const root = String(process.env.DONGHUB_BASE_URL || DEFAULT_BASE_URL).replace(/\/+$/, '');
  return `${root}${path}`;
}

async function refreshToken() {
  const sessionId = String(process.env.DONGHUB_SESSION_ID || '').trim();
  if (!sessionId) {
    throw new Error('Missing DONGHUB_SESSION_ID');
  }

  const headers = makeBaseHeaders();
  const { status, text } = await rawHttpRequest({
    method: 'POST',
    url: baseUrl('/auth/refresh-token'),
    headers,
    bodyForm: { session: sessionId }
  });

  if (status >= 400) {
    throw new Error(`Refresh token failed HTTP ${status}. Body: ${text.slice(0, 240)}`);
  }
  const json = parseEncryptedResponse(text);
  const token = json && json.data && json.data.token ? String(json.data.token) : '';
  if (!token) {
    throw new Error('Failed to refresh donghub token');
  }
  return token;
}

async function requestDetail(path, token) {
  const headers = makeBaseHeaders();
  headers.authorization = `Bearer ${token}`;

  const { status, text } = await rawHttpRequest({
    method: 'GET',
    url: baseUrl(path),
    headers
  });
  if (status >= 400) {
    throw new Error(`Request ${path} failed HTTP ${status}. Body: ${text.slice(0, 240)}`);
  }
  return parseEncryptedResponse(text);
}

function toSizeKb(stream) {
  const sizeValue = Number(stream && stream.sizeValue);
  if (Number.isFinite(sizeValue) && sizeValue >= 0) {
    return Math.round(sizeValue * 1024);
  }
  return null;
}

function buildChapterSlug(sourceSeriesId, sourceEpisodeId) {
  return `dh-${sourceSeriesId}-${sourceEpisodeId}`;
}

function cleanSlug(input) {
  return String(input || '').trim().replace(/^\/+|\/+$/g, '');
}

function extractAnimekitaSlugFromFullUrl(input) {
  const raw = String(input || '').trim();
  if (!raw) return '';

  try {
    const url = new URL(raw);
    const slugFromQuery = cleanSlug(url.searchParams.get('url'));
    if (slugFromQuery) return slugFromQuery;
    const pathParts = url.pathname.split('/').map((x) => cleanSlug(x)).filter(Boolean);
    if (pathParts.length) return cleanSlug(pathParts[pathParts.length - 1]);
    return '';
  } catch (_err) {
    return cleanSlug(raw);
  }
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function fetchJson(url) {
  const maxAttempts = 4;
  let lastErr = null;

  for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
    try {
      const response = await fetch(url, {
        method: 'GET',
        headers: {
          'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
          Accept: 'application/json,text/plain,*/*'
        },
        signal: AbortSignal.timeout(25000)
      });

      if (!response.ok) {
        const text = await response.text();
        throw new Error(`HTTP ${response.status}: ${text.slice(0, 220)}`);
      }

      return await response.json();
    } catch (err) {
      lastErr = err;
      if (attempt < maxAttempts) {
        await sleep(350 * attempt);
        continue;
      }
    }
  }

  const msg = lastErr && lastErr.message ? lastErr.message : String(lastErr || 'unknown fetch error');
  throw new Error(`fetch failed (${url}): ${msg}`);
}

async function fetchAnimekitaSeriesWithFallback(base, rawSlug) {
  const slug = cleanSlug(rawSlug);
  if (!slug) throw new Error('series_slug kosong');

  const primaryUrl = `${base}/series.php?url=${encodeURIComponent(slug)}`;
  try {
    return await fetchJson(primaryUrl);
  } catch (firstErr) {
    const fallbackSlug = `${slug}/`;
    const fallbackUrl = `${base}/series.php?url=${encodeURIComponent(fallbackSlug)}`;
    try {
      return await fetchJson(fallbackUrl);
    } catch (_secondErr) {
      throw firstErr;
    }
  }
}

async function fetchAnimekitaSeriesByFullUrl(fullUrl) {
  const base = String(process.env.ANIMEKITA_BASE_URL || DEFAULT_ANIMEKITA_BASE_URL).replace(/\/+$/, '');
  const seriesSlug = extractAnimekitaSlugFromFullUrl(fullUrl);
  if (!seriesSlug) {
    throw new Error('URL animekita tidak valid. Harus mengandung query ?url=<series_slug>');
  }

  const seriesJson = await fetchAnimekitaSeriesWithFallback(base, seriesSlug);
  const seriesData = Array.isArray(seriesJson && seriesJson.data) && seriesJson.data[0]
    ? seriesJson.data[0]
    : null;
  if (!seriesData) {
    throw new Error('Series animekita tidak ditemukan dari URL tersebut');
  }

  return {
    seriesSlug,
    raw: seriesJson,
    data: seriesData
  };
}

async function fetchDonghubSeriesBySourceId(sourceSeriesId) {
  const idNum = Number(sourceSeriesId);
  if (!Number.isFinite(idNum) || idNum <= 0) {
    throw new Error('source_series_id donghub harus angka > 0');
  }

  const token = await refreshToken();
  const detail = await requestDetail(`/detail/series-single/${idNum}`, token);
  const data = detail && detail.data ? detail.data : null;
  if (!data || typeof data !== 'object') {
    throw new Error('Series donghub tidak ditemukan');
  }

  return {
    sourceSeriesId: idNum,
    raw: detail,
    data
  };
}

async function getOngoingDonghubSeries(limit, filters = {}) {
  const where = [
    `LOWER(TRIM(source_platform)) = 'donghub'`
  ];
  const params = [];

  const onlySeriesId = Number(filters.seriesId);
  const hasSeriesIdFilter = Number.isFinite(onlySeriesId) && onlySeriesId > 0;
  if (Number.isFinite(onlySeriesId) && onlySeriesId > 0) {
    where.push('id = ?');
    params.push(onlySeriesId);
  }

  const sourceSeriesIdRaw = String(
    filters.sourceSeriesIdRaw !== undefined ? filters.sourceSeriesIdRaw : filters.sourceSeriesId
  ).trim();
  const onlySourceSeriesId = Number(sourceSeriesIdRaw);
  const hasSourceSeriesIdFilter = Boolean(sourceSeriesIdRaw);
  if (hasSourceSeriesIdFilter) {
    if (Number.isFinite(onlySourceSeriesId) && onlySourceSeriesId > 0) {
      where.push('(source_series_id = ? OR CAST(source_series_id AS CHAR) = ?)');
      params.push(onlySourceSeriesId, sourceSeriesIdRaw);
    } else {
      where.push('CAST(source_series_id AS CHAR) = ?');
      params.push(sourceSeriesIdRaw);
    }
  }
  if (!hasSeriesIdFilter && !hasSourceSeriesIdFilter) {
    where.push(`status IN ('Ongoing', 'Currently Airing')`);
  }

  const [rows] = await pool.query(
    `SELECT id, title, source_series_id, source_platform, status
     FROM series
     WHERE ${where.join(' AND ')}
     ORDER BY id ASC
     LIMIT ?`,
    [...params, limit]
  );
  return rows;
}

async function getOngoingAnimekitaSeries(limit, filters = {}) {
  const where = [
    `LOWER(TRIM(source_platform)) = 'animekita'`
  ];
  const params = [];

  const onlySeriesId = Number(filters.seriesId);
  const hasSeriesIdFilter = Number.isFinite(onlySeriesId) && onlySeriesId > 0;
  if (Number.isFinite(onlySeriesId) && onlySeriesId > 0) {
    where.push('id = ?');
    params.push(onlySeriesId);
  }

  const sourceSeriesIdRaw = String(
    filters.sourceSeriesIdRaw !== undefined ? filters.sourceSeriesIdRaw : filters.sourceSeriesId
  ).trim();
  const onlySourceSeriesId = Number(sourceSeriesIdRaw);
  const hasSourceSeriesIdFilter = Boolean(sourceSeriesIdRaw);
  if (hasSourceSeriesIdFilter) {
    if (Number.isFinite(onlySourceSeriesId) && onlySourceSeriesId > 0) {
      where.push('(source_series_id = ? OR CAST(source_series_id AS CHAR) = ?)');
      params.push(onlySourceSeriesId, sourceSeriesIdRaw);
    } else {
      where.push('CAST(source_series_id AS CHAR) = ?');
      params.push(sourceSeriesIdRaw);
    }
  }
  if (!hasSeriesIdFilter && !hasSourceSeriesIdFilter) {
    where.push(`status IN ('Ongoing', 'Currently Airing')`);
  }

  const [rows] = await pool.query(
    `SELECT id, title, source_series_id, series_slug, source_platform, status
     FROM series
     WHERE ${where.join(' AND ')}
     ORDER BY id ASC
     LIMIT ?`,
    [...params, limit]
  );
  return rows;
}

async function getExistingEpisodesMap(seriesId) {
  const [rows] = await pool.query(
    `SELECT id, source_episode_id, chapter_slug
     FROM episodes
     WHERE series_id = ?`,
    [seriesId]
  );

  const bySourceEpisodeId = new Map();
  const bySlug = new Map();
  for (const row of rows) {
    if (row.source_episode_id !== null && row.source_episode_id !== undefined) {
      bySourceEpisodeId.set(Number(row.source_episode_id), Number(row.id));
    }
    if (row.chapter_slug) {
      bySlug.set(String(row.chapter_slug), Number(row.id));
    }
  }

  return { bySourceEpisodeId, bySlug };
}

async function getEpisodeIdsWithStreams(seriesId) {
  const [rows] = await pool.query(
    `SELECT DISTINCT e.id
     FROM episodes e
     JOIN episode_streams es ON es.episode_id = e.id
     WHERE e.series_id = ?`,
    [seriesId]
  );
  const set = new Set();
  for (const row of rows) {
    const id = Number(row && row.id);
    if (Number.isFinite(id) && id > 0) set.add(id);
  }
  return set;
}

function chunkArray(arr, size) {
  const out = [];
  for (let i = 0; i < arr.length; i += size) {
    out.push(arr.slice(i, i + size));
  }
  return out;
}

function toSafeConcurrency(rawValue, fallback = 8) {
  const n = Number(rawValue);
  if (!Number.isFinite(n)) return fallback;
  return Math.max(1, Math.min(Math.floor(n), 30));
}

async function mapWithConcurrency(items, concurrency, worker) {
  const out = new Array(items.length);
  let cursor = 0;

  async function runner() {
    while (true) {
      const i = cursor;
      cursor += 1;
      if (i >= items.length) break;
      out[i] = await worker(items[i], i);
    }
  }

  const workers = [];
  const totalWorkers = Math.max(1, Math.min(concurrency, items.length || 1));
  for (let i = 0; i < totalWorkers; i += 1) {
    workers.push(runner());
  }
  await Promise.all(workers);
  return out;
}

async function insertEpisodeStreamsBulk(streamRows) {
  if (!Array.isArray(streamRows) || streamRows.length === 0) return 0;
  let inserted = 0;
  const chunks = chunkArray(streamRows, 500);
  for (const chunk of chunks) {
    const placeholders = chunk.map(() => '(?, ?, ?, ?, ?)').join(', ');
    const values = [];
    for (const row of chunk) {
      values.push(row.episodeId, row.resolution, row.streamSourceId, row.streamUrl, row.sizeKb);
    }
    await pool.query(
      `INSERT INTO episode_streams (episode_id, resolution, stream_source_id, stream_url, size_kb)
       VALUES ${placeholders}`,
      values
    );
    inserted += chunk.length;
  }
  return inserted;
}

function mapDonghubStreams(episodeId, streams) {
  const out = [];
  for (const stream of streams) {
    const streamUrl = String(stream && stream.media || '').trim();
    if (!streamUrl) continue;
    const resolution = String(stream && stream.name || stream.parseName || '').trim() || 'unknown';
    out.push({
      episodeId,
      resolution,
      streamSourceId: null,
      streamUrl,
      sizeKb: toSizeKb(stream)
    });
  }
  return out;
}

function mapAnimekitaStreams(episodeId, streamsMap) {
  const out = [];
  for (const [resoKey, streamRows] of Object.entries(streamsMap || {})) {
    if (!Array.isArray(streamRows)) continue;
    for (const stream of streamRows) {
      const streamUrl = String(stream && stream.link || '').trim();
      if (!streamUrl) continue;
      const resolution = String(stream && stream.reso || resoKey || '').trim() || 'unknown';
      const sourceIdNum = Number(stream && stream.id);
      const streamSourceId = Number.isFinite(sourceIdNum) && sourceIdNum > 0 ? sourceIdNum : null;
      const sizeNum = Number(stream && stream.size_kb);
      const sizeKb = Number.isFinite(sizeNum) && sizeNum >= 0 ? Math.round(sizeNum) : null;
      out.push({
        episodeId,
        resolution,
        streamSourceId,
        streamUrl,
        sizeKb
      });
    }
  }
  return out;
}

async function runDonghubOngoingUpdate(options = {}) {
  const dryRun = Boolean(options.dryRun);
  const identityMode = options.identityMode === 'chapter_slug' ? 'chapter_slug' : 'source_episode_id';
  const syncMode = options.syncMode === 'only_missing_streams' ? 'only_missing_streams' : 'full_sync';
  const onlyMissingStreams = syncMode === 'only_missing_streams';
  const limit = Math.max(1, Math.min(Number(options.limit) || 20, 300));
  const onlySeriesId = Number(options.seriesId);
  const onlySourceSeriesId = Number(options.sourceSeriesId);
  const onlySourceSeriesIdRaw = String(options.sourceSeriesIdRaw !== undefined ? options.sourceSeriesIdRaw : '').trim();
  const envStatus = getEnvStatus();
  if (!envStatus.ok) {
    throw new Error(`Donghub env belum lengkap: ${envStatus.missing.join(', ')}`);
  }

  const startedAt = new Date().toISOString();
  const seriesRows = await getOngoingDonghubSeries(limit, {
    seriesId: onlySeriesId,
    sourceSeriesId: onlySourceSeriesId,
    sourceSeriesIdRaw: onlySourceSeriesIdRaw
  });

  const report = {
    started_at: startedAt,
    dry_run: dryRun,
    identity_mode: identityMode,
    sync_mode: syncMode,
    limit,
    total_series_target: seriesRows.length,
    total_series_processed: 0,
    total_episode_candidates: 0,
    total_episode_new: 0,
    total_episode_skipped_by_mode: 0,
    total_episode_inserted: 0,
    total_stream_inserted: 0,
    total_errors: 0,
    series: []
  };

  for (const series of seriesRows) {
    const seriesReport = {
      series_id: Number(series.id),
      source_series_id: Number(series.source_series_id),
      title: series.title,
      candidates: 0,
      new_episodes: 0,
      skipped_new_episodes: 0,
      inserted_episodes: 0,
      inserted_streams: 0,
      errors: []
    };

    try {
      const seriesToken = await refreshToken();
      const seriesDetail = await requestDetail(`/detail/series-single/${series.source_series_id}`, seriesToken);
      const episodes = Array.isArray(seriesDetail && seriesDetail.data && seriesDetail.data.episodes)
        ? seriesDetail.data.episodes
        : [];

      const existingMap = await getExistingEpisodesMap(series.id);
      const episodeIdsWithStreams = dryRun ? new Set() : await getEpisodeIdsWithStreams(series.id);
      seriesReport.candidates = episodes.length;
      report.total_episode_candidates += episodes.length;

      for (const sourceEpisode of episodes) {
        const sourceEpisodeId = Number(sourceEpisode && sourceEpisode.id);
        if (!Number.isFinite(sourceEpisodeId) || sourceEpisodeId <= 0) continue;

        const chapterSlug = buildChapterSlug(series.source_series_id, sourceEpisodeId);
        const existingEpisodeId = identityMode === 'chapter_slug'
          ? existingMap.bySlug.get(chapterSlug)
          : existingMap.bySourceEpisodeId.get(sourceEpisodeId);
        const alreadyExists = Number.isFinite(Number(existingEpisodeId)) && Number(existingEpisodeId) > 0;

        if (alreadyExists) {
          if (dryRun) continue;
          const needsStreamsBackfill = !episodeIdsWithStreams.has(Number(existingEpisodeId));
          if (!needsStreamsBackfill) continue;

          try {
            const episodeToken = await refreshToken();
            const episodeDetail = await requestDetail(`/detail/episode-single/${sourceEpisodeId}`, episodeToken);
            const streams = Array.isArray(episodeDetail && episodeDetail.data && episodeDetail.data.stream)
              ? episodeDetail.data.stream
              : [];
            const toInsert = mapDonghubStreams(Number(existingEpisodeId), streams);
            const inserted = await insertEpisodeStreamsBulk(toInsert);
            if (inserted > 0) {
              episodeIdsWithStreams.add(Number(existingEpisodeId));
              seriesReport.inserted_streams += inserted;
              report.total_stream_inserted += inserted;
            }
          } catch (err) {
            seriesReport.errors.push(`episode ${sourceEpisodeId} (backfill): ${err.message}`);
            report.total_errors += 1;
          }
          continue;
        }

        seriesReport.new_episodes += 1;
        report.total_episode_new += 1;

        if (onlyMissingStreams) {
          seriesReport.skipped_new_episodes += 1;
          report.total_episode_skipped_by_mode += 1;
          continue;
        }

        if (dryRun) continue;

        let episodeId = null;
        try {
          const [insertEpisode] = await pool.query(
            `INSERT INTO episodes (
               source_platform,
               source_episode_id,
               series_id,
               chapter_label,
               chapter_slug,
               release_date_text
             ) VALUES (?, ?, ?, ?, ?, ?)`,
            [
              'donghub',
              sourceEpisodeId,
              series.id,
              String(sourceEpisode.number || ''),
              chapterSlug,
              String(sourceEpisode.created_at || '')
            ]
          );
          episodeId = Number(insertEpisode.insertId);
        } catch (err) {
          if (err && err.code === 'ER_DUP_ENTRY') {
            const [existing] = await pool.query(
              `SELECT id FROM episodes
               WHERE source_platform = 'donghub' AND source_episode_id = ?
               LIMIT 1`,
              [sourceEpisodeId]
            );
            episodeId = existing && existing[0] ? Number(existing[0].id) : null;
          } else {
            throw err;
          }
        }

        if (!episodeId) continue;
        seriesReport.inserted_episodes += 1;
        report.total_episode_inserted += 1;
        existingMap.bySourceEpisodeId.set(sourceEpisodeId, episodeId);
        existingMap.bySlug.set(chapterSlug, episodeId);

        try {
          // Requirement: refresh token setiap request detail episode.
          const episodeToken = await refreshToken();
          const episodeDetail = await requestDetail(`/detail/episode-single/${sourceEpisodeId}`, episodeToken);
          const streams = Array.isArray(episodeDetail && episodeDetail.data && episodeDetail.data.stream)
            ? episodeDetail.data.stream
            : [];
          const toInsert = mapDonghubStreams(episodeId, streams);
          const inserted = await insertEpisodeStreamsBulk(toInsert);
          if (inserted > 0) {
            episodeIdsWithStreams.add(episodeId);
            seriesReport.inserted_streams += inserted;
            report.total_stream_inserted += inserted;
          }
        } catch (err) {
          seriesReport.errors.push(`episode ${sourceEpisodeId}: ${err.message}`);
          report.total_errors += 1;
        }
      }

      if (!dryRun && seriesReport.inserted_episodes > 0) {
        await pool.query('UPDATE series SET updated_at = CURRENT_TIMESTAMP WHERE id = ? LIMIT 1', [series.id]);
      }
    } catch (err) {
      seriesReport.errors.push(err.message);
      report.total_errors += 1;
    }

    report.total_series_processed += 1;
    report.series.push(seriesReport);
  }

  report.finished_at = new Date().toISOString();
  return report;
}

async function runAnimekitaOngoingUpdate(options = {}) {
  const dryRun = Boolean(options.dryRun);
  const identityMode = options.identityMode === 'chapter_slug' ? 'chapter_slug' : 'source_episode_id';
  const syncMode = options.syncMode === 'only_missing_streams' ? 'only_missing_streams' : 'full_sync';
  const onlyMissingStreams = syncMode === 'only_missing_streams';
  const limit = Math.max(1, Math.min(Number(options.limit) || 20, 300));
  const onlySeriesId = Number(options.seriesId);
  const onlySourceSeriesId = Number(options.sourceSeriesId);
  const onlySourceSeriesIdRaw = String(options.sourceSeriesIdRaw !== undefined ? options.sourceSeriesIdRaw : '').trim();
  const chapterConcurrency = toSafeConcurrency(
    options.chapterConcurrency || process.env.ANIMEKITA_CHAPTER_CONCURRENCY,
    8
  );
  const base = String(process.env.ANIMEKITA_BASE_URL || DEFAULT_ANIMEKITA_BASE_URL).replace(/\/+$/, '');

  const startedAt = new Date().toISOString();
  const seriesRows = await getOngoingAnimekitaSeries(limit, {
    seriesId: onlySeriesId,
    sourceSeriesId: onlySourceSeriesId,
    sourceSeriesIdRaw: onlySourceSeriesIdRaw
  });

  const report = {
    started_at: startedAt,
    dry_run: dryRun,
    identity_mode: identityMode,
    sync_mode: syncMode,
    limit,
    chapter_concurrency: chapterConcurrency,
    platform: 'animekita',
    total_series_target: seriesRows.length,
    total_series_processed: 0,
    total_episode_candidates: 0,
    total_episode_new: 0,
    total_episode_skipped_by_mode: 0,
    total_episode_inserted: 0,
    total_stream_inserted: 0,
    total_errors: 0,
    series: []
  };

  for (const series of seriesRows) {
    const seriesReport = {
      series_id: Number(series.id),
      source_series_id: Number(series.source_series_id),
      title: series.title,
      candidates: 0,
      new_episodes: 0,
      skipped_new_episodes: 0,
      inserted_episodes: 0,
      inserted_streams: 0,
      errors: []
    };

    try {
      const seriesJson = await fetchAnimekitaSeriesWithFallback(base, series.series_slug);
      const seriesData = Array.isArray(seriesJson && seriesJson.data) && seriesJson.data[0]
        ? seriesJson.data[0]
        : null;
      const chapters = Array.isArray(seriesData && seriesData.chapter) ? seriesData.chapter : [];

      const existingMap = await getExistingEpisodesMap(series.id);
      const episodeIdsWithStreams = dryRun ? new Set() : await getEpisodeIdsWithStreams(series.id);
      const backfillInFlight = new Set();
      seriesReport.candidates = chapters.length;
      report.total_episode_candidates += chapters.length;

      await mapWithConcurrency(chapters, chapterConcurrency, async (chapter) => {
        const sourceEpisodeId = Number(chapter && chapter.id);
        if (!Number.isFinite(sourceEpisodeId) || sourceEpisodeId <= 0) return;

        const chapterSlug = cleanSlug(chapter && chapter.url);
        if (!chapterSlug) return;

        const existingEpisodeId = identityMode === 'chapter_slug'
          ? existingMap.bySlug.get(chapterSlug)
          : existingMap.bySourceEpisodeId.get(sourceEpisodeId);
        const alreadyExists = Number.isFinite(Number(existingEpisodeId)) && Number(existingEpisodeId) > 0;
        if (alreadyExists) {
          if (dryRun) return;
          const needsStreamsBackfill = !episodeIdsWithStreams.has(Number(existingEpisodeId));
          if (!needsStreamsBackfill) return;

          if (backfillInFlight.has(Number(existingEpisodeId))) return;
          backfillInFlight.add(Number(existingEpisodeId));

          try {
            const episodeJson = await fetchJson(`${base}/series/episode/data.php?url=${encodeURIComponent(chapterSlug)}`);
            const episodeData = Array.isArray(episodeJson && episodeJson.data) && episodeJson.data[0]
              ? episodeJson.data[0]
              : null;
            const streamsMap = episodeData && typeof episodeData.streams === 'object' ? episodeData.streams : {};
            const toInsert = mapAnimekitaStreams(Number(existingEpisodeId), streamsMap);
            const inserted = await insertEpisodeStreamsBulk(toInsert);
            if (inserted > 0) {
              episodeIdsWithStreams.add(Number(existingEpisodeId));
              seriesReport.inserted_streams += inserted;
              report.total_stream_inserted += inserted;
            }
          } catch (err) {
            seriesReport.errors.push(`episode ${sourceEpisodeId} (backfill): ${err.message}`);
            report.total_errors += 1;
          }
          return;
        }

        seriesReport.new_episodes += 1;
        report.total_episode_new += 1;

        if (onlyMissingStreams) {
          seriesReport.skipped_new_episodes += 1;
          report.total_episode_skipped_by_mode += 1;
          return;
        }

        if (dryRun) return;

        let episodeId = null;
        let insertedNewEpisode = false;
        try {
          const [insertEpisode] = await pool.query(
            `INSERT INTO episodes (
               source_platform,
               source_episode_id,
               series_id,
               chapter_label,
               chapter_slug,
               release_date_text
             ) VALUES (?, ?, ?, ?, ?, ?)`,
            [
              'animekita',
              sourceEpisodeId,
              series.id,
              String(chapter.ch || ''),
              chapterSlug,
              String(chapter.date || '')
            ]
          );
          episodeId = Number(insertEpisode.insertId);
          insertedNewEpisode = true;
        } catch (err) {
          if (err && err.code === 'ER_DUP_ENTRY') {
            const [existing] = await pool.query(
              `SELECT id FROM episodes
               WHERE source_platform = 'animekita' AND source_episode_id = ?
               LIMIT 1`,
              [sourceEpisodeId]
            );
            episodeId = existing && existing[0] ? Number(existing[0].id) : null;
          } else {
            throw err;
          }
        }

        if (!episodeId) return;
        if (insertedNewEpisode) {
          seriesReport.inserted_episodes += 1;
          report.total_episode_inserted += 1;
        }
        existingMap.bySourceEpisodeId.set(sourceEpisodeId, episodeId);
        existingMap.bySlug.set(chapterSlug, episodeId);

        if (!insertedNewEpisode) return;

        try {
          const episodeJson = await fetchJson(`${base}/series/episode/data.php?url=${encodeURIComponent(chapterSlug)}`);
          const episodeData = Array.isArray(episodeJson && episodeJson.data) && episodeJson.data[0]
            ? episodeJson.data[0]
            : null;
          const streamsMap = episodeData && typeof episodeData.streams === 'object' ? episodeData.streams : {};
          const toInsert = mapAnimekitaStreams(episodeId, streamsMap);
          const inserted = await insertEpisodeStreamsBulk(toInsert);
          if (inserted > 0) {
            episodeIdsWithStreams.add(episodeId);
            seriesReport.inserted_streams += inserted;
            report.total_stream_inserted += inserted;
          }
        } catch (err) {
          seriesReport.errors.push(`episode ${sourceEpisodeId}: ${err.message}`);
          report.total_errors += 1;
        }
      });

      if (!dryRun && seriesReport.inserted_episodes > 0) {
        await pool.query('UPDATE series SET updated_at = CURRENT_TIMESTAMP WHERE id = ? LIMIT 1', [series.id]);
      }
    } catch (err) {
      seriesReport.errors.push(err.message);
      report.total_errors += 1;
    }

    report.total_series_processed += 1;
    report.series.push(seriesReport);
  }

  report.finished_at = new Date().toISOString();
  return report;
}

module.exports = {
  getEnvStatus,
  runDonghubOngoingUpdate,
  runAnimekitaOngoingUpdate,
  fetchAnimekitaSeriesByFullUrl,
  fetchDonghubSeriesBySourceId
};
