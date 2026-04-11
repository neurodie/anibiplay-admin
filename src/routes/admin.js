const crypto = require('crypto');
const express = require('express');
const axios = require('axios');
const { pool, userPool } = require('../db');
const { toInt, escapeLike } = require('../utils/http');
const { requireAdmin } = require('../middleware/auth');
const {
  getEnvStatus,
  runDonghubOngoingUpdate,
  runAnimekitaOngoingUpdate,
  fetchAnimekitaSeriesByFullUrl,
  fetchDonghubSeriesBySourceId
} = require('../services/donghub-updater');

const router = express.Router();

function safeCompare(a, b) {
  const aa = Buffer.from(String(a));
  const bb = Buffer.from(String(b));
  if (aa.length !== bb.length) return false;
  return crypto.timingSafeEqual(aa, bb);
}

function cleanSeriesSlug(input) {
  return String(input || '').trim().replace(/^\/+|\/+$/g, '');
}

function cleanSynopsis(input) {
  return String(input || '')
    .replace(/Nonton Anime tanpa iklan di Aplikasi AnimeLovers V3\s*/gi, '')
    .trim();
}

function parseGenreCsv(input) {
  const parts = String(input || '')
    .split(',')
    .map((x) => x.trim())
    .filter(Boolean);

  const seen = new Set();
  const out = [];
  for (const name of parts) {
    const key = name.toLowerCase();
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(name);
  }
  return out;
}

function firstNonEmpty(...values) {
  for (const value of values) {
    const str = String(value == null ? '' : value).trim();
    if (str) return str;
  }
  return '';
}

function normalizeOriginValue(input, fallback = 'anime') {
  const raw = String(input || '').trim().toLowerCase();
  if (!raw) return fallback;
  if (raw === 'donghua' || raw.includes('china') || raw.includes('cn')) return 'donghua';
  if (raw === 'anime' || raw.includes('japan') || raw.includes('jp')) return 'anime';
  return fallback;
}

function extractSlugFromUrlLike(input) {
  const raw = String(input || '').trim();
  if (!raw) return '';
  try {
    const u = new URL(raw);
    const fromQuery = cleanSeriesSlug(u.searchParams.get('url'));
    if (fromQuery) return fromQuery;
    const parts = u.pathname.split('/').map((x) => cleanSeriesSlug(x)).filter(Boolean);
    return parts.length ? parts[parts.length - 1] : '';
  } catch (_err) {
    return cleanSeriesSlug(raw);
  }
}

function normalizeGenreFromAny(input) {
  if (Array.isArray(input)) {
    return parseGenreCsv(
      input
        .map((g) => {
          if (g == null) return '';
          if (typeof g === 'string') return g;
          if (typeof g === 'object') return firstNonEmpty(g.name, g.title, g.label, g.genre);
          return '';
        })
        .filter(Boolean)
        .join(',')
    );
  }
  if (input && typeof input === 'object') {
    return parseGenreCsv(
      Object.values(input)
        .map((v) => String(v || '').trim())
        .filter(Boolean)
        .join(',')
    );
  }
  return parseGenreCsv(String(input || ''));
}

function mapAutoSeriesInputFromRemote(platform, remoteData, options = {}) {
  const p = String(platform || '').trim().toLowerCase();
  const d = remoteData && typeof remoteData === 'object' ? remoteData : {};

  const sourceSeriesId = Number(
    firstNonEmpty(
      d.id,
      d.source_series_id,
      d.series_id,
      d.anime_id,
      d.post_id
    )
  );

  const seriesSlug = cleanSeriesSlug(
    firstNonEmpty(
      options.seriesSlug,
      d.series_id,
      d.url,
      d.series_slug,
      d.slug,
      d.permalink,
      extractSlugFromUrlLike(d.link),
      extractSlugFromUrlLike(d.href)
    )
  );

  const title = firstNonEmpty(d.title, d.judul, d.name, d.anime_name);
  const title2 = firstNonEmpty(d.title2, d.title_alt, d.judul2, d.english_title, d.en_title, d.alt_title);
  const coverUrl = firstNonEmpty(d.cover_url, d.cover, d.image, d.poster, d.thumbnail, d.thumb);
  const type = firstNonEmpty(d.type, d.format, d.kind);
  const status = firstNonEmpty(d.status, d.airing_status);
  const rating = firstNonEmpty(d.rating, d.score, d.vote_average);
  const publishedText = firstNonEmpty(d.published_text, d.published, d.release, d.released, d.date, d.published_at);
  const author = firstNonEmpty(d.author, d.studio, d.producer, d.creator);
  const synopsis = firstNonEmpty(d.synopsis, d.sinopsis, d.description, d.desc, d.overview);
  const genres = normalizeGenreFromAny(d.genres || d.genre || d.tags);

  const contentOrigin = normalizeOriginValue(
    firstNonEmpty(d.content_origin, d.origin, d.country, d.region),
    p === 'donghub' ? 'anime' : 'anime'
  );

  return {
    source_platform: p,
    source_series_id: Number.isFinite(sourceSeriesId) && sourceSeriesId > 0 ? String(sourceSeriesId) : '',
    content_origin: contentOrigin,
    title,
    title2,
    series_slug: seriesSlug,
    cover_url: coverUrl,
    type,
    status,
    rating,
    published_text: publishedText,
    author,
    synopsis,
    genres_csv: genres.join(', ')
  };
}

async function createSeriesFromBuilt(built) {
  const { payload, normalized } = built;
  const genres = payload.genres;

  let conn;
  try {
    conn = await pool.getConnection();
    await conn.beginTransaction();

    const [insertSeries] = await conn.query(
      `INSERT INTO series (
         source_platform, source_series_id, content_origin, title, title2, series_slug,
         cover_url, type, status, rating, published_text, author, synopsis
       ) VALUES (?, ?, ?, ?, NULLIF(?, ''), ?, NULLIF(?, ''), NULLIF(?, ''), NULLIF(?, ''), ?, NULLIF(?, ''), NULLIF(?, ''), NULLIF(?, ''))`,
      [
        payload.source_platform,
        normalized.sourceSeriesId,
        payload.content_origin,
        payload.title,
        payload.title2,
        payload.series_slug,
        payload.cover_url,
        payload.type,
        payload.status,
        normalized.ratingValue,
        payload.published_text,
        payload.author,
        payload.synopsis
      ]
    );

    const newSeriesId = Number(insertSeries.insertId);
    for (const name of genres) {
      const [insertGenre] = await conn.query(
        'INSERT INTO genres (name) VALUES (?) ON DUPLICATE KEY UPDATE id = LAST_INSERT_ID(id)',
        [name]
      );
      await conn.query('INSERT IGNORE INTO series_genres (series_id, genre_id) VALUES (?, ?)', [
        newSeriesId,
        insertGenre.insertId
      ]);
    }

    await conn.commit();
    return newSeriesId;
  } catch (err) {
    if (conn) await conn.rollback();
    throw err;
  } finally {
    if (conn) conn.release();
  }
}

function normalizeGenreInput(input) {
  if (Array.isArray(input)) {
    const joined = input
      .map((x) => String(x || '').trim())
      .filter(Boolean)
      .join(',');
    return parseGenreCsv(joined);
  }
  return parseGenreCsv(String(input || ''));
}

function buildSeriesPayloadFromInput(input = {}) {
  const rawCover =
    input.cover_url !== undefined ? input.cover_url
      : (input.cover !== undefined ? input.cover
        : (input.image !== undefined ? input.image : input.poster));
  const rawSynopsis =
    input.synopsis !== undefined ? input.synopsis
      : (input.sinopsis !== undefined ? input.sinopsis
        : (input.description !== undefined ? input.description : input.desc));

  const payload = {
    source_platform: String(input.source_platform || '').trim(),
    source_series_id: String(input.source_series_id || '').trim(),
    content_origin: String(input.content_origin || '').trim(),
    title: String(input.title || '').trim(),
    title2: String(input.title2 || '').trim(),
    series_slug: cleanSeriesSlug(input.series_slug),
    cover_url: String(rawCover || '').trim(),
    type: String(input.type || '').trim(),
    status: String(input.status || '').trim(),
    rating: String(input.rating || '').trim(),
    published_text: String(input.published_text || '').trim(),
    author: String(input.author || '').trim(),
    synopsis: cleanSynopsis(rawSynopsis),
    genres: normalizeGenreInput(input.genres_csv !== undefined ? input.genres_csv : input.genres)
  };

  if (!payload.source_platform || !payload.content_origin || !payload.title || !payload.series_slug) {
    return { error: 'Field wajib: source_platform, content_origin, title, series_slug' };
  }

  const sourceSeriesId = Number(payload.source_series_id);
  if (!Number.isFinite(sourceSeriesId) || sourceSeriesId <= 0) {
    return { error: 'source_series_id harus angka > 0' };
  }

  let ratingValue = null;
  if (payload.rating) {
    const parsed = Number(payload.rating);
    if (!Number.isFinite(parsed)) {
      return { error: 'rating harus angka' };
    }
    ratingValue = parsed;
  }

  return {
    payload,
    normalized: {
      sourceSeriesId,
      ratingValue
    }
  };
}

function buildEpisodePayloadFromInput(input = {}, options = {}) {
  const requireSeriesId = options.requireSeriesId !== false;
  const payload = {
    source_platform: String(input.source_platform || '').trim(),
    source_episode_id: String(input.source_episode_id || '').trim(),
    series_id: String(input.series_id || '').trim(),
    chapter_label: String(input.chapter_label || '').trim(),
    chapter_slug: cleanSeriesSlug(input.chapter_slug),
    release_date_text: String(input.release_date_text || '').trim()
  };

  if (!payload.source_platform || !payload.chapter_slug) {
    return { error: 'Field wajib: source_platform, chapter_slug' };
  }

  const sourceEpisodeId = Number(payload.source_episode_id);
  if (!Number.isFinite(sourceEpisodeId) || sourceEpisodeId <= 0) {
    return { error: 'source_episode_id harus angka > 0' };
  }

  const seriesId = Number(payload.series_id);
  if (requireSeriesId && (!Number.isFinite(seriesId) || seriesId <= 0)) {
    return { error: 'series_id harus angka > 0' };
  }

  return {
    payload,
    normalized: {
      sourceEpisodeId,
      seriesId: Number.isFinite(seriesId) && seriesId > 0 ? seriesId : null
    }
  };
}

function buildStreamPayloadFromInput(input = {}) {
  const payload = {
    episode_id: String(input.episode_id || '').trim(),
    chapter_slug: cleanSeriesSlug(input.chapter_slug),
    resolution: String(input.resolution || '').trim(),
    stream_source_id: String(input.stream_source_id || '').trim(),
    stream_url: String(input.stream_url || '').trim(),
    size_kb: String(input.size_kb || '').trim()
  };

  if (!payload.resolution || !payload.stream_url) {
    return { error: 'Field wajib: resolution, stream_url' };
  }

  let episodeId = null;
  if (payload.episode_id) {
    const parsed = Number(payload.episode_id);
    if (!Number.isFinite(parsed) || parsed <= 0) {
      return { error: 'episode_id harus kosong atau angka > 0' };
    }
    episodeId = parsed;
  }

  let streamSourceId = null;
  if (payload.stream_source_id !== '') {
    const parsed = Number(payload.stream_source_id);
    if (!Number.isFinite(parsed) || parsed <= 0) {
      return { error: 'stream_source_id harus kosong atau angka > 0' };
    }
    streamSourceId = parsed;
  }

  let sizeKb = null;
  if (payload.size_kb !== '') {
    const parsed = Number(payload.size_kb);
    if (!Number.isFinite(parsed) || parsed < 0) {
      return { error: 'size_kb harus kosong atau angka >= 0' };
    }
    sizeKb = Math.round(parsed);
  }

  return {
    payload,
    normalized: {
      episodeId,
      streamSourceId,
      sizeKb
    }
  };
}

async function resolveSeriesId(conn, item, seriesIdBySlugKey) {
  const directSeriesId = Number(item.series_id);
  if (Number.isFinite(directSeriesId) && directSeriesId > 0) {
    const [[series]] = await conn.query('SELECT id FROM series WHERE id = ? LIMIT 1', [directSeriesId]);
    return series ? Number(series.id) : null;
  }

  const sourcePlatform = String(item.source_platform || '').trim();
  const seriesSlug = cleanSeriesSlug(item.series_slug);
  if (sourcePlatform && seriesSlug) {
    const key = `${sourcePlatform}::${seriesSlug}`;
    if (seriesIdBySlugKey.has(key)) return Number(seriesIdBySlugKey.get(key));
    const [[series]] = await conn.query(
      'SELECT id FROM series WHERE source_platform = ? AND series_slug = ? LIMIT 1',
      [sourcePlatform, seriesSlug]
    );
    return series ? Number(series.id) : null;
  }

  const sourceSeriesId = Number(item.source_series_id);
  if (sourcePlatform && Number.isFinite(sourceSeriesId) && sourceSeriesId > 0) {
    const [[series]] = await conn.query(
      'SELECT id FROM series WHERE source_platform = ? AND source_series_id = ? LIMIT 1',
      [sourcePlatform, sourceSeriesId]
    );
    return series ? Number(series.id) : null;
  }

  return null;
}

async function resolveEpisodeId(conn, item, episodeIdBySlugKey) {
  const chapterSlug = cleanSeriesSlug(item.chapter_slug);
  if (chapterSlug) {
    if (episodeIdBySlugKey.has(chapterSlug)) return Number(episodeIdBySlugKey.get(chapterSlug));
    const [[episode]] = await conn.query('SELECT id FROM episodes WHERE chapter_slug = ? LIMIT 1', [chapterSlug]);
    return episode ? Number(episode.id) : null;
  }

  const directEpisodeId = Number(item.episode_id);
  if (Number.isFinite(directEpisodeId) && directEpisodeId > 0) {
    const [[episode]] = await conn.query('SELECT id FROM episodes WHERE id = ? LIMIT 1', [directEpisodeId]);
    return episode ? Number(episode.id) : null;
  }

  const sourcePlatform = String(item.source_platform || '').trim();
  const sourceEpisodeId = Number(item.source_episode_id);
  if (sourcePlatform && Number.isFinite(sourceEpisodeId) && sourceEpisodeId > 0) {
    const [[episode]] = await conn.query(
      'SELECT id FROM episodes WHERE source_platform = ? AND source_episode_id = ? LIMIT 1',
      [sourcePlatform, sourceEpisodeId]
    );
    return episode ? Number(episode.id) : null;
  }

  return null;
}

const SCHEDULE_DAYS = ['senin', 'selasa', 'rabu', 'kamis', 'jumat', 'sabtu', 'minggu'];
let scheduleTableReady = false;
let announcementTableReady = false;
let topAnimeTableReady = false;

function resolveTopAnimeDbName() {
  const raw = String(process.env.HOME_TOP_DB || process.env.DB_NAME || '').trim();
  if (/^[a-zA-Z0-9_]+$/.test(raw)) return raw;
  return '';
}

function qTable(dbName, tableName) {
  if (!dbName) return `\`${tableName}\``;
  return `\`${dbName}\`.\`${tableName}\``;
}

const TOP_ANIME_DB = resolveTopAnimeDbName();
const TABLE_TOP_ANIME = qTable(TOP_ANIME_DB, 'top_anime');

async function checkUserTableExists(tableName) {
  const [[row]] = await userPool.query(
    `SELECT COUNT(*) AS c
     FROM information_schema.TABLES
     WHERE TABLE_SCHEMA = DATABASE()
       AND TABLE_NAME = ?
     LIMIT 1`,
    [tableName]
  );
  return Number(row && row.c) > 0;
}

function toSafeTimestampString(input) {
  if (!input) return '';
  const date = new Date(input);
  if (Number.isNaN(date.getTime())) return String(input);
  const yyyy = date.getFullYear();
  const mm = String(date.getMonth() + 1).padStart(2, '0');
  const dd = String(date.getDate()).padStart(2, '0');
  const hh = String(date.getHours()).padStart(2, '0');
  const mi = String(date.getMinutes()).padStart(2, '0');
  const ss = String(date.getSeconds()).padStart(2, '0');
  return `${yyyy}-${mm}-${dd} ${hh}:${mi}:${ss}`;
}

function startOfUtcDay(date) {
  return new Date(Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), date.getUTCDate()));
}

function startOfUtcWeekMonday(date) {
  const dayStart = startOfUtcDay(date);
  const day = dayStart.getUTCDay();
  const diffToMonday = (day + 6) % 7;
  dayStart.setUTCDate(dayStart.getUTCDate() - diffToMonday);
  return dayStart;
}

function startOfUtcMonth(date) {
  return new Date(Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), 1));
}

function buildXpLeaderboardWindow(periodInput) {
  const period = String(periodInput || 'weekly').trim().toLowerCase();
  const now = new Date();

  if (period === 'daily') {
    const windowStart = startOfUtcDay(now);
    const windowEnd = new Date(windowStart);
    windowEnd.setUTCDate(windowEnd.getUTCDate() + 1);
    return { period: 'daily', windowStart, windowEnd, label: 'Daily (UTC)' };
  }

  if (period === 'monthly') {
    const windowStart = startOfUtcMonth(now);
    const windowEnd = new Date(windowStart);
    windowEnd.setUTCMonth(windowEnd.getUTCMonth() + 1);
    return { period: 'monthly', windowStart, windowEnd, label: 'Monthly (UTC)' };
  }

  const windowStart = startOfUtcWeekMonday(now);
  const windowEnd = new Date(windowStart);
  windowEnd.setUTCDate(windowEnd.getUTCDate() + 7);
  return { period: 'weekly', windowStart, windowEnd, label: 'Weekly (UTC, Monday start)' };
}

function toUtcDateOnlyString(input) {
  if (!input) return '';
  const d = new Date(input);
  if (Number.isNaN(d.getTime())) return '';
  const y = d.getUTCFullYear();
  const m = String(d.getUTCMonth() + 1).padStart(2, '0');
  const day = String(d.getUTCDate()).padStart(2, '0');
  return `${y}-${m}-${day}`;
}

async function queryXpDailyLeaderboard({ whereSql, whereParams, limit, offset, windowStart, windowEnd }) {
  const dateStart = toUtcDateOnlyString(windowStart);
  const dateEnd = toUtcDateOnlyString(windowEnd);
  const [rowsRaw] = await userPool.query(
    `SELECT
       u.id AS user_id,
       u.name AS user_name,
       u.email AS user_email,
       u.level AS user_level,
       u.role AS user_role,
       u.premium_expires_at AS user_premium_expires_at,
       COALESCE(SUM(d.xp_watch), 0) AS xp_gained
     FROM users u
     LEFT JOIN user_xp_daily d
       ON d.user_id = u.id
      AND d.date_utc >= ?
      AND d.date_utc < ?
     ${whereSql}
     GROUP BY u.id, u.name, u.email, u.level, u.role, u.premium_expires_at
     HAVING xp_gained > 0
     ORDER BY xp_gained DESC, u.id ASC
     LIMIT ? OFFSET ?`,
    [dateStart, dateEnd, ...whereParams, limit, offset]
  );

  const [[countRow]] = await userPool.query(
    `SELECT COUNT(*) AS total
     FROM (
       SELECT u.id
       FROM users u
       LEFT JOIN user_xp_daily d
         ON d.user_id = u.id
        AND d.date_utc >= ?
        AND d.date_utc < ?
       ${whereSql}
       GROUP BY u.id
       HAVING COALESCE(SUM(d.xp_watch), 0) > 0
     ) counted`,
    [dateStart, dateEnd, ...whereParams]
  );

  return {
    rowsRaw,
    total: Number((countRow && countRow.total) || 0)
  };
}

function sanitizePremiumReturnPath(input) {
  const fallback = '/admin/users/premium';
  const value = String(input || '').trim();
  if (!value.startsWith('/admin/users/premium')) return fallback;
  return value;
}

function sanitizeUsersReturnPath(input, userId = 0) {
  const fallback = userId > 0 ? `/admin/users/${userId}` : '/admin/users';
  const value = String(input || '').trim();
  if (!value.startsWith('/admin/users')) return fallback;
  return value;
}

function buildNotificationApiConfig() {
  const baseUrlRaw = String(process.env.PANEL_API_BASE_URL || '').trim().replace(/\/+$/, '');
  const sendUrlRaw = String(process.env.NOTIFICATION_SEND_URL || '').trim();
  const bearerToken = String(process.env.NOTIFICATION_BEARER_TOKEN || '').trim();
  const adminSecret = String(
    process.env.NOTIFICATIONS_ADMIN_SECRET
      || process.env.NOTIFICATION_ADMIN_SECRET
      || ''
  ).trim();

  const sendUrl = sendUrlRaw || (baseUrlRaw ? `${baseUrlRaw}/api/notifications/send` : '');
  const authMode = adminSecret ? 'admin_secret' : (bearerToken ? 'bearer' : 'none');
  return { sendUrl, bearerToken, adminSecret, authMode };
}

function generateIdempotencyKey() {
  return `admin-notif-${Date.now()}-${Math.random().toString(36).slice(2, 10)}`;
}

function parseUserIdsCsv(input) {
  const values = String(input || '')
    .split(',')
    .map((x) => Number(String(x).trim()))
    .filter((n) => Number.isFinite(n) && n > 0)
    .map((n) => Math.trunc(n));
  return Array.from(new Set(values));
}

function normalizeScheduleDay(input) {
  const day = String(input || '').trim().toLowerCase();
  return SCHEDULE_DAYS.includes(day) ? day : '';
}

async function ensureScheduleTable() {
  if (scheduleTableReady) return;
  await pool.query(`
    CREATE TABLE IF NOT EXISTS schedule_entries (
      id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
      series_id BIGINT UNSIGNED NOT NULL,
      day_of_week ENUM('senin','selasa','rabu','kamis','jumat','sabtu','minggu') NOT NULL,
      series_slug_snapshot VARCHAR(255) DEFAULT NULL,
      cover_url_snapshot TEXT DEFAULT NULL,
      time_label VARCHAR(30) DEFAULT NULL,
      sort_order INT NOT NULL DEFAULT 0,
      is_active TINYINT(1) NOT NULL DEFAULT 1,
      created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
      updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
      PRIMARY KEY (id),
      UNIQUE KEY uq_schedule_day_series (day_of_week, series_id),
      KEY idx_schedule_day_sort (day_of_week, sort_order),
      KEY idx_schedule_series (series_id),
      CONSTRAINT fk_schedule_series FOREIGN KEY (series_id) REFERENCES series(id) ON DELETE CASCADE
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
  `);
  try {
    await pool.query('ALTER TABLE schedule_entries ADD COLUMN series_slug_snapshot VARCHAR(255) DEFAULT NULL AFTER day_of_week');
  } catch (err) {
    if (!err || err.code !== 'ER_DUP_FIELDNAME') throw err;
  }
  try {
    await pool.query('ALTER TABLE schedule_entries ADD COLUMN cover_url_snapshot TEXT DEFAULT NULL AFTER series_slug_snapshot');
  } catch (err) {
    if (!err || err.code !== 'ER_DUP_FIELDNAME') throw err;
  }
  scheduleTableReady = true;
}

async function ensureAnnouncementTable() {
  if (announcementTableReady) return;
  await pool.query(`
    CREATE TABLE IF NOT EXISTS announcements (
      id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
      platform VARCHAR(50) NOT NULL,
      is_show TINYINT(1) NOT NULL DEFAULT 0,
      is_maintenance TINYINT(1) NOT NULL DEFAULT 0,
      allow_exit_button TINYINT(1) NOT NULL DEFAULT 1,
      latest_app_version VARCHAR(30) DEFAULT NULL,
      daily_update_text TEXT DEFAULT NULL,
      title VARCHAR(255) DEFAULT NULL,
      message TEXT DEFAULT NULL,
      type VARCHAR(30) NOT NULL DEFAULT 'info',
      cta_text VARCHAR(120) DEFAULT NULL,
      cta_url TEXT DEFAULT NULL,
      created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
      updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
      PRIMARY KEY (id),
      UNIQUE KEY uq_announcements_platform (platform)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
  `);
  try {
    await pool.query('ALTER TABLE announcements ADD COLUMN is_maintenance TINYINT(1) NOT NULL DEFAULT 0 AFTER is_show');
  } catch (err) {
    if (!err || err.code !== 'ER_DUP_FIELDNAME') throw err;
  }
  try {
    await pool.query('ALTER TABLE announcements ADD COLUMN allow_exit_button TINYINT(1) NOT NULL DEFAULT 1 AFTER is_maintenance');
  } catch (err) {
    if (!err || err.code !== 'ER_DUP_FIELDNAME') throw err;
  }
  try {
    await pool.query('ALTER TABLE announcements ADD COLUMN latest_app_version VARCHAR(30) DEFAULT NULL AFTER allow_exit_button');
  } catch (err) {
    if (!err || err.code !== 'ER_DUP_FIELDNAME') throw err;
  }
  try {
    await pool.query('ALTER TABLE announcements ADD COLUMN daily_update_text TEXT DEFAULT NULL AFTER latest_app_version');
  } catch (err) {
    if (!err || err.code !== 'ER_DUP_FIELDNAME') throw err;
  }
  announcementTableReady = true;
}

async function ensureTopAnimeTable() {
  if (topAnimeTableReady) return;
  await pool.query(`
    CREATE TABLE IF NOT EXISTS ${TABLE_TOP_ANIME} (
      id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
      series_id BIGINT UNSIGNED NOT NULL,
      sort_order INT NOT NULL DEFAULT 0,
      is_active TINYINT(1) NOT NULL DEFAULT 1,
      created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
      updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
      PRIMARY KEY (id),
      UNIQUE KEY uq_top_anime_series (series_id),
      KEY idx_top_anime_sort (sort_order, id),
      KEY idx_top_anime_active (is_active)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
  `);
  topAnimeTableReady = true;
}

function buildSeriesWhere(query) {
  const q = String(query.q || '').trim();
  const origin = String(query.content_origin || '').trim();
  const platform = String(query.source_platform || '').trim();
  const type = String(query.type || '').trim();
  const status = String(query.status || '').trim();
  const hasEpisodes = String(query.has_episodes || '').trim();

  const where = [];
  const params = [];

  if (q) {
    where.push('(s.title LIKE ? OR s.title2 LIKE ? OR s.series_slug LIKE ?)');
    const like = `%${escapeLike(q)}%`;
    params.push(like, like, like);
  }
  if (origin) {
    where.push('s.content_origin = ?');
    params.push(origin);
  }
  if (platform) {
    where.push('s.source_platform = ?');
    params.push(platform);
  }
  if (type) {
    where.push('s.type = ?');
    params.push(type);
  }
  if (status) {
    where.push('s.status = ?');
    params.push(status);
  }
  if (hasEpisodes === 'yes') {
    where.push('EXISTS (SELECT 1 FROM episodes e2 WHERE e2.series_id = s.id LIMIT 1)');
  }
  if (hasEpisodes === 'no') {
    where.push('NOT EXISTS (SELECT 1 FROM episodes e2 WHERE e2.series_id = s.id LIMIT 1)');
  }

  return {
    q,
    origin,
    platform,
    type,
    status,
    hasEpisodes,
    whereSql: where.length ? `WHERE ${where.join(' AND ')}` : '',
    params
  };
}

router.get('/login', (req, res) => {
  if (req.session.isAdmin) return res.redirect('/admin');

  return res.render('admin-login', {
    title: 'Admin Login',
    error: req.query.error ? 'Username/password salah.' : ''
  });
});

router.post('/login', (req, res) => {
  const user = String(req.body.username || '');
  const pass = String(req.body.password || '');

  const expectedUser = process.env.ADMIN_USER || 'admin';
  const expectedPass = process.env.ADMIN_PASS || 'admin123';

  if (!safeCompare(user, expectedUser) || !safeCompare(pass, expectedPass)) {
    return res.redirect('/admin/login?error=1');
  }

  req.session.isAdmin = true;
  req.session.adminUser = user;
  return res.redirect('/admin');
});

router.post('/logout', (req, res) => {
  req.session.destroy(() => res.redirect('/admin/login'));
});

router.use(requireAdmin);

router.post('/series/backfill-empty-episodes', async (req, res) => {
  try {
    const [rows] = await pool.query(
      `SELECT s.id, s.source_platform
       FROM series s
       WHERE NOT EXISTS (SELECT 1 FROM episodes e WHERE e.series_id = s.id)
       ORDER BY s.id ASC`
    );

    let inserted = 0;
    for (const row of rows) {
      const seriesId = Number(row.id);
      if (!Number.isFinite(seriesId) || seriesId <= 0) continue;

      const sourcePlatform = String(row.source_platform || '').trim() || 'unknown';
      const sourceEpisodeId = 900000000 + seriesId;
      const chapterSlug = `placeholder-${sourcePlatform}-${seriesId}-1`;

      try {
        await pool.query(
          `INSERT INTO episodes (
             source_platform,
             source_episode_id,
             series_id,
             chapter_label,
             chapter_slug,
             release_date_text
           ) VALUES (?, ?, ?, ?, ?, ?)`,
          [sourcePlatform, sourceEpisodeId, seriesId, '1', chapterSlug, '']
        );
        inserted += 1;
      } catch (err) {
        if (!err || err.code !== 'ER_DUP_ENTRY') throw err;
      }
    }

    return res.redirect(`/admin?backfill_ok=1&backfill_count=${inserted}`);
  } catch (err) {
    return res.redirect(`/admin?error=${encodeURIComponent(err.message || 'Gagal backfill episode placeholder')}`);
  }
});

async function loadUpdaterOverview(selectedPlatform = '') {
  const [ongoingCounts] = await pool.query(
    `SELECT source_platform, COUNT(*) AS total
     FROM series
     WHERE status IN ('Ongoing', 'Currently Airing')
     GROUP BY source_platform
     ORDER BY total DESC`
  );

  const platformSet = new Set(ongoingCounts.map((x) => String(x.source_platform || '')));
  const platformValid = selectedPlatform && platformSet.has(selectedPlatform);

  let ongoingSeries = [];
  if (platformValid) {
    const [rows] = await pool.query(
      `SELECT id, title, source_platform, source_series_id, status
       FROM series
       WHERE status IN ('Ongoing', 'Currently Airing')
         AND source_platform = ?
       ORDER BY title ASC
       LIMIT 500`,
      [selectedPlatform]
    );
    ongoingSeries = rows;
  }

  return { ongoingCounts, ongoingSeries, platformValid };
}

let updaterRunTablesReady = false;
async function ensureUpdaterRunTables() {
  if (updaterRunTablesReady) return;
  await pool.query(`
    CREATE TABLE IF NOT EXISTS updater_runs (
      id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
      run_platform VARCHAR(30) NOT NULL,
      run_mode VARCHAR(20) NOT NULL,
      sync_mode VARCHAR(30) NOT NULL DEFAULT 'full_sync',
      identity_mode VARCHAR(30) NOT NULL,
      series_limit INT NOT NULL DEFAULT 20,
      dry_run TINYINT(1) NOT NULL DEFAULT 1,
      started_at DATETIME NULL,
      finished_at DATETIME NULL,
      total_series_target INT NOT NULL DEFAULT 0,
      total_series_processed INT NOT NULL DEFAULT 0,
      total_episode_candidates INT NOT NULL DEFAULT 0,
      total_episode_new INT NOT NULL DEFAULT 0,
      total_episode_skipped_by_mode INT NOT NULL DEFAULT 0,
      total_episode_inserted INT NOT NULL DEFAULT 0,
      total_stream_inserted INT NOT NULL DEFAULT 0,
      total_errors INT NOT NULL DEFAULT 0,
      status VARCHAR(20) NOT NULL DEFAULT 'success',
      error_text TEXT NULL,
      created_by VARCHAR(120) NULL,
      created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
      PRIMARY KEY (id),
      KEY idx_updater_runs_created_at (created_at)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
  `);
  await pool.query(`
    CREATE TABLE IF NOT EXISTS updater_run_items (
      id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
      run_id BIGINT UNSIGNED NOT NULL,
      series_id BIGINT UNSIGNED NULL,
      source_series_id BIGINT UNSIGNED NULL,
      title VARCHAR(255) NULL,
      candidates INT NOT NULL DEFAULT 0,
      new_episodes INT NOT NULL DEFAULT 0,
      skipped_new_episodes INT NOT NULL DEFAULT 0,
      inserted_episodes INT NOT NULL DEFAULT 0,
      inserted_streams INT NOT NULL DEFAULT 0,
      errors_text TEXT NULL,
      created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
      PRIMARY KEY (id),
      KEY idx_updater_run_items_run (run_id),
      CONSTRAINT fk_updater_run_items_run FOREIGN KEY (run_id) REFERENCES updater_runs(id) ON DELETE CASCADE
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
  `);
  updaterRunTablesReady = true;
}

async function loadUpdaterHistory(limit = 20) {
  await ensureUpdaterRunTables();
  const [rows] = await pool.query(
    `SELECT
       id,
       run_platform,
       run_mode,
       sync_mode,
       identity_mode,
       series_limit,
       dry_run,
       status,
       total_series_target,
       total_series_processed,
       total_episode_new,
       total_episode_skipped_by_mode,
       total_episode_inserted,
       total_stream_inserted,
       total_errors,
       created_by,
       started_at,
       finished_at,
       created_at
     FROM updater_runs
     ORDER BY id DESC
     LIMIT ?`,
    [limit]
  );
  return rows;
}

async function saveUpdaterRunLog({ adminUser, runPlatform, mode, syncMode, identityMode, limit, result, error }) {
  await ensureUpdaterRunTables();
  const startedAt = result && result.started_at ? new Date(result.started_at) : new Date();
  const finishedAt = result && result.finished_at ? new Date(result.finished_at) : new Date();
  const status = error ? 'failed' : 'success';

  const [ins] = await pool.query(
    `INSERT INTO updater_runs (
       run_platform, run_mode, sync_mode, identity_mode, series_limit, dry_run,
       started_at, finished_at,
       total_series_target, total_series_processed,
       total_episode_candidates, total_episode_new, total_episode_skipped_by_mode,
       total_episode_inserted, total_stream_inserted, total_errors,
       status, error_text, created_by
     ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
    [
      runPlatform,
      mode,
      syncMode,
      identityMode,
      limit,
      result ? (result.dry_run ? 1 : 0) : (mode === 'dry_run' ? 1 : 0),
      startedAt,
      finishedAt,
      Number(result && result.total_series_target || 0),
      Number(result && result.total_series_processed || 0),
      Number(result && result.total_episode_candidates || 0),
      Number(result && result.total_episode_new || 0),
      Number(result && result.total_episode_skipped_by_mode || 0),
      Number(result && result.total_episode_inserted || 0),
      Number(result && result.total_stream_inserted || 0),
      Number(result && result.total_errors || (error ? 1 : 0)),
      status,
      error ? String(error.message || error).slice(0, 4000) : null,
      String(adminUser || '').slice(0, 120) || null
    ]
  );

  const runId = Number(ins.insertId);
  const items = Array.isArray(result && result.series) ? result.series : [];
  for (const item of items) {
    await pool.query(
      `INSERT INTO updater_run_items (
         run_id, series_id, source_series_id, title,
         candidates, new_episodes, skipped_new_episodes, inserted_episodes, inserted_streams, errors_text
       ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      [
        runId,
        Number(item.series_id || 0) || null,
        Number(item.source_series_id || 0) || null,
        String(item.title || '').slice(0, 255),
        Number(item.candidates || 0),
        Number(item.new_episodes || 0),
        Number(item.skipped_new_episodes || 0),
        Number(item.inserted_episodes || 0),
        Number(item.inserted_streams || 0),
        Array.isArray(item.errors) && item.errors.length ? item.errors.join(' | ').slice(0, 4000) : null
      ]
    );
  }
}

router.get('/updater', async (req, res) => {
  const runPlatform = String(req.query.run_platform || 'donghub').trim().toLowerCase();
  const selectedRunPlatform = runPlatform === 'animekita' ? 'animekita' : 'donghub';
  const selectedPlatform = String(req.query.platform || '').trim();
  const identityModeRaw = String(req.query.identity_mode || 'source_episode_id').trim().toLowerCase();
  const selectedIdentityMode = identityModeRaw === 'chapter_slug' ? 'chapter_slug' : 'source_episode_id';
  const modeRaw = String(req.query.mode || 'dry_run').trim().toLowerCase();
  const selectedMode = modeRaw === 'apply' ? 'apply' : 'dry_run';
  const syncModeRaw = String(req.query.sync_mode || 'full_sync').trim().toLowerCase();
  const selectedSyncMode = syncModeRaw === 'only_missing_streams' ? 'only_missing_streams' : 'full_sync';
  const selectedChapterConcurrency = toInt(req.query.chapter_concurrency, 8, 1, 30);
  const historyCleared = req.query.history_cleared === '1';
  try {
    const { ongoingCounts, ongoingSeries, platformValid } = await loadUpdaterOverview(selectedPlatform);
    const envStatus = getEnvStatus();
    const historyRows = await loadUpdaterHistory(20);

    return res.render('admin-updater', {
      title: 'Updater',
      envStatus,
      selectedRunPlatform,
      selectedIdentityMode,
      selectedMode,
      selectedSyncMode,
      selectedChapterConcurrency,
      canRun: selectedRunPlatform === 'animekita' ? true : envStatus.ok,
      ongoingCounts,
      ongoingSeries,
      selectedPlatform: platformValid ? selectedPlatform : '',
      historyRows,
      historyCleared,
      result: null,
      error: ''
    });
  } catch (err) {
    return res.render('admin-updater', {
      title: 'Updater',
      envStatus: getEnvStatus(),
      selectedRunPlatform,
      selectedIdentityMode,
      selectedMode,
      selectedSyncMode,
      selectedChapterConcurrency,
      canRun: selectedRunPlatform === 'animekita' ? true : getEnvStatus().ok,
      ongoingCounts: [],
      ongoingSeries: [],
      selectedPlatform: '',
      historyRows: [],
      historyCleared: false,
      result: null,
      error: err.message || 'Gagal load ongoing counts'
    });
  }
});

router.post('/updater/run', async (req, res) => {
  const runPlatformRaw = String(req.body.run_platform || 'donghub').trim().toLowerCase();
  const runPlatform = runPlatformRaw === 'animekita' ? 'animekita' : 'donghub';
  const mode = String(req.body.mode || 'dry_run');
  const identityMode = String(req.body.identity_mode || 'source_episode_id');
  const syncModeRaw = String(req.body.sync_mode || 'full_sync').trim().toLowerCase();
  const syncMode = syncModeRaw === 'only_missing_streams' ? 'only_missing_streams' : 'full_sync';
  const limit = toInt(req.body.limit, 20, 1, 300);
  const dryRun = mode !== 'apply';
  const selectedPlatform = String(req.body.selected_platform || '').trim();
  const chapterConcurrency = toInt(req.body.chapter_concurrency, 8, 1, 30);
  const singleSeriesId = toInt(req.body.single_series_id, 0, 1, Number.MAX_SAFE_INTEGER);
  const singleSourceSeriesIdRaw = String(req.body.single_source_series_id || '').trim();
  const singleSourceSeriesId = toInt(req.body.single_source_series_id, 0, 1, Number.MAX_SAFE_INTEGER);
  const isSingleRun = singleSeriesId > 0 || singleSourceSeriesId > 0 || Boolean(singleSourceSeriesIdRaw);
  const effectiveLimit = isSingleRun ? 1 : limit;

  const runUpdaterWithFilters = async (filters = {}) => {
    if (runPlatform === 'animekita') {
      return runAnimekitaOngoingUpdate({
        dryRun,
        identityMode,
        syncMode,
        limit: effectiveLimit,
        chapterConcurrency,
        ...filters
      });
    }
    return runDonghubOngoingUpdate({
      dryRun,
      identityMode,
      syncMode,
      limit: effectiveLimit,
      ...filters
    });
  };

  try {
    const { ongoingCounts, ongoingSeries, platformValid } = await loadUpdaterOverview(selectedPlatform);
    const envStatus = getEnvStatus();

    let result = await runUpdaterWithFilters({
      seriesId: singleSeriesId > 0 ? singleSeriesId : null,
      sourceSeriesId: singleSourceSeriesId > 0 ? singleSourceSeriesId : null,
      sourceSeriesIdRaw: singleSourceSeriesIdRaw
    });

    if (isSingleRun && Number(result && result.total_series_target || 0) === 0) {
      let resolvedSeriesId = singleSeriesId > 0 ? singleSeriesId : 0;
      if (!resolvedSeriesId && singleSourceSeriesIdRaw) {
        const sourceIdNum = Number(singleSourceSeriesIdRaw);
        const params = [runPlatform];
        let sourceClause = 'CAST(source_series_id AS CHAR) = ?';
        if (Number.isFinite(sourceIdNum) && sourceIdNum > 0) {
          sourceClause = '(source_series_id = ? OR CAST(source_series_id AS CHAR) = ?)';
          params.push(sourceIdNum, singleSourceSeriesIdRaw);
        } else {
          params.push(singleSourceSeriesIdRaw);
        }

        const [[row]] = await pool.query(
          `SELECT id
           FROM series
           WHERE LOWER(TRIM(source_platform)) = ?
             AND ${sourceClause}
           ORDER BY id DESC
           LIMIT 1`,
          params
        );
        resolvedSeriesId = row ? Number(row.id) : 0;
      }

      if (resolvedSeriesId > 0) {
        result = await runUpdaterWithFilters({
          seriesId: resolvedSeriesId,
          sourceSeriesId: null,
          sourceSeriesIdRaw: ''
        });
      }

      if (Number(result && result.total_series_target || 0) === 0) {
        throw new Error(`Series tidak ditemukan untuk platform=${runPlatform} dan source_series_id=${singleSourceSeriesIdRaw || singleSourceSeriesId || '-'} (pastikan series sudah ada di tabel series)`);
      }
    }
    await saveUpdaterRunLog({
      adminUser: req.session.adminUser,
      runPlatform,
      mode,
      syncMode,
      identityMode,
      limit: effectiveLimit,
      result,
      error: null
    });
    const historyRows = await loadUpdaterHistory(20);

    return res.render('admin-updater', {
      title: 'Updater',
      envStatus,
      selectedRunPlatform: runPlatform,
      selectedIdentityMode: identityMode === 'chapter_slug' ? 'chapter_slug' : 'source_episode_id',
      selectedMode: mode === 'apply' ? 'apply' : 'dry_run',
      selectedSyncMode: syncMode,
      selectedChapterConcurrency: chapterConcurrency,
      canRun: runPlatform === 'animekita' ? true : envStatus.ok,
      ongoingCounts,
      ongoingSeries,
      selectedPlatform: platformValid ? selectedPlatform : '',
      historyRows,
      historyCleared: false,
      result,
      error: ''
    });
  } catch (err) {
    let ongoingCounts = [];
    let ongoingSeries = [];
    let platformValid = false;
    try {
      const overview = await loadUpdaterOverview(selectedPlatform);
      ongoingCounts = overview.ongoingCounts;
      ongoingSeries = overview.ongoingSeries;
      platformValid = overview.platformValid;
    } catch (_e) {}
    try {
      await saveUpdaterRunLog({
        adminUser: req.session.adminUser,
        runPlatform,
        mode,
        syncMode,
        identityMode,
        limit: effectiveLimit,
        result: null,
        error: err
      });
    } catch (_saveErr) {}
    let historyRows = [];
    try {
      historyRows = await loadUpdaterHistory(20);
    } catch (_e) {}

    return res.render('admin-updater', {
      title: 'Updater',
      envStatus: getEnvStatus(),
      selectedRunPlatform: runPlatform,
      selectedIdentityMode: identityMode === 'chapter_slug' ? 'chapter_slug' : 'source_episode_id',
      selectedMode: mode === 'apply' ? 'apply' : 'dry_run',
      selectedSyncMode: syncMode,
      selectedChapterConcurrency: chapterConcurrency,
      canRun: runPlatform === 'animekita' ? true : getEnvStatus().ok,
      ongoingCounts,
      ongoingSeries,
      selectedPlatform: platformValid ? selectedPlatform : '',
      historyRows,
      historyCleared: false,
      result: null,
      error: err.message || 'Updater gagal dijalankan'
    });
  }
});

router.post('/updater/history/clear', async (_req, res) => {
  try {
    await ensureUpdaterRunTables();
    await pool.query('DELETE FROM updater_run_items');
    await pool.query('DELETE FROM updater_runs');
    return res.redirect('/admin/updater?history_cleared=1');
  } catch (err) {
    return res.redirect(`/admin/updater?error=${encodeURIComponent(err.message || 'Gagal hapus riwayat updater')}`);
  }
});

router.get('/schedule', async (req, res) => {
  const day = normalizeScheduleDay(req.query.day);
  const origin = String(req.query.origin || '').trim().toLowerCase();
  const platform = String(req.query.platform || '').trim();
  const type = String(req.query.type || '').trim();

  try {
    await ensureScheduleTable();

    const where = [];
    const params = [];
    if (day) {
      where.push('sc.day_of_week = ?');
      params.push(day);
    }
    if (origin) {
      where.push('LOWER(COALESCE(s.content_origin, "")) = ?');
      params.push(origin);
    }
    if (platform) {
      where.push('s.source_platform = ?');
      params.push(platform);
    }
    if (type) {
      where.push('s.type = ?');
      params.push(type);
    }
    const whereSql = where.length ? `WHERE ${where.join(' AND ')}` : '';

    const [originOptionsRows] = await pool.query(
      `SELECT DISTINCT LOWER(TRIM(content_origin)) AS value
       FROM series
       WHERE content_origin IS NOT NULL
         AND TRIM(content_origin) <> ''
       ORDER BY value ASC`
    );
    const [platformOptionsRows] = await pool.query(
      `SELECT DISTINCT source_platform AS value
       FROM series
       WHERE source_platform IS NOT NULL
         AND source_platform <> ''
       ORDER BY value ASC`
    );
    const [typeOptionsRows] = await pool.query(
      `SELECT DISTINCT type AS value
       FROM series
       WHERE type IS NOT NULL
         AND type <> ''
       ORDER BY value ASC`
    );

    const [rows] = await pool.query(
      `SELECT
         sc.id,
         sc.series_id,
         sc.day_of_week,
         sc.time_label,
         sc.sort_order,
         sc.is_active,
         s.title,
         COALESCE(sc.series_slug_snapshot, s.series_slug) AS series_slug,
         COALESCE(sc.cover_url_snapshot, s.cover_url) AS cover_url,
         s.source_platform,
         s.content_origin,
         s.type
       FROM schedule_entries sc
       JOIN series s ON s.id = sc.series_id
       ${whereSql}
       ORDER BY FIELD(sc.day_of_week, 'senin','selasa','rabu','kamis','jumat','sabtu','minggu'),
                sc.sort_order ASC,
                sc.time_label ASC,
                sc.id ASC`,
      params
    );

    res.render('admin-schedule', {
      title: 'Schedule Manager',
      days: SCHEDULE_DAYS,
      selectedDay: day,
      selectedOrigin: origin,
      selectedPlatform: platform,
      selectedType: type,
      filterOptions: {
        origin: originOptionsRows.map((x) => String(x.value || '')).filter(Boolean),
        platform: platformOptionsRows.map((x) => String(x.value || '')).filter(Boolean),
        type: typeOptionsRows.map((x) => String(x.value || '')).filter(Boolean)
      },
      rows,
      created: req.query.created === '1',
      deleted: req.query.deleted === '1',
      deletedAll: req.query.deleted_all === '1',
      error: req.query.error ? String(req.query.error) : ''
    });
  } catch (err) {
    res.status(500).send(err.message);
  }
});

router.get('/announcement', async (req, res) => {
  const selectedPlatform = String(req.query.platform || 'android').trim().toLowerCase() || 'android';
  try {
    await ensureAnnouncementTable();

    const [platformRows] = await pool.query(
      'SELECT platform, updated_at FROM announcements ORDER BY platform ASC'
    );
    const platformSet = new Set(platformRows.map((x) => String(x.platform || '').trim().toLowerCase()).filter(Boolean));
    const platformOptions = Array.from(new Set(['android', 'ios', ...platformSet])).filter(Boolean);

    const [[row]] = await pool.query(
      `SELECT platform, is_show, is_maintenance, allow_exit_button, latest_app_version,
              daily_update_text, title, message, type, cta_text, cta_url, updated_at
       FROM announcements
       WHERE platform = ?
       LIMIT 1`,
      [selectedPlatform]
    );

    return res.render('admin-announcement', {
      title: 'Announcement Manager',
      selectedPlatform,
      platformOptions,
      row: row || null,
      saved: req.query.saved === '1',
      error: req.query.error ? String(req.query.error) : ''
    });
  } catch (err) {
    return res.status(500).send(err.message);
  }
});

router.post('/announcement', async (req, res) => {
  const platform = String(req.body.platform || 'android').trim().toLowerCase();
  const isShow = String(req.body.show || '0') === '1' ? 1 : 0;
  const isMaintenance = String(req.body.maintenance || '0') === '1' ? 1 : 0;
  const allowExitButton = String(req.body.allow_exit_button || '1') === '1' ? 1 : 0;
  const latestAppVersion = String(req.body.latest_app_version || '').trim();
  const dailyUpdateText = String(req.body.daily_update_text || '').trim();
  const title = String(req.body.title || '').trim();
  const message = String(req.body.message || '').trim();
  const typeRaw = String(req.body.type || 'info').trim().toLowerCase();
  const type = ['info', 'warning', 'danger', 'success'].includes(typeRaw) ? typeRaw : 'info';
  const ctaText = String(req.body.cta_text || '').trim();
  const ctaUrl = String(req.body.cta_url || '').trim();

  if (!platform) {
    return res.redirect('/admin/announcement?error=platform wajib diisi');
  }

  try {
    await ensureAnnouncementTable();
    await pool.query(
      `INSERT INTO announcements (
         platform, is_show, is_maintenance, allow_exit_button, latest_app_version,
         daily_update_text, title, message, type, cta_text, cta_url
       ) VALUES (?, ?, ?, ?, NULLIF(?, ''), NULLIF(?, ''), NULLIF(?, ''), NULLIF(?, ''), ?, NULLIF(?, ''), NULLIF(?, ''))
       ON DUPLICATE KEY UPDATE
         is_show = VALUES(is_show),
         is_maintenance = VALUES(is_maintenance),
         allow_exit_button = VALUES(allow_exit_button),
         latest_app_version = VALUES(latest_app_version),
         daily_update_text = VALUES(daily_update_text),
         title = VALUES(title),
         message = VALUES(message),
         type = VALUES(type),
         cta_text = VALUES(cta_text),
         cta_url = VALUES(cta_url),
         updated_at = CURRENT_TIMESTAMP`,
      [platform, isShow, isMaintenance, allowExitButton, latestAppVersion, dailyUpdateText, title, message, type, ctaText, ctaUrl]
    );
    return res.redirect(`/admin/announcement?platform=${encodeURIComponent(platform)}&saved=1`);
  } catch (err) {
    return res.redirect(`/admin/announcement?platform=${encodeURIComponent(platform)}&error=${encodeURIComponent(err.message || 'Gagal simpan announcement')}`);
  }
});

router.get('/notifications/send', (_req, res) => {
  const { sendUrl, bearerToken, adminSecret, authMode } = buildNotificationApiConfig();
  return res.render('admin-notifications-send', {
    title: 'Push Notification',
    saved: false,
    error: '',
    result: null,
    payload: {
      type: 'announcement',
      title: '',
      message: '',
      imageUrl: '',
      actionType: '',
      actionValue: '',
      actionLabel: '',
      dedupeKey: '',
      topic: '',
      user_ids_csv: ''
    },
    configStatus: {
      hasSendUrl: Boolean(sendUrl),
      hasAdminSecret: Boolean(adminSecret),
      hasBearerToken: Boolean(bearerToken),
      authMode,
      sendUrl
    }
  });
});

router.post('/notifications/send', async (req, res) => {
  const input = {
    type: String(req.body.type || 'announcement').trim().toLowerCase(),
    title: String(req.body.title || '').trim(),
    message: String(req.body.message || '').trim(),
    imageUrl: String(req.body.imageUrl || '').trim(),
    actionType: String(req.body.actionType || '').trim(),
    actionValue: String(req.body.actionValue || '').trim(),
    actionLabel: String(req.body.actionLabel || '').trim(),
    dedupeKey: String(req.body.dedupeKey || '').trim(),
    topic: String(req.body.topic || '').trim(),
    user_ids_csv: String(req.body.user_ids_csv || '').trim(),
    idempotencyKey: String(req.body.idempotencyKey || '').trim()
  };

  const { sendUrl, bearerToken, adminSecret, authMode } = buildNotificationApiConfig();
  if (!sendUrl || (!adminSecret && !bearerToken)) {
    return res.render('admin-notifications-send', {
      title: 'Push Notification',
      saved: false,
      error: 'Config belum lengkap. Wajib isi PANEL_API_BASE_URL/NOTIFICATION_SEND_URL dan salah satu auth: NOTIFICATIONS_ADMIN_SECRET atau NOTIFICATION_BEARER_TOKEN',
      result: null,
      payload: input,
      configStatus: {
        hasSendUrl: Boolean(sendUrl),
        hasAdminSecret: Boolean(adminSecret),
        hasBearerToken: Boolean(bearerToken),
        authMode,
        sendUrl
      }
    });
  }

  if (!input.title || !input.message) {
    return res.render('admin-notifications-send', {
      title: 'Push Notification',
      saved: false,
      error: 'Field wajib: title dan message',
      result: null,
      payload: input,
      configStatus: {
        hasSendUrl: Boolean(sendUrl),
        hasAdminSecret: Boolean(adminSecret),
        hasBearerToken: Boolean(bearerToken),
        authMode,
        sendUrl
      }
    });
  }

  const userIds = parseUserIdsCsv(input.user_ids_csv);
  if (!input.topic && !userIds.length) {
    return res.render('admin-notifications-send', {
      title: 'Push Notification',
      saved: false,
      error: 'Isi salah satu target: topic atau user_ids_csv',
      result: null,
      payload: input,
      configStatus: {
        hasSendUrl: Boolean(sendUrl),
        hasAdminSecret: Boolean(adminSecret),
        hasBearerToken: Boolean(bearerToken),
        authMode,
        sendUrl
      }
    });
  }

  const body = {
    type: input.type || 'announcement',
    title: input.title,
    message: input.message
  };
  if (input.imageUrl) body.imageUrl = input.imageUrl;
  if (input.actionType) body.actionType = input.actionType;
  if (input.actionValue) body.actionValue = input.actionValue;
  if (input.actionLabel) body.actionLabel = input.actionLabel;
  if (input.dedupeKey) body.dedupeKey = input.dedupeKey;
  if (input.topic) body.topic = input.topic;
  if (userIds.length) body.userIds = userIds;

  const idempotencyKey = input.idempotencyKey || generateIdempotencyKey();

  try {
    const headers = {
      'Idempotency-Key': idempotencyKey
    };
    if (adminSecret) {
      headers['x-admin-secret'] = adminSecret;
    } else {
      headers.Authorization = `Bearer ${bearerToken}`;
    }

    const response = await axios.post(sendUrl, body, {
      timeout: 25000,
      headers
    });

    return res.render('admin-notifications-send', {
      title: 'Push Notification',
      saved: true,
      error: '',
      result: {
        status: Number(response.status || 200),
        data: response.data || null,
        idempotencyKey
      },
      payload: input,
      configStatus: {
        hasSendUrl: Boolean(sendUrl),
        hasAdminSecret: Boolean(adminSecret),
        hasBearerToken: Boolean(bearerToken),
        authMode,
        sendUrl
      }
    });
  } catch (err) {
    const status = err && err.response ? Number(err.response.status || 500) : 500;
    const data = err && err.response ? err.response.data : null;
    const message = err && err.message ? err.message : 'Gagal kirim notification';

    return res.render('admin-notifications-send', {
      title: 'Push Notification',
      saved: false,
      error: `${message}${data ? ` | response: ${JSON.stringify(data).slice(0, 500)}` : ''}`,
      result: {
        status,
        data,
        idempotencyKey
      },
      payload: input,
      configStatus: {
        hasSendUrl: Boolean(sendUrl),
        hasAdminSecret: Boolean(adminSecret),
        hasBearerToken: Boolean(bearerToken),
        authMode,
        sendUrl
      }
    });
  }
});

router.get('/topanime', async (req, res) => {
  const q = String(req.query.q || '').trim();
  const active = String(req.query.active || '').trim();

  try {
    await ensureTopAnimeTable();

    const where = [];
    const params = [];

    if (q) {
      const like = `%${escapeLike(q)}%`;
      where.push('(s.title LIKE ? OR s.series_slug LIKE ?)');
      params.push(like, like);
    }
    if (active === '1' || active === '0') {
      where.push('ta.is_active = ?');
      params.push(Number(active));
    }
    const whereSql = where.length ? `WHERE ${where.join(' AND ')}` : '';

    const [rows] = await pool.query(
      `SELECT
         ta.id,
         ta.series_id,
         ta.sort_order,
         ta.is_active,
         ta.created_at,
         ta.updated_at,
         s.title,
         s.series_slug,
         s.source_platform,
         s.source_series_id,
         s.status,
         s.rating
       FROM ${TABLE_TOP_ANIME} ta
       JOIN series s ON s.id = ta.series_id
       ${whereSql}
       ORDER BY ta.sort_order ASC, ta.id ASC, s.updated_at DESC`,
      params
    );

    return res.render('admin-topanime', {
      title: 'Top Anime Manager',
      rows,
      q,
      active,
      created: req.query.created === '1',
      updated: req.query.updated === '1',
      deleted: req.query.deleted === '1',
      error: req.query.error ? String(req.query.error) : ''
    });
  } catch (err) {
    return res.status(500).send(err.message);
  }
});

router.get('/topanime/series-search', async (req, res) => {
  const q = String(req.query.q || '').trim();
  const limit = toInt(req.query.limit, 10, 1, 30);
  try {
    if (!q || q.length < 2) {
      return res.json({ data: [] });
    }
    const like = `%${escapeLike(q)}%`;
    const [rows] = await pool.query(
      `SELECT
         id,
         title,
         series_slug,
         source_platform,
         source_series_id,
         type,
         content_origin,
         status,
         rating
       FROM series
       WHERE (title LIKE ? OR series_slug LIKE ?)
       ORDER BY title ASC
       LIMIT ?`,
      [like, like, limit]
    );
    return res.json({ data: rows });
  } catch (err) {
    return res.status(500).json({ error: err.message });
  }
});

router.post('/topanime/create', async (req, res) => {
  const seriesId = toInt(req.body.series_id, 0, 1, Number.MAX_SAFE_INTEGER);
  const sortOrder = toInt(req.body.sort_order, 0, 0, 1000000);
  const isActive = String(req.body.is_active || '1') === '1' ? 1 : 0;

  if (!seriesId) {
    return res.redirect('/admin/topanime?error=series_id wajib valid');
  }

  try {
    await ensureTopAnimeTable();
    const [[series]] = await pool.query(
      `SELECT id
       FROM series
       WHERE id = ?
       LIMIT 1`,
      [seriesId]
    );
    if (!series) {
      return res.redirect('/admin/topanime?error=Series tidak ditemukan');
    }

    await pool.query(
      `INSERT INTO ${TABLE_TOP_ANIME} (series_id, sort_order, is_active)
       VALUES (?, ?, ?)`,
      [seriesId, sortOrder, isActive]
    );

    return res.redirect('/admin/topanime?created=1');
  } catch (err) {
    if (err && err.code === 'ER_DUP_ENTRY') {
      return res.redirect('/admin/topanime?error=Series sudah ada di daftar top anime');
    }
    return res.redirect(`/admin/topanime?error=${encodeURIComponent(err.message || 'Gagal tambah top anime')}`);
  }
});

router.post('/topanime/:id/update', async (req, res) => {
  const id = toInt(req.params.id, 0, 1, Number.MAX_SAFE_INTEGER);
  const sortOrder = toInt(req.body.sort_order, 0, 0, 1000000);
  const isActive = String(req.body.is_active || '1') === '1' ? 1 : 0;
  try {
    await ensureTopAnimeTable();
    await pool.query(
      `UPDATE ${TABLE_TOP_ANIME}
       SET sort_order = ?,
           is_active = ?,
           updated_at = CURRENT_TIMESTAMP
       WHERE id = ?
       LIMIT 1`,
      [sortOrder, isActive, id]
    );
    return res.redirect('/admin/topanime?updated=1');
  } catch (err) {
    return res.redirect(`/admin/topanime?error=${encodeURIComponent(err.message || 'Gagal update top anime')}`);
  }
});

router.post('/topanime/:id/delete', async (req, res) => {
  const id = toInt(req.params.id, 0, 1, Number.MAX_SAFE_INTEGER);
  try {
    await ensureTopAnimeTable();
    await pool.query(`DELETE FROM ${TABLE_TOP_ANIME} WHERE id = ? LIMIT 1`, [id]);
    return res.redirect('/admin/topanime?deleted=1');
  } catch (err) {
    return res.redirect(`/admin/topanime?error=${encodeURIComponent(err.message || 'Gagal hapus top anime')}`);
  }
});

router.get('/schedule/series-search', async (req, res) => {
  const q = String(req.query.q || '').trim();
  const limit = toInt(req.query.limit, 10, 1, 30);
  const origin = String(req.query.origin || '').trim().toLowerCase();
  const platform = String(req.query.platform || '').trim();
  const type = String(req.query.type || '').trim();

  try {
    if (!q || q.length < 2) {
      return res.json({ data: [] });
    }
    const like = `%${escapeLike(q)}%`;
    const where = ['(title LIKE ? OR series_slug LIKE ?)'];
    const params = [like, like];
    if (origin) {
      where.push('LOWER(COALESCE(content_origin, "")) = ?');
      params.push(origin);
    }
    if (platform) {
      where.push('source_platform = ?');
      params.push(platform);
    }
    if (type) {
      where.push('type = ?');
      params.push(type);
    }
    const [rows] = await pool.query(
      `SELECT id, title, series_slug, source_platform, content_origin, type
       FROM series
       WHERE ${where.join(' AND ')}
       ORDER BY title ASC
       LIMIT ?`,
      [...params, limit]
    );
    return res.json({ data: rows });
  } catch (err) {
    return res.status(500).json({ error: err.message });
  }
});

router.post('/schedule/create', async (req, res) => {
  const seriesId = toInt(req.body.series_id, 0, 1, Number.MAX_SAFE_INTEGER);
  const day = normalizeScheduleDay(req.body.day_of_week);
  const timeLabel = String(req.body.time_label || '').trim();
  const sortOrder = toInt(req.body.sort_order, 0, 0, 1000000);
  const isActive = String(req.body.is_active || '1') === '1' ? 1 : 0;

  if (!seriesId || !day) {
    return res.redirect('/admin/schedule?error=series_id dan day_of_week wajib valid');
  }

  try {
    await ensureScheduleTable();

    const [[series]] = await pool.query('SELECT id, series_slug, cover_url FROM series WHERE id = ? LIMIT 1', [seriesId]);
    if (!series) {
      return res.redirect('/admin/schedule?error=Series tidak ditemukan');
    }

    await pool.query(
      `INSERT INTO schedule_entries (
         series_id, day_of_week, series_slug_snapshot, cover_url_snapshot, time_label, sort_order, is_active
       ) VALUES (?, ?, ?, ?, NULLIF(?, ''), ?, ?)`,
      [seriesId, day, series.series_slug, series.cover_url, timeLabel, sortOrder, isActive]
    );

    return res.redirect('/admin/schedule?created=1');
  } catch (err) {
    if (err && err.code === 'ER_DUP_ENTRY') {
      return res.redirect('/admin/schedule?error=Series sudah ada di hari tersebut');
    }
    return res.redirect(`/admin/schedule?error=${encodeURIComponent(err.message || 'Gagal membuat jadwal')}`);
  }
});

router.get('/schedule/:id/edit', async (req, res) => {
  const id = toInt(req.params.id, 0, 1, Number.MAX_SAFE_INTEGER);
  try {
    await ensureScheduleTable();
    const [[row]] = await pool.query(
      `SELECT
         sc.id,
         sc.series_id,
         sc.day_of_week,
         sc.time_label,
         sc.sort_order,
         sc.is_active,
         s.title,
         s.source_platform,
         COALESCE(sc.series_slug_snapshot, s.series_slug) AS series_slug
       FROM schedule_entries sc
       JOIN series s ON s.id = sc.series_id
       WHERE sc.id = ?
       LIMIT 1`,
      [id]
    );

    if (!row) return res.status(404).send('Jadwal tidak ditemukan');

    res.render('admin-schedule-edit', {
      title: `Edit Schedule #${id}`,
      days: SCHEDULE_DAYS,
      row,
      error: req.query.error ? String(req.query.error) : ''
    });
  } catch (err) {
    res.status(500).send(err.message);
  }
});

router.post('/schedule/:id/edit', async (req, res) => {
  const id = toInt(req.params.id, 0, 1, Number.MAX_SAFE_INTEGER);
  const seriesId = toInt(req.body.series_id, 0, 1, Number.MAX_SAFE_INTEGER);
  const day = normalizeScheduleDay(req.body.day_of_week);
  const timeLabel = String(req.body.time_label || '').trim();
  const sortOrder = toInt(req.body.sort_order, 0, 0, 1000000);
  const isActive = String(req.body.is_active || '1') === '1' ? 1 : 0;

  if (!seriesId || !day) {
    return res.redirect(`/admin/schedule/${id}/edit?error=series_id dan day_of_week wajib valid`);
  }

  try {
    await ensureScheduleTable();
    const [[series]] = await pool.query('SELECT id, series_slug, cover_url FROM series WHERE id = ? LIMIT 1', [seriesId]);
    if (!series) {
      return res.redirect(`/admin/schedule/${id}/edit?error=Series tidak ditemukan`);
    }
    await pool.query(
      `UPDATE schedule_entries
       SET series_id = ?,
           day_of_week = ?,
           series_slug_snapshot = ?,
           cover_url_snapshot = ?,
           time_label = NULLIF(?, ''),
           sort_order = ?,
           is_active = ?,
           updated_at = CURRENT_TIMESTAMP
       WHERE id = ?
       LIMIT 1`,
      [seriesId, day, series.series_slug, series.cover_url, timeLabel, sortOrder, isActive, id]
    );
    return res.redirect('/admin/schedule');
  } catch (err) {
    if (err && err.code === 'ER_DUP_ENTRY') {
      return res.redirect(`/admin/schedule/${id}/edit?error=Series sudah ada di hari tersebut`);
    }
    return res.redirect(`/admin/schedule/${id}/edit?error=${encodeURIComponent(err.message || 'Gagal update jadwal')}`);
  }
});

router.post('/schedule/:id/delete', async (req, res) => {
  const id = toInt(req.params.id, 0, 1, Number.MAX_SAFE_INTEGER);
  try {
    await ensureScheduleTable();
    await pool.query('DELETE FROM schedule_entries WHERE id = ? LIMIT 1', [id]);
    return res.redirect('/admin/schedule?deleted=1');
  } catch (err) {
    return res.redirect(`/admin/schedule?error=${encodeURIComponent(err.message || 'Gagal hapus jadwal')}`);
  }
});

router.post('/schedule/delete-all', async (_req, res) => {
  try {
    await ensureScheduleTable();
    await pool.query('DELETE FROM schedule_entries');
    return res.redirect('/admin/schedule?deleted_all=1');
  } catch (err) {
    return res.redirect(`/admin/schedule?error=${encodeURIComponent(err.message || 'Gagal hapus semua jadwal')}`);
  }
});

router.get('/', async (req, res) => {
  try {
    const flashError = String(req.query.error || '').trim();

    const [[seriesRow]] = await pool.query('SELECT COUNT(*) AS c FROM series');
    const [[episodesRow]] = await pool.query('SELECT COUNT(*) AS c FROM episodes');
    const [[streamsRow]] = await pool.query('SELECT COUNT(*) AS c FROM episode_streams');
    const [[seriesNoEpisodeRow]] = await pool.query(
      'SELECT COUNT(*) AS c FROM series s WHERE NOT EXISTS (SELECT 1 FROM episodes e WHERE e.series_id = s.id)'
    );

    const [originRows] = await pool.query(
      'SELECT content_origin, COUNT(*) AS total FROM series GROUP BY content_origin ORDER BY total DESC'
    );
    const [platformRows] = await pool.query(
      'SELECT source_platform, COUNT(*) AS total FROM series GROUP BY source_platform ORDER BY total DESC'
    );
    const [typeRows] = await pool.query(
      'SELECT COALESCE(type, "-") AS type_label, COUNT(*) AS total FROM series GROUP BY type_label ORDER BY total DESC LIMIT 10'
    );

    const [latestSeries] = await pool.query(
      `SELECT
         s.id,
         s.source_platform,
         s.content_origin,
         s.type,
         s.title,
         s.series_slug,
         s.updated_at,
         (SELECT COUNT(*) FROM episodes e WHERE e.series_id = s.id) AS episode_count
       FROM series s
       ORDER BY s.updated_at DESC
       LIMIT 12`
    );

    const [[episodesNoStreamRow]] = await pool.query(
      `SELECT COUNT(*) AS c
       FROM episodes e
       WHERE NOT EXISTS (SELECT 1 FROM episode_streams es WHERE es.episode_id = e.id)`
    );

    const [[seriesWithNoStreamRow]] = await pool.query(
      `SELECT COUNT(DISTINCT e.series_id) AS c
       FROM episodes e
       WHERE NOT EXISTS (SELECT 1 FROM episode_streams es WHERE es.episode_id = e.id)`
    );

    const [seriesMissingStreams] = await pool.query(
      `SELECT
         s.id,
         s.title,
         s.source_platform,
         COUNT(e.id) AS total_episodes,
         SUM(
           CASE
             WHEN NOT EXISTS (
               SELECT 1 FROM episode_streams es WHERE es.episode_id = e.id
             ) THEN 1 ELSE 0
           END
         ) AS missing_stream_episodes
       FROM series s
       JOIN episodes e ON e.series_id = s.id
       GROUP BY s.id, s.title, s.source_platform
       HAVING missing_stream_episodes > 0
       ORDER BY missing_stream_episodes DESC, s.id ASC
       LIMIT 25`
    );

    res.render('admin-dashboard', {
      title: 'Admin Dashboard',
      adminUser: req.session.adminUser,
      stats: {
        series: seriesRow.c,
        episodes: episodesRow.c,
        streams: streamsRow.c,
        seriesNoEpisode: seriesNoEpisodeRow.c,
        episodesNoStream: episodesNoStreamRow.c,
        seriesWithNoStream: seriesWithNoStreamRow.c
      },
      originRows,
      platformRows,
      typeRows,
      latestSeries,
      seriesMissingStreams,
      flashError
    });
  } catch (err) {
    res.status(500).send(err.message);
  }
});

router.get('/users/premium', async (req, res) => {
  const page = toInt(req.query.page, 1, 1, 100000);
  const limit = 30;
  const offset = (page - 1) * limit;
  const q = String(req.query.q || '').trim();
  const role = String(req.query.role || '').trim().toLowerCase();
  const activeOnly = String(req.query.active_only || '1').trim() === '1';
  const flashError = String(req.query.error || '').trim();
  const flashSuccess = String(req.query.success || '').trim();

  try {
    const userTableExists = await checkUserTableExists('users');
    if (!userTableExists) {
      return res.render('admin-users-premium', {
        title: 'Manage Premium',
        rows: [],
        page,
        total: 0,
        totalPages: 1,
        q,
        role,
        activeOnly,
        roleOptions: [],
        tableMissing: true,
        error: flashError,
        success: flashSuccess
      });
    }

    const [roleRows] = await userPool.query(
      `SELECT LOWER(role) AS role_value
       FROM users
       WHERE role IS NOT NULL AND role <> ''
       GROUP BY role_value
       ORDER BY role_value ASC`
    );

    const where = [];
    const params = [];
    if (role) {
      where.push('LOWER(u.role) = ?');
      params.push(role);
    } else {
      where.push('u.premium_expires_at IS NOT NULL');
      if (activeOnly) where.push('u.premium_expires_at > NOW()');
    }
    if (q) {
      const like = `%${escapeLike(q)}%`;
      where.push('(u.name LIKE ? OR u.email LIKE ? OR CAST(u.id AS CHAR) LIKE ?)');
      params.push(like, like, like);
    }
    const whereSql = where.length ? `WHERE ${where.join(' AND ')}` : '';

    const [[countRow]] = await userPool.query(
      `SELECT COUNT(*) AS total
       FROM users u
       ${whereSql}`,
      params
    );
    const total = Number(countRow && countRow.total ? countRow.total : 0);
    const totalPages = Math.max(1, Math.ceil(total / limit));

    const [rowsRaw] = await userPool.query(
      `SELECT
         u.id,
         u.email,
         u.name,
         u.role,
         u.is_active,
         u.is_private,
         u.premium_expires_at,
         u.level,
         u.xp_total,
         u.last_seen_at,
         (u.premium_expires_at IS NOT NULL AND u.premium_expires_at > NOW()) AS premium_active
       FROM users u
       ${whereSql}
       ORDER BY u.premium_expires_at DESC, u.id DESC
       LIMIT ? OFFSET ?`,
      [...params, limit, offset]
    );

    const rows = rowsRaw.map((r) => ({
      ...r,
      premium_expires_at_text: toSafeTimestampString(r.premium_expires_at),
      last_seen_at_text: toSafeTimestampString(r.last_seen_at)
    }));

    return res.render('admin-users-premium', {
      title: 'Manage Premium',
      rows,
      page,
      total,
      totalPages,
      q,
      role,
      activeOnly,
      roleOptions: roleRows.map((x) => String(x.role_value || '')).filter(Boolean),
      tableMissing: false,
      error: flashError,
      success: flashSuccess
    });
  } catch (err) {
    return res.status(500).send(err.message);
  }
});

router.post('/users/:id/premium/add', async (req, res) => {
  const userId = toInt(req.params.id, 0, 1, Number.MAX_SAFE_INTEGER);
  const days = toInt(req.body.days, 0, 1, 3650);
  const returnTo = sanitizePremiumReturnPath(req.body.return_to);

  if (!userId) return res.redirect(`${returnTo}?error=${encodeURIComponent('userId tidak valid')}`);
  if (!days) return res.redirect(`${returnTo}?error=${encodeURIComponent('days harus angka 1-3650')}`);

  let conn;
  try {
    const userTableExists = await checkUserTableExists('users');
    if (!userTableExists) {
      return res.redirect(`${returnTo}?error=${encodeURIComponent('Table users tidak ditemukan di user DB')}`);
    }

    conn = await userPool.getConnection();
    await conn.beginTransaction();

    const [[target]] = await conn.query(
      'SELECT id, premium_expires_at FROM users WHERE id = ? LIMIT 1 FOR UPDATE',
      [userId]
    );
    if (!target) {
      await conn.rollback();
      return res.redirect(`${returnTo}?error=${encodeURIComponent(`User #${userId} tidak ditemukan`)}`);
    }

    const nowMs = Date.now();
    const currentExpiryMs = target.premium_expires_at ? new Date(target.premium_expires_at).getTime() : 0;
    const baseMs = currentExpiryMs > nowMs ? currentExpiryMs : nowMs;
    const nextExpiry = new Date(baseMs + (days * 24 * 60 * 60 * 1000));

    await conn.query(
      'UPDATE users SET premium_expires_at = ? WHERE id = ? LIMIT 1',
      [nextExpiry, userId]
    );

    await conn.commit();
    return res.redirect(`${returnTo}?success=${encodeURIComponent(`Premium user #${userId} +${days} hari`)}`);
  } catch (err) {
    if (conn) await conn.rollback();
    return res.redirect(`${returnTo}?error=${encodeURIComponent(err.message || 'Gagal update premium')}`);
  } finally {
    if (conn) conn.release();
  }
});

router.post('/users/:id/premium/revoke', async (req, res) => {
  const userId = toInt(req.params.id, 0, 1, Number.MAX_SAFE_INTEGER);
  const returnTo = sanitizePremiumReturnPath(req.body.return_to);

  if (!userId) return res.redirect(`${returnTo}?error=${encodeURIComponent('userId tidak valid')}`);

  try {
    const userTableExists = await checkUserTableExists('users');
    if (!userTableExists) {
      return res.redirect(`${returnTo}?error=${encodeURIComponent('Table users tidak ditemukan di user DB')}`);
    }

    const [result] = await userPool.query(
      'UPDATE users SET premium_expires_at = NULL WHERE id = ? LIMIT 1',
      [userId]
    );
    if (!result.affectedRows) {
      return res.redirect(`${returnTo}?error=${encodeURIComponent(`User #${userId} tidak ditemukan`)}`);
    }
    return res.redirect(`${returnTo}?success=${encodeURIComponent(`Premium user #${userId} direvoke`)}`);
  } catch (err) {
    return res.redirect(`${returnTo}?error=${encodeURIComponent(err.message || 'Gagal revoke premium')}`);
  }
});

router.get('/leaderboard-xp', async (req, res) => {
  const page = toInt(req.query.page, 1, 1, 100000);
  const limit = toInt(req.query.limit, 50, 1, 100);
  const offset = (page - 1) * limit;
  const q = String(req.query.q || '').trim();
  const window = buildXpLeaderboardWindow(req.query.period);

  try {
    const userTableExists = await checkUserTableExists('users');
    const xpDailyExists = await checkUserTableExists('user_xp_daily');
    if (!userTableExists || !xpDailyExists) {
      return res.render('admin-leaderboard-xp', {
        title: 'Leaderboard XP',
        rows: [],
        page,
        total: 0,
        totalPages: 1,
        q,
        limit,
        period: window.period,
        periodLabel: window.label,
        windowStartText: '',
        windowEndText: '',
        tableMissing: true,
        error: (!userTableExists ? 'users ' : '') + (!xpDailyExists ? 'user_xp_daily' : '')
      });
    }

    const where = ['u.is_private = 0'];
    const whereParams = [];
    if (q) {
      const like = `%${escapeLike(q)}%`;
      where.push('(u.name LIKE ? OR u.email LIKE ? OR CAST(u.id AS CHAR) LIKE ?)');
      whereParams.push(like, like, like);
    }
    const whereSql = `WHERE ${where.join(' AND ')}`;

    const { rowsRaw, total } = await queryXpDailyLeaderboard({
      whereSql,
      whereParams,
      limit,
      offset,
      windowStart: window.windowStart,
      windowEnd: window.windowEnd
    });
    const totalPages = Math.max(1, Math.ceil(total / limit));
    const rows = rowsRaw.map((r, idx) => ({
      ...r,
      rank_no: offset + idx + 1,
      premium_expires_at_text: toSafeTimestampString(r.user_premium_expires_at)
    }));

    return res.render('admin-leaderboard-xp', {
      title: 'Leaderboard XP',
      rows,
      page,
      total,
      totalPages,
      q,
      limit,
      period: window.period,
      periodLabel: window.label,
      windowStartText: toSafeTimestampString(window.windowStart),
      windowEndText: toSafeTimestampString(window.windowEnd),
      tableMissing: false,
      error: ''
    });
  } catch (err) {
    return res.status(500).send(err.message);
  }
});

router.get('/users', async (req, res) => {
  const page = toInt(req.query.page, 1, 1, 100000);
  const limit = 30;
  const offset = (page - 1) * limit;
  const q = String(req.query.q || '').trim();
  const role = String(req.query.role || '').trim().toLowerCase();
  const premium = String(req.query.premium || '').trim().toLowerCase();
  const active = String(req.query.active || '').trim().toLowerCase();
  const flashError = String(req.query.error || '').trim();
  const flashSuccess = String(req.query.success || '').trim();

  try {
    const exists = await checkUserTableExists('users');
    if (!exists) {
      return res.render('admin-users', {
        title: 'User Dashboard',
        rows: [],
        page,
        total: 0,
        totalPages: 1,
        q,
        role,
        premium,
        active,
        roleOptions: [],
        tableMissing: true,
        error: flashError,
        success: flashSuccess
      });
    }

    const [roleRows] = await userPool.query(
      `SELECT LOWER(role) AS role_value
       FROM users
       WHERE role IS NOT NULL AND role <> ''
       GROUP BY role_value
       ORDER BY role_value ASC`
    );

    const where = [];
    const params = [];

    if (q) {
      const like = `%${escapeLike(q)}%`;
      where.push('(CAST(u.id AS CHAR) LIKE ? OR u.name LIKE ? OR u.email LIKE ? OR u.firebase_uid LIKE ?)');
      params.push(like, like, like, like);
    }
    if (role) {
      where.push('LOWER(u.role) = ?');
      params.push(role);
    }
    if (premium === 'active') {
      where.push('u.premium_expires_at IS NOT NULL AND u.premium_expires_at > NOW()');
    } else if (premium === 'expired') {
      where.push('u.premium_expires_at IS NOT NULL AND u.premium_expires_at <= NOW()');
    } else if (premium === 'none') {
      where.push('u.premium_expires_at IS NULL');
    }
    if (active === 'yes') {
      where.push('u.is_active = 1');
    } else if (active === 'no') {
      where.push('u.is_active = 0');
    }

    const whereSql = where.length ? `WHERE ${where.join(' AND ')}` : '';

    const [[countRow]] = await userPool.query(
      `SELECT COUNT(*) AS total
       FROM users u
       ${whereSql}`,
      params
    );
    const total = Number(countRow && countRow.total ? countRow.total : 0);
    const totalPages = Math.max(1, Math.ceil(total / limit));

    const [rowsRaw] = await userPool.query(
      `SELECT
         u.id,
         u.firebase_uid,
         u.email,
         u.name,
         u.role,
         u.is_active,
         u.is_private,
         u.premium_expires_at,
         u.level,
         u.xp_total,
         u.created_at,
         u.last_seen_at,
         CASE
           WHEN u.premium_expires_at IS NULL THEN 'none'
           WHEN u.premium_expires_at <= NOW() THEN 'expired'
           ELSE 'active'
         END AS premium_kind
       FROM users u
       ${whereSql}
       ORDER BY u.id DESC
       LIMIT ? OFFSET ?`,
      [...params, limit, offset]
    );

    const rows = rowsRaw.map((r) => ({
      ...r,
      premium_expires_at_text: toSafeTimestampString(r.premium_expires_at),
      created_at_text: toSafeTimestampString(r.created_at),
      last_seen_at_text: toSafeTimestampString(r.last_seen_at)
    }));

    return res.render('admin-users', {
      title: 'User Dashboard',
      rows,
      page,
      total,
      totalPages,
      q,
      role,
      premium,
      active,
      roleOptions: roleRows.map((x) => String(x.role_value || '')).filter(Boolean),
      tableMissing: false,
      error: flashError,
      success: flashSuccess
    });
  } catch (err) {
    return res.status(500).send(err.message);
  }
});

router.post('/users/:id(\\d+)/reset-xp', async (req, res) => {
  const userId = toInt(req.params.id, 0, 1, Number.MAX_SAFE_INTEGER);
  const returnTo = sanitizeUsersReturnPath(req.body.return_to, userId);
  if (!userId) return res.redirect(`${returnTo}?error=${encodeURIComponent('userId tidak valid')}`);

  let conn;
  try {
    const usersExists = await checkUserTableExists('users');
    if (!usersExists) {
      return res.redirect(`${returnTo}?error=${encodeURIComponent('Table users tidak ditemukan di user DB')}`);
    }
    const xpLedgerExists = await checkUserTableExists('user_xp_ledger');
    const xpStateExists = await checkUserTableExists('user_watch_xp_state');
    const xpDailyExists = await checkUserTableExists('user_xp_daily');
    const watchProgressExists = await checkUserTableExists('watch_progress');

    conn = await userPool.getConnection();
    await conn.beginTransaction();

    const [[user]] = await conn.query(
      'SELECT id FROM users WHERE id = ? LIMIT 1 FOR UPDATE',
      [userId]
    );
    if (!user) {
      await conn.rollback();
      return res.redirect(`${returnTo}?error=${encodeURIComponent(`User #${userId} tidak ditemukan`)}`);
    }

    if (xpLedgerExists) {
      await conn.query('DELETE FROM user_xp_ledger WHERE user_id = ?', [userId]);
    }
    if (xpStateExists) {
      await conn.query('DELETE FROM user_watch_xp_state WHERE user_id = ?', [userId]);
    }
    if (xpDailyExists) {
      await conn.query('DELETE FROM user_xp_daily WHERE user_id = ?', [userId]);
    }
    if (watchProgressExists) {
      await conn.query('DELETE FROM watch_progress WHERE user_id = ?', [userId]);
    }
    try {
      await conn.query(
        'UPDATE users SET xp_total = 0, level = 1, xp_updated_at = NOW() WHERE id = ? LIMIT 1',
        [userId]
      );
    } catch (err) {
      if (!err || err.code !== 'ER_BAD_FIELD_ERROR') throw err;
      await conn.query(
        'UPDATE users SET xp_total = 0, level = 1 WHERE id = ? LIMIT 1',
        [userId]
      );
    }

    await conn.commit();
    return res.redirect(`${returnTo}?success=${encodeURIComponent(`User #${userId} level/xp direset + history dihapus`)}`);
  } catch (err) {
    if (conn) await conn.rollback();
    return res.redirect(`${returnTo}?error=${encodeURIComponent(err.message || 'Gagal reset level/xp')}`);
  } finally {
    if (conn) conn.release();
  }
});

router.post('/users/:id(\\d+)/set-level-xp', async (req, res) => {
  const userId = toInt(req.params.id, 0, 1, Number.MAX_SAFE_INTEGER);
  const returnTo = sanitizeUsersReturnPath(req.body.return_to, userId);
  const level = toInt(req.body.level, 0, 1, 1000000);
  const xpTotal = toInt(req.body.xp_total, -1, 0, Number.MAX_SAFE_INTEGER);

  if (!userId) return res.redirect(`${returnTo}?error=${encodeURIComponent('userId tidak valid')}`);
  if (!level) return res.redirect(`${returnTo}?error=${encodeURIComponent('level harus angka >= 1')}`);
  if (xpTotal < 0) return res.redirect(`${returnTo}?error=${encodeURIComponent('xp_total harus angka >= 0')}`);

  try {
    const usersExists = await checkUserTableExists('users');
    if (!usersExists) {
      return res.redirect(`${returnTo}?error=${encodeURIComponent('Table users tidak ditemukan di user DB')}`);
    }

    try {
      const [result] = await userPool.query(
        'UPDATE users SET level = ?, xp_total = ?, xp_updated_at = NOW() WHERE id = ? LIMIT 1',
        [level, xpTotal, userId]
      );
      if (!result.affectedRows) {
        return res.redirect(`${returnTo}?error=${encodeURIComponent(`User #${userId} tidak ditemukan`)}`);
      }
    } catch (err) {
      if (!err || err.code !== 'ER_BAD_FIELD_ERROR') throw err;
      const [result] = await userPool.query(
        'UPDATE users SET level = ?, xp_total = ? WHERE id = ? LIMIT 1',
        [level, xpTotal, userId]
      );
      if (!result.affectedRows) {
        return res.redirect(`${returnTo}?error=${encodeURIComponent(`User #${userId} tidak ditemukan`)}`);
      }
    }

    return res.redirect(`${returnTo}?success=${encodeURIComponent(`User #${userId} level/xp diupdate`)}`);
  } catch (err) {
    return res.redirect(`${returnTo}?error=${encodeURIComponent(err.message || 'Gagal update level/xp')}`);
  }
});

router.post('/users/:id(\\d+)/delete-comments', async (req, res) => {
  const userId = toInt(req.params.id, 0, 1, Number.MAX_SAFE_INTEGER);
  const returnTo = sanitizeUsersReturnPath(req.body.return_to, userId);
  if (!userId) return res.redirect(`${returnTo}?error=${encodeURIComponent('userId tidak valid')}`);

  try {
    const usersExists = await checkUserTableExists('users');
    if (!usersExists) {
      return res.redirect(`${returnTo}?error=${encodeURIComponent('Table users tidak ditemukan di user DB')}`);
    }
    const commentTableExists = await checkUserTableExists('episode_comments');
    if (!commentTableExists) {
      return res.redirect(`${returnTo}?error=${encodeURIComponent('Table episode_comments tidak ditemukan di user DB')}`);
    }

    const [[user]] = await userPool.query(
      'SELECT id FROM users WHERE id = ? LIMIT 1',
      [userId]
    );
    if (!user) {
      return res.redirect(`${returnTo}?error=${encodeURIComponent(`User #${userId} tidak ditemukan`)}`);
    }

    const [result] = await userPool.query(
      'DELETE FROM episode_comments WHERE user_id = ?',
      [userId]
    );
    const deletedCount = Number(result && result.affectedRows ? result.affectedRows : 0);

    return res.redirect(`${returnTo}?success=${encodeURIComponent(`User #${userId} comment dihapus (${deletedCount})`)}`);
  } catch (err) {
    return res.redirect(`${returnTo}?error=${encodeURIComponent(err.message || 'Gagal hapus comment user')}`);
  }
});

router.get('/users/:id(\\d+)', async (req, res) => {
  const userId = toInt(req.params.id, 0, 1, Number.MAX_SAFE_INTEGER);
  const flashError = String(req.query.error || '').trim();
  const flashSuccess = String(req.query.success || '').trim();

  try {
    const usersExists = await checkUserTableExists('users');
    if (!usersExists) {
      return res.status(404).send('Table users tidak ditemukan di user DB');
    }

    const [[user]] = await userPool.query(
      `SELECT
         id,
         firebase_uid,
         email,
         name,
         avatar_url,
         banner_url,
         role,
         is_active,
         is_private,
         premium_expires_at,
         xp_total,
         level,
         created_at,
         last_seen_at
       FROM users
       WHERE id = ?
       LIMIT 1`,
      [userId]
    );

    if (!user) {
      return res.status(404).send('User tidak ditemukan');
    }

    const [commentTableExists, historyTableExists, favoriteTableExists, bannedTableExists] = await Promise.all([
      checkUserTableExists('episode_comments'),
      checkUserTableExists('watch_progress'),
      checkUserTableExists('anime_favorites'),
      checkUserTableExists('list_banned')
    ]);

    let commentCount = 0;
    let historyCount = 0;
    let favoriteCount = 0;
    let activeBanCount = 0;

    const comments = [];
    const histories = [];
    const favorites = [];
    const bans = [];

    if (commentTableExists) {
      const [[row]] = await userPool.query('SELECT COUNT(*) AS total FROM episode_comments WHERE user_id = ?', [userId]);
      commentCount = Number(row && row.total ? row.total : 0);
      const [rows] = await userPool.query(
        `SELECT id, anime_title, episode_url, content, created_at
         FROM episode_comments
         WHERE user_id = ?
         ORDER BY created_at DESC
         LIMIT 15`,
        [userId]
      );
      comments.push(...rows.map((r) => ({ ...r, created_at_text: toSafeTimestampString(r.created_at) })));
    }

    if (historyTableExists) {
      const [[row]] = await userPool.query('SELECT COUNT(*) AS total FROM watch_progress WHERE user_id = ?', [userId]);
      historyCount = Number(row && row.total ? row.total : 0);
      const [rows] = await userPool.query(
        `SELECT id, anime_title, episode_label, episode_url, progress_percent, updated_at
         FROM watch_progress
         WHERE user_id = ?
         ORDER BY updated_at DESC
         LIMIT 15`,
        [userId]
      );
      histories.push(...rows.map((r) => ({ ...r, updated_at_text: toSafeTimestampString(r.updated_at) })));
    }

    if (favoriteTableExists) {
      const [[row]] = await userPool.query('SELECT COUNT(*) AS total FROM anime_favorites WHERE user_id = ?', [userId]);
      favoriteCount = Number(row && row.total ? row.total : 0);
      const [rows] = await userPool.query(
        `SELECT id, anime_title, series_url, created_at
         FROM anime_favorites
         WHERE user_id = ?
         ORDER BY created_at DESC
         LIMIT 15`,
        [userId]
      );
      favorites.push(...rows.map((r) => ({ ...r, created_at_text: toSafeTimestampString(r.created_at) })));
    }

    if (bannedTableExists) {
      const [[row]] = await userPool.query(
        `SELECT COUNT(*) AS total
         FROM list_banned
         WHERE user_id = ?
           AND revoked_at IS NULL
           AND (expires_at IS NULL OR expires_at > NOW())`,
        [userId]
      );
      activeBanCount = Number(row && row.total ? row.total : 0);
      const [rows] = await userPool.query(
        `SELECT
           id,
           device_id,
           reason,
           banned_by,
           expires_at,
           revoked_at,
           created_at,
           CASE
             WHEN revoked_at IS NOT NULL THEN 'revoked'
             WHEN expires_at IS NOT NULL AND expires_at <= NOW() THEN 'expired'
             ELSE 'active'
           END AS status_kind
         FROM list_banned
         WHERE user_id = ?
         ORDER BY created_at DESC
         LIMIT 20`,
        [userId]
      );
      bans.push(
        ...rows.map((r) => ({
          ...r,
          expires_at_text: toSafeTimestampString(r.expires_at),
          revoked_at_text: toSafeTimestampString(r.revoked_at),
          created_at_text: toSafeTimestampString(r.created_at)
        }))
      );
    }

    return res.render('admin-user-detail', {
      title: `User #${userId}`,
      user: {
        ...user,
        premium_expires_at_text: toSafeTimestampString(user.premium_expires_at),
        created_at_text: toSafeTimestampString(user.created_at),
        last_seen_at_text: toSafeTimestampString(user.last_seen_at)
      },
      stats: {
        comments: commentCount,
        history: historyCount,
        favorites: favoriteCount,
        activeBans: activeBanCount
      },
      comments,
      histories,
      favorites,
      bans,
      tableFlags: {
        commentTableExists,
        historyTableExists,
        favoriteTableExists,
        bannedTableExists
      },
      error: flashError,
      success: flashSuccess
    });
  } catch (err) {
    return res.status(500).send(err.message);
  }
});

router.get('/episode-error-reports', async (req, res) => {
  const page = toInt(req.query.page, 1, 1, 100000);
  const limit = 50;
  const offset = (page - 1) * limit;
  const q = String(req.query.q || '').trim();
  const seriesId = String(req.query.series_id || '').trim();
  const flashError = String(req.query.error || '').trim();
  const flashDeleted = String(req.query.deleted || '').trim() === '1';

  try {
    const exists = await checkUserTableExists('episode_error_reports');
    if (!exists) {
      return res.render('admin-episode-error-reports', {
        title: 'Episode Error Reports',
        rows: [],
        q,
        seriesId,
        page,
        total: 0,
        totalPages: 1,
        tableMissing: true,
        error: flashError,
        deleted: flashDeleted,
        seriesSummary: []
      });
    }

    const where = [];
    const params = [];
    if (q) {
      const like = `%${escapeLike(q)}%`;
      where.push('(CAST(r.user_id AS CHAR) LIKE ? OR CAST(r.series_id AS CHAR) LIKE ? OR r.episode_url LIKE ? OR r.reason LIKE ?)');
      params.push(like, like, like, like);
    }
    if (seriesId) {
      where.push('r.series_id = ?');
      params.push(seriesId);
    }
    const whereSql = where.length ? `WHERE ${where.join(' AND ')}` : '';

    const summaryWhere = [];
    const summaryParams = [];
    if (q) {
      const like = `%${escapeLike(q)}%`;
      summaryWhere.push('(CAST(user_id AS CHAR) LIKE ? OR CAST(series_id AS CHAR) LIKE ? OR episode_url LIKE ? OR reason LIKE ?)');
      summaryParams.push(like, like, like, like);
    }
    const summaryWhereSql = summaryWhere.length ? `WHERE ${summaryWhere.join(' AND ')}` : '';

    const [seriesSummary] = await userPool.query(
      `SELECT
         series_id,
         COUNT(*) AS total_reports,
         COUNT(DISTINCT user_id) AS total_users
       FROM episode_error_reports
       ${summaryWhereSql}
       GROUP BY series_id
       ORDER BY total_users DESC, total_reports DESC, series_id ASC
       LIMIT 25`,
      summaryParams
    );

    const [[countRow]] = await userPool.query(
      `SELECT COUNT(*) AS total
       FROM episode_error_reports r
       ${whereSql}`,
      params
    );
    const total = Number(countRow.total || 0);
    const totalPages = Math.max(1, Math.ceil(total / limit));

    const [rows] = await userPool.query(
      `SELECT
         r.id,
         r.user_id,
         r.series_id,
         r.episode_url,
         r.reason,
         r.created_at,
         s.total_reports,
         s.total_users
       FROM episode_error_reports r
       JOIN (
         SELECT
           series_id,
           COUNT(*) AS total_reports,
           COUNT(DISTINCT user_id) AS total_users
         FROM episode_error_reports
         GROUP BY series_id
       ) s ON s.series_id = r.series_id
       ${whereSql}
       ORDER BY r.id DESC
       LIMIT ? OFFSET ?`,
      [...params, limit, offset]
    );

    return res.render('admin-episode-error-reports', {
      title: 'Episode Error Reports',
      rows: rows.map((r) => ({
        ...r,
        created_at_text: toSafeTimestampString(r.created_at)
      })),
      q,
      seriesId,
      page,
      total,
      totalPages,
      tableMissing: false,
      error: flashError,
      deleted: flashDeleted,
      seriesSummary
    });
  } catch (err) {
    return res.render('admin-episode-error-reports', {
      title: 'Episode Error Reports',
      rows: [],
      q,
      seriesId,
      page,
      total: 0,
      totalPages: 1,
      tableMissing: false,
      error: err && err.message ? err.message : 'Gagal load episode_error_reports',
      deleted: flashDeleted,
      seriesSummary: []
    });
  }
});

router.post('/episode-error-reports/:id/delete', async (req, res) => {
  const id = toInt(req.params.id, 0, 1, Number.MAX_SAFE_INTEGER);
  const q = String(req.query.q || '').trim();
  const seriesId = String(req.query.series_id || '').trim();
  const page = toInt(req.query.page, 1, 1, 100000);

  try {
    const exists = await checkUserTableExists('episode_error_reports');
    if (!exists) {
      return res.redirect('/admin/episode-error-reports?error=Table%20episode_error_reports%20tidak%20ditemukan');
    }

    await userPool.query(
      'DELETE FROM episode_error_reports WHERE id = ? LIMIT 1',
      [id]
    );

    return res.redirect(
      `/admin/episode-error-reports?deleted=1&page=${page}&q=${encodeURIComponent(q)}&series_id=${encodeURIComponent(seriesId)}`
    );
  } catch (err) {
    return res.redirect(
      `/admin/episode-error-reports?error=${encodeURIComponent(err && err.message ? err.message : 'Gagal delete report')}`
    );
  }
});

router.get('/list-banned', async (req, res) => {
  const page = toInt(req.query.page, 1, 1, 100000);
  const limit = 50;
  const offset = (page - 1) * limit;
  const q = String(req.query.q || '').trim();
  const status = String(req.query.status || '').trim().toLowerCase();
  const flashError = String(req.query.error || '').trim();
  const flashRevoked = String(req.query.revoked || '').trim() === '1';
  const flashAdded = String(req.query.added || '').trim() === '1';

  try {
    const exists = await checkUserTableExists('list_banned');
    if (!exists) {
      return res.render('admin-list-banned', {
        title: 'List Banned',
        rows: [],
        q,
        status,
        page,
        total: 0,
        totalPages: 1,
        tableMissing: true,
        error: flashError,
        revoked: flashRevoked,
        added: flashAdded,
        nowEpochMs: Date.now()
      });
    }

    const where = [];
    const params = [];

    if (q) {
      const like = `%${escapeLike(q)}%`;
      where.push(
        '(CAST(user_id AS CHAR) LIKE ? OR COALESCE(device_id, "") LIKE ? OR COALESCE(reason, "") LIKE ? OR COALESCE(banned_by, "") LIKE ?)'
      );
      params.push(like, like, like, like);
    }

    if (status === 'active') {
      where.push('revoked_at IS NULL AND (expires_at IS NULL OR expires_at > NOW())');
    } else if (status === 'expired') {
      where.push('revoked_at IS NULL AND expires_at IS NOT NULL AND expires_at <= NOW()');
    } else if (status === 'revoked') {
      where.push('revoked_at IS NOT NULL');
    }

    const whereSql = where.length ? `WHERE ${where.join(' AND ')}` : '';

    const [[countRow]] = await userPool.query(
      `SELECT COUNT(*) AS total
       FROM list_banned
       ${whereSql}`,
      params
    );
    const total = Number(countRow.total || 0);
    const totalPages = Math.max(1, Math.ceil(total / limit));

    const [rows] = await userPool.query(
      `SELECT
         id,
         user_id,
         device_id,
         reason,
         banned_by,
         expires_at,
         revoked_at,
         created_at,
         updated_at,
         CASE
           WHEN revoked_at IS NOT NULL THEN 'revoked'
           WHEN expires_at IS NOT NULL AND expires_at <= NOW() THEN 'expired'
           ELSE 'active'
         END AS status_kind
       FROM list_banned
       ${whereSql}
       ORDER BY id DESC
       LIMIT ? OFFSET ?`,
      [...params, limit, offset]
    );

    return res.render('admin-list-banned', {
      title: 'List Banned',
      rows: rows.map((r) => ({
        ...r,
        expires_at_text: toSafeTimestampString(r.expires_at),
        revoked_at_text: toSafeTimestampString(r.revoked_at),
        created_at_text: toSafeTimestampString(r.created_at),
        updated_at_text: toSafeTimestampString(r.updated_at)
      })),
      q,
      status,
      page,
      total,
      totalPages,
      tableMissing: false,
      error: flashError,
      revoked: flashRevoked,
      added: flashAdded,
      nowEpochMs: Date.now()
    });
  } catch (err) {
    return res.render('admin-list-banned', {
      title: 'List Banned',
      rows: [],
      q,
      status,
      page,
      total: 0,
      totalPages: 1,
      tableMissing: false,
      error: err && err.message ? err.message : 'Gagal load list_banned',
      revoked: flashRevoked,
      added: flashAdded,
      nowEpochMs: Date.now()
    });
  }
});

router.post('/list-banned/add', async (req, res) => {
  const userId = toInt(req.body.user_id, 0, 1, Number.MAX_SAFE_INTEGER);
  const deviceId = String(req.body.device_id || '').trim();
  const reason = String(req.body.reason || '').trim();
  const bannedByInput = String(req.body.banned_by || '').trim();
  const expiresAtInput = String(req.body.expires_at || '').trim();
  const page = toInt(req.body.page, 1, 1, 100000);
  const q = String(req.body.q || '').trim();
  const status = String(req.body.status || '').trim().toLowerCase();

  if (!userId) {
    return res.redirect(
      `/admin/list-banned?error=${encodeURIComponent('user_id wajib angka > 0')}&page=${page}&q=${encodeURIComponent(q)}&status=${encodeURIComponent(status)}`
    );
  }
  if (!reason) {
    return res.redirect(
      `/admin/list-banned?error=${encodeURIComponent('reason wajib diisi')}&page=${page}&q=${encodeURIComponent(q)}&status=${encodeURIComponent(status)}`
    );
  }

  const bannedBy = bannedByInput ? toInt(bannedByInput, 0, 1, Number.MAX_SAFE_INTEGER) : 0;
  if (bannedByInput && !bannedBy) {
    return res.redirect(
      `/admin/list-banned?error=${encodeURIComponent('banned_by harus user ID angka atau kosong')}&page=${page}&q=${encodeURIComponent(q)}&status=${encodeURIComponent(status)}`
    );
  }

  let expiresAt = null;
  if (expiresAtInput) {
    const parsed = new Date(expiresAtInput);
    if (Number.isNaN(parsed.getTime())) {
      return res.redirect(
        `/admin/list-banned?error=${encodeURIComponent('expires_at tidak valid')}&page=${page}&q=${encodeURIComponent(q)}&status=${encodeURIComponent(status)}`
      );
    }
    expiresAt = parsed;
  }

  try {
    const exists = await checkUserTableExists('list_banned');
    if (!exists) {
      return res.redirect('/admin/list-banned?error=Table%20list_banned%20tidak%20ditemukan');
    }

    const [[activeDup]] = await userPool.query(
      `SELECT id
       FROM list_banned
       WHERE user_id = ?
         AND revoked_at IS NULL
         AND (expires_at IS NULL OR expires_at > NOW())
         AND (
           (? = '' AND (device_id IS NULL OR device_id = ''))
           OR device_id = ?
         )
       LIMIT 1`,
      [userId, deviceId, deviceId]
    );
    if (activeDup) {
      return res.redirect(
        `/admin/list-banned?error=${encodeURIComponent(`Ban aktif sudah ada (id=${activeDup.id}) untuk user/device ini`)}&page=${page}&q=${encodeURIComponent(q)}&status=${encodeURIComponent(status)}`
      );
    }

    await userPool.query(
      `INSERT INTO list_banned (
         user_id, device_id, reason, banned_by, expires_at, revoked_at, created_at, updated_at
       ) VALUES (?, NULLIF(?, ''), ?, ?, ?, NULL, NOW(), NOW())`,
      [userId, deviceId, reason, bannedBy || null, expiresAt]
    );

    return res.redirect(
      `/admin/list-banned?added=1&page=${page}&q=${encodeURIComponent(q)}&status=${encodeURIComponent(status)}`
    );
  } catch (err) {
    return res.redirect(
      `/admin/list-banned?error=${encodeURIComponent(err && err.message ? err.message : 'Gagal tambah ban')}&page=${page}&q=${encodeURIComponent(q)}&status=${encodeURIComponent(status)}`
    );
  }
});

router.get('/list-banned/device-suggest', async (req, res) => {
  const userId = toInt(req.query.user_id, 0, 1, Number.MAX_SAFE_INTEGER);
  if (!userId) {
    return res.status(400).json({ ok: false, error: 'user_id wajib angka > 0' });
  }

  try {
    const exists = await checkUserTableExists('list_banned');
    if (!exists) {
      return res.status(404).json({ ok: false, error: 'Table list_banned tidak ditemukan' });
    }

    const suggestions = [];
    const seen = new Set();
    const pushSuggestion = (raw) => {
      const v = String(raw || '').trim();
      if (!v) return;
      if (seen.has(v)) return;
      seen.add(v);
      suggestions.push(v);
    };

    // Primary source: user_push_tokens (latest active device first).
    let source = 'none';
    try {
      const [rows] = await userPool.query(
        `SELECT device_id
         FROM user_push_tokens
         WHERE user_id = ?
           AND device_id IS NOT NULL
           AND device_id <> ''
         ORDER BY is_active DESC, last_seen_at DESC, updated_at DESC, id DESC
         LIMIT 10`,
        [userId]
      );
      for (const row of rows) pushSuggestion(row && row.device_id);
      if (suggestions.length) source = 'user_push_tokens';
    } catch (err) {
      if (!err || err.code !== 'ER_NO_SUCH_TABLE') throw err;
    }

    // Secondary source: previous bans.
    const [rowsBanned] = await userPool.query(
      `SELECT device_id
       FROM list_banned
       WHERE user_id = ?
         AND device_id IS NOT NULL
         AND device_id <> ''
       ORDER BY id DESC
       LIMIT 10`,
      [userId]
    );
    for (const row of rowsBanned) pushSuggestion(row && row.device_id);
    if (source === 'none' && suggestions.length) source = 'list_banned';

    // Last fallback: users.device_info (if available).
    try {
      const [[u]] = await userPool.query(
        'SELECT device_info FROM users WHERE id = ? LIMIT 1',
        [userId]
      );
      const deviceInfoRaw = u && u.device_info ? String(u.device_info) : '';
      if (deviceInfoRaw) {
        try {
          const obj = JSON.parse(deviceInfoRaw);
          pushSuggestion(obj && (obj.deviceId || obj.device_id || obj.androidId || obj.android_id));
        } catch (_err) {
          // If non-JSON string, use as-is.
          pushSuggestion(deviceInfoRaw);
        }
      }
      if (source === 'none' && suggestions.length) source = 'users.device_info';
    } catch (err) {
      if (!err || err.code !== 'ER_BAD_FIELD_ERROR') throw err;
    }

    return res.json({
      ok: true,
      user_id: userId,
      device_id: suggestions[0] || '',
      source,
      suggestions
    });
  } catch (err) {
    return res.status(500).json({ ok: false, error: err && err.message ? err.message : 'Gagal cari device_id' });
  }
});

router.post('/list-banned/:id/revoke', async (req, res) => {
  const id = toInt(req.params.id, 0, 1, Number.MAX_SAFE_INTEGER);
  const q = String(req.query.q || '').trim();
  const status = String(req.query.status || '').trim().toLowerCase();
  const page = toInt(req.query.page, 1, 1, 100000);

  try {
    const exists = await checkUserTableExists('list_banned');
    if (!exists) {
      return res.redirect('/admin/list-banned?error=Table%20list_banned%20tidak%20ditemukan');
    }

    await userPool.query(
      `UPDATE list_banned
       SET revoked_at = NOW(),
           updated_at = NOW()
       WHERE id = ?
         AND revoked_at IS NULL
       LIMIT 1`,
      [id]
    );

    return res.redirect(
      `/admin/list-banned?revoked=1&page=${page}&q=${encodeURIComponent(q)}&status=${encodeURIComponent(status)}`
    );
  } catch (err) {
    return res.redirect(
      `/admin/list-banned?error=${encodeURIComponent(err && err.message ? err.message : 'Gagal revoke ban')}`
    );
  }
});

router.get('/episodes/no-stream', async (req, res) => {
  const page = toInt(req.query.page, 1, 1, 100000);
  const limit = 50;
  const offset = (page - 1) * limit;
  const q = String(req.query.q || '').trim();
  const platform = String(req.query.source_platform || '').trim();

  const where = ['NOT EXISTS (SELECT 1 FROM episode_streams es WHERE es.episode_id = e.id)'];
  const params = [];

  if (q) {
    where.push('(s.title LIKE ? OR e.chapter_slug LIKE ?)');
    const like = `%${escapeLike(q)}%`;
    params.push(like, like);
  }
  if (platform) {
    where.push('s.source_platform = ?');
    params.push(platform);
  }

  const whereSql = where.length ? `WHERE ${where.join(' AND ')}` : '';

  try {
    const [platformOptionsRows] = await pool.query(
      'SELECT source_platform FROM series GROUP BY source_platform ORDER BY source_platform ASC'
    );
    const platformOptions = platformOptionsRows.map((x) => String(x.source_platform || '')).filter(Boolean);

    const [[countRow]] = await pool.query(
      `SELECT COUNT(*) AS total
       FROM episodes e
       JOIN series s ON s.id = e.series_id
       ${whereSql}`,
      params
    );
    const total = Number(countRow.total || 0);
    const totalPages = Math.max(1, Math.ceil(total / limit));

    const [rows] = await pool.query(
      `SELECT
         e.id,
         e.source_platform,
         e.source_episode_id,
         e.chapter_label,
         e.chapter_slug,
         e.release_date_text,
         e.created_at,
         s.id AS series_id,
         s.title AS series_title,
         s.source_platform AS series_platform
       FROM episodes e
       JOIN series s ON s.id = e.series_id
       ${whereSql}
       ORDER BY e.id ASC
       LIMIT ? OFFSET ?`,
      [...params, limit, offset]
    );

    return res.render('admin-episodes-no-stream', {
      title: 'Episodes Without Stream',
      rows,
      q,
      platform,
      platformOptions,
      page,
      total,
      totalPages
    });
  } catch (err) {
    return res.status(500).send(err.message);
  }
});

router.get('/series', async (req, res) => {
  const page = toInt(req.query.page, 1, 1, 100000);
  const limit = 30;
  const offset = (page - 1) * limit;

  const filters = buildSeriesWhere(req.query);

  const sort = String(req.query.sort || 'id_desc');
  const sortMap = {
    id_desc: 's.id DESC',
    id_asc: 's.id ASC',
    updated_desc: 's.updated_at DESC',
    title_asc: 's.title ASC',
    title_desc: 's.title DESC'
  };
  const orderSql = sortMap[sort] || sortMap.id_desc;

  try {
    const [platformOptions] = await pool.query(
      'SELECT source_platform AS value FROM series GROUP BY source_platform ORDER BY source_platform ASC'
    );
    const [typeOptions] = await pool.query(
      'SELECT type AS value FROM series WHERE type IS NOT NULL AND type <> "" GROUP BY type ORDER BY type ASC'
    );
    const [statusOptions] = await pool.query(
      'SELECT status AS value FROM series WHERE status IS NOT NULL AND status <> "" GROUP BY status ORDER BY status ASC'
    );
    const [originOptions] = await pool.query(
      'SELECT content_origin AS value FROM series GROUP BY content_origin ORDER BY content_origin ASC'
    );

    const [[countRow]] = await pool.query(
      `SELECT COUNT(*) AS total FROM series s ${filters.whereSql}`,
      filters.params
    );

    const [rows] = await pool.query(
      `SELECT
         s.id,
         s.source_platform,
         s.source_series_id,
         s.content_origin,
         s.title,
         s.title2,
         s.series_slug,
         s.status,
         s.type,
         s.updated_at,
         (SELECT COUNT(*) FROM episodes e WHERE e.series_id = s.id) AS episode_count
       FROM series s
       ${filters.whereSql}
       ORDER BY ${orderSql}
       LIMIT ? OFFSET ?`,
      [...filters.params, limit, offset]
    );

    res.render('admin-series', {
      title: 'Series Manager',
      rows,
      page,
      limit,
      total: countRow.total,
      totalPages: Math.max(1, Math.ceil(countRow.total / limit)),
      sort,
      filters,
      options: {
        platform: platformOptions.map((x) => x.value),
        type: typeOptions.map((x) => x.value),
        status: statusOptions.map((x) => x.value),
        origin: originOptions.map((x) => x.value)
      },
      deletedId: req.query.deleted_id ? Number(req.query.deleted_id) : null,
      createdId: req.query.created_id ? Number(req.query.created_id) : null,
      error: req.query.error ? String(req.query.error) : ''
    });
  } catch (err) {
    res.status(500).send(err.message);
  }
});

router.get('/series/create', async (req, res) => {
  res.render('admin-series-create', {
    title: 'Create Series',
    error: req.query.error ? String(req.query.error) : ''
  });
});

router.get('/series/create/autofill', async (req, res) => {
  const platformRaw = String(req.query.platform || '').trim().toLowerCase();
  const platform = platformRaw === 'donghub' ? 'donghub' : 'animekita';
  const sourceUrl = String(req.query.source_url || '').trim();
  const sourceSeriesIdRaw = String(req.query.source_series_id || '').trim();

  try {
    let mapped = null;

    if (platform === 'animekita') {
      if (!sourceUrl) {
        return res.status(400).json({ error: 'source_url wajib diisi untuk animekita' });
      }
      const fetched = await fetchAnimekitaSeriesByFullUrl(sourceUrl);
      mapped = mapAutoSeriesInputFromRemote('animekita', fetched.data, {
        seriesSlug: fetched.seriesSlug
      });
    } else {
      const sourceSeriesId = Number(sourceSeriesIdRaw || sourceUrl);
      if (!Number.isFinite(sourceSeriesId) || sourceSeriesId <= 0) {
        return res.status(400).json({ error: 'source_series_id donghub harus angka > 0' });
      }
      const fetched = await fetchDonghubSeriesBySourceId(sourceSeriesId);
      mapped = mapAutoSeriesInputFromRemote('donghub', fetched.data);
      if (!mapped.source_series_id) mapped.source_series_id = String(sourceSeriesId);
      if (!mapped.series_slug) mapped.series_slug = `dh-${sourceSeriesId}`;
    }

    const built = buildSeriesPayloadFromInput(mapped);
    if (built.error) {
      return res.status(422).json({ error: built.error, data: mapped });
    }

    return res.json({ data: mapped });
  } catch (err) {
    return res.status(500).json({ error: err && err.message ? err.message : 'Gagal auto fill series' });
  }
});

router.post('/series/create/auto-insert', async (req, res) => {
  const platformRaw = String(req.body.platform || '').trim().toLowerCase();
  const platform = platformRaw === 'donghub' ? 'donghub' : 'animekita';
  const sourceUrl = String(req.body.source_url || '').trim();
  const sourceSeriesIdRaw = String(req.body.source_series_id || '').trim();

  try {
    let mapped = null;
    if (platform === 'animekita') {
      if (!sourceUrl) {
        return res.redirect('/admin/series/create?error=source_url animekita wajib diisi');
      }
      const fetched = await fetchAnimekitaSeriesByFullUrl(sourceUrl);
      mapped = mapAutoSeriesInputFromRemote('animekita', fetched.data, {
        seriesSlug: fetched.seriesSlug
      });
    } else {
      const sourceSeriesId = Number(sourceSeriesIdRaw || sourceUrl);
      if (!Number.isFinite(sourceSeriesId) || sourceSeriesId <= 0) {
        return res.redirect('/admin/series/create?error=source_series_id donghub harus angka > 0');
      }
      const fetched = await fetchDonghubSeriesBySourceId(sourceSeriesId);
      mapped = mapAutoSeriesInputFromRemote('donghub', fetched.data);
      if (!mapped.source_series_id) mapped.source_series_id = String(sourceSeriesId);
      if (!mapped.series_slug) mapped.series_slug = `dh-${sourceSeriesId}`;
    }

    const built = buildSeriesPayloadFromInput(mapped);
    if (built.error) {
      return res.redirect(`/admin/series/create?error=${encodeURIComponent(built.error)}`);
    }

    const newSeriesId = await createSeriesFromBuilt(built);
    return res.redirect(`/admin/series?created_id=${newSeriesId}`);
  } catch (err) {
    if (err && err.code === 'ER_DUP_ENTRY') {
      return res.redirect('/admin/series/create?error=Data duplikat. Cek source_platform/source_series_id atau series_slug.');
    }
    return res.redirect(`/admin/series/create?error=${encodeURIComponent(err && err.message ? err.message : 'Gagal auto insert series')}`);
  }
});

router.get('/series/import-json', async (req, res) => {
  res.render('admin-series-import-json', {
    title: 'Import Series JSON',
    error: req.query.error ? String(req.query.error) : '',
    result: null,
    payloadText: ''
  });
});

router.get('/episodes/import-json', async (req, res) => {
  res.render('admin-episodes-import-json', {
    title: 'Import Episodes JSON',
    error: req.query.error ? String(req.query.error) : '',
    result: null,
    payloadText: ''
  });
});

router.post('/episodes/import-json', async (req, res) => {
  const payloadText = String(req.body.json_payload || '').trim();
  if (!payloadText) {
    return res.render('admin-episodes-import-json', {
      title: 'Import Episodes JSON',
      error: 'json_payload wajib diisi',
      result: null,
      payloadText
    });
  }

  let parsed;
  try {
    parsed = JSON.parse(payloadText);
  } catch (err) {
    return res.render('admin-episodes-import-json', {
      title: 'Import Episodes JSON',
      error: `JSON tidak valid: ${err.message}`,
      result: null,
      payloadText
    });
  }

  const items = Array.isArray(parsed)
    ? parsed
    : (parsed && Array.isArray(parsed.data) ? parsed.data : []);
  if (!items.length) {
    return res.render('admin-episodes-import-json', {
      title: 'Import Episodes JSON',
      error: 'JSON harus array episode atau object { "data": [...] }',
      result: null,
      payloadText
    });
  }

  const report = {
    total: items.length,
    inserted: 0,
    skipped_duplicate: 0,
    failed: 0,
    created_ids: [],
    errors: []
  };

  let conn;
  try {
    conn = await pool.getConnection();
    for (let i = 0; i < items.length; i += 1) {
      const row = buildEpisodePayloadFromInput(items[i]);
      if (row.error) {
        report.failed += 1;
        report.errors.push(`#${i + 1}: ${row.error}`);
        continue;
      }
      const { payload, normalized } = row;
      try {
        const [[series]] = await conn.query('SELECT id FROM series WHERE id = ? LIMIT 1', [normalized.seriesId]);
        if (!series) {
          report.failed += 1;
          report.errors.push(`#${i + 1}: series_id ${normalized.seriesId} tidak ditemukan`);
          continue;
        }
        const [ins] = await conn.query(
          `INSERT INTO episodes (
             source_platform, source_episode_id, series_id, chapter_label, chapter_slug, release_date_text
           ) VALUES (?, ?, ?, NULLIF(?, ''), ?, NULLIF(?, ''))`,
          [
            payload.source_platform,
            normalized.sourceEpisodeId,
            normalized.seriesId,
            payload.chapter_label,
            payload.chapter_slug,
            payload.release_date_text
          ]
        );
        report.inserted += 1;
        report.created_ids.push(Number(ins.insertId));
      } catch (err) {
        if (err && err.code === 'ER_DUP_ENTRY') {
          report.skipped_duplicate += 1;
          continue;
        }
        report.failed += 1;
        report.errors.push(`#${i + 1}: ${err.message || 'gagal insert'}`);
      }
    }

    return res.render('admin-episodes-import-json', {
      title: 'Import Episodes JSON',
      error: '',
      result: report,
      payloadText
    });
  } catch (err) {
    return res.render('admin-episodes-import-json', {
      title: 'Import Episodes JSON',
      error: err.message || 'Gagal import JSON',
      result: report,
      payloadText
    });
  } finally {
    if (conn) conn.release();
  }
});

router.get('/streams/import-json', async (req, res) => {
  res.render('admin-streams-import-json', {
    title: 'Import Streams JSON',
    error: req.query.error ? String(req.query.error) : '',
    result: null,
    payloadText: ''
  });
});

router.post('/streams/import-json', async (req, res) => {
  const payloadText = String(req.body.json_payload || '').trim();
  if (!payloadText) {
    return res.render('admin-streams-import-json', {
      title: 'Import Streams JSON',
      error: 'json_payload wajib diisi',
      result: null,
      payloadText
    });
  }

  let parsed;
  try {
    parsed = JSON.parse(payloadText);
  } catch (err) {
    return res.render('admin-streams-import-json', {
      title: 'Import Streams JSON',
      error: `JSON tidak valid: ${err.message}`,
      result: null,
      payloadText
    });
  }

  const items = Array.isArray(parsed)
    ? parsed
    : (parsed && Array.isArray(parsed.data) ? parsed.data : []);
  if (!items.length) {
    return res.render('admin-streams-import-json', {
      title: 'Import Streams JSON',
      error: 'JSON harus array stream atau object { "data": [...] }',
      result: null,
      payloadText
    });
  }

  const report = {
    total: items.length,
    inserted: 0,
    skipped_duplicate: 0,
    failed: 0,
    created_ids: [],
    errors: []
  };

  let conn;
  try {
    conn = await pool.getConnection();
    for (let i = 0; i < items.length; i += 1) {
      const row = buildStreamPayloadFromInput(items[i]);
      if (row.error) {
        report.failed += 1;
        report.errors.push(`#${i + 1}: ${row.error}`);
        continue;
      }
      const { payload, normalized } = row;
      try {
        let episodeId = normalized.episodeId;
        if (!episodeId) {
          if (!payload.chapter_slug) {
            report.failed += 1;
            report.errors.push(`#${i + 1}: episode_id atau chapter_slug wajib ada`);
            continue;
          }
          const [[episode]] = await conn.query('SELECT id FROM episodes WHERE chapter_slug = ? LIMIT 1', [
            payload.chapter_slug
          ]);
          if (!episode) {
            report.failed += 1;
            report.errors.push(`#${i + 1}: chapter_slug ${payload.chapter_slug} tidak ditemukan`);
            continue;
          }
          episodeId = Number(episode.id);
        } else {
          const [[episode]] = await conn.query('SELECT id FROM episodes WHERE id = ? LIMIT 1', [episodeId]);
          if (!episode) {
            report.failed += 1;
            report.errors.push(`#${i + 1}: episode_id ${episodeId} tidak ditemukan`);
            continue;
          }
        }

        const [[dup]] = await conn.query(
          `SELECT id FROM episode_streams
           WHERE episode_id = ? AND resolution = ? AND stream_url = ?
           LIMIT 1`,
          [episodeId, payload.resolution, payload.stream_url]
        );
        if (dup) {
          report.skipped_duplicate += 1;
          continue;
        }

        const [ins] = await conn.query(
          `INSERT INTO episode_streams (
             episode_id, resolution, stream_source_id, stream_url, size_kb
           ) VALUES (?, ?, ?, ?, ?)`,
          [episodeId, payload.resolution, normalized.streamSourceId, payload.stream_url, normalized.sizeKb]
        );
        report.inserted += 1;
        report.created_ids.push(Number(ins.insertId));
      } catch (err) {
        if (err && err.code === 'ER_DUP_ENTRY') {
          report.skipped_duplicate += 1;
          continue;
        }
        report.failed += 1;
        report.errors.push(`#${i + 1}: ${err.message || 'gagal insert'}`);
      }
    }

    return res.render('admin-streams-import-json', {
      title: 'Import Streams JSON',
      error: '',
      result: report,
      payloadText
    });
  } catch (err) {
    return res.render('admin-streams-import-json', {
      title: 'Import Streams JSON',
      error: err.message || 'Gagal import JSON',
      result: report,
      payloadText
    });
  } finally {
    if (conn) conn.release();
  }
});

router.get('/import-json-all', async (req, res) => {
  res.render('admin-import-json-all', {
    title: 'Import All-in-One JSON',
    error: req.query.error ? String(req.query.error) : '',
    result: null,
    payloadText: ''
  });
});

router.post('/import-json-all', async (req, res) => {
  const payloadText = String(req.body.json_payload || '').trim();
  if (!payloadText) {
    return res.render('admin-import-json-all', {
      title: 'Import All-in-One JSON',
      error: 'json_payload wajib diisi',
      result: null,
      payloadText
    });
  }

  let parsed;
  try {
    parsed = JSON.parse(payloadText);
  } catch (err) {
    return res.render('admin-import-json-all', {
      title: 'Import All-in-One JSON',
      error: `JSON tidak valid: ${err.message}`,
      result: null,
      payloadText
    });
  }

  const seriesItems = Array.isArray(parsed.series) ? parsed.series : [];
  const episodeItems = Array.isArray(parsed.episodes) ? parsed.episodes : [];
  const streamItems = Array.isArray(parsed.streams) ? parsed.streams : [];
  if (!seriesItems.length && !episodeItems.length && !streamItems.length) {
    return res.render('admin-import-json-all', {
      title: 'Import All-in-One JSON',
      error: 'Gunakan format object dengan key: series, episodes, streams',
      result: null,
      payloadText
    });
  }

  const report = {
    series: { total: seriesItems.length, inserted: 0, skipped_duplicate: 0, failed: 0 },
    episodes: { total: episodeItems.length, inserted: 0, skipped_duplicate: 0, failed: 0 },
    streams: { total: streamItems.length, inserted: 0, skipped_duplicate: 0, failed: 0 },
    errors: []
  };

  let conn;
  try {
    conn = await pool.getConnection();
    const seriesIdBySlugKey = new Map();
    const episodeIdBySlugKey = new Map();

    for (let i = 0; i < seriesItems.length; i += 1) {
      const row = buildSeriesPayloadFromInput(seriesItems[i]);
      if (row.error) {
        report.series.failed += 1;
        report.errors.push(`series #${i + 1}: ${row.error}`);
        continue;
      }
      const { payload, normalized } = row;
      try {
        await conn.beginTransaction();
    const [ins] = await conn.query(
      `INSERT INTO series (
         source_platform, source_series_id, content_origin, title, title2, series_slug,
         cover_url, type, status, rating, published_text, author, synopsis
       ) VALUES (?, ?, ?, ?, NULLIF(?, ''), ?, NULLIF(?, ''), NULLIF(?, ''), NULLIF(?, ''), ?, NULLIF(?, ''), NULLIF(?, ''), NULLIF(?, ''))`,
      [
        payload.source_platform,
        normalized.sourceSeriesId,
        payload.content_origin,
        payload.title,
        payload.title2,
        payload.series_slug,
        payload.cover_url,
        payload.type,
        payload.status,
            normalized.ratingValue,
            payload.published_text,
            payload.author,
            payload.synopsis
          ]
        );
        const seriesId = Number(ins.insertId);
        for (const name of payload.genres) {
          const [insertGenre] = await conn.query(
            'INSERT INTO genres (name) VALUES (?) ON DUPLICATE KEY UPDATE id = LAST_INSERT_ID(id)',
            [name]
          );
          const genreId = insertGenre.insertId;
          await conn.query('INSERT IGNORE INTO series_genres (series_id, genre_id) VALUES (?, ?)', [seriesId, genreId]);
        }
        await conn.commit();
        report.series.inserted += 1;
        seriesIdBySlugKey.set(`${payload.source_platform}::${payload.series_slug}`, seriesId);
      } catch (err) {
        await conn.rollback();
        if (err && err.code === 'ER_DUP_ENTRY') {
          report.series.skipped_duplicate += 1;
          const [[bySourceId]] = await conn.query(
            `SELECT id, title, series_slug
             FROM series
             WHERE source_platform = ? AND source_series_id = ?
             LIMIT 1`,
            [payload.source_platform, normalized.sourceSeriesId]
          );
          if (bySourceId) {
            report.errors.push(
              `series #${i + 1}: duplicate source id -> existing #${bySourceId.id} (${bySourceId.title}) slug=${bySourceId.series_slug}`
            );
          } else {
            const [[bySlug]] = await conn.query(
              `SELECT id, title, source_platform
               FROM series
               WHERE series_slug = ?
               LIMIT 1`,
              [payload.series_slug]
            );
            if (bySlug) {
              report.errors.push(
                `series #${i + 1}: duplicate slug -> existing #${bySlug.id} (${bySlug.title}) platform=${bySlug.source_platform}`
              );
            }
          }
          const [[s]] = await conn.query(
            'SELECT id FROM series WHERE source_platform = ? AND series_slug = ? LIMIT 1',
            [payload.source_platform, payload.series_slug]
          );
          if (s) seriesIdBySlugKey.set(`${payload.source_platform}::${payload.series_slug}`, Number(s.id));
          continue;
        }
        report.series.failed += 1;
        report.errors.push(`series #${i + 1}: ${err.message || 'gagal insert'}`);
      }
    }

    for (let i = 0; i < episodeItems.length; i += 1) {
      const item = episodeItems[i];
      const row = buildEpisodePayloadFromInput(item, { requireSeriesId: false });
      if (row.error) {
        report.episodes.failed += 1;
        report.errors.push(`episode #${i + 1}: ${row.error}`);
        continue;
      }
      const { payload, normalized } = row;
      try {
        const safeSourcePlatform = String(item.source_platform || '').trim();
        const safeSeriesSlug = cleanSeriesSlug(item.series_slug);
        if (!safeSourcePlatform || !safeSeriesSlug) {
          report.episodes.failed += 1;
          report.errors.push(`episode #${i + 1}: wajib pakai source_platform + series_slug`);
          continue;
        }
        const resolvedSeriesId = await resolveSeriesId(conn, item, seriesIdBySlugKey);
        if (!resolvedSeriesId) {
          report.episodes.failed += 1;
          report.errors.push(`episode #${i + 1}: series tidak ditemukan dari ${safeSourcePlatform}/${safeSeriesSlug}`);
          continue;
        }
        const [ins] = await conn.query(
          `INSERT INTO episodes (
             source_platform, source_episode_id, series_id, chapter_label, chapter_slug, release_date_text
           ) VALUES (?, ?, ?, NULLIF(?, ''), ?, NULLIF(?, ''))`,
          [
            payload.source_platform,
            normalized.sourceEpisodeId,
            resolvedSeriesId,
            payload.chapter_label,
            payload.chapter_slug,
            payload.release_date_text
          ]
        );
        const episodeId = Number(ins.insertId);
        report.episodes.inserted += 1;
        episodeIdBySlugKey.set(payload.chapter_slug, episodeId);
      } catch (err) {
        if (err && err.code === 'ER_DUP_ENTRY') {
          report.episodes.skipped_duplicate += 1;
          const [[e]] = await conn.query('SELECT id FROM episodes WHERE chapter_slug = ? LIMIT 1', [payload.chapter_slug]);
          if (e) episodeIdBySlugKey.set(payload.chapter_slug, Number(e.id));
          continue;
        }
        report.episodes.failed += 1;
        report.errors.push(`episode #${i + 1}: ${err.message || 'gagal insert'}`);
      }
    }

    for (let i = 0; i < streamItems.length; i += 1) {
      const item = streamItems[i];
      const row = buildStreamPayloadFromInput(item);
      if (row.error) {
        report.streams.failed += 1;
        report.errors.push(`stream #${i + 1}: ${row.error}`);
        continue;
      }
      const { payload, normalized } = row;
      try {
        const safeChapterSlug = cleanSeriesSlug(item.chapter_slug);
        if (!safeChapterSlug) {
          report.streams.failed += 1;
          report.errors.push(`stream #${i + 1}: wajib pakai chapter_slug`);
          continue;
        }
        const episodeId = await resolveEpisodeId(conn, item, episodeIdBySlugKey);
        if (!episodeId) {
          report.streams.failed += 1;
          report.errors.push(`stream #${i + 1}: episode tidak ditemukan dari chapter_slug ${safeChapterSlug}`);
          continue;
        }

        const [[dup]] = await conn.query(
          `SELECT id FROM episode_streams
           WHERE episode_id = ? AND resolution = ? AND stream_url = ?
           LIMIT 1`,
          [episodeId, payload.resolution, payload.stream_url]
        );
        if (dup) {
          report.streams.skipped_duplicate += 1;
          continue;
        }

        await conn.query(
          `INSERT INTO episode_streams (
             episode_id, resolution, stream_source_id, stream_url, size_kb
           ) VALUES (?, ?, ?, ?, ?)`,
          [episodeId, payload.resolution, normalized.streamSourceId, payload.stream_url, normalized.sizeKb]
        );
        report.streams.inserted += 1;
      } catch (err) {
        if (err && err.code === 'ER_DUP_ENTRY') {
          report.streams.skipped_duplicate += 1;
          continue;
        }
        report.streams.failed += 1;
        report.errors.push(`stream #${i + 1}: ${err.message || 'gagal insert'}`);
      }
    }

    return res.render('admin-import-json-all', {
      title: 'Import All-in-One JSON',
      error: '',
      result: report,
      payloadText
    });
  } catch (err) {
    return res.render('admin-import-json-all', {
      title: 'Import All-in-One JSON',
      error: err.message || 'Gagal import all-in-one',
      result: report,
      payloadText
    });
  } finally {
    if (conn) conn.release();
  }
});

router.post('/series/import-json', async (req, res) => {
  const payloadText = String(req.body.json_payload || '').trim();
  if (!payloadText) {
    return res.render('admin-series-import-json', {
      title: 'Import Series JSON',
      error: 'json_payload wajib diisi',
      result: null,
      payloadText
    });
  }

  let parsed;
  try {
    parsed = JSON.parse(payloadText);
  } catch (err) {
    return res.render('admin-series-import-json', {
      title: 'Import Series JSON',
      error: `JSON tidak valid: ${err.message}`,
      result: null,
      payloadText
    });
  }

  const items = Array.isArray(parsed)
    ? parsed
    : (parsed && Array.isArray(parsed.data) ? parsed.data : []);
  if (!items.length) {
    return res.render('admin-series-import-json', {
      title: 'Import Series JSON',
      error: 'JSON harus array series atau object { "data": [...] }',
      result: null,
      payloadText
    });
  }

  const report = {
    total: items.length,
    inserted: 0,
    skipped_duplicate: 0,
    failed: 0,
    created_ids: [],
    errors: []
  };

  let conn;
  try {
    conn = await pool.getConnection();

    for (let i = 0; i < items.length; i += 1) {
      const item = items[i];
      const row = buildSeriesPayloadFromInput(item);
      if (row.error) {
        report.failed += 1;
        report.errors.push(`#${i + 1}: ${row.error}`);
        continue;
      }

      const { payload, normalized } = row;
      try {
        await conn.beginTransaction();

        const [insertSeries] = await conn.query(
          `INSERT INTO series (
             source_platform, source_series_id, content_origin, title, title2, series_slug,
             cover_url, type, status, rating, published_text, author, synopsis
           ) VALUES (?, ?, ?, ?, NULLIF(?, ''), ?, NULLIF(?, ''), NULLIF(?, ''), NULLIF(?, ''), ?, NULLIF(?, ''), NULLIF(?, ''), NULLIF(?, ''))`,
          [
            payload.source_platform,
            normalized.sourceSeriesId,
            payload.content_origin,
            payload.title,
            payload.title2,
            payload.series_slug,
            payload.cover_url,
            payload.type,
            payload.status,
            normalized.ratingValue,
            payload.published_text,
            payload.author,
            payload.synopsis
          ]
        );

        const seriesId = Number(insertSeries.insertId);
        for (const name of payload.genres) {
          const [insertGenre] = await conn.query(
            'INSERT INTO genres (name) VALUES (?) ON DUPLICATE KEY UPDATE id = LAST_INSERT_ID(id)',
            [name]
          );
          const genreId = insertGenre.insertId;
          await conn.query('INSERT IGNORE INTO series_genres (series_id, genre_id) VALUES (?, ?)', [
            seriesId,
            genreId
          ]);
        }

        await conn.commit();
        report.inserted += 1;
        report.created_ids.push(seriesId);
      } catch (err) {
        await conn.rollback();
        if (err && err.code === 'ER_DUP_ENTRY') {
          report.skipped_duplicate += 1;
          continue;
        }
        report.failed += 1;
        report.errors.push(`#${i + 1}: ${err.message || 'gagal insert'}`);
      }
    }

    return res.render('admin-series-import-json', {
      title: 'Import Series JSON',
      error: '',
      result: report,
      payloadText
    });
  } catch (err) {
    return res.render('admin-series-import-json', {
      title: 'Import Series JSON',
      error: err.message || 'Gagal import JSON',
      result: report,
      payloadText
    });
  } finally {
    if (conn) conn.release();
  }
});

router.post('/series/create', async (req, res) => {
  const built = buildSeriesPayloadFromInput(req.body);
  if (built.error) {
    return res.redirect(`/admin/series/create?error=${encodeURIComponent(built.error)}`);
  }
  try {
    const newSeriesId = await createSeriesFromBuilt(built);
    return res.redirect(`/admin/series?created_id=${newSeriesId}`);
  } catch (err) {
    if (err && err.code === 'ER_DUP_ENTRY') {
      return res.redirect(`/admin/series/create?error=${encodeURIComponent('Data duplikat. Cek source_platform/source_series_id atau series_slug.')}`);
    }
    return res.redirect(`/admin/series/create?error=${encodeURIComponent(err.message || 'Gagal create series')}`);
  }
});

router.get('/series/:id/episodes/create', async (req, res) => {
  const id = toInt(req.params.id, 0, 1, Number.MAX_SAFE_INTEGER);

  try {
    const [[series]] = await pool.query(
      'SELECT id, title, source_platform FROM series WHERE id = ? LIMIT 1',
      [id]
    );
    if (!series) return res.status(404).send('Series tidak ditemukan');

    res.render('admin-episode-create', {
      title: `Create Episode for Series #${id}`,
      series,
      error: req.query.error ? String(req.query.error) : ''
    });
  } catch (err) {
    res.status(500).send(err.message);
  }
});

router.post('/series/:id/episodes/create', async (req, res) => {
  const id = toInt(req.params.id, 0, 1, Number.MAX_SAFE_INTEGER);

  const sourceEpisodeId = Number(String(req.body.source_episode_id || '').trim());
  const chapterLabel = String(req.body.chapter_label || '').trim();
  const chapterSlug = cleanSeriesSlug(req.body.chapter_slug);
  const releaseDateText = String(req.body.release_date_text || '').trim();

  if (!Number.isFinite(sourceEpisodeId) || sourceEpisodeId <= 0) {
    return res.redirect(
      `/admin/series/${id}/episodes/create?error=${encodeURIComponent('source_episode_id harus angka > 0')}`
    );
  }
  if (!chapterSlug) {
    return res.redirect(
      `/admin/series/${id}/episodes/create?error=${encodeURIComponent('chapter_slug wajib diisi')}`
    );
  }

  try {
    const [[series]] = await pool.query(
      'SELECT id, source_platform FROM series WHERE id = ? LIMIT 1',
      [id]
    );
    if (!series) {
      return res.redirect(`/admin/series?error=${encodeURIComponent('Series tidak ditemukan')}`);
    }

    await pool.query(
      `INSERT INTO episodes (
         source_platform,
         source_episode_id,
         series_id,
         chapter_label,
         chapter_slug,
         release_date_text
       ) VALUES (?, ?, ?, NULLIF(?, ''), ?, NULLIF(?, ''))`,
      [
        series.source_platform,
        sourceEpisodeId,
        id,
        chapterLabel,
        chapterSlug,
        releaseDateText
      ]
    );

    return res.redirect(`/admin/series/${id}?episode_saved=1`);
  } catch (err) {
    if (err && err.code === 'ER_DUP_ENTRY') {
      return res.redirect(
        `/admin/series/${id}/episodes/create?error=${encodeURIComponent('Episode duplikat. Cek source_episode_id atau chapter_slug.')}`
      );
    }
    return res.redirect(
      `/admin/series/${id}/episodes/create?error=${encodeURIComponent(err.message || 'Gagal create episode')}`
    );
  }
});

router.get('/episodes/:id/edit', async (req, res) => {
  const id = toInt(req.params.id, 0, 1, Number.MAX_SAFE_INTEGER);

  try {
    const [[episode]] = await pool.query(
      `SELECT
         e.id,
         e.source_platform,
         e.source_episode_id,
         e.series_id,
         e.chapter_label,
         e.chapter_slug,
         e.release_date_text,
         s.title AS series_title
       FROM episodes e
       JOIN series s ON s.id = e.series_id
       WHERE e.id = ?
       LIMIT 1`,
      [id]
    );

    if (!episode) return res.status(404).send('Episode tidak ditemukan');

    res.render('admin-episode-edit', {
      title: `Edit Episode #${id}`,
      episode,
      error: req.query.error ? String(req.query.error) : ''
    });
  } catch (err) {
    res.status(500).send(err.message);
  }
});

router.post('/episodes/:id/edit', async (req, res) => {
  const id = toInt(req.params.id, 0, 1, Number.MAX_SAFE_INTEGER);

  const sourcePlatform = String(req.body.source_platform || '').trim();
  const sourceEpisodeId = Number(String(req.body.source_episode_id || '').trim());
  const chapterLabel = String(req.body.chapter_label || '').trim();
  const chapterSlug = cleanSeriesSlug(req.body.chapter_slug);
  const releaseDateText = String(req.body.release_date_text || '').trim();

  if (!sourcePlatform || !Number.isFinite(sourceEpisodeId) || sourceEpisodeId <= 0 || !chapterSlug) {
    return res.redirect(
      `/admin/episodes/${id}/edit?error=${encodeURIComponent('Field wajib: source_platform, source_episode_id > 0, chapter_slug')}`
    );
  }

  try {
    const [[episode]] = await pool.query('SELECT id, series_id FROM episodes WHERE id = ? LIMIT 1', [id]);
    if (!episode) {
      return res.redirect('/admin/series?error=Episode tidak ditemukan');
    }

    await pool.query(
      `UPDATE episodes
       SET source_platform = ?,
           source_episode_id = ?,
           chapter_label = NULLIF(?, ''),
           chapter_slug = ?,
           release_date_text = NULLIF(?, ''),
           updated_at = CURRENT_TIMESTAMP
       WHERE id = ?
       LIMIT 1`,
      [sourcePlatform, sourceEpisodeId, chapterLabel, chapterSlug, releaseDateText, id]
    );

    return res.redirect(`/admin/series/${episode.series_id}?episode_updated=1`);
  } catch (err) {
    if (err && err.code === 'ER_DUP_ENTRY') {
      return res.redirect(
        `/admin/episodes/${id}/edit?error=${encodeURIComponent('Episode duplikat. Cek source_platform/source_episode_id atau chapter_slug.')}`
      );
    }
    return res.redirect(
      `/admin/episodes/${id}/edit?error=${encodeURIComponent(err.message || 'Gagal update episode')}`
    );
  }
});

router.post('/episodes/:id/delete', async (req, res) => {
  const id = toInt(req.params.id, 0, 1, Number.MAX_SAFE_INTEGER);

  try {
    const [[episode]] = await pool.query('SELECT id, series_id FROM episodes WHERE id = ? LIMIT 1', [id]);
    if (!episode) {
      return res.redirect('/admin/series?error=Episode tidak ditemukan');
    }

    const [result] = await pool.query('DELETE FROM episodes WHERE id = ? LIMIT 1', [id]);
    if (!result.affectedRows) {
      return res.redirect(`/admin/series/${episode.series_id}?error=Episode tidak ditemukan`);
    }

    return res.redirect(`/admin/series/${episode.series_id}?episode_deleted=1`);
  } catch (err) {
    return res.redirect(`/admin/series?error=${encodeURIComponent(err.message || 'Gagal delete episode')}`);
  }
});

router.get('/series/:id', async (req, res) => {
  const id = toInt(req.params.id, 0, 1, Number.MAX_SAFE_INTEGER);

  try {
    const [[series]] = await pool.query(
      `SELECT
         id, source_platform, source_series_id, content_origin, title, title2, series_slug, cover_url,
         type, status, rating, published_text, author, synopsis, created_at, updated_at
       FROM series
       WHERE id = ?
       LIMIT 1`,
      [id]
    );

    if (!series) return res.status(404).send('Series tidak ditemukan');

    const [genres] = await pool.query(
      `SELECT g.name
       FROM series_genres sg
       JOIN genres g ON g.id = sg.genre_id
       WHERE sg.series_id = ?
       ORDER BY g.name ASC`,
      [id]
    );

    const [episodes] = await pool.query(
      `SELECT
         e.id,
         e.source_platform,
         e.source_episode_id,
         e.chapter_label,
         e.chapter_slug,
         e.release_date_text,
         (SELECT COUNT(*) FROM episode_streams es WHERE es.episode_id = e.id) AS stream_count
       FROM episodes e
       WHERE e.series_id = ?
       ORDER BY e.id DESC
       LIMIT 500`,
      [id]
    );

    const success = [];
    if (req.query.saved === '1') success.push('Series berhasil diupdate.');
    if (req.query.episode_saved === '1') success.push('Episode baru berhasil ditambahkan.');
    if (req.query.episode_updated === '1') success.push('Episode berhasil diupdate.');
    if (req.query.episode_deleted === '1') success.push('Episode berhasil dihapus.');

    res.render('admin-series-detail', {
      title: `Series #${id}`,
      series,
      genres,
      episodes,
      success,
      error: req.query.error ? String(req.query.error) : ''
    });
  } catch (err) {
    res.status(500).send(err.message);
  }
});

router.get('/series/:id/edit', async (req, res) => {
  const id = toInt(req.params.id, 0, 1, Number.MAX_SAFE_INTEGER);

  try {
    const [[series]] = await pool.query(
      `SELECT
         id, source_platform, source_series_id, content_origin, title, title2, series_slug, cover_url,
         type, status, rating, published_text, author, synopsis
       FROM series
       WHERE id = ?
       LIMIT 1`,
      [id]
    );

    if (!series) return res.status(404).send('Series tidak ditemukan');

    const [genres] = await pool.query(
      `SELECT g.name
       FROM series_genres sg
       JOIN genres g ON g.id = sg.genre_id
       WHERE sg.series_id = ?
       ORDER BY g.name ASC`,
      [id]
    );

    res.render('admin-series-edit', {
      title: `Edit Series #${id}`,
      series,
      genreCsv: genres.map((g) => g.name).join(', '),
      error: req.query.error ? String(req.query.error) : ''
    });
  } catch (err) {
    res.status(500).send(err.message);
  }
});

router.post('/series/:id/edit', async (req, res) => {
  const id = toInt(req.params.id, 0, 1, Number.MAX_SAFE_INTEGER);

  const payload = {
    source_platform: String(req.body.source_platform || '').trim(),
    source_series_id: String(req.body.source_series_id || '').trim(),
    content_origin: String(req.body.content_origin || '').trim(),
    title: String(req.body.title || '').trim(),
    title2: String(req.body.title2 || '').trim(),
    series_slug: cleanSeriesSlug(req.body.series_slug),
    cover_url: String(req.body.cover_url || '').trim(),
    type: String(req.body.type || '').trim(),
    status: String(req.body.status || '').trim(),
    rating: String(req.body.rating || '').trim(),
    published_text: String(req.body.published_text || '').trim(),
    author: String(req.body.author || '').trim(),
    synopsis: cleanSynopsis(req.body.synopsis),
    genres_csv: String(req.body.genres_csv || '')
  };

  if (!payload.source_platform || !payload.content_origin || !payload.title || !payload.series_slug) {
    return res.redirect(
      `/admin/series/${id}/edit?error=${encodeURIComponent('Field wajib: source_platform, content_origin, title, series_slug')}`
    );
  }

  const sourceSeriesId = Number(payload.source_series_id);
  if (!Number.isFinite(sourceSeriesId) || sourceSeriesId <= 0) {
    return res.redirect(`/admin/series/${id}/edit?error=${encodeURIComponent('source_series_id harus angka > 0')}`);
  }

  let ratingValue = null;
  if (payload.rating) {
    const parsed = Number(payload.rating);
    if (!Number.isFinite(parsed)) {
      return res.redirect(`/admin/series/${id}/edit?error=${encodeURIComponent('rating harus angka')}`);
    }
    ratingValue = parsed;
  }

  const genres = parseGenreCsv(payload.genres_csv);

  let conn;
  try {
    conn = await pool.getConnection();
    await conn.beginTransaction();

    const [[conflictSourceId]] = await conn.query(
      `SELECT id, title
       FROM series
       WHERE id <> ?
         AND source_platform = ?
         AND source_series_id = ?
       LIMIT 1`,
      [id, payload.source_platform, sourceSeriesId]
    );
    if (conflictSourceId) {
      await conn.rollback();
      return res.redirect(
        `/admin/series/${id}/edit?error=${encodeURIComponent(
          `Bentrok source id: ${payload.source_platform} + ${sourceSeriesId} sudah dipakai series #${conflictSourceId.id} (${conflictSourceId.title})`
        )}`
      );
    }

    const [[conflictSlug]] = await conn.query(
      `SELECT id, title, source_platform
       FROM series
       WHERE id <> ?
         AND series_slug = ?
       LIMIT 1`,
      [id, payload.series_slug]
    );
    if (conflictSlug) {
      await conn.rollback();
      return res.redirect(
        `/admin/series/${id}/edit?error=${encodeURIComponent(
          `Bentrok slug: ${payload.series_slug} sudah dipakai series #${conflictSlug.id} (${conflictSlug.title}) platform ${conflictSlug.source_platform}`
        )}`
      );
    }

    await conn.query(
      `UPDATE series
       SET source_platform = ?,
           source_series_id = ?,
           content_origin = ?,
           title = ?,
           title2 = NULLIF(?, ''),
           series_slug = ?,
           cover_url = NULLIF(?, ''),
           type = NULLIF(?, ''),
           status = NULLIF(?, ''),
           rating = ?,
           published_text = NULLIF(?, ''),
           author = NULLIF(?, ''),
           synopsis = NULLIF(?, ''),
           updated_at = CURRENT_TIMESTAMP
       WHERE id = ?
       LIMIT 1`,
      [
        payload.source_platform,
        sourceSeriesId,
        payload.content_origin,
        payload.title,
        payload.title2,
        payload.series_slug,
        payload.cover_url,
        payload.type,
        payload.status,
        ratingValue,
        payload.published_text,
        payload.author,
        payload.synopsis,
        id
      ]
    );

    await conn.query('DELETE FROM series_genres WHERE series_id = ?', [id]);

    for (const name of genres) {
      const [insertGenre] = await conn.query(
        'INSERT INTO genres (name) VALUES (?) ON DUPLICATE KEY UPDATE id = LAST_INSERT_ID(id)',
        [name]
      );
      const genreId = insertGenre.insertId;
      await conn.query('INSERT IGNORE INTO series_genres (series_id, genre_id) VALUES (?, ?)', [id, genreId]);
    }

    await conn.commit();
    return res.redirect(`/admin/series/${id}?saved=1`);
  } catch (err) {
    if (conn) await conn.rollback();
    if (err && err.code === 'ER_DUP_ENTRY') {
      const detail = String(err.sqlMessage || '').slice(0, 180);
      return res.redirect(
        `/admin/series/${id}/edit?error=${encodeURIComponent(`Data duplikat. Cek source_platform/source_series_id atau series_slug. Detail: ${detail}`)}`
      );
    }
    return res.redirect(`/admin/series/${id}/edit?error=${encodeURIComponent(err.message || 'Gagal update series')}`);
  } finally {
    if (conn) conn.release();
  }
});

router.post('/series/:id/delete', async (req, res) => {
  const id = toInt(req.params.id, 0, 1, Number.MAX_SAFE_INTEGER);

  try {
    const [result] = await pool.query('DELETE FROM series WHERE id = ? LIMIT 1', [id]);
    if (!result.affectedRows) {
      return res.redirect('/admin/series?error=Series tidak ditemukan');
    }
    return res.redirect(`/admin/series?deleted_id=${id}`);
  } catch (err) {
    return res.redirect(`/admin/series/${id}?error=${encodeURIComponent(err.message || 'Gagal delete series')}`);
  }
});

router.get('/episodes/:id/streams', async (req, res) => {
  const id = toInt(req.params.id, 0, 1, Number.MAX_SAFE_INTEGER);

  try {
    const [[episode]] = await pool.query(
      `SELECT
         e.id,
         e.series_id,
         e.source_episode_id,
         e.chapter_label,
         e.chapter_slug,
         s.title AS series_title
       FROM episodes e
       JOIN series s ON s.id = e.series_id
       WHERE e.id = ?
       LIMIT 1`,
      [id]
    );

    if (!episode) return res.status(404).send('Episode tidak ditemukan');

    const [streams] = await pool.query(
      `SELECT id, resolution, stream_source_id, stream_url, size_kb, created_at
       FROM episode_streams
       WHERE episode_id = ?
       ORDER BY resolution ASC, id ASC`,
      [id]
    );

    res.render('admin-episode-streams', {
      title: `Episode #${id} Streams`,
      episode,
      streams,
      success: req.query.saved === '1' ? 'Stream berhasil ditambahkan.' : (req.query.deleted === '1' ? 'Stream berhasil dihapus.' : ''),
      error: req.query.error ? String(req.query.error) : ''
    });
  } catch (err) {
    res.status(500).send(err.message);
  }
});

router.get('/episodes/:id/streams/create', async (req, res) => {
  const id = toInt(req.params.id, 0, 1, Number.MAX_SAFE_INTEGER);
  try {
    const [[episode]] = await pool.query(
      `SELECT
         e.id,
         e.series_id,
         e.source_episode_id,
         e.chapter_label,
         e.chapter_slug,
         s.title AS series_title
       FROM episodes e
       JOIN series s ON s.id = e.series_id
       WHERE e.id = ?
       LIMIT 1`,
      [id]
    );
    if (!episode) return res.status(404).send('Episode tidak ditemukan');

    res.render('admin-stream-create', {
      title: `Create Stream for Episode #${id}`,
      episode,
      error: req.query.error ? String(req.query.error) : ''
    });
  } catch (err) {
    res.status(500).send(err.message);
  }
});

router.post('/episodes/:id/streams/create', async (req, res) => {
  const id = toInt(req.params.id, 0, 1, Number.MAX_SAFE_INTEGER);

  const resolution = String(req.body.resolution || '').trim();
  const streamUrl = String(req.body.stream_url || '').trim();
  const sourceRaw = String(req.body.stream_source_id || '').trim();
  const sizeRaw = String(req.body.size_kb || '').trim();

  if (!resolution || !streamUrl) {
    return res.redirect(
      `/admin/episodes/${id}/streams/create?error=${encodeURIComponent('Field wajib: resolution, stream_url')}`
    );
  }

  const streamSourceId = sourceRaw === '' ? null : Number(sourceRaw);
  if (sourceRaw !== '' && (!Number.isFinite(streamSourceId) || streamSourceId <= 0)) {
    return res.redirect(
      `/admin/episodes/${id}/streams/create?error=${encodeURIComponent('stream_source_id harus kosong atau angka > 0')}`
    );
  }

  const sizeKb = sizeRaw === '' ? null : Number(sizeRaw);
  if (sizeRaw !== '' && (!Number.isFinite(sizeKb) || sizeKb < 0)) {
    return res.redirect(
      `/admin/episodes/${id}/streams/create?error=${encodeURIComponent('size_kb harus kosong atau angka >= 0')}`
    );
  }

  try {
    const [[episode]] = await pool.query('SELECT id FROM episodes WHERE id = ? LIMIT 1', [id]);
    if (!episode) {
      return res.redirect(`/admin/series?error=${encodeURIComponent('Episode tidak ditemukan')}`);
    }

    await pool.query(
      `INSERT INTO episode_streams (episode_id, resolution, stream_source_id, stream_url, size_kb)
       VALUES (?, ?, ?, ?, ?)`,
      [id, resolution, streamSourceId, streamUrl, sizeKb]
    );

    return res.redirect(`/admin/episodes/${id}/streams?saved=1`);
  } catch (err) {
    return res.redirect(
      `/admin/episodes/${id}/streams/create?error=${encodeURIComponent(err.message || 'Gagal create stream')}`
    );
  }
});

router.post('/streams/:id/delete', async (req, res) => {
  const id = toInt(req.params.id, 0, 1, Number.MAX_SAFE_INTEGER);
  try {
    const [[stream]] = await pool.query('SELECT id, episode_id FROM episode_streams WHERE id = ? LIMIT 1', [id]);
    if (!stream) {
      return res.redirect('/admin/series?error=Stream tidak ditemukan');
    }
    await pool.query('DELETE FROM episode_streams WHERE id = ? LIMIT 1', [id]);
    return res.redirect(`/admin/episodes/${stream.episode_id}/streams?deleted=1`);
  } catch (err) {
    return res.redirect(`/admin/series?error=${encodeURIComponent(err.message || 'Gagal delete stream')}`);
  }
});

module.exports = router;
