function toInt(value, fallback, min = 1, max = 100) {
  const n = Number(value);
  if (!Number.isFinite(n)) return fallback;
  const i = Math.trunc(n);
  if (i < min) return min;
  if (i > max) return max;
  return i;
}

function escapeLike(input) {
  return String(input || '').replace(/[\\%_]/g, '\\$&');
}

module.exports = {
  toInt,
  escapeLike
};
