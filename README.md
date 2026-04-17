# Admin Anibiplay (Admin-Only Server)

Project ini berisi endpoint dan UI admin yang diambil dari `server-anibi`, tanpa route API publik lainnya.

## Scope
- Route aktif: `/admin/*`
- Tidak include: `/api/*`
- Tambahan halaman admin:
  - `/admin/episode-error-reports`
  - `/admin/list-banned`
  - `/admin/topanime`

## Menjalankan
1. Copy env:
   ```bash
   cp .env.example .env
   ```
2. Isi koneksi database dan kredensial admin di `.env`
3. Install dependency:
   ```bash
   npm install
   ```
4. Jalankan server:
   ```bash
   npm run dev
   ```

## Donghub Updater (Python)
Script: `scripts/donghub_updater.py`

Env script terpisah (tidak pakai `.env` admin):
```bash
cp scripts/.env.example scripts/.env
```

Contoh:
```bash
python3 scripts/donghub_updater.py --dry-run
python3 scripts/donghub_updater.py --limit 30 --sync-mode full_sync
python3 scripts/donghub_updater.py --source-series-id 3135
```

Opsional custom env file:
```bash
python3 scripts/donghub_updater.py --env-file /path/to/script.env --dry-run
```

## Bot Donghua (Simple + Notify)
Script: `scripts/bot-anibi-donghua.py`

Contoh:
```bash
python3 scripts/bot-anibi-donghua.py --dry-run
python3 scripts/bot-anibi-donghua.py --limit 20
python3 scripts/bot-anibi-donghua.py --source-series-id 3135 --notify
```

## Bot AnimeKita dari Otakudesu Ongoing
Script: `scripts/bot-anibi-otakudesu.py`

Contoh pakai source slug dari Otakudesu:
```bash
python3 scripts/bot-anibi-otakudesu.py --otakudesu-max-pages 5
python3 scripts/bot-anibi-otakudesu.py --otakudesu-max-pages 5 --notify
```

Pastikan dependency Python terpasang:
```bash
pip install pycryptodome pymysql requests
```

Default port: `3001`

## Struktur utama
- `src/routes/admin.js` -> seluruh controller/admin route
- `views/admin-*.ejs` -> template halaman admin
- `public/admin.css` -> style admin
