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

Default port: `3001`

## Struktur utama
- `src/routes/admin.js` -> seluruh controller/admin route
- `views/admin-*.ejs` -> template halaman admin
- `public/admin.css` -> style admin
