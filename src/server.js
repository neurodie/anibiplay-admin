require('dotenv').config();

const path = require('path');
const express = require('express');
const session = require('express-session');

const { pool } = require('./db');
const adminRoutes = require('./routes/admin');

const app = express();
const port = Number(process.env.PORT || 3001);
const bodyLimit = String(process.env.BODY_LIMIT || '50mb').trim() || '50mb';

app.set('view engine', 'ejs');
app.set('views', path.join(__dirname, '..', 'views'));

app.use(express.urlencoded({ extended: true, limit: bodyLimit }));
app.use(express.json({ limit: bodyLimit }));
app.use('/public', express.static(path.join(__dirname, '..', 'public')));

app.use(
  session({
    name: 'anibiplay.admin.sid',
    secret: process.env.SESSION_SECRET || 'replace-this-secret',
    resave: false,
    saveUninitialized: false,
    cookie: {
      httpOnly: true,
      sameSite: 'lax',
      secure: process.env.NODE_ENV === 'production',
      maxAge: 1000 * 60 * 60 * 12
    }
  })
);

app.get('/', (_req, res) => {
  res.json({
    name: 'anibiplay-admin-server',
    status: 'ok',
    admin: '/admin/login'
  });
});

app.use('/admin', adminRoutes);

app.use((req, res) => {
  res.status(404).json({ error: 'Not found' });
});

app.use((err, req, res, _next) => {
  console.error(err);
  const status = Number(err && (err.status || err.statusCode)) || 500;
  if (status === 413 || err.type === 'entity.too.large') {
    return res.status(413).json({ error: `Payload too large. Current BODY_LIMIT=${bodyLimit}` });
  }
  res.status(status).json({ error: err && err.message ? err.message : 'Internal server error' });
});

async function bootstrap() {
  try {
    await pool.query('SELECT 1');
    app.listen(port, () => {
      console.log(`Admin server running on http://0.0.0.0:${port}`);
    });
  } catch (err) {
    console.error('Failed to start admin server:', err.message);
    process.exit(1);
  }
}

bootstrap();
