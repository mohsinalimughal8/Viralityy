const CACHE_NAME = 'viralityy-v1';
const OFFLINE_URL = '/';

// App shell — resources to cache on install
const PRECACHE_URLS = [
  '/',
  'https://fonts.googleapis.com/css2?family=DM+Serif+Display:ital@0;1&family=DM+Sans:wght@300;400;500;600;700&family=DM+Mono&display=swap',
];

// ── Install: cache the app shell ──────────────────────────────────────────────
self.addEventListener('install', event => {
  event.waitUntil(
    caches.open(CACHE_NAME).then(cache =>
      cache.addAll(PRECACHE_URLS).catch(() => {})
    ).then(() => self.skipWaiting())
  );
});

// ── Activate: clean up old caches ─────────────────────────────────────────────
self.addEventListener('activate', event => {
  event.waitUntil(
    caches.keys().then(keys =>
      Promise.all(
        keys.filter(k => k !== CACHE_NAME).map(k => caches.delete(k))
      )
    ).then(() => self.clients.claim())
  );
});

// ── Fetch: network-first with offline fallback ─────────────────────────────────
self.addEventListener('fetch', event => {
  const { request } = event;
  const url = new URL(request.url);

  // Never intercept API, auth, or non-GET requests
  if (request.method !== 'GET') return;
  if (url.pathname.startsWith('/api') || url.pathname.startsWith('/auth') || url.pathname.startsWith('/webhooks')) return;

  // Navigation requests (HTML pages): network-first, serve cached app shell offline
  if (request.mode === 'navigate') {
    event.respondWith(
      fetch(request)
        .then(response => {
          // Refresh the app shell cache on successful navigation
          const clone = response.clone();
          caches.open(CACHE_NAME).then(cache => cache.put(OFFLINE_URL, clone));
          return response;
        })
        .catch(async () => {
          const cached = await caches.match(OFFLINE_URL);
          if (cached) return cached;
          // Hard offline fallback
          return new Response(
            `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Viralityy — Offline</title>
<style>
  *{margin:0;padding:0;box-sizing:border-box}
  body{background:#07070d;color:#e8e8f5;font-family:system-ui,sans-serif;
       display:flex;flex-direction:column;align-items:center;justify-content:center;
       min-height:100vh;text-align:center;padding:2rem;gap:1rem}
  .logo{width:64px;height:64px;background:linear-gradient(135deg,#6366f1,#ec4899);
        border-radius:16px;display:flex;align-items:center;justify-content:center;
        font-size:2rem;font-weight:700;color:#fff;margin-bottom:.5rem}
  h1{font-size:1.4rem;font-weight:700}
  p{font-size:.9rem;color:rgba(232,232,245,.55);max-width:320px;line-height:1.6}
  button{margin-top:.5rem;padding:.6rem 1.5rem;background:#6366f1;color:#fff;
         border:none;border-radius:8px;font-size:.85rem;font-weight:600;cursor:pointer}
</style>
</head>
<body>
  <div class="logo">V</div>
  <h1>You are offline</h1>
  <p>Viralityy needs an internet connection. Check your network and try again.</p>
  <button onclick="location.reload()">Retry</button>
</body>
</html>`,
            { headers: { 'Content-Type': 'text/html' } }
          );
        })
    );
    return;
  }

  // Static assets: stale-while-revalidate
  if (url.origin === location.origin || url.hostname === 'fonts.googleapis.com' || url.hostname === 'fonts.gstatic.com') {
    event.respondWith(
      caches.match(request).then(cached => {
        const networkFetch = fetch(request).then(response => {
          if (response.ok) {
            caches.open(CACHE_NAME).then(cache => cache.put(request, response.clone()));
          }
          return response;
        }).catch(() => cached);
        return cached || networkFetch;
      })
    );
  }
});
