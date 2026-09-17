// The only reason this file exists: Chrome will not offer "Install app" unless
// the origin registers a service worker with a fetch handler. It deliberately
// does not cache — the whole app is live market data, and a stale cache would
// show yesterday's prices with today's timestamp. Straight pass-through.
self.addEventListener("install", () => self.skipWaiting());
self.addEventListener("activate", (e) => e.waitUntil(self.clients.claim()));
self.addEventListener("fetch", (e) => { e.respondWith(fetch(e.request)); });
