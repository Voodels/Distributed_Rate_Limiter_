import http from 'k6/http';
import { check, sleep } from 'k6';

export const options = {
  iterations: 1,
  vus: 1,
};

const BASE_URL = 'http://localhost:8081';
const CLIENT_ID = 'test_tasali';

export default function () {
  const results = [];

  for (let burst = 0; burst < 5; burst++) {
    console.log(`\n=== Burst ${burst + 1} ===`);
    let retryAfter = 0;
    let got429 = false;
    let hitCount = 0;

    // 1. Hammer rapidly to exhaust tokens
    for (let i = 0; i < 15; i++) {
      const start = Date.now();
      const res = http.get(`${BASE_URL}/api/v1/products`, {
        headers: { 'X-API-Key': CLIENT_ID },
      });
      const elapsed = Date.now() - start;
      hitCount++;

      if (res.status === 429) {
        const body = JSON.parse(res.body);
        retryAfter = body.retryAfter;
        got429 = true;
        console.log(`  Req ${i+1}: 429 | retryAfter=${retryAfter}s | latency=${elapsed}ms`);
        results.push({ burst: burst+1, req: i+1, status: 429, retryAfter, latency: elapsed });
        break;
      } else if (res.status === 200) {
        const remaining = res.headers['X-RateLimit-Remaining'] || '?';
        console.log(`  Req ${i+1}: 200 | remaining=${remaining} | latency=${elapsed}ms`);
        results.push({ burst: burst+1, req: i+1, status: 200, remaining, latency: elapsed });
      }
    }

    check(null, { 'got rate limited': () => got429 });

    if (!got429) {
      console.log('  Never got rate limited — stopping');
      break;
    }

    // 2. Wait exactly retryAfter seconds
    const waitTime = Math.max(0, retryAfter - (hitCount * 0.003));
    console.log(`  Waiting ${waitTime.toFixed(1)}s...`);
    sleep(waitTime);

    // 3. Hit again — should succeed
    const res = http.get(`${BASE_URL}/api/v1/products`, {
      headers: { 'X-API-Key': CLIENT_ID },
    });
    const elapsed = Date.now() - res.timings.waiting;

    const allowed = res.status === 200;
    check(res, { 'succeeded after retryAfter wait': () => allowed });

    const remaining = res.headers['X-RateLimit-Remaining'] || '?';
    console.log(`  After wait: ${res.status} | remaining=${remaining} | latency=${elapsed}ms`);
    results.push({ burst: burst+1, req: 'after-wait', status: res.status, remaining, latency: elapsed });

    // 4. Exhaust again immediately to see retryAfter pattern
    if (allowed) {
      for (let i = 0; i < 12; i++) {
        const r = http.get(`${BASE_URL}/api/v1/products`, {
          headers: { 'X-API-Key': CLIENT_ID },
        });
        if (r.status === 429) {
          const body = JSON.parse(r.body);
          console.log(`  Re-exhausted at ${i+1}: 429 retryAfter=${body.retryAfter}s`);
          results.push({ burst: burst+1, req: `re-exhaust-${i+1}`, status: 429, retryAfter: body.retryAfter });
          break;
        }
        results.push({ burst: burst+1, req: `re-exhaust-${i+1}`, status: r.status });
      }
    }

    sleep(0.5);
  }

  // Summary
  console.log('\n============== SUMMARY ==============');
  for (const r of results) {
    if (r.status === 429) {
      console.log(`Burst ${r.burst} Req ${r.req}: 429 retryAfter=${r.retryAfter}s latency=${r.latency || '?'}ms`);
    } else if (r.status === 200) {
      console.log(`Burst ${r.burst} Req ${r.req}: 200 remaining=${r.remaining || '?'} latency=${r.latency || '?'}ms`);
    }
  }
  console.log('====================================');
}
