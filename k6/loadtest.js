import http from 'k6/http';
import { check, sleep } from 'k6';
import { Counter, Rate, Trend } from 'k6/metrics';

// Custom metrics
const rejectedRequests = new Counter('rejected_requests');
const allowedRequests  = new Counter('allowed_requests');
const rejectionRate    = new Rate('rejection_rate');
const rlLatency        = new Trend('rl_latency_ms');

export const options = {
  stages: [
    { duration: '10s', target: 10  },  // warm up
    { duration: '30s', target: 100 },  // ramp up
    { duration: '30s', target: 500 },  // stress
    { duration: '10s', target: 0   },  // ramp down
  ],
  thresholds: {
    http_req_duration:  ['p(95)<100'],  // 95% of requests under 100ms
    rejection_rate:     ['rate<0.3'],   // less than 30% rejected overall
    rl_latency_ms:      ['p(99)<50'],   // 99th percentile under 50ms
  },
};

const BASE_URL = 'http://localhost:8081';

export default function () {
  // Simulate different clients
  const clientId = `client-${Math.floor(Math.random() * 2how 0)}`;

  const start = Date.now();

  const res = http.get(`${BASE_URL}/api/v1/products`, {
    headers: { 'X-API-Key': clientId },
  });

  rlLatency.add(Date.now() - start);

  const allowed  = res.status === 200;
  const rejected = res.status === 429;

  check(res, {
    'status is 200 or 429': (r) => r.status === 200 || r.status === 429,
    'has X-RateLimit-Remaining header': (r) => r.headers['X-Ratelimit-Remaining'] !== undefined,
    'has Retry-After on 429': (r) => r.status !== 429 || r.headers['Retry-After'] !== undefined,
  });

  if (allowed)  allowedRequests.add(1);
  if (rejected) {
    rejectedRequests.add(1);
    rejectionRate.add(1);
  } else {
    rejectionRate.add(0);
  }

  sleep(0.1);
}

export function handleSummary(data) {
  return {
    'stdout': JSON.stringify({
      allowed:   data.metrics.allowed_requests?.values?.count  ?? 0,
      rejected:  data.metrics.rejected_requests?.values?.count ?? 0,
      p95_ms:    data.metrics.http_req_duration?.values?.['p(95)'] ?? 0,
      p99_rl_ms: data.metrics.rl_latency_ms?.values?.['p(99)']     ?? 0,
    }, null, 2),
  };
}
