import http from 'k6/http';
import { check, sleep } from 'k6';
import { Trend, Rate, Counter } from 'k6/metrics';

export const writeLatency = new Trend('task_write_latency', true);
export const writeFailures = new Rate('task_write_failures');
export const idemReplays = new Counter('task_write_idem_replays');

const API_URL = __ENV.API_URL || 'http://127.0.0.1:80';
const TOKEN = __ENV.TOKEN || '';
const PROJECT_CODE = __ENV.PROJECT_CODE || '';
const STAGE_CODE = __ENV.STAGE_CODE || '';
const ASSIGN_TO = __ENV.ASSIGN_TO || '';

export const options = {
  scenarios: {
    task_write: {
      executor: 'ramping-vus',
      startVUs: 1,
      stages: [
        { duration: '30s', target: 20 },
        { duration: '2m', target: 20 },
        { duration: '30s', target: 0 },
      ],
      gracefulRampDown: '10s',
    },
  },
  thresholds: {
    http_req_duration: ['p(95)<800', 'p(99)<1500'],
    task_write_failures: ['rate<0.01'],
  },
  summaryTrendStats: ['avg', 'min', 'med', 'p(90)', 'p(95)', 'p(99)', 'max'],
};

function headers(requestId) {
  return {
    'Content-Type': 'application/x-www-form-urlencoded',
    Authorization: `Bearer ${TOKEN}`,
    'X-Tenant-Id': __ENV.TENANT_ID || '1',
    'X-Request-Id': requestId,
  };
}

export default function () {
  const mode = __ENV.REQUEST_ID_MODE || 'unique';
  const seed = mode === 'fixed' ? 'loadtest-fixed' : `${__VU}-${__ITER}`;
  const name = `loadtest_task_${seed}`;

  const payload = [
    `name=${encodeURIComponent(name)}`,
    `project_code=${encodeURIComponent(PROJECT_CODE)}`,
    `stage_code=${encodeURIComponent(STAGE_CODE)}`,
    `assign_to=${encodeURIComponent(ASSIGN_TO)}`,
  ].join('&');

  const res = http.post(`${API_URL}/iteration/requirement/save`, payload, {
    headers: headers(seed),
    tags: { name: 'requirement_save' },
  });

  writeLatency.add(res.timings.duration);

  const ok = check(res, {
    'status is 200': (r) => r.status === 200,
    'business code is 200': (r) => {
      try {
        return JSON.parse(r.body).code === 200;
      } catch (e) {
        return false;
      }
    },
  });
  writeFailures.add(!ok);

  if (res.status === 200) {
    try {
      const body = JSON.parse(res.body);
      if (body.msg && String(body.msg).indexOf('重复') >= 0) {
        idemReplays.add(1);
      }
    } catch (e) {
      idemReplays.add(0);
    }
  }

  sleep(0.5);
}
