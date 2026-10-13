import http from 'k6/http';
import { check, sleep } from 'k6';

const API_URL = __ENV.API_URL || 'http://127.0.0.1:80';
const TOKEN = __ENV.TOKEN || '';
const PROJECT_CODE = __ENV.PROJECT_CODE || '';
const TASK_CODE = __ENV.TASK_CODE || '';

export const options = {
  scenarios: {
    read_heavy: {
      executor: 'constant-arrival-rate',
      rate: Number(__ENV.RATE || 300),
      timeUnit: '1s',
      duration: __ENV.DURATION || '3m',
      preAllocatedVUs: 50,
      maxVUs: 400,
    },
  },
  thresholds: {
    http_req_duration: ['p(95)<300', 'p(99)<800'],
    http_req_failed: ['rate<0.005'],
  },
  summaryTrendStats: ['avg', 'min', 'med', 'p(90)', 'p(95)', 'p(99)', 'max'],
};

function headers() {
  return {
    'Content-Type': 'application/x-www-form-urlencoded',
    Authorization: `Bearer ${TOKEN}`,
    'X-Tenant-Id': __ENV.TENANT_ID || '1',
  };
}

export default function () {
  const now = Date.now();

  const endpoints = [
    ['/iteration/index', ''],
    ['/iteration/node', ''],
    ['/iteration/requirement/log', `taskCode=${encodeURIComponent(TASK_CODE)}&page=1&pageSize=10`],
    ['/iteration/analysis/interation/delivery', `projectCode=${encodeURIComponent(PROJECT_CODE)}&beginTime=${now - 30 * 24 * 3600 * 1000}&endTime=${now}`],
  ];

  const pick = endpoints[__ITER % endpoints.length];
  const res = http.post(`${API_URL}${pick[0]}`, pick[1], {
    headers: headers(),
    tags: { name: pick[0] },
  });

  check(res, {
    'status is 200': (r) => r.status === 200,
    'business code is 200': (r) => {
      try {
        return JSON.parse(r.body).code === 200;
      } catch (e) {
        return false;
      }
    },
  });

  sleep(0.1);
}
