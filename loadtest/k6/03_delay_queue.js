import http from 'k6/http';
import { check, sleep } from 'k6';
import { Trend, Counter } from 'k6/metrics';

export const queuePending = new Trend('delay_queue_pending');
export const queueInflight = new Trend('delay_queue_inflight');
export const delayScheduled = new Counter('delay_scheduled_total');

const PROJECT_URL = __ENV.PROJECT_URL || 'http://127.0.0.1:8081';
const METRICS_URL = __ENV.METRICS_URL || 'http://127.0.0.1:9091/metrics';
const TASK_ID = __ENV.TASK_ID || '1';
const SCHEDULE_RATE = Number(__ENV.SCHEDULE_RATE || 20);
const DURATION = __ENV.DURATION || '2m';

export const options = {
  scenarios: {
    delay_schedule: {
      executor: 'constant-arrival-rate',
      rate: SCHEDULE_RATE,
      timeUnit: '1s',
      duration: DURATION,
      preAllocatedVUs: 20,
      maxVUs: 100,
      exec: 'schedule',
    },
    queue_probe: {
      executor: 'constant-arrival-rate',
      rate: 1,
      timeUnit: '1s',
      duration: DURATION,
      preAllocatedVUs: 2,
      maxVUs: 4,
      exec: 'probe',
    },
  },
};

export function schedule() {
  const taskId = `${TASK_ID}${__VU}${__ITER}`;
  const payload = `type=task_remind&taskId=${taskId}`;
  const res = http.post(`${PROJECT_URL}/iteration/delay/schedule`, payload, {
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
  });

  const ok = check(res, {
    'schedule status is 200': (r) => r.status === 200,
    'schedule business code is 200': (r) => {
      try {
        return JSON.parse(r.body).code === 200;
      } catch (e) {
        return false;
      }
    },
  });
  if (ok) {
    delayScheduled.add(1);
  }
  sleep(0.05);
}

export function probe() {
  const res = http.post(`${PROJECT_URL}/iteration/delay/stats`, '', {
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
  });

  check(res, { 'stats status is 200': (r) => r.status === 200 });

  try {
    const body = JSON.parse(res.body);
    if (body.data) {
      queuePending.add(Number(body.data.pending || 0));
      queueInflight.add(Number(body.data.inflight || 0));
    }
  } catch (e) {
    queuePending.add(0);
  }

  const metrics = http.get(METRICS_URL, { tags: { name: 'metrics_scrape' } });
  check(metrics, { 'metrics reachable': (r) => r.status === 200 });

  sleep(1);
}
