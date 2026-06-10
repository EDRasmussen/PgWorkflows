import http from "k6/http";
import { check } from "k6";

export const options = {
  vus: 20,
  iterations: 10000,
};

const BASE_URL = __ENV.BASE_URL || "http://localhost:8080";

export default function () {
  const payload = JSON.stringify({
    name: `user-${Math.random().toString(36).slice(2, 10)}`,
  });

  const res = http.post(`${BASE_URL}/greetings`, payload, {
    headers: { "Content-Type": "application/json" },
  });

  check(res, {
    "status is 202": (r) => r.status === 202,
    "has workflowRunId": (r) => r.json("workflowRunId") !== undefined,
  });
}
