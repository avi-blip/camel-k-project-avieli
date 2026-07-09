# Distributed rate limiting (SLA) — Redis token bucket, plugin + YAML routes

A per-key rate limit that is **global across all pods**: the limiter state lives in Redis,
not in the pod, so scaling the integration up or down never changes the effective rate.

The design separates the **limiter** (one reusable Java file) from the **routes** (plain
YAML). To add a rate-limited route you drop another YAML in `routes/` — no Java change.

## Layout

| Path | What |
|---|---|
| `plugin/RateLimit.java` | The **only** Java file. A RouteBuilder that defines no routes — it just registers the `rateLimit` bean (Redis token bucket). |
| `routes/*.yaml` | One route per file. Each calls the `rateLimit` bean and configures its own key/rate/burst inline. |
| `redis.yaml` | Redis (ns `redis`) — the shared bucket store. |
| `echo-server.yaml` | Tiny nginx used as the downstream target of the `https-to-https` route. |
| `multi-route-nodeport.yaml` | Exposes the bridge's HTTP (8080→30081) and pod-TLS HTTPS (8443→30444) listeners as NodePorts. |
| `MultiRouteLoaders.java` | Optional load generators — one timer per route (50 ms ≈ 20 msg/s) to drive traffic well over each SLA so rejections are visible. |
| `test-http-rate.sh` | Fires HTTP requests for a fixed window and reports 202 vs 429. |

## Algorithm

**Token bucket as an atomic Redis Lua script** (`plugin/RateLimit.java`):

- One Redis hash per key (`ratelimit:<key>`) holding `tokens` + last refill `ts`.
- The script refills continuously (`elapsed * rate`, capped at burst capacity) and takes a
  token in the same atomic step — no race between pods.
- Time comes from `redis.call('TIME')` (the Redis server clock), so pod clock skew can't
  distort the SLA.
- Chosen over fixed/sliding window: token bucket gives a smooth sustained rate *and*
  tolerates short bursts up to capacity — the standard choice for client SLAs
  (Stripe, AWS API Gateway, Spring Cloud Gateway all use it).

## Per-source semantics

| Source | Over-limit behaviour | Bean call |
|---|---|---|
| Kafka | **Block** the consumer until a token is free (backpressure — nothing dropped, lag grows instead) | `block('<key>', <rate>, <burst>)` |
| HTTP / HTTPS | **Reject** with `429` + `Retry-After: 1` and stop the route | `http(${exchange}, '<key>', <rate>, <burst>)` |

> `${exchange}` is mandatory on the HTTP call: Camel binds an explicit argument list
> positionally and will **not** auto-inject the `Exchange` (omitting it throws
> `ParameterBindingException`, trying to convert the String key into an `Exchange`).

## Routes shipped here

| Route (id) | Source | SLA | Burst | Sink |
|---|---|---|---|---|
| `kafka-to-kafka` | `kafka:kk-source` | 10 msg/s | 20 | `kafka:kk-sink` |
| `https-to-https` | `platform-http:/ingest/hh` (8080 + pod-TLS 8443) | 5 req/s | 10 | `http:echo-server/echo` |
| `https-to-kafka` | `platform-http:/ingest/hk` (8080 + pod-TLS 8443) | 5 req/s | 10 | `kafka:hk-sink` |

## How a YAML route wires the limiter

```yaml
- route:
    id: https-to-kafka
    from:
      uri: "platform-http:/ingest/hk"
      steps:
        - bean:
            ref: rateLimit
            method: "http(${exchange}, 'hk', 5.0, 10)"   # ← configure the SLA here
        - to:
            uri: "kafka:hk-sink"
            parameters: { brokers: ..., securityProtocol: SASL_PLAINTEXT, ... }
```

## Deploy

```sh
kubectl apply -f redis.yaml          # Redis (ns redis)
kubectl apply -f echo-server.yaml    # downstream for https-to-https

# One plugin + every route in routes/
kamel run plugin/RateLimit.java routes/*.yaml \
  --name multi-route-bridge -n camel-k \
  --config secret:kafka-scram-credentials \
  -d mvn:redis.clients:jedis:5.2.0 -d camel:http \
  --resource secret:ratelimit-tls@/etc/tls \
  -p quarkus.http.ssl-port=8443 \
  -p quarkus.http.ssl.certificate.files=/etc/tls/tls.crt \
  -p quarkus.http.ssl.certificate.key-files=/etc/tls/tls.key \
  -t prometheus.enabled=true -t prometheus.pod-monitor=false

kubectl apply -f multi-route-nodeport.yaml   # optional: external HTTP/HTTPS access
```

Scale (SLA is unaffected — that's the point):

```sh
kubectl scale integration multi-route-bridge -n camel-k --replicas=2
```

Use `kubectl scale integration` — **not** `kubectl scale deployment`, which the operator
immediately reverts. Verified: aggregate throughput identical at 1 and 2 pods.

## Adding a new rate-limited route

Drop a new file in `routes/`, e.g. `routes/https-to-log.yaml`, and call the bean with a
fresh bucket key. Re-run the same `kamel run` (the `routes/*.yaml` glob picks it up). No
change to `RateLimit.java`.

## Verify

```sh
# Traffic generators (optional — drives all three routes over their SLA)
kamel run MultiRouteLoaders.java --name multi-route-loaders -n camel-k \
  --config secret:kafka-scram-credentials -d camel:http \
  -t prometheus.enabled=true -t prometheus.pod-monitor=false

# HTTP: port-forward the integration service, then hammer a route
kubectl port-forward -n camel-k svc/multi-route-bridge 8080:80 &
./test-http-rate.sh http://localhost:8080/ingest/hh 10   # ~5 accepted/s + burst, rest 429
```

The aggregate 202 rate stays at the SLA no matter how many pods serve the requests, and
`429`s appear as soon as callers exceed it. Kafka rejections show as consumer lag, not
errors (blocking backpressure).

## Air-gap note

Uses only artifacts already in the bundle: `redis.clients:jedis` + camel-quarkus-platform-http
+ camel:http (in the bundled Maven repo) and `redis:7.4-alpine` / `nginx:1.27-alpine`
(in the image tar). Offline deployment steps: §8.1 of `airgap-bundle/AIRGAP-DEPLOY.md`.
