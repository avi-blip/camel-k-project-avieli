# Per-client rate limiting (SLA) — Redis token bucket

Camel K integration `rate-limited-bridge` enforces a per-client SLA that is **global
across all pods**: the limiter state lives in Redis, not in the pod, so scaling the
integration up or down never changes a client's effective rate.

## Algorithm

**Token bucket as an atomic Redis Lua script** (`RateLimitedRoutes.java`):

- One Redis hash per client (`ratelimit:<client>`) holding `tokens` + last refill `ts`.
- The script refills continuously (`elapsed * rate`, capped at burst capacity) and takes a
  token in the same atomic step — no race between pods.
- Time comes from `redis.call('TIME')` (the Redis server clock), so pod clock skew can't
  distort the SLA.
- Chosen over fixed/sliding window: token bucket gives a smooth sustained rate *and*
  tolerates short bursts up to capacity — the standard choice for client SLAs
  (Stripe, AWS API Gateway, Spring Cloud Gateway all use it).

## Per-source semantics

| Source | Over-limit behaviour |
|---|---|
| Kafka | **Block** the consumer until a token is free (backpressure — nothing dropped, lag grows instead) |
| HTTP / HTTPS | **Reject** with `429` + `Retry-After: 1` |

## Routes (one per client)

| Route | Source | SLA | Burst | Sink |
|---|---|---|---|---|
| `client-a-kafka` | `kafka:client-a-topic` | 5 msg/s | 10 | `client-a-sink` |
| `client-b-kafka` | `kafka:client-b-topic` | 2 msg/s | 4 | `client-b-sink` |
| `client-c-http` | `platform-http:/ingest/client-c` (HTTP + HTTPS via Traefik ingress) | 3 req/s | 6 | `client-c-sink` |

## Deploy

```sh
kubectl apply -f redis.yaml                 # Redis (ns redis)
kubectl apply -f ingress-https.yaml         # HTTPS: TLS ingress for host ratelimit.local
kamel run RateLimitedRoutes.java --name rate-limited-bridge -n camel-k \
  --config secret:kafka-scram-credentials \
  -d mvn:redis.clients:jedis:5.2.0 \
  -t prometheus.enabled=true -t prometheus.pod-monitor=false
```

Scale (SLA is unaffected — that's the point):

```sh
kubectl scale deployment rate-limited-bridge -n camel-k --replicas=2
```

`client-a-topic` / `client-b-topic` have 3 partitions so multiple pods actually share
consumption; sinks have 1 partition to make rate measurement trivial.

## Verify

```sh
# Kafka: burst 60 msgs in, watch them drain at the SLA rate
./test-kafka-rate.sh client-a 60     # expect ~5 msg/s
./test-kafka-rate.sh client-b 30     # expect ~2 msg/s

# HTTP (port-forward the integration service)
kubectl port-forward -n camel-k svc/rate-limited-bridge 8080:80 &
./test-http-rate.sh http://localhost:8080/ingest/client-c 10          # ~3 accepted/s + burst

# HTTPS (port-forward traefik, SNI host ratelimit.local, self-signed cert)
kubectl port-forward -n kube-system svc/traefik 8443:443 &
./test-http-rate.sh "https://ratelimit.local:8443/ingest/client-c" 10 \
  -k --resolve ratelimit.local:8443:127.0.0.1
```

Repeat any test after scaling to 2 replicas: the aggregate rate stays at the SLA.

## Air-gap note

The integration adds Maven artifacts not present in the original mirror snapshot
(`redis.clients:jedis` + camel-quarkus-platform-http stack). The local mirror hostPath
(`/opt/maven-repo/m2` on the k3d node) was extended with them, the air-gap `settings.xml`
restored, `airgap-bundle/maven/camel-k-m2.tgz` regenerated from the mirror (4,238 files),
and `redis:7.4-alpine` added to the image list. Offline deployment steps live in
`airgap-bundle/AIRGAP-DEPLOY.md` §8.1 (deployables are copied into
`airgap-bundle/manifests/`).
