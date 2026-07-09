# Camel K Kafka Bridge — local dev + air-gapped deployment

Apache Camel K 2.10.1 integration that bridges Kafka `source-topic` → `sink-topic`,
with a local dev setup (k3d on Colima, macOS) and a full **air-gapped deployment kit**
for a RHEL x86_64 server running k3s (Nutanix).

## Layout

| Path | What |
|---|---|
| `kafka.yaml` | Single-node KRaft Kafka (`apache/kafka:3.9.1`), ns `kafka` — client listener secured with SASL/SCRAM-SHA-256 |
| `kafka-bridge.yaml` | The Camel route (YAML DSL): kafka:source-topic → log → kafka:sink-topic, authenticates via SCRAM |
| `rate-limit/` | Distributed rate limiting (SLA) via a Redis token bucket — **one Java plugin (`plugin/RateLimit.java`) + per-route YAML (`routes/*.yaml`)**, for Kafka + HTTP/HTTPS — **see `rate-limit/README.md`** |
| `monitoring/` | Helm values for local Grafana + Prometheus + Loki (dashboard `camel-k-routes`, login admin/camelk) |
| `airgap-bundle/` | Everything for the offline RHEL/k3s deployment — **start with `airgap-bundle/AIRGAP-DEPLOY.md`** |
| `airgap-bundle/manifests/` | registry (NodePort 30500), nginx Maven mirror + settings.xml, kafka, 5-second producer, kafka-bridge, redis, and the rate-limit plugin + `routes/` + echo-server + nodeport |

## Kafka security (SASL/SCRAM-SHA-256)

The broker's client listener (`9092`, the only one exposed by the Service) requires
SASL/SCRAM-SHA-256; unauthenticated clients are rejected. A loopback-only PLAINTEXT
listener (`9094`) inside the pod handles inter-broker traffic and admin commands
(topic creation, credential bootstrap). The SCRAM user is registered automatically
on broker start via a `postStart` hook.

Default dev credentials live in the `kafka-scram-credentials` Secret in `kafka.yaml`
(user `camel` / password `camel-bridge-2026`) — **change the password for anything
beyond local dev**. The producer and the Camel route read the same Secret.

The integration runs in the `camel-k` namespace, so copy the Secret there and pass
it to `kamel run`:

```bash
kubectl get secret kafka-scram-credentials -n kafka -o yaml \
  | sed 's/namespace: kafka/namespace: camel-k/' | kubectl apply -f -
kamel run kafka-bridge.yaml -n camel-k --name kafka-bridge \
  --config secret:kafka-scram-credentials
```

## Distributed rate limiting (Redis token bucket — plugin + YAML routes)

`rate-limit/` adds the `multi-route-bridge` integration, packaged as **one reusable Java
plugin plus per-route YAML**. `plugin/RateLimit.java` registers a `rateLimit` bean (a token
bucket run as an atomic Lua script in Redis) and defines no routes; each `routes/*.yaml`
file is a route that calls the bean and configures its own key/rate/burst inline. The SLA
is enforced **globally across all pods**, so replica count never changes the aggregate rate
(verified identical at 1 and 2 pods). Kafka sources block (backpressure); HTTP/HTTPS
(pod-terminated TLS on :8443) reject with `429 + Retry-After`. Add a route = drop another
YAML in `routes/`. Deploy, test script, and design rationale: `rate-limit/README.md`;
offline deployment: §8.1 of the air-gap guide.

## Large artifacts NOT in git (GitHub 100 MB limit)

**Download ready-made:** the complete transfer bundle (`camel-k-airgap-bundle.zip`, ~1 GB —
images incl. Redis, Maven repo incl. jedis/platform-http, CLIs, charts, manifests, guide) is
attached to the [v1.1.0 release](https://github.com/avi-blip/camel-k-project-avieli/releases/tag/v1.1.0):

```bash
gh release download v1.1.0 --repo avi-blip/camel-k-project-avieli
```

Or regenerate on any online machine:

```bash
# Container images (linux/amd64), ~700 MB
cd airgap-bundle/images
while read -r i; do docker pull --platform linux/amd64 "$i"; done < image-list-amd64.txt
docker save --platform linux/amd64 -o all-images-amd64.tar $(tr '\n' ' ' < image-list-amd64.txt)

# Maven repository, ~200 MB — extracted from a camel-k operator that has built ALL the
# routes at least once (kafka-bridge AND the rate-limit plugin+routes, so the repo
# includes camel-quarkus-platform-http + redis.clients:jedis + camel:http)
OP=$(kubectl get pod -n camel-k -l app=camel-k -o jsonpath='{.items[0].metadata.name}')
kubectl exec -n camel-k $OP -- tar czf - -C /etc/maven m2 > ../maven/camel-k-m2.tgz

# Linux amd64 CLIs
cd ../cli && mkdir -p linux-amd64 && cd linux-amd64
curl -sSLO https://dl.k8s.io/release/v1.36.2/bin/linux/amd64/kubectl
curl -sSL https://get.helm.sh/helm-v4.2.2-linux-amd64.tar.gz | tar xz --strip-components=1 linux-amd64/helm
curl -sSL https://github.com/apache/camel-k/releases/download/v2.10.1/camel-k-client-2.10.1-linux-amd64.tar.gz | tar xz kamel
chmod +x kubectl helm kamel

# Then zip the transfer bundle
cd ../../.. && zip -r camel-k-airgap-bundle.zip airgap-bundle -x "*.DS_Store"
```

Offline-build design (validated end to end): the operator builds integration images
in-cluster with Jib; Maven artifacts come from a bundled repo served by an nginx mirror
(`mirrorOf: *`), the Jib base image (`eclipse-temurin:17-jdk`) is seeded into the
in-cluster registry, and built kit images are pushed/pulled via that same registry over
plain HTTP. Details and troubleshooting: `airgap-bundle/AIRGAP-DEPLOY.md`.
