# Air-Gapped Deployment Guide — Camel K + Kafka on RHEL / k3s

Deploy Apache Camel K 2.10.1, a single-node Kafka, the `kafka-bridge` (Kafka→Kafka)
integration, a 5-second test-data producer, and (optionally) the `multi-route-bridge`
integration — distributed rate limiting backed by Redis, delivered as one reusable Java
plugin plus per-route YAML files, for Kafka and HTTP/HTTPS sources (§8.1) — on an
**air-gapped RHEL server (x86_64) that already runs a k3s cluster** (e.g. on Nutanix AHV),
using only the artifacts in this bundle. **No internet access is required at any step.**

Key design decision: Camel K normally downloads Maven artifacts from the internet to
build integration images. This bundle ships the **complete Maven repository** (validated:
a from-scratch build pulled 2,812 artifacts exclusively from it) and an **offline Maven
mirror** you deploy in the cluster, so the operator builds integrations entirely offline —
on the correct CPU architecture, since images are built in-cluster.

---

## 1. Bundle contents

```
airgap-bundle/
├── AIRGAP-DEPLOY.md                ← this guide
├── images/
│   ├── all-images-amd64.tar        ← all 6 container images, linux/amd64 (~750 MB)
│   └── image-list-amd64.txt
├── charts/
│   └── camel-k-2.10.1.tgz          ← Camel K operator Helm chart
├── cli/linux-amd64/
│   └── kubectl (v1.36.2), helm (v4.2.2), kamel (2.10.1)   ← static ELF x86-64 binaries
├── maven/
│   └── camel-k-m2.tgz              ← complete Maven repo (~4,346 files, 202 MB compressed; incl. jedis + platform-http + brotli4j native-linux-x86_64)
└── manifests/
    ├── registry.yaml               ← in-cluster registry (NodePort 30500)
    ├── maven-mirror.yaml           ← nginx Maven mirror + settings.xml ConfigMap
    ├── kafka.yaml                  ← namespace + single-node KRaft Kafka (SASL/SCRAM-SHA-256) + credentials Secret
    ├── kafka-producer.yaml         ← produces a message to source-topic every 5 s (SCRAM-authenticated)
    ├── kafka-bridge.yaml           ← the Camel route (source-topic → sink-topic, SCRAM-authenticated)
    ├── redis.yaml                  ← Redis (ns redis) — shared state for distributed rate limiting
    ├── RateLimit.java              ← the rate-limit plugin: registers the `rateLimit` bean (Redis token bucket), no routes
    ├── routes/*.yaml               ← rate-limited routes (kafka-to-kafka, https-to-https, https-to-kafka) — configure SLA inline
    ├── echo-server.yaml            ← tiny nginx, downstream target of the https-to-https route
    ├── MultiRouteLoaders.java      ← optional load generators (one timer per route) to drive traffic over the SLA
    └── multi-route-nodeport.yaml   ← exposes the bridge HTTP (30081) + pod-TLS HTTPS (30444) listeners
```

Versions: Camel K 2.10.1 (runtime catalog 3.15.3, Camel 4.8.5, Quarkus/JVM mode, Jib publish,
base image `eclipse-temurin:17-jdk`) · Kafka 3.9.1 (KRaft, `apache/kafka`) · registry:2 ·
nginx:1.27-alpine.

Images in the tar: `apache/camel-k:2.10.1`, `apache/kafka:3.9.1`, `registry:2`,
`eclipse-temurin:17-jdk` (Jib base image — required for builds), `nginx:1.27-alpine`,
`redis:7.4-alpine` (rate-limiter state).

## 2. Assumptions & prerequisites

- RHEL x86_64 server, k3s installed and healthy (`kubectl get nodes` works). Single-node
  assumed; for multi-node see the note in §8.
- Root/sudo access (image import, `/opt` directories, k3s registry config).
- ~5 GB free disk for images + registry, ~1 GB for the Maven repo.
- Transfer `camel-k-airgap-bundle.zip` to the server, then:
  `unzip camel-k-airgap-bundle.zip && cd airgap-bundle`

### 2.1 Install CLIs

```bash
sudo install -m 0755 cli/linux-amd64/{kubectl,helm,kamel} /usr/local/bin/
# k3s already provides kubectl; the bundled one is optional
export KUBECONFIG=/etc/rancher/k3s/k3s.yaml
```

## 3. Import images into k3s

```bash
sudo k3s ctr images import images/all-images-amd64.tar
sudo k3s crictl images | grep -E 'camel-k|kafka|registry|temurin|nginx'
```

## 4. In-cluster registry (Camel K pushes built integration images here)

```bash
kubectl apply -f manifests/registry.yaml
kubectl rollout status deployment/registry -n registry --timeout=180s

# Node IP — used as the registry address everywhere below
export NODE_IP=$(kubectl get node -o jsonpath='{.items[0].status.addresses[?(@.type=="InternalIP")].address}')
echo "Registry: ${NODE_IP}:30500"
```

Tell k3s/containerd this registry is plain HTTP — create `/etc/rancher/k3s/registries.yaml`:

```bash
sudo tee /etc/rancher/k3s/registries.yaml <<EOF
mirrors:
  "${NODE_IP}:30500":
    endpoint:
      - "http://${NODE_IP}:30500"
EOF
sudo systemctl restart k3s   # brief API interruption, pods keep running
```

### 4.1 Seed the Jib base image into the registry (required for offline builds)

Camel K builds integration images with Jib **inside the operator pod**, and Jib pulls the
base image straight from a registry — it does NOT see images imported into containerd.
Push the bundled `eclipse-temurin:17-jdk` into the in-cluster registry (no docker needed,
`k3s ctr` can push):

```bash
sudo k3s ctr images tag docker.io/library/eclipse-temurin:17-jdk ${NODE_IP}:30500/eclipse-temurin:17-jdk
sudo k3s ctr images push --plain-http ${NODE_IP}:30500/eclipse-temurin:17-jdk
curl -s http://${NODE_IP}:30500/v2/eclipse-temurin/tags/list   # sanity: {"name":"eclipse-temurin","tags":["17-jdk"]}
```

## 5. Offline Maven mirror (the artifacts Camel K needs to build)

```bash
# Unpack the Maven repository onto the node
sudo mkdir -p /opt/maven-repo
sudo tar xzf maven/camel-k-m2.tgz -C /opt/maven-repo    # creates /opt/maven-repo/m2

# nginx serving it + settings.xml that routes ALL Maven traffic to the mirror
kubectl create namespace camel-k
kubectl apply -f manifests/maven-mirror.yaml
kubectl rollout status deployment/maven-mirror -n camel-k --timeout=180s
```

## 6. Camel K operator 2.10.1

```bash
helm install camel-k charts/camel-k-2.10.1.tgz \
  --namespace camel-k \
  --set platform.build.registry.address=${NODE_IP}:30500 \
  --set platform.build.registry.insecure=true \
  --wait --timeout 10m

# Point builds at the offline Maven mirror AND the local base image (both mandatory offline)
kubectl patch integrationplatform camel-k -n camel-k --type merge \
  -p '{"spec":{"build":{"maven":{"settings":{"configMapKeyRef":{"name":"maven-airgap-settings","key":"settings.xml"}}}}}}'
kubectl patch integrationplatform camel-k -n camel-k --type merge \
  -p "{\"spec\":{\"build\":{\"baseImage\":\"${NODE_IP}:30500/eclipse-temurin:17-jdk\"}}}"

kubectl wait integrationplatform/camel-k -n camel-k \
  --for=jsonpath='{.status.phase}'=Ready --timeout=300s
```

## 7. Kafka, topics, and the 5-second producer

The broker requires **SASL/SCRAM-SHA-256** on its client listener (9092). Credentials
come from the `kafka-scram-credentials` Secret in `manifests/kafka.yaml` (default
`camel` / `camel-bridge-2026` — **edit the Secret before deploying** if this reaches
anything shared). The SCRAM user is registered automatically on broker start; a
loopback-only PLAINTEXT listener on 9094 exists inside the pod for admin commands,
which is what the topic-creation commands below use.

```bash
kubectl apply -f manifests/kafka.yaml
kubectl rollout status deployment/kafka -n kafka --timeout=300s

POD=$(kubectl get pod -n kafka -l app=kafka -o jsonpath='{.items[0].metadata.name}')
for t in source-topic sink-topic; do
  kubectl exec -n kafka $POD -- /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9094 --create --topic $t --partitions 1 --replication-factor 1
done

# Continuous test data: one timestamped message to source-topic every 5 seconds
kubectl apply -f manifests/kafka-producer.yaml
kubectl logs -n kafka deployment/kafka-producer -f   # "produced: auto-msg-N <timestamp>"
```

## 8. Deploy the integration (built offline, in-cluster)

The route authenticates with SCRAM via `{{kafka.user}}`/`{{kafka.password}}`
placeholders, resolved from the `kafka-scram-credentials` Secret — copy it into the
`camel-k` namespace and pass it to `kamel run`:

```bash
kubectl get secret kafka-scram-credentials -n kafka -o yaml \
  | sed 's/namespace: kafka/namespace: camel-k/' | kubectl apply -f -

kamel run manifests/kafka-bridge.yaml -n camel-k --name kafka-bridge \
  --config secret:kafka-scram-credentials

# First build fetches ~2,800 artifacts from the mirror; takes ~1–3 min
kubectl wait integration/kafka-bridge -n camel-k --for=condition=Ready --timeout=600s
```

Watch the build if curious: `kubectl get build -n camel-k -w` and
`kubectl logs -n camel-k deployment/maven-mirror -f` (you'll see the artifact GETs).

Multi-node note: `maven-mirror` and `registry` use hostPath volumes — pin them to the node
holding `/opt/maven-repo` and `/opt/registry-storage` with a nodeSelector, or put the data
on shared storage (e.g. Nutanix Files/Volumes).

## 8.1 Optional: distributed rate limiting (Redis token bucket — plugin + YAML routes)

Rate limiting is packaged as **one reusable Java plugin plus per-route YAML files**:

- `manifests/RateLimit.java` is a RouteBuilder that defines **no routes** — it only
  registers the `rateLimit` bean, a token bucket evaluated as an atomic Lua script in
  Redis (using the Redis server clock). The SLA is enforced **globally across all pods**,
  so scaling the integration up/down never changes the aggregate rate.
- `manifests/routes/*.yaml` are the routes. Each calls the bean and configures its own
  key/rate/burst inline. Kafka routes *block* when over-limit (backpressure, nothing
  dropped) via `block('<key>', <rate>, <burst>)`; HTTP/HTTPS routes reject with
  `429 + Retry-After` via `http(${exchange}, '<key>', <rate>, <burst>)`.

Routes shipped: `kafka-to-kafka` (`kk-source`→`kk-sink`, 10 msg/s), `https-to-https`
(`/ingest/hh`→echo-server, 5 req/s), `https-to-kafka` (`/ingest/hk`→`hk-sink`, 5 req/s).
Add a route by dropping another YAML in `manifests/routes/` — no Java change.

```bash
kubectl apply -f manifests/redis.yaml
kubectl rollout status deployment/redis -n redis --timeout=180s
kubectl apply -f manifests/echo-server.yaml          # downstream for https-to-https

# Topics the routes use
POD=$(kubectl get pod -n kafka -l app=kafka -o jsonpath='{.items[0].metadata.name}')
for t in kk-source:1 kk-sink:1 hk-sink:1; do
  kubectl exec -n kafka $POD -- /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9094 \
    --create --if-not-exists --topic ${t%%:*} --partitions ${t##*:} --replication-factor 1
done

# Pod-terminated TLS cert for the HTTPS (8443) listener
openssl req -x509 -newkey rsa:2048 -keyout tls.key -out tls.crt -days 365 -nodes \
  -subj "/CN=ratelimit.local" -addext "subjectAltName=DNS:ratelimit.local"
kubectl create secret tls ratelimit-tls -n camel-k --cert=tls.crt --key=tls.key

# Requires the kafka-scram-credentials Secret in camel-k (already copied in §8).
# One plugin + every route in routes/:
kamel run manifests/RateLimit.java manifests/routes/*.yaml \
  -n camel-k --name multi-route-bridge \
  --config secret:kafka-scram-credentials \
  -d mvn:redis.clients:jedis:5.2.0 -d camel:http \
  --resource secret:ratelimit-tls@/etc/tls \
  -p quarkus.http.ssl-port=8443 \
  -p quarkus.http.ssl.certificate.files=/etc/tls/tls.crt \
  -p quarkus.http.ssl.certificate.key-files=/etc/tls/tls.key
kubectl wait integration/multi-route-bridge -n camel-k --for=condition=Ready --timeout=600s

kubectl apply -f manifests/multi-route-nodeport.yaml   # external HTTP 30081 / HTTPS 30444

# Optional: load generators that drive all three routes well over their SLA
kamel run manifests/MultiRouteLoaders.java -n camel-k --name multi-route-loaders \
  --config secret:kafka-scram-credentials -d camel:http
```

Scale with `kubectl scale integration multi-route-bridge -n camel-k --replicas=2` —
**not** `kubectl scale deployment`, which the operator immediately reverts. The rate stays
at the configured SLA regardless of replica count (verified: identical aggregate
throughput at 1 and 2 pods).

Quick checks: HTTP from inside the cluster answers `202` until the bucket empties, then
`429` (`curl -X POST -H 'Content-Type: text/plain' --data t http://multi-route-bridge.camel-k/ingest/hh`);
externally via NodePort `http://<NODE_IP>:30081/ingest/hh` or pod-TLS
`https://<NODE_IP>:30444/ingest/hh` (`-k`). All Maven artifacts for these routes
(`redis.clients:jedis`, camel-quarkus-platform-http, camel:http) and the `nginx:1.27-alpine`
echo-server image are already in the bundle.

## 9. End-to-end verification

The producer feeds `source-topic` every 5 s, so the pipeline is already flowing:

```bash
# Messages arriving at the sink, consumed over the SCRAM-authenticated listener
POD=$(kubectl get pod -n kafka -l app=kafka -o jsonpath='{.items[0].metadata.name}')
kubectl exec -n kafka $POD -- sh -c 'cat > /tmp/client.properties <<EOF
security.protocol=SASL_PLAINTEXT
sasl.mechanism=SCRAM-SHA-256
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="$SCRAM_USER" password="$SCRAM_PASSWORD";
EOF
/opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka.kafka.svc.cluster.local:9092 \
  --consumer.config /tmp/client.properties \
  --topic sink-topic --from-beginning --timeout-ms 20000'
# → a stream of "auto-msg-N <timestamp>" lines
# (without --consumer.config the same command fails: the listener rejects unauthenticated clients)

# Same messages crossing the Camel route
kamel logs kafka-bridge -n camel-k    # "Bridging message: auto-msg-N ..."
```

## 10. What works offline / what doesn't

| Capability | Offline? |
|---|---|
| Running + rebuilding `kafka-bridge` | ✅ fully validated (cold build, 2,812 artifacts from mirror) |
| Running + rebuilding `multi-route-bridge` (RateLimit.java plugin + YAML routes: kafka + platform-http + jedis + http) | ✅ artifacts included in the bundled repo |
| New integrations using camel:kafka / camel:platform-http / log / yaml-dsl / java | ✅ same dependency set |
| New integrations with **other** camel components (e.g. camel:sql) | ⚠️ only after adding their artifacts to the mirror (see below) |
| Native/Quarkus-native builds | ❌ not bundled |

To extend the mirror: on any online machine run a Camel K build (or `mvn dependency:go-offline`
on the generated project) with the same runtime version (3.15.3), grab the additional
artifacts from its local repo, and drop them into `/opt/maven-repo/m2` (same layout).
No restart needed — nginx serves them immediately.

**Cross-architecture gotcha:** if the build machine is Apple Silicon (aarch64), Maven only
downloads `*-linux-aarch64` native JARs (e.g. `brotli4j/native-linux-aarch64`). The RHEL
x86_64 target needs `*-linux-x86_64` variants. After any mirror population from an ARM
machine, audit for architecture-specific artifacts and fetch the x86_64 twins explicitly:

```bash
# Find aarch64-only artifacts in the mirror and list their x86_64 counterparts
find /opt/maven-repo/m2 -type d -name "*aarch64*" | sed 's/aarch64/x86_64/g'

# Example: fetch brotli4j native-linux-x86_64 for a given version V
V=1.16.0
BASE=https://repo1.maven.org/maven2/com/aayushatharva/brotli4j/native-linux-x86_64/$V
DEST=/opt/maven-repo/m2/com/aayushatharva/brotli4j/native-linux-x86_64/$V
mkdir -p $DEST
for f in native-linux-x86_64-$V.jar native-linux-x86_64-$V.jar.sha1 \
          native-linux-x86_64-$V.pom native-linux-x86_64-$V.pom.sha1; do
  curl -fsSL $BASE/$f -o $DEST/$f
done
```

## 11. Troubleshooting

| Symptom | Fix |
|---|---|
| Pod `ImagePullBackOff` | Image not imported: `sudo k3s ctr images import images/all-images-amd64.tar`; verify with `sudo k3s crictl images` |
| Build fails downloading artifacts | `kubectl logs -n camel-k deployment/maven-mirror` — 404s on `.jar`/`.pom` files mean a missing artifact (§10). 404s on `maven-metadata.xml` only are normal. |
| Build fails with `could not resolve ... native-linux-x86_64` (or similar platform artifact) | The mirror was populated from an Apple Silicon / aarch64 build; Maven only downloaded the aarch64 native JARs. Download the missing x86_64 artifact from Maven Central, drop it under `/opt/maven-repo/m2/<group-path>/<version>/` with matching `.pom` and `.sha1` files, then retry — no mirror restart needed. |
| Build fails pushing image | Registry address/insecure flag: `kubectl get ip camel-k -n camel-k -o yaml \| grep -A3 registry`; must be `<NODE_IP>:30500`, `insecure: true`, and `registries.yaml` in place (k3s restarted) |
| Build fails pulling base image (docker.io/eclipse-temurin timeout) | `spec.build.baseImage` patch missing (§6) or the image wasn't pushed to the registry (§4.1) |
| Integration pod `ErrImagePull` from `<NODE_IP>:30500` | `/etc/rancher/k3s/registries.yaml` missing/typo → kubelet tries HTTPS. Fix and `sudo systemctl restart k3s` |
| IntegrationPlatform not Ready | `kubectl logs -n camel-k deploy/camel-k-operator` |
| No messages on sink-topic | `kubectl logs -n kafka deployment/kafka-producer` (producing?) then `kamel logs kafka-bridge -n camel-k` (bridging?) |
| Client logs `disconnected` / broker logs `Failed authentication ... during SASL handshake` | Client is missing SASL config or has wrong credentials. Bridge: secret copied to `camel-k` ns and `--config secret:kafka-scram-credentials` passed? Changed the password? Restart the kafka pod (postStart re-registers the SCRAM user) and re-copy the secret. |
| Kafka pod restarts right after start (postStart failure event) | `kubectl describe pod -n kafka <pod>` — the postStart hook timed out registering the SCRAM user; check broker logs for startup errors. |
