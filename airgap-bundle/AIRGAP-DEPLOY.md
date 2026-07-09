# Air-Gapped Deployment Guide — Camel K + Kafka on RHEL / k3s

Deploy Apache Camel K 2.10.1, a single-node Kafka, the `kafka-bridge` (Kafka→Kafka)
integration, and a 5-second test-data producer on an **air-gapped RHEL server (x86_64)
that already runs a k3s cluster** (e.g. on Nutanix AHV), using only the artifacts in
this bundle. **No internet access is required at any step.**

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
│   ├── all-images-amd64.tar        ← all 5 container images, linux/amd64 (~700 MB)
│   └── image-list-amd64.txt
├── charts/
│   └── camel-k-2.10.1.tgz          ← Camel K operator Helm chart
├── cli/linux-amd64/
│   └── kubectl (v1.36.2), helm (v4.2.2), kamel (2.10.1)   ← static ELF x86-64 binaries
├── maven/
│   └── camel-k-m2.tgz              ← complete Maven repo (~6,000 files, 197 MB compressed)
└── manifests/
    ├── registry.yaml               ← in-cluster registry (NodePort 30500)
    ├── maven-mirror.yaml           ← nginx Maven mirror + settings.xml ConfigMap
    ├── kafka.yaml                  ← namespace + single-node KRaft Kafka
    ├── kafka-producer.yaml         ← produces a message to source-topic every 5 s
    └── kafka-bridge.yaml           ← the Camel route (source-topic → sink-topic)
```

Versions: Camel K 2.10.1 (runtime catalog 3.15.3, Camel 4.8.5, Quarkus/JVM mode, Jib publish,
base image `eclipse-temurin:17-jdk`) · Kafka 3.9.1 (KRaft, `apache/kafka`) · registry:2 ·
nginx:1.27-alpine.

Images in the tar: `apache/camel-k:2.10.1`, `apache/kafka:3.9.1`, `registry:2`,
`eclipse-temurin:17-jdk` (Jib base image — required for builds), `nginx:1.27-alpine`.

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

```bash
kubectl apply -f manifests/kafka.yaml
kubectl rollout status deployment/kafka -n kafka --timeout=300s

POD=$(kubectl get pod -n kafka -l app=kafka -o jsonpath='{.items[0].metadata.name}')
for t in source-topic sink-topic; do
  kubectl exec -n kafka $POD -- /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 --create --topic $t --partitions 1 --replication-factor 1
done

# Continuous test data: one timestamped message to source-topic every 5 seconds
kubectl apply -f manifests/kafka-producer.yaml
kubectl logs -n kafka deployment/kafka-producer -f   # "produced: auto-msg-N <timestamp>"
```

## 8. Deploy the integration (built offline, in-cluster)

```bash
kamel run manifests/kafka-bridge.yaml -n camel-k --name kafka-bridge

# First build fetches ~2,800 artifacts from the mirror; takes ~1–3 min
kubectl wait integration/kafka-bridge -n camel-k --for=condition=Ready --timeout=600s
```

Watch the build if curious: `kubectl get build -n camel-k -w` and
`kubectl logs -n camel-k deployment/maven-mirror -f` (you'll see the artifact GETs).

Multi-node note: `maven-mirror` and `registry` use hostPath volumes — pin them to the node
holding `/opt/maven-repo` and `/opt/registry-storage` with a nodeSelector, or put the data
on shared storage (e.g. Nutanix Files/Volumes).

## 9. End-to-end verification

The producer feeds `source-topic` every 5 s, so the pipeline is already flowing:

```bash
# Messages arriving at the sink (Ctrl-C to stop)
POD=$(kubectl get pod -n kafka -l app=kafka -o jsonpath='{.items[0].metadata.name}')
kubectl exec -n kafka $POD -- /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic sink-topic --from-beginning --timeout-ms 20000
# → a stream of "auto-msg-N <timestamp>" lines

# Same messages crossing the Camel route
kamel logs kafka-bridge -n camel-k    # "Bridging message: auto-msg-N ..."
```

## 10. What works offline / what doesn't

| Capability | Offline? |
|---|---|
| Running + rebuilding `kafka-bridge` | ✅ fully validated (cold build, 2,812 artifacts from mirror) |
| New integrations using camel:kafka / log / yaml-dsl | ✅ same dependency set |
| New integrations with **other** camel components (e.g. camel:http, camel:sql) | ⚠️ only after adding their artifacts to the mirror (see below) |
| Native/Quarkus-native builds | ❌ not bundled |

To extend the mirror: on any online machine run a Camel K build (or `mvn dependency:go-offline`
on the generated project) with the same runtime version (3.15.3), grab the additional
artifacts from its local repo, and drop them into `/opt/maven-repo/m2` (same layout).
No restart needed — nginx serves them immediately.

## 11. Troubleshooting

| Symptom | Fix |
|---|---|
| Pod `ImagePullBackOff` | Image not imported: `sudo k3s ctr images import images/all-images-amd64.tar`; verify with `sudo k3s crictl images` |
| Build fails downloading artifacts | `kubectl logs -n camel-k deployment/maven-mirror` — 404s on `.jar`/`.pom` files mean a missing artifact (§10). 404s on `maven-metadata.xml` only are normal. |
| Build fails pushing image | Registry address/insecure flag: `kubectl get ip camel-k -n camel-k -o yaml \| grep -A3 registry`; must be `<NODE_IP>:30500`, `insecure: true`, and `registries.yaml` in place (k3s restarted) |
| Build fails pulling base image (docker.io/eclipse-temurin timeout) | `spec.build.baseImage` patch missing (§6) or the image wasn't pushed to the registry (§4.1) |
| Integration pod `ErrImagePull` from `<NODE_IP>:30500` | `/etc/rancher/k3s/registries.yaml` missing/typo → kubelet tries HTTPS. Fix and `sudo systemctl restart k3s` |
| IntegrationPlatform not Ready | `kubectl logs -n camel-k deploy/camel-k-operator` |
| No messages on sink-topic | `kubectl logs -n kafka deployment/kafka-producer` (producing?) then `kamel logs kafka-bridge -n camel-k` (bridging?) |
