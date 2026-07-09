# Camel K Kafka Bridge — local dev + air-gapped deployment

Apache Camel K 2.10.1 integration that bridges Kafka `source-topic` → `sink-topic`,
with a local dev setup (k3d on Colima, macOS) and a full **air-gapped deployment kit**
for a RHEL x86_64 server running k3s (Nutanix).

## Layout

| Path | What |
|---|---|
| `kafka.yaml` | Single-node KRaft Kafka (`apache/kafka:3.9.1`), ns `kafka` |
| `kafka-bridge.yaml` | The Camel route (YAML DSL): kafka:source-topic → log → kafka:sink-topic |
| `monitoring/` | Helm values for local Grafana + Prometheus + Loki (dashboard `camel-kafka-bridge`, login admin/camelk) |
| `airgap-bundle/` | Everything for the offline RHEL/k3s deployment — **start with `airgap-bundle/AIRGAP-DEPLOY.md`** |
| `airgap-bundle/manifests/` | registry (NodePort 30500), nginx Maven mirror + settings.xml, kafka, 5-second producer, route |

## Large artifacts NOT in git (GitHub 100 MB limit)

Regenerate on any online machine:

```bash
# Container images (linux/amd64), ~700 MB
cd airgap-bundle/images
while read -r i; do docker pull --platform linux/amd64 "$i"; done < image-list-amd64.txt
docker save --platform linux/amd64 -o all-images-amd64.tar $(tr '\n' ' ' < image-list-amd64.txt)

# Maven repository, ~200 MB — extracted from a camel-k operator that has built the route once
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
