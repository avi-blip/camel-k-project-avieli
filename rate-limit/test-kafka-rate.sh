#!/usr/bin/env bash
# Burst-produce N messages to a client topic, then measure the rate at which the
# rate-limited bridge delivers them to the client's sink topic.
# Usage: ./test-kafka-rate.sh <client> <count>   e.g. ./test-kafka-rate.sh client-a 60
set -euo pipefail

CLIENT=${1:-client-a}
COUNT=${2:-60}
KPOD=$(kubectl get pod -n kafka -l app=kafka -o jsonpath='{.items[0].metadata.name}')
PRIOR=$(kubectl exec -n kafka "$KPOD" -- /opt/kafka/bin/kafka-get-offsets.sh \
  --bootstrap-server localhost:9094 --topic "${CLIENT}-sink" 2>/dev/null | awk -F: '{s+=$3} END {print s+0}')
TARGET=$((PRIOR + COUNT))

echo "== producing $COUNT messages to ${CLIENT}-topic (keyed, so they spread across partitions/pods) =="
seq 1 "$COUNT" | sed "s/^\(.*\)$/\1:$CLIENT-msg-\1/" | kubectl exec -i -n kafka "$KPOD" -- \
  /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9094 --topic "${CLIENT}-topic" \
  --property parse.key=true --property key.separator=:

echo "== waiting for messages to drain to ${CLIENT}-sink (this is the rate-limited part) =="
DEADLINE=$(( $(date +%s) + 120 ))
OUT=$(mktemp)
while :; do
  kubectl exec -n kafka "$KPOD" -- /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server localhost:9094 --topic "${CLIENT}-sink" --from-beginning \
    --timeout-ms 5000 --property print.timestamp=true 2>/dev/null > "$OUT" || true
  GOT=$(grep -c "$CLIENT-msg-" "$OUT" || true)
  echo "   drained so far (all runs): $GOT"
  [ "$GOT" -ge "$TARGET" ] && break
  [ "$(date +%s)" -ge "$DEADLINE" ] && { echo "timed out waiting for drain"; break; }
  sleep 5
done

python3 - "$OUT" "$COUNT" <<'EOF'
import sys
ts = sorted(int(l.split("\t")[0].split(":")[1]) for l in open(sys.argv[1]) if l.startswith("CreateTime"))
ts = ts[-int(sys.argv[2]):]  # only this run's burst
n = len(ts)
span = (ts[-1] - ts[0]) / 1000.0
print(f"\n== RESULT: {n} messages delivered over {span:.1f}s "
      f"=> effective rate {(n-1)/span:.2f} msg/s (first-to-last)" if span > 0 else f"{n} messages, zero span")
EOF
