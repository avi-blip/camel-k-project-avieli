#!/usr/bin/env bash
# Fire requests at a bridge HTTP endpoint for a fixed window and report how many
# got 202 (within SLA) vs 429 (rate limited). Aggregate 202 rate should match the SLA
# (5/s + initial burst of 10) regardless of how many pods serve the requests.
# Usage: ./test-http-rate.sh <url> <seconds> [curl-extra-args...]
#   HTTP :  kubectl port-forward -n camel-k svc/multi-route-bridge 8080:80 &
#           ./test-http-rate.sh http://localhost:8080/ingest/hh 10 -X POST -H 'Content-Type: text/plain' --data-binary t
#   HTTPS:  ./test-http-rate.sh https://<NODE_IP>:30444/ingest/hh 10 -k -X POST -H 'Content-Type: text/plain' --data-binary t
set -euo pipefail

URL=$1; SECONDS_TO_RUN=${2:-10}; shift 2 || true

OK=0; LIMITED=0; OTHER=0
END=$(( $(date +%s) + SECONDS_TO_RUN ))
while [ "$(date +%s)" -lt "$END" ]; do
  CODE=$(curl -s -o /dev/null -w '%{http_code}' "$@" "$URL" || echo 000)
  case "$CODE" in
    202) OK=$((OK+1));;
    429) LIMITED=$((LIMITED+1));;
    *) OTHER=$((OTHER+1));;
  esac
  sleep 0.05
done

TOTAL=$((OK+LIMITED+OTHER))
echo "== RESULT over ${SECONDS_TO_RUN}s: total=$TOTAL accepted(202)=$OK limited(429)=$LIMITED other=$OTHER"
echo "   accepted rate: $(python3 -c "print(f'{$OK/$SECONDS_TO_RUN:.2f}')") req/s (SLA is 3/s + burst 6)"
