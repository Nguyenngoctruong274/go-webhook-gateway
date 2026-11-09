#!/bin/sh
# Smoke test of the compose stack, from the host: publish an event,
# wait until the receiver has it, then check that Prometheus scrapes
# webhookd, has loaded the alert rules and sees the delivery.
# PROM=none skips the Prometheus checks.
#
#   make stack-up && ./deploy/smoke.sh
set -eu
API=${API:-http://localhost:8080}
PROM=${PROM:-http://localhost:9090}
TOKEN=${API_TOKEN:-stack-demo-token-change-me}
RECEIVER_URL=${RECEIVER_URL:-http://receiver:9000/webhook}
H="Authorization: Bearer $TOKEN"
fail() { echo "smoke: $*" >&2; exit 1; }

i=0
until curl -fsS "$API/readyz" >/dev/null 2>&1; do
  i=$((i + 1)); [ "$i" -ge 60 ] && fail "webhookd not ready"; sleep 1
done
i=0
until curl -fsS -H "$H" "$API/v1/endpoints" | grep -q "\"url\":\"$RECEIVER_URL\""; do
  i=$((i + 1)); [ "$i" -ge 60 ] && fail "the setup service did not register the receiver"; sleep 1
done

key="smoke-$(date +%s)"
pub=$(curl -fsS -X POST "$API/v1/events" -H "$H" -H "Content-Type: application/json" -H "Idempotency-Key: $key" \
  -d '{"type":"order.paid","payload":{"order":42,"amount":"10.00"}}')
ev=$(printf '%s' "$pub" | sed -n 's/.*"event":{"id":\([0-9]*\),.*/\1/p')
[ -n "$ev" ] || fail "publish: $pub"
echo "smoke: published event $ev"

i=0
while :; do
  state=$(curl -fsS -H "$H" "$API/v1/events/$ev" | sed -n 's/.*"deliveries":\[{[^]]*"state":"\([a-z_]*\)".*/\1/p')
  [ "$state" = delivered ] && break
  i=$((i + 1)); [ "$i" -ge 60 ] && fail "delivery of event $ev is '$state' after 60s"
  sleep 1
done
echo "smoke: event $ev delivered to the receiver"
[ "$PROM" = none ] && exit 0

i=0
until curl -fsS "$PROM/api/v1/targets" | grep -q '"health":"up"'; do
  i=$((i + 1)); [ "$i" -ge 60 ] && fail "Prometheus does not scrape webhookd"; sleep 1
done
curl -fsS "$PROM/api/v1/rules" | grep -q '"name":"WebhookBacklogGrowing"' || fail "alert rules not loaded"
i=0
until curl -fsS "$PROM/api/v1/query?query=sum(webhook_attempts_total%7Boutcome%3D%22delivered%22%7D)" | grep -q '"value":\[[0-9.]*,"[1-9]'; do
  i=$((i + 1)); [ "$i" -ge 30 ] && fail "Prometheus has no delivered attempts"; sleep 1
done
echo "smoke: Prometheus scrapes webhookd, has the alert rules and counts the delivery"
