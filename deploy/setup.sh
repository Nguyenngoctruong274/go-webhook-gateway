#!/bin/sh
# Registers the demo receiver with webhookd and hands it its secret.
# Runs once in the compose stack (service "setup"); safe to run again:
# an endpoint is only created when the secrets file does not exist yet.
#
#   WEBHOOKD_URL    where the API is            (default http://webhookd:8080)
#   API_TOKEN       the bearer token            (default: none)
#   RECEIVER_URL    the URL to register         (default http://receiver:9000/webhook)
#   SECRETS_FILE    where the receiver reads it (default /secrets/receiver)
set -eu
WEBHOOKD_URL=${WEBHOOKD_URL:-http://webhookd:8080}
RECEIVER_URL=${RECEIVER_URL:-http://receiver:9000/webhook}
SECRETS_FILE=${SECRETS_FILE:-/secrets/receiver}
AUTH=""
[ -n "${API_TOKEN:-}" ] && AUTH="Authorization: Bearer ${API_TOKEN}"

if [ -s "$SECRETS_FILE" ]; then
  echo "setup: $SECRETS_FILE exists; the receiver is already registered"
  exit 0
fi

i=0
until curl -fsS "$WEBHOOKD_URL/readyz" >/dev/null 2>&1; do
  i=$((i + 1))
  [ "$i" -ge 60 ] && { echo "setup: webhookd not ready after 60s" >&2; exit 1; }
  sleep 1
done

resp=$(curl -fsS -X POST "$WEBHOOKD_URL/v1/endpoints" \
  -H "Content-Type: application/json" ${AUTH:+-H "$AUTH"} \
  -d "{\"url\":\"$RECEIVER_URL\",\"description\":\"demo receiver\"}")
secret=$(printf '%s' "$resp" | sed -n 's/.*"secret":"\(whsec_[0-9a-f]*\)".*/\1/p')
id=$(printf '%s' "$resp" | sed -n 's/^{"id":\([0-9]*\),.*/\1/p')
if [ -z "$secret" ]; then
  echo "setup: no secret in the response: $resp" >&2
  exit 1
fi

# Write then rename, so the receiver never reads half a secret.
umask 022
printf '%s\n' "$secret" > "$SECRETS_FILE.tmp"
mv "$SECRETS_FILE.tmp" "$SECRETS_FILE"
echo "setup: endpoint $id registered for $RECEIVER_URL; secret handed to the receiver"
