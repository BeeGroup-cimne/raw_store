#!/bin/sh
# livenessProbe: falla si el poll de Kafka (/tmp/heartbeat) o el check de dependencies (/tmp/health_deps)
# no s'han actualitzat en 300s, o no existeixen.
d=${HEALTH_DIR:-/tmp}
now=$(date +%s)
for f in "$d/heartbeat" "$d/health_deps"; do
  [ -e "$f" ] && [ $((now - $(stat -c %Y "$f"))) -lt 300 ] || exit 1
done
