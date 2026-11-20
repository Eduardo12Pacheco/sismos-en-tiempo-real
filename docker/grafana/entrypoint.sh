#!/bin/sh
# Keep Grafana's official startup (permission checks, plugin prep) for the
# server, but let `docker compose run grafana grafana cli ...` execute the CLI
# instead of appending the arguments to `grafana server`.
set -e
if [ "${1:-}" = "grafana" ] && [ "${2:-}" = "cli" ]; then
    exec "$@"
fi
exec /run.sh "$@"
