#!/usr/bin/env bash
# realtime-seismic-stream installer (Linux and macOS).
#
# Checks the local prerequisites, builds the Compose stack, starts the services
# and launches the offline replay. The script is idempotent: running it again
# rebuilds only what changed and never removes data volumes.
#
# Compatible with the system Bash 3.2 shipped by macOS.
set -euo pipefail

PROJECT_NAME="realtime-seismic-stream"
GRAFANA_PORT="13000"
MIN_COMPOSE_MAJOR="2"
MIN_COMPOSE_MINOR="20"
MIN_MEMORY_KIB=$((4 * 1024 * 1024))
MIN_DISK_KIB=$((5 * 1024 * 1024))
SAMPLE_SPAN_SECONDS="604800"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

CHECK_ONLY=0
JSON=0
DO_REPLAY=0
DO_DOWN=0
DO_PURGE=0
ASSUME_YES=0
SPEED=""
CHAOS=0

CHECK_IDS=()
CHECK_STATUSES=()
CHECK_DETAILS=()

usage() {
  cat <<'EOF'
realtime-seismic-stream installer (Linux and macOS)

Usage: ./install.sh [options]

Options:
  --check          Check prerequisites only and exit (0 pass/warn, 1 failure)
  --json           With --check, print machine-readable JSON
  --replay         Run the replay in the foreground after starting the stack
  --speed VALUE    Replay compression factor (> 0) or "max" (no pacing)
  --chaos          Inject malformed records, duplicates and revisions
  --down           Stop the stack and remove orphan containers (keep volumes)
  --purge          Remove containers, volumes, local images and .env
  --yes            Do not ask for confirmation (only meaningful with --purge)
  --help           Show this help and exit

Environment:
  SEISMIC_REPLAY_SPEED   Default replay speed (1440 = one day per minute)
  SEISMIC_REPLAY_CHAOS   true/false, enables deterministic chaos injection
  SEISMIC_CHAOS_SEED     Chaos seed (default 42)

Examples:
  ./install.sh
  ./install.sh --replay --speed 3600 --chaos
  ./install.sh --check --json
  ./install.sh --down
EOF
}

info() {
  if [ "$JSON" -eq 0 ]; then
    printf '%s\n' "$*"
  fi
}

error() {
  printf 'error: %s\n' "$*" >&2
}

usage_error() {
  error "$1"
  usage >&2
  exit 2
}

add_check() {
  CHECK_IDS+=("$1")
  CHECK_STATUSES+=("$2")
  CHECK_DETAILS+=("$3")
}

check_shell() {
  local major="${BASH_VERSINFO[0]}"
  local minor="${BASH_VERSINFO[1]}"
  if [ "$major" -gt 3 ] || { [ "$major" -eq 3 ] && [ "$minor" -ge 2 ]; }; then
    add_check "shell" "pass" "Bash ${BASH_VERSION}"
  else
    add_check "shell" "fail" "Bash ${BASH_VERSION}; version 3.2 or newer is required"
  fi
}

check_docker_cli() {
  if ! command -v docker >/dev/null 2>&1; then
    add_check "docker_cli" "fail" "docker executable not found on PATH"
    return
  fi
  local version
  version="$(docker --version 2>/dev/null || true)"
  if [ -z "$version" ]; then
    add_check "docker_cli" "fail" "docker is installed but 'docker --version' failed"
    return
  fi
  add_check "docker_cli" "pass" "$(printf '%s' "$version" | tr -d '\r\n')"
}

check_docker_compose() {
  if ! command -v docker >/dev/null 2>&1; then
    add_check "docker_compose" "fail" "docker executable not found on PATH"
    return
  fi
  local raw version major minor rest
  raw="$(docker compose version --short 2>/dev/null || true)"
  if [ -z "$raw" ]; then
    raw="$(docker compose version 2>/dev/null || true)"
  fi
  version="$(printf '%s' "$raw" | grep -oE '[0-9]+\.[0-9]+(\.[0-9]+)?' | head -n 1 || true)"
  if [ -z "$version" ]; then
    add_check "docker_compose" "fail" "Docker Compose v2 is required but was not found"
    return
  fi
  IFS='.' read -r major minor rest <<< "$version"
  if [ "$major" -lt "$MIN_COMPOSE_MAJOR" ] ||
    { [ "$major" -eq "$MIN_COMPOSE_MAJOR" ] && [ "$minor" -lt "$MIN_COMPOSE_MINOR" ]; }; then
    add_check "docker_compose" "fail" \
      "Docker Compose $version found; >= ${MIN_COMPOSE_MAJOR}.${MIN_COMPOSE_MINOR} is required"
    return
  fi
  add_check "docker_compose" "pass" "Docker Compose $version"
}

check_docker_engine() {
  if ! command -v docker >/dev/null 2>&1; then
    add_check "docker_engine" "fail" "docker executable not found on PATH"
    return
  fi
  local ostype
  ostype="$(docker info --format '{{.OSType}}' 2>/dev/null | tr -d '\r\n' || true)"
  if [ "$ostype" = "linux" ]; then
    add_check "docker_engine" "pass" "Docker engine reachable with Linux containers"
  elif [ -z "$ostype" ]; then
    add_check "docker_engine" "fail" "Docker daemon unreachable; start Docker and try again"
  else
    add_check "docker_engine" "fail" \
      "Docker engine uses '$ostype' containers; Linux containers are required"
  fi
}

check_memory() {
  local mem_kib="" bytes="" gib
  if [ -r /proc/meminfo ]; then
    mem_kib="$(awk '/^MemTotal:/ {print $2}' /proc/meminfo 2>/dev/null | head -n 1)"
  elif command -v sysctl >/dev/null 2>&1; then
    bytes="$(sysctl -n hw.memsize 2>/dev/null || true)"
    if printf '%s' "$bytes" | grep -qE '^[0-9]+$'; then
      mem_kib=$((bytes / 1024))
    fi
  fi
  if ! printf '%s' "$mem_kib" | grep -qE '^[0-9]+$'; then
    add_check "memory" "warn" "unable to determine total memory"
    return
  fi
  gib="$(awk -v k="$mem_kib" 'BEGIN { printf "%.1f", k / 1048576 }')"
  if [ "$mem_kib" -lt "$MIN_MEMORY_KIB" ]; then
    add_check "memory" "warn" "${gib} GiB detected; 4 GiB or more is recommended"
  else
    add_check "memory" "pass" "${gib} GiB total memory"
  fi
}

check_disk() {
  local avail=""
  if ! command -v df >/dev/null 2>&1; then
    add_check "disk" "warn" "unable to determine free disk space"
    return
  fi
  avail="$(df -Pk "$SCRIPT_DIR" 2>/dev/null | awk 'NR == 2 {print $4}')"
  if ! printf '%s' "$avail" | grep -qE '^[0-9]+$'; then
    add_check "disk" "warn" "unable to determine free disk space"
    return
  fi
  local gib
  gib="$(awk -v k="$avail" 'BEGIN { printf "%.1f", k / 1048576 }')"
  if [ "$avail" -lt "$MIN_DISK_KIB" ]; then
    add_check "disk" "warn" "${gib} GiB free; 5 GiB or more is recommended"
  else
    add_check "disk" "pass" "${gib} GiB free"
  fi
}

check_port_13000() {
  local owners="" other=""
  if command -v docker >/dev/null 2>&1; then
    owners="$(docker ps --filter "publish=${GRAFANA_PORT}" \
      --format '{{.Label "com.docker.compose.project"}}' 2>/dev/null || true)"
  fi
  if printf '%s\n' "$owners" | grep -qx "$PROJECT_NAME"; then
    add_check "port_13000" "pass" \
      "port ${GRAFANA_PORT} is published by this project's Grafana"
    return
  fi
  if [ -n "$(printf '%s' "$owners" | tr -d '[:space:]')" ]; then
    other="$(printf '%s\n' "$owners" | grep -v '^$' | head -n 1)"
    add_check "port_13000" "fail" \
      "port ${GRAFANA_PORT} is published by another Compose project ('${other}')"
    return
  fi
  if (exec 3<>"/dev/tcp/127.0.0.1/${GRAFANA_PORT}") 2>/dev/null; then
    add_check "port_13000" "fail" \
      "port ${GRAFANA_PORT} is already in use by a non-Compose process"
    return
  fi
  add_check "port_13000" "pass" "port ${GRAFANA_PORT} is free"
}

json_escape() {
  printf '%s' "$1" | sed -e 's/\\/\\\\/g' -e 's/"/\\"/g' -e 's/	/ /g'
}

print_checks_json() {
  local ok="$1" index=0 count="${#CHECK_IDS[@]}"
  printf '{"checks":['
  while [ "$index" -lt "$count" ]; do
    if [ "$index" -gt 0 ]; then
      printf ','
    fi
    printf '{"id":"%s","status":"%s","detail":"%s"}' \
      "$(json_escape "${CHECK_IDS[$index]}")" \
      "$(json_escape "${CHECK_STATUSES[$index]}")" \
      "$(json_escape "${CHECK_DETAILS[$index]}")"
    index=$((index + 1))
  done
  printf '],"ok":%s}\n' "$ok"
}

print_checks_human() {
  local ok="$1" index=0 count="${#CHECK_IDS[@]}" label
  printf 'Prerequisite checks:\n'
  while [ "$index" -lt "$count" ]; do
    case "${CHECK_STATUSES[$index]}" in
      pass) label="[PASS]" ;;
      warn) label="[WARN]" ;;
      *) label="[FAIL]" ;;
    esac
    printf '  %-6s %-15s %s\n' "$label" "${CHECK_IDS[$index]}" "${CHECK_DETAILS[$index]}"
    index=$((index + 1))
  done
  if [ "$ok" = "true" ]; then
    printf 'All prerequisites satisfied (warnings do not block the install).\n'
  else
    printf 'One or more prerequisites failed; fix them and run ./install.sh --check again.\n'
  fi
}

run_checks() {
  check_shell
  check_docker_cli
  check_docker_compose
  check_docker_engine
  check_memory
  check_disk
  check_port_13000

  local ok="true" index=0 count="${#CHECK_IDS[@]}"
  while [ "$index" -lt "$count" ]; do
    if [ "${CHECK_STATUSES[$index]}" = "fail" ]; then
      ok="false"
    fi
    index=$((index + 1))
  done

  if [ "$JSON" -eq 1 ]; then
    print_checks_json "$ok"
  else
    print_checks_human "$ok"
  fi
  [ "$ok" = "true" ]
}

ensure_env_file() {
  if [ ! -f .env ]; then
    cp .env.example .env
    info "Created .env from .env.example (edit it to change the defaults)."
  fi
}

compose() {
  docker compose "$@"
}

effective_speed() {
  local value="${SPEED:-${SEISMIC_REPLAY_SPEED:-1440}}"
  if [ "$value" = "max" ]; then
    printf 'max'
    return
  fi
  if printf '%s' "$value" | grep -qE '^[0-9]+(\.[0-9]+)?$'; then
    printf '%s' "$value"
    return
  fi
  printf '1440'
}

replay_estimate() {
  local speed="$1"
  if [ "$speed" = "max" ]; then
    printf 'instant (pacing disabled)'
    return
  fi
  awk -v span="$SAMPLE_SPAN_SECONDS" -v value="$speed" \
    'BEGIN { printf "about %.1f minutes of wall time", (span / value) / 60 }'
}

run_replay_foreground() {
  local speed
  speed="$(effective_speed)"
  export SEISMIC_REPLAY_SPEED="$speed"
  if [ "$CHAOS" -eq 1 ]; then
    export SEISMIC_REPLAY_CHAOS="true"
  fi
  info "Running the replay in the foreground (speed ${speed})..."
  compose run --rm ingest
}

stop_stack() {
  info "Stopping the stack (volumes are kept)..."
  compose down --remove-orphans
  info "Stack stopped."
}

purge_stack() {
  if [ "$ASSUME_YES" -ne 1 ]; then
    printf 'This removes every container, volume, local image and .env for %s. Continue? [y/N] ' \
      "$PROJECT_NAME" >&2
    local reply=""
    read -r reply || reply=""
    case "$reply" in
      y | Y | yes | YES) ;;
      *)
        info "Purge cancelled."
        exit 1
        ;;
    esac
  fi
  info "Purging containers, volumes, local images and .env..."
  compose down -v --remove-orphans --rmi local || true
  rm -f .env
  info "Purge complete."
}

install_stack() {
  ensure_env_file

  info "Building the application images (this can take a few minutes)..."
  compose build

  info "Starting Redpanda, ClickHouse, Prometheus, Grafana, processor, sink and monitor..."
  compose up -d --wait redpanda clickhouse prometheus grafana processor sink monitor

  local speed
  speed="$(effective_speed)"
  if [ "$DO_REPLAY" -eq 1 ]; then
    run_replay_foreground
  else
    if [ -n "$SPEED" ]; then
      export SEISMIC_REPLAY_SPEED="$SPEED"
    fi
    if [ "$CHAOS" -eq 1 ]; then
      export SEISMIC_REPLAY_CHAOS="true"
    fi
    info "Starting the replay (speed ${speed})..."
    compose up -d ingest
  fi

  printf '\nGrafana: http://localhost:%s\n' "$GRAFANA_PORT"
  printf 'Replay: speed %s (%s)\n' "$speed" "$(replay_estimate "$speed")"
  printf 'Commands: ./install.sh --down | ./install.sh --purge --yes | ./install.sh --help\n'
}

main() {
  if [ "$CHECK_ONLY" -eq 1 ]; then
    if run_checks; then
      exit 0
    fi
    exit 1
  fi

  if [ "$DO_PURGE" -eq 1 ]; then
    purge_stack
    exit 0
  fi

  if [ "$DO_DOWN" -eq 1 ]; then
    stop_stack
    exit 0
  fi

  if ! run_checks; then
    exit 1
  fi

  install_stack
}

while [ "$#" -gt 0 ]; do
  case "$1" in
    --check)
      CHECK_ONLY=1
      shift
      ;;
    --json)
      JSON=1
      shift
      ;;
    --replay)
      DO_REPLAY=1
      shift
      ;;
    --speed)
      if [ "$#" -lt 2 ]; then
        usage_error "--speed requires a value"
      fi
      SPEED="$2"
      shift 2
      ;;
    --speed=*)
      SPEED="${1#*=}"
      shift
      ;;
    --chaos)
      CHAOS=1
      shift
      ;;
    --down)
      DO_DOWN=1
      shift
      ;;
    --purge)
      DO_PURGE=1
      shift
      ;;
    --yes)
      ASSUME_YES=1
      shift
      ;;
    --help | -h)
      usage
      exit 0
      ;;
    *)
      usage_error "unknown option: $1"
      ;;
  esac
done

if [ -n "$SPEED" ] && [ "$SPEED" != "max" ]; then
  if ! printf '%s' "$SPEED" | grep -qE '^[0-9]+(\.[0-9]+)?$'; then
    usage_error "--speed must be a positive number or 'max'"
  fi
  if awk -v value="$SPEED" 'BEGIN { exit !(value > 0) }'; then
    :
  else
    usage_error "--speed must be greater than zero"
  fi
fi

main
