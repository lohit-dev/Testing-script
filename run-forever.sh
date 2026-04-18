#!/bin/sh
set -eu

RUN_MODE="${RUN_MODE:-testing_script}"
RESTART_DELAY_SECONDS="${RESTART_DELAY_SECONDS:-10}"
REPORT_DIR="${REPORT_DIR:-/app/reports}"
MAX_REPORT_FILES="${MAX_REPORT_FILES:-200}"

case "$RUN_MODE" in
  testing_script)
    CMD="${TESTING_SCRIPT_CMD:-/usr/local/bin/testing_script}"
    ;;
  flood_test)
    CMD="${FLOOD_TEST_CMD:-/usr/local/bin/flood_test}"
    ;;
  *)
    echo "Invalid RUN_MODE: $RUN_MODE (use testing_script or flood_test)"
    exit 1
    ;;
esac

echo "Runner started: mode=$RUN_MODE delay=${RESTART_DELAY_SECONDS}s"
echo "Reports: dir=$REPORT_DIR max_files=$MAX_REPORT_FILES"
mkdir -p "$REPORT_DIR"

while true; do
  ts="$(date -u +%Y%m%dT%H%M%SZ)"
  run_file="$REPORT_DIR/report_${RUN_MODE}_${ts}.log"

  echo "Starting $CMD at $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "Writing report to $run_file"
  "$CMD" > "$run_file" 2>&1
  exit_code=$?

  ln -sf "$run_file" "$REPORT_DIR/latest_${RUN_MODE}.log"

  # Keep only newest N report files for the active mode.
  old_files="$(ls -1t "$REPORT_DIR"/report_"$RUN_MODE"_*.log 2>/dev/null | tail -n +"$((MAX_REPORT_FILES + 1))" || true)"
  if [ -n "$old_files" ]; then
    echo "$old_files" | xargs rm -f
  fi

  echo "$CMD exited with code $exit_code (report: $run_file)"
  echo "Restarting in ${RESTART_DELAY_SECONDS}s..."
  sleep "$RESTART_DELAY_SECONDS"
done
