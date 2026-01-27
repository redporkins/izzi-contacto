#!/usr/bin/env bash
set -euo pipefail

# -----------------------------
# Usage:
#   ./runner.sh --from "YYYY-MM-DD HH:MM:SS" [--to "YYYY-MM-DD HH:MM:SS"] [--estado ...]
#
# Examples:
#   ./runner.sh --from "2025-12-16 00:00:00" --to "2025-12-22 23:59:59"
#   ./runner.sh --from "2025-12-16 00:00:00" --to "2025-12-22 23:59:59" --estado CANCELADO --estado "NOT DONE"
#   ./runner.sh --from "2025-12-16 00:00:00"   # uses now for --to
# -----------------------------

# Load the Postman enviroment and collection variables
echo "Loading Postman environment variables..."
python3 load_postman.py

FROM=""
TO=""
# ESTADOS=()

# Simple arg parsing (supports repeatable --estado)
while [[ $# -gt 0 ]]; do
  case "$1" in
    --from)
      FROM="${2:-}"
      shift 2
      ;;
    --to)
      TO="${2:-}"
      shift 2
      ;;
    # --estado)
    #   ESTADOS+=("${2:-}")
    #   shift 2
    #   ;;
    -h|--help)
      sed -n '1,40p' "$0"
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      exit 2
      ;;
  esac
done

if [[ -z "$FROM" ]]; then
  echo "Error: --from is required (format: YYYY-MM-DD HH:MM:SS)" >&2
  exit 2
fi

# If --to not provided, use current local time in the same format
if [[ -z "$TO" ]]; then
  TO="$(date '+%Y-%m-%d %H:%M:%S')"
  echo "No --to provided. Using current time: $TO"
fi

# Basic format validation (lightweight)
if [[ ! "$FROM" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}\ [0-9]{2}:[0-9]{2}:[0-9]{2}$ ]]; then
  echo "Error: --from must be 'YYYY-MM-DD HH:MM:SS' (got: $FROM)" >&2
  exit 2
fi
if [[ ! "$TO" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}\ [0-9]{2}:[0-9]{2}:[0-9]{2}$ ]]; then
  echo "Error: --to must be 'YYYY-MM-DD HH:MM:SS' (got: $TO)" >&2
  exit 2
fi

# Extract date-only for SQL script
FROM_DATE="${FROM:0:10}"
TO_DATE="${TO:0:10}"

# Build repeatable --estado args
# ESTADO_ARGS=()
# for e in "${ESTADOS[@]}"; do
#   ESTADO_ARGS+=(--estado "$e")
# done

echo "Running with:"
echo "  FROM (datetime): $FROM"
echo "  TO   (datetime): $TO"
echo "  FROM (date):     $FROM_DATE"
echo "  TO   (date):     $TO_DATE"
# if [[ ${#ESTADOS[@]} -gt 0 ]]; then
#   echo "  ESTADOS:         ${ESTADOS[*]}"
# fi
# echo ""

# Optional: use a virtualenv if you have one (uncomment and adjust)
# source .venv/bin/activate

echo "1) Running get_tiktok_data.py..."
python3 get_tiktok_data.py --from "$FROM" --to "$TO"

echo "2) Running get_sql_data.py..."
# python3 get_sql_data.py --from "$FROM_DATE" --to "$TO_DATE" "${ESTADO_ARGS[@]}"
python3 get_sql_data.py --from "$FROM_DATE" --to "$TO_DATE" 

echo "3) Running get_hibot_data.py..."
python3 get_hibot_data.py --from "$FROM" --to "$TO"

# echo "4) Running load_vicidial.py"
# python3 load_vicidial.py

echo "5) Running load_fb_pixel.py..."
python3 load_fb_pixel.py

echo ""
echo "Done."
