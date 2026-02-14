#!/usr/bin/env bash
set -euo pipefail

FILE="BybitLinearExchangeInfoSnapshot.json"

if [[ ! -f "$FILE" ]]; then
  echo "File not found: $FILE"
  exit 1
fi

if ! command -v jq >/dev/null 2>&1; then
  echo "Missing dependency: jq"
  echo "Install with: sudo apt install -y jq"
  exit 1
fi

echo "Unique contractType values in $FILE:"
echo

jq -r '.result.list[].contractType' "$FILE" \
  | sort \
  | uniq \
  | sed 's/^/  - /'

echo
echo "Done."

