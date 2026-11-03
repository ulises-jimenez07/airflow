#!/usr/bin/env bash
# Usage: report.sh <date>
# Prints a fake report for the given date. Exits non-zero if no date is given,
# which makes the Airflow task fail.
set -euo pipefail

RUN_DATE="${1:?usage: report.sh <YYYY-MM-DD>}"

echo "Building report for ${RUN_DATE}"
echo "Working directory: $(pwd)"
echo "Files in script dir:"
ls -1 "$(dirname "$0")"
echo "Report for ${RUN_DATE} done"
