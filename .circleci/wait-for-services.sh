#!/bin/bash
set -e

for service in googlecloud:8085; do
    "$(dirname "$0")/wait-for-it.sh" -t 60 "$service"
done
