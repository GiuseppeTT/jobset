#!/bin/bash

set -e

echo "INFO: Starting exit-on-demand script"

while true; do
  for file in /tmp/exit*; do
    if [ -f "$file" ]; then
      exit_code_str="${file#/tmp/exit}"
      if [[ "$exit_code_str" =~ ^[0-9]+$ ]]; then
        echo "INFO: Found exit file $file. Exiting with code $exit_code_str"
        exit "$exit_code_str"
      fi
    fi
  done
  sleep 1
done