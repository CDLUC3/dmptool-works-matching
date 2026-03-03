#!/usr/bin/env bash

# Logs memory usage every 10 seconds to a CSV file

# Create the file and add the column headers
echo "Timestamp,Total_RAM_MB,Used_RAM_MB,Total_Swap_MB,Used_Swap_MB" > memory_log.csv

# Start the loop to log data every 10 seconds
while true; do
    TIMESTAMP=$(date '+%Y-%m-%d %H:%M:%S')
    free -m | awk -v ts="$TIMESTAMP" '
        /^Mem:/ { ram_total=$2; ram_used=$3 }
        /^Swap:/ { swap_total=$2; swap_used=$3 }
        END { print ts "," ram_total "," ram_used "," swap_total "," swap_used }
    ' >> memory_log.csv
    sleep 10
done