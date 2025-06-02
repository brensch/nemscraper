#!/usr/bin/env bash
#
# stats.sh: print counts for *Proc*/ *Down*, disk‐usage (excluding .tmp),
# and whether "nemscraper" is running.
#
# Usage:   chmod +x stats.sh
#          watch -n 2 ./stats.sh

# ─── 1) Enable Bash extglob + nullglob so that an empty glob produces zero matches,
#     and we can selectively exclude “*.tmp” without calling find each time:
shopt -s nullglob extglob

# ─── 2) Count “*Proc*” (but exclude any “.tmp”):
proc_files=(assets/history/*Proc*!(*.tmp))
proc=${#proc_files[@]}

# ─── 3) Count “*Down*” (exclude “.tmp”):
down_files=(assets/history/*Down*!(*.tmp))
down=${#down_files[@]}

# ─── 4) Compute percentage safely:
if (( down > 0 )); then
  pct=$(( proc * 100 / down ))
else
  pct=0
fi

echo "=== Processed Percentage ==="
echo "Processed files:  $proc"
echo "Downloaded files: $down"
echo "Complete:         $pct%"

echo
echo "=== Disk Usage (excluding *.tmp) ==="
# Note: du -sh ./* will show each top‐level item’s size.  We pass --exclude="*.tmp"
# so du will skip any file or directory whose name ends in “.tmp”, but still
# bill directories as a whole (minus any matching files inside).
du -sh --exclude="*.tmp" ./assets/* 2>/dev/null

echo
echo "=== Process Matching \"nemscraper\" ==="
if pgrep -x "nemscraper" >/dev/null; then
  echo "nemscraper is running (PID(s): $(pgrep -x "nemscraper" | tr '\n' ' '))"
else
  echo "No nemscraper process"
fi
