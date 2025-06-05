#!/bin/sh
#
# Every 60 seconds, compute “du -sb” on each subdirectory of /assets
# and write a Prometheus .prom file under /textfile/.

ASSETS_ROOT="/assets"
OUTPUT_FILE="/textfile/assets_size.prom"

while true; do
  {
    echo "# HELP assets_directory_size_bytes Bytes used by each subdirectory under /assets"
    echo "# TYPE assets_directory_size_bytes gauge"

    # Loop over every immediate child (file or directory) in /assets
    for entry in "${ASSETS_ROOT}"/*; do
      # Skip if nothing matches
      [ -e "$entry" ] || continue

      # Use basename as the “dir” label
      name=$(basename "$entry")

      # Only measure directories (skip loose files)
      [ -d "$entry" ] || continue

      # Get total bytes under that directory
      bytes=$(du -sb "$entry" 2>/dev/null | awk '{print $1}')

      # Emit one line per directory:
      # assets_directory_size_bytes{dir="subdir_name"} <bytes>
      echo "assets_directory_size_bytes{dir=\"$name\"} ${bytes}"
    done
  } > "$OUTPUT_FILE"

  # Sleep 60 seconds before repeating
  sleep 60
done
