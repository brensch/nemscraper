# nemscraper
scrape all files from the nem archives and compress them to a more usable parquet format.

# Usage
## monitor files

From within the `assets` directory, you can monitor the number of files in each subdirectory and the disk usage of the assets directory using the following command:

```bash
watch -n 2 '
  echo "=== File Counts ==="
  for dir in */; do
    echo -n "${dir%/}: "
    find "$dir" -maxdepth 1 -type f | wc -l
  done

  echo
  echo "=== Disk Usage ==="
  du -sh ./*

  echo
  echo "=== Processes Matching \"nemscraper\" ==="
  top -b -n1 | awk "/^ *PID/ || /nemscraper/" || echo "No nemscraper process"
'

```

## 

# Benchmarking Compression Algorithms

Note using brotli 5 for compression, it performed the best

Group      | Algorithm    | Level | CompressedSize  | TimeMs    
-----------------------------------------------------------------
smallest   | BROTLI       | 5     | 2301            | 0         
smallest   | BROTLI       | 7     | 2301            | 4         
smallest   | ZSTD         | 1     | 2365            | 0         
smallest   | ZSTD         | 10    | 2365            | 16        
smallest   | ZSTD         | 15    | 2365            | 63        
largest    | BROTLI       | 5     | 551840901       | 50088     
largest    | BROTLI       | 7     | 549416093       | 72331     
largest    | ZSTD         | 1     | 742352382       | 23886     
largest    | ZSTD         | 10    | 582382671       | 67637     
largest    | ZSTD         | 15    | 581370235       | 286124    