# Use a minimal base
FROM ubuntu:24.04

# 1) Install runtime deps
RUN apt-get update \
 && apt-get install -y --no-install-recommends wget ca-certificates \
 && rm -rf /var/lib/apt/lists/*

# 2) Grab the DuckDB CLI
RUN wget -qO /usr/local/bin/duckdb \
     https://github.com/duckdb/duckdb/releases/download/v1.2.2/duckdb_cli-linux-amd64 \
 && chmod +x /usr/local/bin/duckdb

# 3) Set up working dir & mount point
WORKDIR /data
VOLUME ["/data/parquet"]

# 4) Expose the HTTP port
EXPOSE 9999

# 5) Start DuckDB in “in-memory” mode, load the httpserver extension, and keep the server alive
ENTRYPOINT ["duckdb","-c","INSTALL httpserver; LOAD httpserver; SELECT httpserve_start('0.0.0.0', 9999, '');"]
