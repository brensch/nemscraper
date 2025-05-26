# nemscraper
scrape all files from the nem archives and compress them to a more usable parquet format.


docker run -d \
  --name=grafana \
  -u "$(id -u):$(id -g)" \
  --restart=always \
  -p 3000:3000 \
  -v $(pwd)/grafana/plugins/motherduck-duckdb-datasource:/var/lib/grafana/plugins/motherduck-duckdb-datasource \
  -v "$(pwd)/grafana/dashboards:/etc/grafana/provisioned_dashboards" \
  -v "$(pwd)/grafana/provisioning/dashboards:/etc/grafana/provisioning/dashboards" \
  -v "$(pwd)/grafana/provisioning/datasources:/etc/grafana/provisioning/datasources" \
  -v $(pwd)/parquet:/data/parquet \
  -e "GF_PLUGINS_ALLOW_LOADING_UNSIGNED_PLUGINS=motherduck-duckdb-datasource" \
  grafana/grafana:latest-ubuntu

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