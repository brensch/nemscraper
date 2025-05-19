# nemscraper
scrape all files from the nem archives and compress them to a more usable parquet format.


docker run -d \
  --name=grafana \
  -p 3000:3000 \
  -v $(pwd)/motherduck-duckdb-datasource:/var/lib/grafana/plugins/motherduck-duckdb-datasource \
  -v /home/brensch/nemscraper/tests/output:/data/parquet           \
  -e "GF_PLUGINS_ALLOW_LOADING_UNSIGNED_PLUGINS=motherduck-duckdb-datasource" \
  grafana/grafana:latest-ubuntu