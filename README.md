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

