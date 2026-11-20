# Grafana with the signed ClickHouse datasource baked in at build time (D15).
# Installing the plugin in the image (instead of downloading it at startup)
# keeps the dashboard stack reproducible and safe to run offline.
FROM grafana/grafana:13.3.1

USER root
RUN grafana cli --pluginsDir "${GF_PATHS_PLUGINS}" plugins install grafana-clickhouse-datasource 4.23.5
COPY docker/grafana/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh
ENTRYPOINT ["/entrypoint.sh"]
USER grafana
