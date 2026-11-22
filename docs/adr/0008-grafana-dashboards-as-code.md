# ADR 0008: Dashboards de Grafana como código y un Geomap offline

- **Estado:** Aceptada
- **Fecha:** 2026-11-20
- **Relacionados:** [Arquitectura](../ARCHITECTURE.md) · [Operación](../OPERATIONS.md) · [ADR 0007](0007-replay-event-time-and-data-clock.md)

## Contexto

S16 exige dos dashboards terminados (`seismic-live`, `pipeline-health`) que funcionen con el replay, cuyos eventos están fechados en el pasado. Grafana debe renderizarlos sin conexión a internet, sin servidor de tiles y sin aprovisionamiento manual. El JSON del dashboard se revisa como código y no debe divergir de un generador ni de la versión de Grafana en ejecución.

Tres decisiones necesitaron evidencia del stack real:

1. Cómo alcanza el datasource de ClickHouse de Grafana a la base de datos (puerto nativo 9000 o HTTP 8123) y qué valores numéricos de `format` usa el target SQL.
2. Cómo obtiene el panel Geomap un mapa del mundo sin descargar tiles.
3. Cómo se instala el plugin de ClickHouse para que la imagen sea reproducible.

## Decisión

1. **Los dashboards se generan, nunca se editan a mano.** `scripts/build_dashboards.py` contiene cada panel, query, layout, variable y link. Escribe `docker/grafana/dashboards/{seismic-live,pipeline-health}.json` de forma determinista, y `--check` falla cuando los archivos confirmados difieren. El file provider tiene `allowUiUpdates: false`, así que el JSON es la única fuente de verdad.
2. **ClickHouse sobre HTTP en el puerto 8123 con el plugin `grafana-clickhouse-datasource` 4.23.5.** El endpoint de salud `/api/datasources/uid/clickhouse/health` devuelve `"status": "OK"` con `jsonData.host=clickhouse`, `port=8123`, `protocol=http`, `defaultDatabase=seismic`. El protocolo nativo (9000) se documentó como alternativa; se eligió HTTP porque usa el mismo protocolo y puerto que el cliente Python `clickhouse-connect`, así que hay una sola ruta de red que validar. Los targets SQL usan `editorType: "sql"`, `queryType: "table" | "timeseries"`, `format: 1` para tablas y `format: 0` para series temporales, y `pluginVersion: "4.23.5"` (D17). Cada query devolvió frames sin error mediante `/api/ds/query`, y cada panel marcado con `expect_data` devolvió filas tras un replay.
3. **Reloj de datos en el momento de la consulta.** Los eventos del replay tienen días de antigüedad. Cada columna de tiempo se desplaza con `dateDiff('second', max(event_time), now())` dentro de la query, de modo que el evento almacenado más nuevo se mapea a "ahora" y el rango `now-24h..now` se llena. La estadística "Data clock" muestra el evento real más nuevo (`toUnixTimestamp64Milli`, unidad `dateTimeAsIso`) y el desplazamiento en segundos (`dateDiff`, unidad `s`). Las filas almacenadas nunca se mutan, así que el modo en vivo (desplazamiento aproximadamente cero) y el replay comparten los mismos dashboards, y el dashboard no puede mentir sobre el tiempo de evento grabado.
4. **Geomap offline con el GeoJSON de países incluido en Grafana.** La capa de basemap del Geomap es de tipo `geojson` con `src: "public/maps/countries.geojson"`; no se configura ningún basemap de tiles (`osm`, `carto`, `esri`, `xyz`, `maplibre`). Los terremotos son una capa `markers` ubicada por `latitude`/`longitude`, con tamaño según `magnitude` y color según `depth_km`. Por lo tanto, el dashboard renderiza un mapa con cero requests externos, y el check de Playwright verifica que el canvas del mapa tenga píxeles no en blanco en ambos temas.
5. **Plugin instalado al construir la imagen con `grafana cli`.** El Dockerfile parte de `grafana/grafana:13.3.1` y ejecuta `grafana cli --pluginsDir "${GF_PATHS_PLUGINS}" plugins install grafana-clickhouse-datasource 4.23.5` como root antes de volver al usuario `grafana`. `docker compose run --rm --no-deps grafana grafana cli plugins ls` informa `grafana-clickhouse-datasource @ 4.23.5`. El fallback documentado `GF_PLUGINS_PREINSTALL` no fue necesario.
6. **Credenciales mediante interpolación de entorno en el aprovisionamiento.** El archivo de provisioning del datasource usa `$__env{SEISMIC_CLICKHOUSE_USER}` y `$__env{SEISMIC_CLICKHOUSE_PASSWORD}` en `secureJsonData`; Compose inyecta las mismas variables que usan los servicios de la aplicación. No se confirma ninguna contraseña. Grafana solo se expone en `127.0.0.1:13000`, con acceso anónimo de Viewer, chequeos de analytics y de actualizaciones deshabilitados, y el tema oscuro como predeterminado.
7. **`schemaVersion` registrado.** Grafana 13.3.1 estampó `schemaVersion: 42` al cargar ambos dashboards; el generador usa esa constante para que las futuras actualizaciones de Grafana sean un diff explícito y no una deriva silenciosa.

## Consecuencias

- Un cambio de dashboard es un diff de Python revisado más el JSON regenerado; el test unitario exige ids de panel únicos, `gridPos` sin superposición, títulos, descripciones, unidades, `noValue`, uids de datasource, la forma de target D17 y las señales operativamente requeridas.
- El Geomap no muestra etiquetas de países ni relieve porque no se descargan tiles. Es el trade-off offline buscado; los bordes provienen del asset incluido en Grafana y los marcadores transmiten la información.
- Desplazar valores en el momento de la consulta mantiene los datos inmutables, pero el timestamp mostrado en una fila de tabla no es el timestamp crudo almacenado. El panel "Data clock" expone el evento real más nuevo y el desplazamiento para que el operador siempre pueda recuperar el tiempo original.
- Los dashboards son de solo lectura en la UI. Los paneles ad hoc deben construirse en un dashboard sin guardar; los cambios aceptados pasan por el generador.
- La línea de auditoría de feature toggles de Grafana se filtra con `GF_LOG_FILTERS=featuremgmt:error`; sin ese filtro, una línea informativa benigna contiene las subcadenas "error" y "provision" y contamina el chequeo de errores de provisioning.

## Alternativas consideradas

- **JSON escrito a mano y confirmado directamente.** Descartada: las reglas de paneles (ids únicos, layout, unidades, forma del datasource) son fáciles de romper e imposibles de revisar de forma consistente.
- **Un SDK de dashboards de terceros.** Descartada por D17: los diccionarios simples mantienen el generador sin dependencias y el diff del JSON predecible.
- **El datasource nativo de ClickHouse de Grafana.** No existe en Grafana 13.3.1; se requiere el plugin comunitario firmado.
- **Protocolo nativo en el puerto 9000.** Viable, pero un segundo protocolo y puerto sin ganancia funcional para SQL de solo lectura. HTTP 8123 se comparte con la aplicación y pasó el health check.
- **Un basemap de tiles (OSM o CARTO).** Descartada: requiere internet, filtra requests desde una demo local-first y falla offline. El GeoJSON incluido basta para una vista general del mundo.
- **`GF_PLUGINS_PREINSTALL` en lugar de un paso de build.** Descartada como ruta principal porque descarga al iniciar el contenedor (más lento y menos reproducible); la instalación con CLI en build fue verificada y está registrada arriba.
- **Desplazar valores al ingerir o almacenar timestamps desplazados.** Descartada por ADR 0007: corrompería el tiempo de evento grabado y rompería las comparaciones entre origen y almacenamiento.
