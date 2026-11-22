# Operación

Este es el runbook del stack local `realtime-seismic-stream`: requisitos previos, comandos, configuración, puertos, métricas, simulacros de caos, modo en vivo, resolución de problemas y desinstalación. La arquitectura y los contratos se describen en [ARCHITECTURE.md](ARCHITECTURE.md); la procedencia de los datos está en [DATA.md](DATA.md).

## Requisitos previos

| Requisito | Linux | macOS | Windows |
|---|---|---|---|
| Runtime de contenedores | Docker Engine 24+ con Compose v2 | Docker Desktop con Compose v2 | Docker Desktop con contenedores Linux, backend WSL2 |
| Compose | `docker compose` >= 2.20 | >= 2.20 | >= 2.20 |
| Memoria para Docker | 4 GiB | 4 GiB | 4 GiB |
| Disco libre | 5 GiB | 5 GiB | 5 GiB |
| Herramientas opcionales de desarrollo | Python 3.12 y [uv](https://docs.astral.sh/uv/) | igual | igual |

El stack no necesita GPU, API key, cuenta en la nube ni acceso a internet después de descargar las imágenes: el modo replay funciona totalmente offline. Los usuarios de Windows deben ejecutar Docker Desktop en modo **Linux containers** (no contenedores de Windows) y tener WSL2 instalado; `.\install.ps1 -Check -Json` informa el modo detectado.

## Inicio rápido

Linux y macOS:

```bash
git clone https://github.com/Eduardo12Pacheco/sismos-en-tiempo-real.git
cd sismos-en-tiempo-real
./install.sh
```

Windows (PowerShell):

```powershell
git clone https://github.com/Eduardo12Pacheco/sismos-en-tiempo-real.git
Set-Location sismos-en-tiempo-real
.\install.ps1
```

Abre `http://localhost:13000` cuando Grafana esté saludable. El instalador verifica los requisitos previos, crea `.env` a partir de `.env.example` si falta, construye la imagen de aplicación compartida, inicia Redpanda, ClickHouse, Prometheus, Grafana, el processor, el sink y el monitor, y luego lanza el consumidor del replay. La velocidad de replay por defecto es `1440` (un día de tiempo de evento por minuto, unos siete minutos para la semana incluida).

Volver a ejecutar el instalador es idempotente: reconstruye solo lo que cambió y nunca elimina volúmenes de datos.

## Comandos

### Instalador

| Comando | Propósito |
|---|---|
| `./install.sh --check` | Verifica solo los requisitos previos; sale con código 1 si una verificación falla |
| `./install.sh --check --json` | Informe de requisitos previos legible por máquina |
| `./install.sh` | Construye e inicia el stack y luego ejecuta el replay |
| `./install.sh --replay` | Inicia el stack y mantiene el replay en primer plano |
| `./install.sh --replay --chaos` | Replay con registros malformados, duplicados y revisiones |
| `./install.sh --replay --speed 3600` | Replay rápido; usa `max` para desactivar el control de ritmo |
| `./install.sh --down` | Detiene los contenedores y conserva los volúmenes |
| `./install.sh --purge --yes` | Elimina contenedores, volúmenes, imágenes locales y `.env` |

`install.ps1` acepta las mismas opciones con la capitalización de PowerShell: `-Check -Json`, `-Replay`, `-Speed`, `-Chaos`, `-Down`, `-Purge`, `-Yes`.

### CLI en contenedor

El servicio `cli` está bajo el perfil `tools` y reutiliza la imagen de la aplicación:

```bash
docker compose run --rm cli doctor
docker compose run --rm cli verify --timeout 180
docker compose run --rm cli replay-info --json
```

### CLI en el host

Con `uv sync --frozen`, los mismos comandos se ejecutan en el host contra el broker y la base de datos publicados en loopback:

```bash
uv run seismic-stream bootstrap
uv run seismic-stream ingest --source replay --speed max
uv run seismic-stream process
uv run seismic-stream sink
uv run seismic-stream monitor
uv run seismic-stream doctor
uv run seismic-stream verify --timeout 180
uv run seismic-stream replay-info --file data/samples/usgs_2026-11-02_2026-11-09_m1.geojson
```

`ingest` también acepta `--file`, `--chaos/--no-chaos`, `--seed`, `--limit` y `--dry-run`; `process` acepta `--timeout` y `--count`; `sink` acepta `--timeout`; `verify` acepta `--file`, `--timeout` y `--json`. Cada comando termina con 0 si tiene éxito, con 1 ante un fallo de ejecución o de verificación, y con 2 ante un error de uso o de configuración.

### Comandos de desarrollo

| Comando | Propósito |
|---|---|
| `make help` | Lista todos los targets de Make |
| `make sync` | Instala el entorno bloqueado |
| `make lint` / `make fmt` | Lint y verificación de formato con Ruff / autocorrección |
| `make types` | `mypy --strict src` |
| `make arch` | Contratos de import-linter |
| `make test` | Tests unitarios, de propiedades y de arquitectura |
| `make test-int` / `make test-e2e` / `make test-ui` | Suites que requieren Docker |
| `make cov` | Suite completa con el umbral de cobertura del 85% |
| `make ci` | Equivalente local de cada job de CI |
| `make down` | Detiene el stack y conserva los volúmenes |
| `make replay` / `make demo-chaos` | Reinicia el servicio ingest |

## Configuración

Todas las variables son opcionales y las lee `pydantic-settings` con el prefijo `SEISMIC_`. Los valores por defecto apuntan al stack local de Compose. Los valores inválidos fallan de inmediato al arrancar, con código de salida 2 y un mensaje que nombra la variable en falla.

| Variable | Valor por defecto | Descripción |
|---|---|---|
| `SEISMIC_KAFKA_BOOTSTRAP` | `localhost:19092` | Endpoint de la API de Kafka; dentro de Compose es `redpanda:9092`. |
| `SEISMIC_SCHEMA_REGISTRY_URL` | `http://localhost:13081` | URL del Schema Registry compatible con Confluent. |
| `SEISMIC_CLICKHOUSE_HOST` | `localhost` | Host de ClickHouse; dentro de Compose es `clickhouse`. |
| `SEISMIC_CLICKHOUSE_PORT` | `8123` | Puerto HTTP de ClickHouse. |
| `SEISMIC_CLICKHOUSE_USER` | `seismic` | Usuario de ClickHouse. |
| `SEISMIC_CLICKHOUSE_PASSWORD` | `seismic-local` | Contraseña solo para desarrollo; reemplázala fuera del uso local. |
| `SEISMIC_CLICKHOUSE_DATABASE` | `seismic` | Base de datos que contiene las migraciones y las tablas. |
| `SEISMIC_INGEST_SOURCE` | `replay` | `replay` lee la muestra incluida; `usgs` consulta el feed en vivo. |
| `SEISMIC_REPLAY_FILE` | `data/samples/usgs_2026-11-02_2026-11-09_m1.geojson` | FeatureCollection de GeoJSON o NDJSON de features. |
| `SEISMIC_REPLAY_SPEED` | `1440` | Factor de compresión positivo, o `max` para desactivar el control de ritmo. |
| `SEISMIC_REPLAY_CHAOS` | `false` | Activa registros malformados, duplicados y revisiones deterministas. |
| `SEISMIC_CHAOS_SEED` | `42` | Semilla del plan de caos. |
| `SEISMIC_CHAOS_MALFORMED_RATE` | `0.02` | Probabilidad de una copia malformada, en `[0, 1]`. |
| `SEISMIC_CHAOS_DUPLICATE_RATE` | `0.05` | Probabilidad de un duplicado exacto, en `[0, 1]`. |
| `SEISMIC_CHAOS_REVISION_RATE` | `0.02` | Probabilidad de una revisión, en `[0, 1]`. |
| `SEISMIC_LIVE_FEED` | `all_hour` | Nombre del feed resumen de USGS; ver la lista permitida abajo. |
| `SEISMIC_LIVE_POLL_SECONDS` | `60` | Intervalo de consulta; los valores menores a 60 se rechazan. |
| `SEISMIC_INCLUDE_EVENT_TYPES` | `earthquake` | Tipos de evento de USGS a publicar, separados por comas. |
| `SEISMIC_ALERT_WARNING_MAG` | `6.0` | Umbral de WARNING, inclusivo. |
| `SEISMIC_ALERT_WATCH_MAG` | `5.0` | Umbral de WATCH, inclusivo. |
| `SEISMIC_ALERT_ECUADOR_MAG` | `4.0` | Umbral de WATCH dentro de la caja delimitadora de Ecuador. |
| `SEISMIC_WINDOW_MINUTES` | `60` | Duración de la ventana tumbling en tiempo de evento. |
| `SEISMIC_WINDOW_GRACE_MINUTES` | `10` | Retraso permitido después de que cierra una ventana. |
| `SEISMIC_STATE_DIR` | `./.state` | Directorio de estado local de Quix Streams; `/var/lib/seismic/state` en Compose. |
| `SEISMIC_SINK_BATCH_SIZE` | `500` | Máximo de registros vaciados por lote del sink. |
| `SEISMIC_SINK_BATCH_MAX_WAIT_SECONDS` | `1.0` | Espera máxima para completar un lote del sink. |
| `SEISMIC_RETRY_BASE_SECONDS` | `0.5` | Retardo base de la política de reintentos con backoff exponencial y tope. |
| `SEISMIC_RETRY_MAX_SECONDS` | `30` | Retardo máximo de reintento. |
| `SEISMIC_PRODUCER_QUEUE_MAX_MESSAGES` | `10000` | Límite de la cola del productor de librdkafka; las colas llenas activan esperas de contrapresión. |
| `SEISMIC_METRICS_PORT` | `9101` | Puerto de `/metrics`, `/healthz` y `/readyz`; cada servicio inyecta su propio valor por defecto. |
| `SEISMIC_MONITOR_GROUPS` | `seismic-processor,seismic-sink` | Grupos de consumidores vigilados por `monitor`, separados por comas. |
| `SEISMIC_MONITOR_INTERVAL_SECONDS` | `5` | Intervalo de muestreo del lag. |
| `SEISMIC_LOG_LEVEL` | `INFO` | Uno de `DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL`. |
| `SEISMIC_LOG_FORMAT` | `json` | `json` para logs estructurados, `console` para desarrollo local. |

`SEISMIC_IMAGE` en `.env.example` lo lee Compose (no `Settings`) y selecciona el tag de la imagen de aplicación construida localmente.

Feeds en vivo permitidos: `all_hour`, `all_day`, `all_week`, `2.5_hour`, `2.5_day`, `4.5_hour`, `4.5_day`, `significant_hour`, `significant_day`.

## Puertos

| Endpoint | Enlace en el host | Puerto del contenedor | Notas |
|---|---|---|---|
| Grafana | `127.0.0.1:13000` | 3000 | El único servicio publicado en el stack por defecto. |
| Redpanda Kafka | ninguno en el stack por defecto | 9092 interno, 19092 listener externo | El override de test publica `127.0.0.1:19092`. |
| Schema Registry | ninguno en el stack por defecto | 8081 interno, 18081 listener externo | El override de test publica `127.0.0.1:13081`. |
| Métricas admin de Redpanda | ninguno | 9644 | Prometheus raspa `/public_metrics`. |
| ClickHouse HTTP | ninguno en el stack por defecto | 8123 | El override de test publica `127.0.0.1:13123`. |
| Prometheus | ninguno | 9090 | API interna de scrape y consultas. |
| Métricas de ingest | ninguno | 9101 | `/metrics`, `/healthz`, `/readyz`. |
| Métricas de processor | ninguno | 9102 | `/metrics`, `/healthz`, `/readyz`. |
| Métricas de sink | ninguno | 9103 | `/metrics`, `/healthz`, `/readyz`. |
| Métricas de monitor | ninguno | 9104 | `/metrics`, `/healthz`, `/readyz`. |

Todos los enlaces del host usan loopback (`127.0.0.1`) y se mantienen dentro del rango del portafolio 13000-13999 / 19000-19999.

## Catálogo de métricas

Cada servicio registra solo las métricas que le pertenecen; `/metrics` devuelve texto OpenMetrics. Los nombres, tipos y etiquetas siguientes son la única fuente de verdad en `METRIC_CATALOG` y tienen pruebas de deriva contra esta tabla.

| Métrica | Tipo | Etiquetas | Significado |
|---|---|---|---|
| `seismic_build_info` | gauge | `service`, `version` | Información de build del servicio en ejecución. |
| `seismic_ingest_records_total` | counter | `source`, `outcome` | Registros de origen leídos por ingest, según `published`, `filtered` o `invalid`. |
| `seismic_ingest_backpressure_waits_total` | counter | ninguna | Esperas por cola llena del productor durante ingest. |
| `seismic_replay_schedule_lag_seconds` | gauge | ninguna | Cuánto retrasa el replay pacer respecto del cronograma. |
| `seismic_replay_progress_ratio` | gauge | ninguna | Avance del replay como fracción de los registros de origen. |
| `seismic_processor_events_total` | counter | `outcome` | Decisiones del processor: `new`, `revision` o `duplicate`. |
| `seismic_processor_alerts_total` | counter | `level` | Alertas emitidas, por `WARNING` o `WATCH`. |
| `seismic_processor_window_updates_total` | counter | ninguna | Actualizaciones de estadísticas de ventana emitidas. |
| `seismic_processor_skipped_total` | counter | `reason` | Registros omitidos como `undecodable` o `processing_error`. |
| `seismic_sink_rows_total` | counter | `table` | Filas almacenadas por el sink, por tabla de ClickHouse. |
| `seismic_sink_batches_total` | counter | `table`, `outcome` | Lotes del sink procesados, por tabla y resultado. |
| `seismic_sink_insert_seconds` | histogram | `table` | Duración de los inserts en ClickHouse. |
| `seismic_sink_retries_total` | counter | ninguna | Reintentos transitorios del sink. |
| `seismic_sink_paused` | gauge | ninguna | `1` mientras la asignación del consumidor está pausada. |
| `seismic_sink_end_to_end_seconds` | histogram | ninguna | Latencia desde la ingesta hasta el almacenamiento. |
| `seismic_dlq_messages_total` | counter | `stage`, `error_type` | Dead letters, por etapa y tipo de error. |
| `seismic_consumer_lag_messages` | gauge | `group`, `topic` | Lag del consumidor en mensajes. |
| `seismic_topic_end_offset` | gauge | `topic` | Offset final actual de un topic. |

El puerto de métricas de `ingest` desaparece entre replays porque el servicio es un contenedor de una sola ejecución; Prometheus marca ese target como caído hasta el siguiente replay, lo cual es esperado.

## Simulacros de caos y caídas

La inyección determinista de fallos agrega registros malformados, duplicados exactos y revisiones justo después de su registro de origen. Solo agrega registros, así que el conjunto de ids válidos nunca depende del plan de caos.

```bash
./install.sh --replay --chaos
docker compose run --rm cli verify --timeout 180
docker compose exec -T clickhouse clickhouse-client --user seismic --password seismic-local \
  -q "SELECT stage, error_type, count() FROM seismic.dead_letters_latest GROUP BY stage, error_type"
```

Por defecto, la misma semilla (`42`) y la misma muestra producen el mismo plan. Para observar la contrapresión y la recuperación, detén ClickHouse mientras un replay está en ejecución y vuelve a iniciarlo:

```bash
docker compose stop clickhouse
sleep 10
docker compose start clickhouse
docker compose run --rm cli verify --timeout 180
```

El sink pausa su consumidor, `seismic_sink_paused` pasa a 1 y el lag del consumidor sube; el sink reintenta con backoff exponencial y tope sin confirmar offsets, luego reanuda y se pone al día. `/readyz` responde 503 tras 30 segundos de fallos continuos. No se pierde ningún evento.

## Recetas de broker con rpk

`rpk` se ejecuta dentro del contenedor de Redpanda, así que funciona en cualquier sistema operativo del host:

```bash
docker compose exec redpanda rpk topic list
docker compose exec redpanda rpk topic consume seismic.dlq.v1 -n 5
docker compose exec redpanda rpk topic consume seismic.events.v1 -n 1 --offset start
docker compose exec redpanda rpk group list
docker compose exec redpanda rpk group describe seismic-sink
```

Inspecciona el registro de esquemas desde el host:

```bash
curl -s http://127.0.0.1:13081/subjects
curl -s http://127.0.0.1:13081/config/seismic.events.v1-value
```

El puerto del Schema Registry lo publica `compose.test.yaml`; sin ese override, ejecuta el `curl` equivalente desde dentro de la red `backend`.

## Modo en vivo

El modo en vivo es opcional y requiere salida a internet solo para el contenedor `ingest`. Consulta el feed resumen GeoJSON documentado de USGS (como máximo una vez cada 60 segundos, con peticiones condicionales `If-Modified-Since` y backoff exponencial ante errores):

```bash
docker compose -f compose.yaml -f compose.live.yaml up -d --wait
docker compose logs -f ingest
```

O con la CLI del host: `SEISMIC_INGEST_SOURCE=usgs SEISMIC_LIVE_FEED=all_hour uv run seismic-stream ingest`. El resto sigue en la red interna `backend`. Los tests nunca llaman al feed en vivo; usan fixtures derivados de la muestra grabada.

## Resolución de problemas

| Síntoma | Verificación | Solución |
|---|---|---|
| Grafana no responde | `docker compose ps` | Espera al healthcheck; confirma que el puerto 13000 esté libre. |
| Servicio no saludable | `docker compose logs <service> --tail 50` | Revisa la línea de log en JSON; `doctor` informa topics, esquemas o tablas faltantes. |
| `verify` informa ids faltantes | `docker compose logs ingest sink --tail 100` | Espera a que termine el replay, luego vuelve a ejecutar `doctor`; revisa el lag del sink en Grafana. |
| Sink pausado por mucho tiempo | `docker compose ps clickhouse`, logs de ClickHouse | ClickHouse está caído o sin disco; el sink reanuda solo. |
| Configuración inválida | error de arranque que nombra `SEISMIC_*` | Corrige el valor en la shell o en `.env`; el código de salida es 2. |
| Datos de replay obsoletos | `docker compose exec redpanda rpk topic list` | Ejecuta `./install.sh --purge --yes` para un estado local limpio. |
| Error de modo Docker en Windows | `docker info --format '{{.OSType}}'` | Cambia Docker Desktop a contenedores Linux. |
| Replay demasiado lento | tiempo transcurrido frente a la estimación de velocidad | Usa `--speed max` para tests o una velocidad mayor en el instalador. |

Diagnósticos útiles:

```bash
docker compose ps
docker compose run --rm cli doctor
docker compose run --rm cli verify --timeout 180
docker compose exec -T prometheus wget -qO- http://localhost:9090/api/v1/targets
```

## Desinstalación

```bash
./install.sh --down        # detiene los contenedores y conserva volúmenes y .env
./install.sh --purge --yes # elimina contenedores, volúmenes, imágenes locales y .env
```

En Windows usa `.\install.ps1 -Down` y `.\install.ps1 -Purge -Yes`. La purga elimina los volúmenes con nombre, así que se pierde el historial de ClickHouse y Redpanda; nunca toca otros proyectos de Compose.
