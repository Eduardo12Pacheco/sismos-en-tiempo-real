# ADR 0004: ClickHouse ReplacingMergeTree con tokens de deduplicación de inserts

- **Estado:** Aceptada
- **Fecha:** 2026-11-20
- **Relacionados:** [Arquitectura](../ARCHITECTURE.md) · [ADR 0005](0005-delivery-semantics-effectively-once.md) · [ADR 0006](0006-dead-letter-channel-and-backpressure.md)

## Contexto

El sink almacena cuatro flujos orientados a append: eventos sísmicos, agregados de ventanas, alertas y dead letters. La entrega de Kafka es al menos una vez, reproducir la muestra incluida es una acción de demo de primera clase, y las revisiones de un mismo evento de USGS llegan con un `updated_at_ms` más nuevo. Por lo tanto, el repositorio debe aceptar la misma fila lógica más de una vez y aun así exponer una fila actual por identidad.

El almacenamiento debe ser local, columnar, compartido por varios contenedores y consultable desde Grafana mientras el stack corre. También debe funcionar en Linux, macOS y Windows mediante Docker Desktop, sin cuenta en la nube.

## Decisión

1. Usar `clickhouse/clickhouse-server:26.8` (la línea LTS) como única base de datos, y `clickhouse-connect` sobre HTTP 8123 desde el sink. Se descartaron DuckDB (un solo escritor, sin servicio compartido en vivo) y PostgreSQL (store por filas, escaneos columnares más débiles).
2. Guardar eventos y alertas en tablas `ReplacingMergeTree(<version>)` con clave `event_id`, usando `updated_at` como versión. Guardar dead letters con clave `dlq_id`, usando `failed_at` como versión. Guardar agregados con clave `(region, window_start)`, usando `event_count` como versión, de modo que gane la actualización de agregado más nueva.
3. **No** particionar las tablas. ClickHouse reemplaza filas solo dentro de una misma partición, y una revisión puede cambiar `event_time`; una sola partición mantiene correctas la semántica de identidad y versión para este volumen de datos.
4. Leer el estado actual solo a través de vistas `*_latest` que usan `FINAL`: `events_latest`, `window_stats_latest`, `alerts_latest` y `dead_letters_latest`. Una vista `data_clock` expone el `event_time` más nuevo almacenado, el reloj de pared y su desplazamiento para los dashboards.
5. Dar a cada insert un `insert_deduplication_token` explícito con la forma `"<topic>:<partition>:<first_offset>-<last_offset>"` y configurar `non_replicated_deduplication_window = 1000` en cada tabla. Así, un lote de Kafka reintentado inserta cero filas crudas adicionales, incluso antes de los merges en segundo plano. Los sub-lotes bisectados agregan rangos de índices de fila `:<lo>-<hi>` para que sus escrituras se distingan del intento padre.
6. Distribuir el esquema como seis archivos `.sql` incluidos, aplicados en orden por `ClickHouseMigrator` y registrados en `seismic.schema_migrations`. Cada sentencia es `IF NOT EXISTS`, así que `bootstrap` es idempotente y una caída a mitad de una migración puede reintentarse de forma segura.
7. Tratar la tabla raw como un log de append y la vista `*_latest` como la tabla lógica. Reproducir la misma muestra dos veces mantiene `events_latest` estable mientras el conteo de filas de `events` raw puede crecer; esa diferencia se informa, nunca se afirma que sea cero.

## Consecuencias

- `events_latest` expone exactamente una fila por id de evento, y la revisión más nueva gana incluso cuando una revisión más antigua llega después. Reproducir dos veces deja el conteo lógico sin cambios.
- Los lotes idénticos se deduplican en el momento del insert, así que los reintentos tras una caída entre ClickHouse y el commit de Kafka no multiplican filas.
- Los merges en segundo plano hacen el reemplazo físico; los lectores pagan el costo de `FINAL`. A la escala de la muestra es despreciable y mantiene la demo honesta.
- Una revisión que cambia el tiempo de evento permanece en la misma partición, así que la resolución de versiones no se pierde.
- El ledger de migraciones registra versiones solo después de que todas las sentencias de un archivo tienen éxito. Agregar una columna o un índice más adelante requiere un nuevo archivo numerado, nunca editar uno ya aplicado.
- El adaptador traduce cada excepción del driver a la taxonomía transitoria/permanente de D10 antes de que llegue al código de aplicación; el sink nunca importa `clickhouse_connect`.

## Alternativas consideradas

- **DuckDB con un archivo en un volumen compartido.** Descartada: bloqueo de escritor único, sin servicio HTTP en vivo para Grafana y sin ventana de deduplicación de inserts.
- **PostgreSQL.** Descartada: no es columnar, escaneos analíticos más débiles sobre los agregados y menos interesante para una demostración de ingeniería de datos.
- **`ReplacingMergeTree` con `PARTITION BY toYYYYMM(event_time)`.** Descartada: las revisiones que cambian de mes evitarían el reemplazo; el volumen de datos no justifica particiones.
- **Consultar y luego insertar o actualizar a nivel de aplicación.** Descartada: tiene condiciones de carrera, es más lenta e innecesaria cuando el motor ya versiona filas y deduplica lotes.
