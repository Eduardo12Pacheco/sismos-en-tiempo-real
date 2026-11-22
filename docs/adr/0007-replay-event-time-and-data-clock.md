# ADR 0007: Replay, tiempo de evento y reloj de datos

- **Estado:** Aceptada
- **Fecha:** 2026-11-20
- **Relacionados:** [Arquitectura](../ARCHITECTURE.md) · [Datos](../DATA.md) · [ADR 0008](0008-grafana-dashboards-as-code.md)

## Contexto

El proyecto debe demostrar un pipeline de streaming completo sin servicios externos, sin acceso a red y con resultados reproducibles. Se incluye una semana de USGS grabada, pero reproducirla a velocidad real haría inútiles las demos y los tests end-to-end. Al mismo tiempo, el ventaneo por tiempo de evento debe seguir siendo exactamente reproducible sin importar qué tan rápido corra el replay.

El modo en vivo escribe eventos cuyo `event_time` está cerca del `now` del reloj de pared. El replay escribe eventos cuyo `event_time` está a días de distancia en el pasado, así que un dashboard guiado por el reloj de pared mostraría paneles vacíos hasta que el replay alcance al reloj.

## Decisión

1. **Muestra grabada como fuente por defecto.** `ReplaySource` lee el FeatureCollection GeoJSON incluido (o NDJSON) y produce `SourceRecord` ordenados por `(properties.time, id)`; las features sin timestamp utilizable mantienen el orden del archivo al final. El ordenamiento hace determinista un replay sin control de ritmo.
2. **Compresión temporal con un único pacer.** `ReplayPacer` ancla en el primer registro con tiempo en `(wall0, event0)` y duerme `max(0, wall0 + (event_ms - event0) / 1000 / speed - clock.now())`. `speed` es un float mayor que cero; `--speed max` se mapea a `None`, que nunca duerme. Para tiempos de evento crecientes, la suma total de espera programada es exactamente `(last_event_ms - first_event_ms) / 1000 / speed`; el pacer se limita a cero y expone `schedule_lag_seconds` para la métrica `seismic_replay_schedule_lag_seconds`.
3. **El caos solo agrega registros.** `ChaoticSource` decora cualquier `EventSource` y, después de cada registro de origen, puede agregar una copia malformada, un duplicado exacto o una revisión producida a partir de un `random.Random` con semilla. Nunca elimina, descarta ni reordena registros de origen, así que el conjunto de ids válidos almacenados no depende del plan de caos. `ChaosPolicy.plan()` recorre el mismo camino de código que la fuente, lo que permite a los tests derivar los conteos esperados de inyección.
4. **Tiempo de evento, no tiempo de procesamiento.** La topología de Quix Streams extrae `event_time_ms` para el ventaneo y usa ventanas tumbling con período de gracia. La velocidad del reloj de pared solo cambia cuánto tarda un replay, nunca los agregados que produce.
5. **Una sola partición.** Todos los topics tienen una partición para que el orden por tiempo de evento se preserve de extremo a extremo y los resultados de ventanas sean exactamente reproducibles a cualquier velocidad. El escalado a escala de ADR (más particiones, ventanas paralelas por clave) se documenta como trabajo futuro y no se implementa.
6. **Reloj de datos para los dashboards.** Los paneles sísmicos calculan `shift_seconds = dateDiff('second', max(event_time), now())` desde ClickHouse y muestran `event_time + shift_seconds`, de modo que el evento almacenado más nuevo se lee como "ahora". Un panel estadístico muestra el tiempo real del evento más nuevo y el desplazamiento. En modo en vivo el desplazamiento es aproximadamente cero, así que los mismos dashboards funcionan en ambos modos.
7. **Sin loop de replay.** El replay termina cuando se agota el archivo. Hacer loop duplicaría ids de evento y ocultaría las garantías de entrega; volver a ejecutar el comando es explícito e idempotente por construcción en el lado del almacenamiento.

## Consecuencias

- Las demos y los tests e2e corren en minutos; `SEISMIC_REPLAY_SPEED=1440` reproduce la semana en unos siete minutos, y `max` está limitado solo por la velocidad de procesamiento.
- Los agregados son independientes de la velocidad del replay, lo que hace significativa la implementación de referencia en los tests.
- El pacer es determinista con un `Clock` inyectado y no necesita dormir en los tests unitarios.
- Las revisiones no se recuentan en las ventanas (el processor excluye las revisiones), y los resultados de ventanas siguen siendo provisionales hasta que transcurra el período de gracia. Esto se documenta como limitación.
- El orden por tiempo de evento depende de la partición única; aumentar el paralelismo requiere una nueva decisión de particionado y un nuevo ADR.

## Alternativas consideradas

- **Replay a velocidad real.** Descartada: una corrida de una semana es inutilizable para demos y CI.
- **Dormir según timestamps de procesamiento.** Descartada: los resultados de ventanas dependerían de la carga de la máquina y de la velocidad del replay.
- **Hacer loop del archivo para mantener ocupado el dashboard.** Descartada: los ids duplicados interferirían con la deduplicación y con los tests de garantías de entrega.
- **Desplazar los datos al ingerir.** Descartada: mutaría el tiempo de evento e impediría comparar las filas almacenadas con la grabación de origen. Desplazar solo en el momento de la consulta mantiene fieles los datos almacenados.
- **Varias particiones con ventanas por clave.** Descartada para v0.1.0: la muestra es pequeña, una partición mantiene trivial la reproducibilidad y la ruta de escalado se documenta en su lugar.
