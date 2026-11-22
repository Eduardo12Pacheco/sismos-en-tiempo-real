# ADR 0002: Quix Streams como procesador de streams con estado

- **Estado:** Aceptada
- **Fecha:** 2026-11-20
- **Relacionados:** [Arquitectura](../ARCHITECTURE.md) · [ADR 0001](0001-redpanda.md) · [ADR 0007](0007-replay-event-time-and-data-clock.md)

## Contexto

El processor debe demostrar procesamiento de streams con forma de producción: detección de duplicados exactos con estado indexada por `event_id`, ventanas tumbling de tiempo de evento con período de gracia, una rama de alertas configurable y registros que pueden compararse exactamente contra una implementación de referencia independiente. Se ejecuta localmente dentro del stack de Compose, solo en CPU, con un presupuesto de memoria de Docker de 4 GB, Python 3.12 y sin cuenta en la nube.

Las librerías candidatas se evaluaron el 2026-11-19. Evidencia:

| Candidata | Evidencia de releases | Ventanas y estado | Schema Registry / Avro | Veredicto |
|---|---|---|---|---|
| **Quix Streams 3.28.1** | Subida a PyPI el 2026-10-15; Apache-2.0; Python `>=3.11,<4`; releases cada 2-4 semanas (3.25.0 el 24 de jul, 3.26.0 el 14 de sep, 3.27.0 el 30 de sep, 3.28.0/3.28.1 el 15 de oct); repositorio activo (último push el 2026-11-18) | Ventanas tumbling, hopping, sliding y session integradas; estado en RocksDB con topics de changelog; `apply(stateful=True)` | `AvroSerializer`/`AvroDeserializer` y `SchemaRegistryClientConfig` incluidos | **Seleccionada** |
| Bytewax 0.21.1 | Subido el 2024-11-25; sin release en unos 12 meses al momento de la verificación | Dataflow en Python con ventanas; estado vía recuperación | Sin gestión integrada de subjects y versiones | Descartada por desactualizada |
| faust-streaming 0.15.4 | Subido el 2026-10-28; fork comunitario mantenido | Modelo de objetos asyncio, API declarativa de ventanas más débil | Sin adaptadores integrados de Schema Registry ni Avro | Descartada: más código de pegamento, menos probada |
| Apache Flink | Maduro y muy usado | Ventanas de tiempo de evento y estado ricos | Requiere integración externa de Avro y registry | Descartado: JVM y una huella de clúster excesiva para una laptop |

La biblioteca elegida fija `confluent-kafka<2.13`, `pydantic<2.14` y `pydantic-settings<2.16`; el lockfile respeta esos pins en lugar de forzar versiones más nuevas. Hay wheels para Linux y wheels nativos de Windows para `confluent-kafka` 2.16.2 y `rocksdict` 0.3.29 (cp312) publicados, así que la matriz de tests unitarios puede instalar de forma nativa; el stack de runtime en sí corre en contenedores Linux.

## Decisión

1. Usar **Quix Streams 3.28.1** (`quixstreams[avro]`) solo para el processor. Ingest y el sink siguen con `confluent-kafka` simple, como exige D9.
2. Construir la `Application` con el consumer group `seismic-processor`, `auto_offset_reset="earliest"`, `processing_guarantee="at-least-once"`, un `state_dir` configurable (el volumen `processor-state` en Compose) y un `timestamp_extractor` que devuelve `event_time_ms` (D12).
3. Deduplicar con un `apply` con estado indexado por la clave de evento de Kafka. El estado guarda `updated_at_ms` por `event_id`; `Deduplicator.decide` devuelve NEW para un id no visto, REVISION para un `updated_at_ms` estrictamente mayor, y DUPLICATE en otro caso. Los duplicados se filtran; las revisiones fluyen a alertas pero nunca a las ventanas.
4. Agregar los eventos NEW en ventanas tumbling (`SEISMIC_WINDOW_MINUTES`, por defecto 60; gracia `SEISMIC_WINDOW_GRACE_MINUTES`, por defecto 10) agrupadas por `region`, usando `Count`, `Max`, `Mean` y `Sum` sobre la energía de Gutenberg-Richter, y emitir cada actualización con `.current()` para que el último mensaje por `(region, window_start_ms)` sea el agregado final.
5. Serializar `window_stats` y `alert` con los serializadores Avro de Quix configurados con `auto_register_schemas=False` y `use_latest_version=True`, de modo que solo `bootstrap` registre contratos (D4).
6. Convertir solo los fallos de procesamiento `KeyError`/`ValueError` en filas omitidas; los bytes no decodificables se registran, se cuentan como `seismic_processor_skipped_total{reason="undecodable"}` y se omiten, de modo que los bytes venenosos no puedan detener el pipeline.
7. Derivar los timestamps de emisión de las ventanas desde el fin de la ventana, nunca desde el reloj de pared, manteniendo los agregados reproducibles a cualquier velocidad de replay.

## Consecuencias

- Los resultados de ventanas son deterministas y pueden compararse campo por campo con `tests/support/reference.py`, que recorre las features crudas de USGS con Python simple y no importa Quix Streams.
- La deduplicación con estado conserva un `updated_at_ms` por id de evento visto. El estado es teóricamente ilimitado, lo que se documenta como limitación para v0.1.0.
- `.current()` emite un mensaje por evento y ventana abierta. Los lectores downstream deben usar la última versión por clave; el `ReplacingMergeTree` del sink de ClickHouse provee esa semántica en T9.
- La biblioteca agrega `rocksdict` y un topic de changelog de estado al stack, lo que es aceptable dentro del presupuesto de memoria y da recuperación ante caídas sin costo adicional.
- La API de Quix queda envuelta detrás de `adapters/stream/topology.py`; reemplazarla solo requeriría un nuevo adaptador, porque la semántica de deduplicación, energía y alertas vive en `application/` y `domain/`.
- Usar Quix para el processor mientras el sink sigue siendo un loop propio implica dos stacks de clientes de Kafka, pero cada uno se usa donde es más fuerte (D9).

## Alternativas consideradas

- **Quix Streams también para ingest y el sink.** Descartada: el almacenamiento debe seguir siendo un puerto `EventStore` con pausa y reanudación explícitas, reintentos y bisección por lotes, que es política de aplicación (D9/D10).
- **Bytewax.** Descartada: sin release durante aproximadamente un año al momento de la evaluación y sin integración integrada con Schema Registry.
- **faust-streaming.** Descartada: mantenido, pero su API de objetos asyncio no ofrece soporte integrado para Avro ni Schema Registry, y su API declarativa de ventanas es más débil.
- **Apache Flink.** Descartada: la huella de JVM más clúster es desproporcionada para una demo local de un solo nodo.
- **Ventanas escritas a mano sobre `confluent-kafka` crudo.** Descartada: reimplementaría almacenes de estado, changelogs y cierre por tiempo de evento, sumando riesgo sin demostrar una capacidad distinta.
