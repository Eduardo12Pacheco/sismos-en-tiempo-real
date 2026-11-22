# ADR 0005: Entrega effectively-once sin transacciones de Kafka

- **Estado:** Aceptada
- **Fecha:** 2026-11-20
- **Relacionados:** [Arquitectura](../ARCHITECTURE.md) · [ADR 0004](0004-clickhouse-replacingmergetree-and-dedup-tokens.md) · [ADR 0006](0006-dead-letter-channel-and-backpressure.md)

## Contexto

Cualquier etapa puede caerse o reintentarse: el productor puede reentregar tras perderse un acuse del broker, el processor escribe el estado y la salida por separado, y el sink confirma offsets de Kafka después de escribir en ClickHouse. Las transacciones de Kafka solo cubrirían escrituras Kafka a Kafka; el almacenamiento final es ClickHouse, fuera de la transacción, así que una transacción por sí sola no puede hacer atómico el efecto de extremo a extremo.

El proyecto debe declarar con honestidad qué garantía de entrega ofrece y qué duplicados todavía pueden observarse.

## Decisión

1. Ejecutar cada consumidor con `enable.auto.commit=false` y `auto.offset.reset=earliest`, y confirmar solo después de que los efectos del lote sean duraderos (salida del processor producida, o filas del sink y dead letters escritas).
2. Aceptar entrega al menos una vez y hacer idempotente a cada receptor:
   - el processor guarda un `updated_at_ms` por `event_id` en el estado de Quix y descarta duplicados;
   - las tablas de ClickHouse son tablas `ReplacingMergeTree` versionadas, leídas a través de vistas `FINAL`;
   - cada insert lleva un `insert_deduplication_token` derivado de Kafka;
   - las dead letters tienen un `dlq_id = sha256(stage|payload)[:32]` determinístico.
3. **No** usar transacciones de Kafka ni el modo `exactly-once` de Quix. El sink es un consumidor propio fuera de Quix (D9), y las transacciones agregarían latencia y complejidad operativa sin cubrir la escritura en ClickHouse.
4. Confirmar el offset más alto consumido más uno por partición solo después de que el productor de DLQ haya hecho flush, de modo que una fila venenosa no pueda perderse entre la DLQ y la confirmación del offset.
5. Documentar los duplicados transitorios en las tablas raw como esperados: una caída después de un insert en ClickHouse pero antes del commit en Kafka vuelve a insertar las mismas filas con el mismo token, que ClickHouse luego deduplica; si un merge ya reemplazó filas, `FINAL` sigue devolviendo una fila lógica.

## Consecuencias

- La garantía observable es **effectively-once**: las lecturas lógicas (`events_latest`, `alerts_latest`, `dead_letters_latest`, agregado final por clave) son estables frente a replays y reintentos, mientras que las tablas raw pueden contener temporalmente historial duplicado.
- `seismic-stream verify` puede afirmar conjuntos de ids y lag, e informar la diferencia entre filas raw y filas latest en lugar de fingir que es cero.
- El orden de confirmación "al menos una vez" significa que una caída puede reproducir un lote; eso es seguro porque cada receptor es idempotente. No puede perder un mensaje reconocido.
- El estado de deduplicación del processor es local y teóricamente ilimitado; la pérdida del volumen `processor-state` volvería a emitir agregados, lo que la tabla versionada de agregados de ClickHouse absorbe.
- Sin coordinador de transacciones, sin `transactional.id` y sin two-phase commit: menos piezas móviles para una demo local, a costa de una garantía explícitamente acotada en alcance.

## Alternativas consideradas

- **Transacciones de Kafka de extremo a extremo.** Descartada: ClickHouse es el sink final y no forma parte de una transacción de Kafka; las lecturas aún necesitarían coordinación `read_committed`, y el exactly-once de Quix agrega latencia sin cubrir el almacenamiento.
- **Exactly-once deduplicando solo en ClickHouse.** Descartada: los duplicados inflarían las filas raw y los costos de insert aunque las vistas los ocultaran.
- **At-most-once (confirmar antes de escribir).** Descartada: una caída podría perder eventos en silencio, lo que viola S11/S12.
