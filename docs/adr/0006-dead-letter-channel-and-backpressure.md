# ADR 0006: Canal de dead letters, pausa del consumidor y bisección de lotes

- **Estado:** Aceptada
- **Fecha:** 2026-11-20
- **Relacionados:** [Arquitectura](../ARCHITECTURE.md) · [Operación](../OPERATIONS.md) · [ADR 0005](0005-delivery-semantics-effectively-once.md)

## Contexto

Un pipeline de streaming debe sobrevivir a datos malos y a una base de datos no disponible sin perder datos buenos ni girar para siempre. El sink recibe cuatro topics Avro, escribe en ClickHouse por HTTP, y puede enfrentar dos fallos muy distintos:

- **transitorio**: conexión rechazada, timeout, HTTP 5xx o un código de sobrecarga de ClickHouse (159, 202, 209, 210, 241, 252);
- **permanente**: una fila que el servidor rechaza por su tipo o su valor.

Tratar ambos como reintentables bloquearía para siempre por una sola fila venenosa. Tratar ambos como venenosos descartaría un lote entero durante un reinicio de la base de datos.

## Decisión

1. Clasificar cada error de almacenamiento en el adaptador (`classify_clickhouse_error`) como `TransientStoreError` o `PermanentStoreError`; la capa de aplicación nunca importa un tipo de excepción del driver.
2. Ante un error transitorio, pausar todas las particiones asignadas, poner `seismic_sink_paused=1`, contar `seismic_sink_retries_total`, dormir `min(SEISMIC_RETRY_MAX_SECONDS, SEISMIC_RETRY_BASE_SECONDS * 2^attempt)` con jitter completo a través del `Clock` inyectado, y reintentar el mismo lote. No confirmar offsets hasta que el lote tenga éxito. Reanudar y limpiar el gauge tras el éxito. `/readyz` pasa a no listo tras 30 s de fallos continuos (S11).
3. Ante un error permanente, bisectar el lote: probar el padre, luego las mitades, hasta que se rechace una sola fila. Almacenar cada fila aceptada y emitir solo las filas rechazadas como dead letters `sink_persist`. Los tokens de sub-lote agregan `:<lo>-<hi>` para que las mitades aceptadas no se confundan con el intento padre.
4. Enrutar los bytes de Kafka no decodificables a `sink_decode` con el payload crudo y el topic, la partición y el offset de origen, y continuar. Nunca crear una dead letter de una dead letter; un registro de DLQ no decodificable se registra y se omite.
5. Usar `dlq_id = sha256(stage|payload)[:32]`, publicar en `seismic.dlq.v1` con clave ese id, persistir el envelope en `dead_letters` y contar `seismic_dlq_messages_total{stage,error_type}`.
6. Hacer flush del productor de DLQ antes de confirmar los offsets de origen, de modo que una fila rechazada no pueda reconocerse sin que su dead letter sea duradera.
7. Manejar SIGTERM terminando el lote actual, haciendo flush de la DLQ y confirmando antes de salir con 0; un sink inactivo también sale limpiamente tras `--timeout`.

## Consecuencias

- Una caída de ClickHouse provoca contrapresión en lugar de pérdida de datos: el lag sube mientras el consumidor está pausado y vuelve a cero tras la recuperación.
- Una mala fila no puede envenenar un lote completo; las filas sanas del mismo lote se almacenan y solo la fila venenosa va a dead letter.
- La bisección cuesta O(log n) escrituras extra por fila venenosa y está acotada por el tamaño del lote, que es pequeño (`SEISMIC_SINK_BATCH_SIZE`, por defecto 500).
- El jitter de reintento es determinista en los tests porque el RNG y el reloj se inyectan.
- El loop de reintento espera indefinidamente un fallo transitorio por diseño; los operadores ven `seismic_sink_paused` y `/readyz`, y pueden detener el servicio con SIGTERM, que aún termina el lote actual antes de salir.
- Los envelopes de DLQ son deterministas, así que reproducir los mismos bytes venenosos produce el mismo id y ClickHouse reemplaza en lugar de duplicar la dead letter.

## Alternativas consideradas

- **Reintentar cualquier error.** Descartada: una fila rechazada de forma permanente bloquearía el pipeline para siempre.
- **Enviar a DLQ el lote completo ante cualquier error permanente.** Descartada: descartaría filas buenas que compartían lote con una fila venenosa.
- **Caer y reiniciar ante errores transitorios.** Descartada: los reinicios agitan el consumidor y pierden el límite del lote en vuelo; la pausa y reanudación es explícita y observable.
- **Una transacción de Kafka alrededor de la publicación en DLQ y la confirmación de offsets.** Descartada: el sink es un loop propio y el productor de DLQ es compartido; el flush antes de confirmar da el orden requerido con menos maquinaria (ADR 0005).
