# ADR 0003: Contratos Avro y compatibilidad con Schema Registry

- **Estado:** Aceptada
- **Fecha:** 2026-11-20
- **Relacionados:** [Arquitectura](../ARCHITECTURE.md) · [ADR 0001](0001-redpanda.md) · [ADR 0005](0005-delivery-semantics-effectively-once.md)

## Contexto

Cuatro topics de Kafka transportan datos entre servicios independientes (`ingest`, `processor`, `sink`). Los payloads deben estar versionados, ser autodescriptivos y ser legibles por servicios construidos en momentos distintos, sin incrustar esquemas en los mensajes ni depender de nombres de campos JSON escritos a mano. El broker local ya provee un Schema Registry compatible con Confluent (ADR 0001), y el proyecto tiene requisitos de cero cuentas y funcionamiento offline: los contratos deben vivir en el repositorio y poder probarse sin un registry en ejecución.

## Decisión

1. Usar Avro para los cuatro contratos de valor: `earthquake_event.v1`, `window_stats.v1`, `alert.v1` y `dead_letter.v1`. Las claves siguen siendo strings UTF-8 simples (`event_id`, `<region>|<window_start_ms>`, `dlq_id`).
2. Guardar los esquemas como archivos `.avsc` dentro del paquete instalado (`seismic_stream.adapters.kafka.schemas`) y cargarlos con `importlib.resources`, de modo que el wheel y los tests lean exactamente los mismos bytes.
3. Nombrar los subjects con la convención `TopicNameStrategy`: `<topic>-value`.
4. Exigir compatibilidad `BACKWARD` en cada subject. Los consumidores se actualizan antes que los productores; por lo tanto, un campo nuevo debe ser opcional y tener valor por defecto.
5. Registrar esquemas solo desde `seismic-stream bootstrap`. Los codecs de servicio usan `auto.register.schemas=false` y `use.latest.version=true`, de modo que un servicio nunca pueda crear una versión nueva en silencio.
6. No usar logical types de Avro: los milisegundos siguen siendo campos `long` llamados `*_ms`, lo que mantiene a los decodificadores devolviendo enteros en lugar de objetos con zona horaria.
7. Probar la evolución offline con fastavro (un esquema compatible v2 lee datos v1; un campo requerido sin valor por defecto lanza `SchemaResolutionError`) y online con la API `test_compatibility` del registry en la suite de integración.

## Consecuencias

- Un cambio de esquema es un cambio revisado en el repositorio más un test de compatibilidad, no un efecto secundario implícito en runtime.
- Productores y consumidores comparten una sola fuente de contratos; los codecs son mapeos delgados a dataclasses de dominio congeladas.
- El contrato v1 no puede agregar luego un campo requerido sin una nueva versión del subject y un plan de migración; esta es exactamente la garantía de compatibilidad que el proyecto quiere demostrar.
- La disponibilidad del Schema Registry es requerida en runtime para la deserialización (el id del esquema escritor se resuelve a través de él), lo cual es aceptable porque el broker y el registry forman parte del mismo stack de Compose.
- Los archivos `.avsc` son pequeños y se incluyen en el wheel, así que `uv tool install` y la imagen de contenedor pueden ejecutarse sin un checkout.

## Alternativas consideradas

- **JSON plano sin registry.** Descartada: sin garantía de compatibilidad ni id de esquema, y el drift de campos aparecería solo en runtime.
- **JSON Schema con el registry.** Descartada: tipado numérico y de uniones más débil, y mapeo menos natural a los registros de dominio fijos; Avro es el formato nativo del stack elegido de Quix Streams y Kafka.
- **Protobuf.** Descartada: requiere código generado y un paso de compilador en la construcción de la imagen; el mapeo por diccionarios de Avro mantiene los adaptadores delgados y los contratos legibles en los tests.
- **Incrustar el esquema escritor en cada mensaje.** Descartada: sobrecarga en el payload y una segunda fuente de verdad; el registry ya guarda los esquemas por id.
- **Registrar de forma perezosa desde cada servicio.** Descartada: un error de tipeo o un experimento local publicaría una nueva versión de esquema desde cualquier proceso. Un único bootstrap idempotente hace que el registro sea un paso de despliegue explícito.
- **Compatibilidad `FULL`.** Descartada: más estricta de lo necesario para una evolución aditiva con consumidores primero, y más difícil de demostrar con la topología de un solo escritor.
