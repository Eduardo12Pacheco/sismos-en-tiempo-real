# ADR 0001: Redpanda como broker local con API de Kafka

- **Estado:** Aceptada
- **Fecha:** 2026-11-20
- **Relacionados:** [Arquitectura](../ARCHITECTURE.md) · [Operación](../OPERATIONS.md) · [ADR 0003](0003-avro-schema-registry.md)

## Contexto

El proyecto es un pipeline de streaming local-first: un solo `docker compose up` en una laptop (presupuesto de memoria de Docker de 8 GB, 4 GB como objetivo) debe proveer un broker con API de Kafka, un Schema Registry y un endpoint de métricas, sin servicios en la nube, API keys ni un clúster multi-nodo. La demo tiene que arrancar rápido, sobrevivir a reinicios mediante volúmenes con nombre y exponer las mismas APIs de productor y consumidor que usan Quix Streams y confluent-kafka.

El pipeline publica unos 1.700 eventos por semana grabada y los reproduce a hasta `--speed max`. El orden importa más que el throughput: las ventanas de tiempo de evento deben ser reproducibles, por lo que es preferible una sola partición.

## Decisión

1. Usar `redpandadata/redpanda:v26.2.7`, un solo nodo iniciado con `--mode dev-container --smp 1 --memory 1G`, como único broker.
2. Habilitar el Schema Registry integrado en la misma imagen en lugar de ejecutar un contenedor Confluent Schema Registry separado. Esto reduce a la mitad el costo de memoria y de arranque, y deja una sola dependencia de salud.
3. Mantener una partición y factor de replicación 1 para los cuatro topics. La retención es de 7 días para `seismic.events.v1` y `seismic.aggregates.v1`, y de 30 días para `seismic.alerts.v1` y `seismic.dlq.v1`.
4. Publicar dos listeners de Kafka: `internal://redpanda:9092` para los servicios de Compose y `external://127.0.0.1:19092` para herramientas y tests del host. El Schema Registry replica esto con el listener interno en 8081 y el listener externo solo para tests en el puerto del host 13081 (rango de puertos L4).
5. Enlazar los puertos de test del host solo a `127.0.0.1`, y mantener la red `backend` con `internal: true` para que los contenedores de aplicación no tengan salida a internet por construcción.

## Consecuencias

- Un solo contenedor provee broker, registry y métricas públicas del broker (`:9644/public_metrics`), lo que mantiene el stack dentro del presupuesto de memoria.
- Una sola partición preserva el orden de tiempo de evento de extremo a extremo, lo que hace exactos los tests de referencia de ventanas. El paralelismo requeriría repartir y un nuevo ADR.
- Factor de replicación 1 significa que no hay alta disponibilidad: aceptable para una demo local y listado explícitamente como limitación.
- Los comandos operativos específicos de Redpanda (`rpk`) aparecen en OPERATIONS.md, pero el código de la aplicación solo usa el protocolo de Kafka y la API del Schema Registry compatible con Confluent, así que el broker puede reemplazarse por Kafka sin tocar el código de dominio ni de aplicación.
- El modo dev-container desactiva algunos chequeos de producción; nunca debe usarse como receta de despliegue productivo.

## Alternativas consideradas

- **Apache Kafka con KRaft más un Schema Registry separado.** Descartada para v0.1.0: dos servicios basados en JVM, y más memoria y tiempo de arranque para una demo local de un solo nodo.
- **Kafka con ZooKeeper.** Descartada: un servicio legacy extra sin beneficio para un proyecto nuevo.
- **Redpanda Cloud o un registry administrado.** Descartados por la restricción local-first y de cero cuentas.
- **Varios brokers o factor de replicación 3.** Descartados: triplica el uso de memoria y no agrega una capacidad demostrable para este volumen; el escalado se documenta en su lugar.
- **Varias particiones desde el inicio.** Descartadas: el estado de ventanas con clave y la reproducibilidad se volverían más difíciles sin un beneficio de throughput medible.
