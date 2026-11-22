# Contribuir

Gracias por revisar `sismos-en-tiempo-real`. Esta guía cubre el flujo de trabajo local, los controles de calidad y las convenciones usadas en este repositorio.

## Requisitos

- Python 3.12 (gestionado con [uv](https://docs.astral.sh/uv/); ver `.python-version`)
- Docker con Compose v2 para las suites de integración y end-to-end
- `make` es una comodidad en Linux y macOS; los usuarios de Windows ejecutan directamente los comandos de `uv` subyacentes

## Preparación del entorno

```bash
uv sync --frozen
uv run seismic-stream --version
```

## Comandos

| Comando | Propósito |
|---|---|
| `make lint` | Lint de Ruff más verificación de formato |
| `make fmt` | Aplica las correcciones y el formato de Ruff |
| `make types` | `mypy --strict src` |
| `make arch` | Contratos hexagonales de import-linter |
| `make test` | Tests unitarios, de propiedades y de arquitectura (sin Docker) |
| `make test-int` | Tests de integración (requiere Docker) |
| `make test-e2e` | Tests end-to-end (stack completo) |
| `make test-ui` | Tests del dashboard con Playwright (stack completo) |
| `make cov` | Todos los tests con el umbral de cobertura del 85% |
| `make check` | Verificaciones de requisitos del instalador (`./install.sh --check`) |
| `make install` | Construye el stack e inicia el replay |
| `make down` | Detiene el stack y conserva sus volúmenes |
| `make shellcheck` | Lint de `install.sh` |
| `make compose-config` | Valida cada combinación de overrides de Compose |
| `make actionlint` | Lint de los workflows de GitHub Actions |
| `make audit` | Auditoría de dependencias con `pip-audit` |
| `make ci` | Equivalente local de cada job de CI (requiere Docker) |

Los tests nunca salen a la red: `pytest-socket` bloquea todos los hosts excepto `127.0.0.1`/`::1`. Los tests que necesitan un servicio se enlazan a puertos efímeros (puerto `0`).

## Documentación

La documentación forma parte de la definición de terminado y tiene pruebas de deriva en `tests/unit/test_docs.py`:

| Archivo | Audiencia y contenido |
|---|---|
| `README.md`, `README.en.md` | Presentación pública, inicio rápido y limitaciones; `README.md` (español, principal) y `README.en.md` (inglés) mantienen cada uno 150 líneas o menos |
| `docs/ARCHITECTURE.md` | Capas, patrones D28 con su módulo de implementación, flujo de datos, esquemas, semántica de entrega y ruta de escalado |
| `docs/DATA.md` | Procedencia de la muestra, consulta, checksum, regrabación, licencia y crédito |
| `docs/OPERATIONS.md` | Requisitos previos, comandos, cada variable de entorno, métricas, puertos y resolución de problemas |
| `docs/adr/` | Decisiones numeradas con estado, fecha y alternativas |

Cuando cambies el comportamiento:

- un campo `Settings` nuevo o modificado debe aparecer en `.env.example` y en la tabla de configuración de `docs/OPERATIONS.md`, con el nombre `SEISMIC_<FIELD>`;
- una entrada nueva o modificada de `METRIC_CATALOG` debe aparecer en la tabla de métricas de `docs/OPERATIONS.md`;
- cada ADR conserva las secciones `Estado`, `Fecha`, `Contexto`, `Decisión`, `Consecuencias` y `Alternativas consideradas`;
- los enlaces relativos deben resolver, los bloques Mermaid deben empezar con una palabra clave de diagrama conocida y mantener delimitadores balanceados, y no puede sobrevivir ningún marcador de posición `TODO`/`TBD`/`FIXME`/`lorem`;
- los números de benchmark pertenecen solo al `docs/BENCHMARKS.md` generado.

Ejecuta las verificaciones con:

```bash
uv run pytest tests/unit/test_docs.py -q
```

## Convención de commits

Los commits siguen [Conventional Commits](https://www.conventionalcommits.org/): `feat:`, `fix:`, `test:`, `docs:`, `chore:`, `ci:`, `build:`, `refactor:`. Mantén cada commit como una unidad coherente y revisable (alrededor de 400 líneas cambiadas o menos) y deja los tests y la documentación en el mismo commit que el comportamiento que describen. No incluyas líneas de atribución de IA.

## Reglas de arquitectura

El código es hexagonal: `domain` no tiene I/O ni imports de terceros, `application` depende solo de `domain`, y `adapters` depende de ambos. La capa `cli`/`composition` conecta todo. Estas reglas las aplican `import-linter` y `tests/architecture/test_import_contracts.py`; nunca debilites un contrato para que un cambio pase.

## Excepciones de verificación de tipos

`mypy --strict` se ejecuta sobre `src/`. Cualquier override a nivel de módulo de `ignore_missing_imports` debe listarse aquí con su motivo.

| Módulo | Motivo |
|---|---|
| `confluent_kafka` (y submódulos) | No incluye marcador PEP 561 ni stubs; los wrappers tipados viven en `adapters/kafka`. |
| `authlib` (y submódulos) | Extra transitivo de confluent-kafka sin stubs; solo se importa su clase de advertencia para silenciar ruido de terceros. |

## Tests

Escribe primero el test que falla y luego la implementación. No omitas, borres ni debilites tests para obtener una ejecución en verde. Los tests basados en propiedades usan perfiles de Hypothesis: `dev` es el predeterminado y `ci` (`HYPOTHESIS_PROFILE=ci`) fija `max_examples=100` y elimina la aleatoriedad (derandomize).

## Licencia

Al contribuir, aceptas que tu contribución se licencia bajo la Licencia MIT.
