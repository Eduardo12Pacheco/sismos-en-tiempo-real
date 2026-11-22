# Política de seguridad

## Versiones con soporte

Solo la línea de releases `0.1.x` más reciente recibe correcciones de seguridad.

## Reporte de vulnerabilidades

Por favor, no abras un issue público por una vulnerabilidad sospechada. Repórtala de forma privada mediante la función de GitHub para reportar vulnerabilidades ([Report a vulnerability](https://docs.github.com/en/code-security/security-advisories/guidance-on-reporting-and-writing-information-about-vulnerabilities)) en este repositorio, e incluye:

- la versión o commit afectado,
- una reproducción mínima,
- el impacto que consideras que tiene.

Recibirás un acuse de recibo lo antes posible. Este es un proyecto de portafolio local-first de un solo usuario, sin autenticación ni TLS, así que el impacto esperado de la mayoría de los hallazgos se limita a la máquina local y a sus volúmenes de datos.

## Alcance

- Nunca se suben secretos al repositorio; el historial se analiza con gitleaks.
- Las dependencias se auditan en CI con `pip-audit`.
- El stack por defecto publica los puertos solo en `127.0.0.1`.
- Las contraseñas de desarrollo en `.env.example` son valores por defecto locales, no secretos.
