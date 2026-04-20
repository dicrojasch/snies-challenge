# 📝 Respuestas Puntuales al Reto SNIES

Este documento ha sido creado con el propósito de responder de manera directa y centralizada a las preguntas planteadas en el reto técnico. Cabe destacar que toda la documentación técnica detallada, guías de instalación y especificaciones profundas se encuentran en el [**README.md**](./README.md), el cual sirve como fuente única de verdad para la operación del sistema.

---

### 🏛️ ¿Cómo saber si la institución pertenece al SUE o no?
La pertenencia al **Sistema Universitario Estatal (SUE)** se gestiona mediante una tabla de referencia cruzada (seed) en dbt, basada en el listado oficial de instituciones públicas.
- **Visualización**: En el panel de [**Grafana**](http://localhost:3000/d/857298ea-10f9-41b2-9481-f0e0b3257782/snies-gold-table-dashboard?orgId=1&from=2022-01-01T00:00:00.000Z&to=2024-07-01T00:00:00.000Z&timezone=browser&refresh=1m) (ver [**README.md**](./README.md#observability--visualization-grafana)), algunas métricas permiten filtrar o distinguir por sector (Oficial vs. Privado).
- **Consultas Ad-hoc**: Dado que la base de datos está expuesta, se puede consultar la tabla `silver.dim_instituciones` para verificar el campo de sector o cruzar con el seed `sue_institutions`.
- **Conexión**: Los datos de conexión (host, puerto, credenciales) están especificados en la sección [**Connectivity & External Tools**](./README.md#connectivity--external-tools) del README.

### 📊 ¿Cómo conocer el número de estudiantes por docente (2022-2024)?
Esta métrica es el núcleo analítico de la Capa Gold de nuestra arquitectura Medallion.
- **Visualización**: El [**Dashboard de Grafana**](./README.md#-observability--visualization-grafana) presenta visualizaciones específicas de este ratio para instituciones en Bogotá dentro del rango de años solicitado.
- **Consultas Ad-hoc**: Se puede consultar directamente la tabla `gold.student_teacher_ratio` en PostgreSQL.
- **Conexión**: Ver los parámetros de conexión en el [**README.md**](./README.md#connectivity--external-tools).

### ⚙️ ¿Qué herramienta de orquestación se usó?
Se utilizó **Prefect** como motor de orquestación.
- **Archivos relevantes**: Los flujos y tareas se encuentran en la carpeta [`./orchestration/flows/`](./orchestration/flows/).
- **Funcionalidad**: Su operación, capacidades de sensores de archivos y reintento lógico se explican en la sección [**Pipeline Orchestration**](./README.md#pipeline-orchestration) del README.

### 📐 ¿Qué esquema de base de datos se usó y cómo se hace trazabilidad?
Se implementó una **Arquitectura Medallion** en PostgreSQL:
1.  **Bronze (Raw)**: Estructura prácticamente idéntica al origen (Excel/CSV) para mantener la fidelidad de los datos crudos.
2.  **Silver (Normalized)**: Modelo dimensional normalizado (**Snowflake Schema**). Se utiliza para aplicar controles de integridad, restricciones de relaciones y asegurar la consistencia de datos únicos.
3.  **Gold (Aggregated)**: Modelo dimensional desnormalizado (**Star Schema**). Optimizado para herramientas de BI, contiene los datos agrupados con el cálculo de la tasa de profesores y estudiantes.
- **Trazabilidad**: Se realiza mediante el linaje de **dbt** (ver [**README.md**](./README.md#data-documentation-dbt)) . Un diagrama detallado de las relaciones y modelos se puede encontrar en [**models.md**](./models.md).

### 🔌 ¿Es accesible la base de datos?
**Sí.** La base de datos PostgreSQL está expuesta al host mediante [Docker Compose](./docker-compose.yaml). Los detalles de conexión (puerto 5432, base de datos `snies`, etc.) están documentales en la sección de [**Connectivity**](./README.md#connectivity--external-tools) del README.

### 🐳 ¿La solución está en un entorno Docker?
**Sí.** Toda la infraestructura (PostgreSQL, Prefect, Grafana, dbt) está contenida en contenedores y se despliega de forma reproducible mediante el archivo [**docker-compose.yaml**](./docker-compose.yaml).

### 🗺️ Diagrama de la arquitectura implementada
El diagrama de flujo de datos y la interacción de capas se encuentra en el README bajo la sección [**System Architecture**](./README.md#system-architecture).

### 📖 Guía de instalación y ejecución
Las instrucciones paso a paso para configurar secretos de Docker, levantar el entorno y disparar el pipeline están en el [**README.md**](./README.md#quick-start).

### 💡 ¿Por qué estas decisiones técnicas?
Se eligió la **Arquitectura Medallion** con modelado dimensional para asegurar tanto la robustez del dato (Silver) como el rendimiento analítico (Gold). El stack (Postgres + Redis + dbt + Prefect + Python + Bash + Grafana) ofrece un equilibrio óptimo entre agilidad de desarrollo, facilidad de mantenimiento y ligereza de recursos.

### 📈 ¿Cómo escalaría la solución?
Aunque la solución actual soporta volúmenes significativos (como los datos de todo el país), para una escala masiva se propone:
- **Almacenamiento**: Migración a Data Warehouses como **Snowflake** o **Amazon Redshift**.
- **Procesamiento**: Uso de herramientas distribuidas como **AWS Glue**.
- **Ingesta**: Uso de colas (**SQS**) o streaming (**Kinesis**) para procesamiento en tiempo casi real.

### 🧹 Manejo de nulos, duplicados y normalización
Se gestionó mediante la funcionalidad de **seeds** y **tests** de dbt:
- **Validación**: dbt asegura que columnas clave no sean nulas y sigan valores permitidos.
- **Normalización**: Mediante archivos en la carpeta `seeds`, se estandarizan nombres de IES y catálogos, permitiendo unificar registros que varían por espacios o caracteres especiales en los archivos fuente originales.

### 🤖 ¿Cómo se usó Antigravity (Vibe Code)?
Esta solución fue desarrollada integralmente usando el IDE **Antigravity**, empleando agentes de IA de última generación:
- **Modelos**: Principalmente **Gemini 3 Flash**, y para lógica compleja con restricciones de contexto, **Gemini 3.1 Pro (High)** y **Claude Sonnet 4.6 (Thinking)**.
- **Trazabilidad**: Cada cambio importante cuenta con un `implementation_plan` (blueprint técnico), un `task` (objetivo de ejecución) y un `walkthrough` (informe de entrega) en la carpeta [`./implementation_plans`](./implementation_plans).
- **Gemini Web**: Se utilizó la interfaz de [gemini.google.com](https://gemini.google.com/) alternando modos según la complejidad (**Fast**, **Thinking**, **Pro**) para optimizar el consumo de tokens y precisión.
- **Commits**: La mayoría de los commits están co-autores por el agente **Antigravity**.

---
*Documento centralizado de respuestas - SNIES Challenge.*
