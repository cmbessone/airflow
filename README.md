# Pipeline RAG de Reportes Financieros con Airflow y FastAPI

Este proyecto implementa un pipeline completo de **Retrieval-Augmented Generation (RAG)** orquestado por Apache Airflow. El sistema descarga reportes 10-K de la SEC, los procesa y vectoriza, y expone una API con FastAPI para generar resúmenes de estos documentos bajo demanda usando un modelo de lenguaje local.

El objetivo es demostrar una arquitectura moderna, modular y asíncrona para procesar datos no estructurados y controlarla a través de una API REST.

## Arquitectura del Sistema

El proyecto consta de tres componentes principales que se ejecutan en contenedores Docker aislados:

1.  **Apache Airflow:** Actúa como el motor orquestador. Gestiona dos flujos de trabajo (DAGs) principales:
    * **Indexación:** Un DAG que obtiene los documentos, extrae el texto, genera embeddings y los almacena en una base de datos vectorial.
    * **Reporte:** Un DAG que, a partir de una consulta, busca información relevante en la base de datos, genera un resumen con un LLM y crea un reporte en PDF.
2.  **FastAPI:** Provee una capa de API RESTful que actúa como "panel de control" para el sistema. Permite a usuarios o sistemas externos:
    * Descubrir los flujos de trabajo disponibles.
    * Disparar ejecuciones de los DAGs.
    * Monitorear el estado de las ejecuciones.
    * Descargar los artefactos resultantes (reportes en PDF).
3.  **Ollama:** Sirve un modelo de lenguaje grande (LLM) de forma local, permitiendo que el sistema realice tareas de IA generativa sin depender de APIs de pago.

El flujo de una petición del usuario es el siguiente:
`Usuario → FastAPI → Airflow API → DAG de Reporte → (ChromaDB + Ollama) → PDF`

## Tecnologías Utilizadas

-   **Orquestación:** Apache Airflow
-   **API:** FastAPI
-   **Contenerización:** Docker, Docker Compose
-   **Base de Datos Vectorial:** ChromaDB
-   **IA / NLP:**
    -   **LLM:** Ollama (con el modelo `tinydolphin`)
    -   **Embeddings:** `sentence-transformers`
    -   **Framework:** `LangChain`
-   **Procesamiento de Datos:** Pandas

## Estructura del Proyecto

```
.
├── config/
│   └── airflow.cfg                    # Configuración de Airflow (ej. SMTP)
├── dags/
│   ├── document_indexing_pipeline.py  # Lógica del pipeline de indexación
│   ├── rag_reporting_dag.py           # DAG de reporte
│   ├── reporting_pipeline.py          # Lógica del pipeline de reporte
│   └── sec_filings_indexer.py         # DAG de indexación
├── fastapi_app/
│   ├── Dockerfile                     # Dockerfile para la API
│   ├── main.py                        # Código de la aplicación FastAPI
│   └── requirements.txt               # Dependencias de la API
├── logs/                              # Carpeta de logs de Airflow (creada por Docker)
├── output/                            # Carpeta donde se guardan los PDFs generados
├── chromadb_data/                     # Carpeta donde se persiste la DB vectorial
├── plugins/                           # Carpeta de plugins de Airflow
├── Dockerfile                         # Dockerfile principal para Airflow
├── docker-compose.yml                 # Archivo de orquestación de todos los servicios
└── requirements.txt                   # Dependencias de Python para Airflow
```

## Configuración y Puesta en Marcha

Sigue estos pasos para levantar el proyecto completo en tu máquina local.

### Prerrequisitos

-   Docker y Docker Compose instalados.
-   Ollama instalado en tu máquina host. (Instrucciones en [ollama.com](https://ollama.com))

### Pasos de Instalación

1.  **Clonar el Repositorio:**
    ```bash
    git clone [URL-DE-TU-REPOSITORIO]
    cd [NOMBRE-DE-LA-CARPETA]
    ```

2.  **Descargar un Modelo con Ollama:**
    Abre una terminal en tu máquina (no en Docker) y descarga un modelo ligero. Luego, déjalo corriendo.
    ```bash
    ollama pull tinydolphin
    ollama run tinydolphin
    ```
    *Deja esta terminal abierta.*

3.  **Construir y Levantar los Contenedores:**
    Este comando construirá las imágenes de Docker para Airflow y FastAPI, y levantará todos los servicios.
    ```bash
    docker-compose up --build -d
    ```
    Espera unos minutos a que todos los servicios se inicien correctamente.

4.  **Acceder a los Servicios:**
    -   **Airflow UI:** `http://localhost:8080` (Usuario: `airflow`, Contraseña: `airflow`)
    -   **FastAPI Docs:** `http://localhost:8001/docs`

5.  **Ejecutar el Pipeline de Indexación:**
    Para poder generar reportes, primero necesitas tener datos en tu base de datos vectorial. Dispara el DAG `sec_filings_indexer` una vez desde la UI de Airflow o a través de la API.

## Cómo Usar la API

La API expone varios endpoints para controlar el sistema. Puedes probarlos fácilmente desde la documentación interactiva en `http://localhost:8001/docs`.

### `GET /dags`
Obtiene una lista de todos los DAGs disponibles en Airflow.

### `POST /trigger-dag/{dag_id}`
Dispara la ejecución de un DAG. Para el `rag_reporting_dag`, puedes pasar una consulta en el cuerpo de la petición.
- **Ejemplo de cuerpo para `rag_reporting_dag`:**
  ```json
  {
    "conf": {
      "query": "Cuales son los principales riesgos de la empresa?"
    }
  }
