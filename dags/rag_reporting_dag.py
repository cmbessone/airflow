# dags/rag_reporting_dag.py

from airflow.decorators import dag, task
from datetime import datetime


# Importar las funciones del nuevo pipeline
from reporting_pipeline import query_and_summarize, create_summary_pdf


@dag(
    dag_id="rag_reporting_dag",
    default_args={
        "owner": "airflow",
        "start_date": datetime(2025, 1, 1),
        "retries": 0,
        # Asegúrate de que la conexión de OpenAI esté configurada
        "inlets": ["openai_default"],
    },
    schedule=None,  # Lo activaremos manualmente
    catchup=False,
    tags=["sec", "rag", "reporting", "langchain"],
    doc_md="""
    ### DAG de Reporte con RAG
    Este DAG se activa manualmente y genera un resumen de un filing 10-K.
    
    **Para ejecutarlo:**
    1. Activa el DAG.
    2. Haz clic en 'Trigger DAG w/ config'.
    3. Pasa un JSON con tu pregunta, por ejemplo:
       `{"query": "Resume los principales factores de riesgo financiero."}`
    """,
)
def rag_reporting_dag():

    @task
    def summarize_task(query: str) -> dict:
        """Llama a la lógica RAG para obtener el resumen."""
        # Podemos pasar el ticker si queremos analizar otras compañías en el futuro
        return query_and_summarize(query=query, ticker="Apple")

    @task
    def create_pdf_task(summary_data: dict, query: str) -> str:
        """Toma el resumen y genera un archivo PDF."""
        pdf_path = create_summary_pdf(summary_data=summary_data, query=query)
        return pdf_path

    # Usamos 'params' para obtener la configuración pasada al ejecutar el DAG.
    # Si no se pasa ninguna, usamos una pregunta por defecto.
    user_query = "{{ params.get('query', 'Resume los resultados financieros clave y la estrategia de negocio.') }}"

    summary_info = summarize_task(query=user_query)
    create_pdf_task(summary_data=summary_info, query=user_query)


dag = rag_reporting_dag()
