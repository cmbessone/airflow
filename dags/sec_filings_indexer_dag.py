from airflow.decorators import dag, task
from datetime import datetime
import pandas as pd

from document_indexing_pipeline import fetch_10K_docs, process_and_index


@dag(
    dag_id="sec_filings_indexer",
    default_args={
        "owner": "airflow",
        "start_date": datetime(2025, 1, 1),
        "retries": 0,
    },
    schedule=None,
    catchup=False,
    tags=["sec", "apple", "10k", "vector_index"],
)
def sec_filings_indexer():
    @task()
    def fetch_task() -> pd.DataFrame:
        return fetch_10K_docs()

    @task()
    def index_task(df: pd.DataFrame) -> None:
        process_and_index(df.head(1), chunk_size=500, overlap=0)

    df = fetch_task()
    index_task(df)


dag = sec_filings_indexer()
