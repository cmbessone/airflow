# fastapi_app/main.py

import os
import requests
from typing import List, Optional
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from pydantic import BaseModel
from datetime import datetime, timezone

# --- Configuración ---
# Usamos el nombre del servicio que está corriendo la UI y la API de Airflow
# Basado en los últimos logs, este debería ser 'airflow-apiserver'.
AIRFLOW_URL = "http://airflow-apiserver:8080"
AIRFLOW_USER = os.getenv("AIRFLOW_USER", "airflow")
AIRFLOW_PASSWORD = os.getenv("AIRFLOW_PASSWORD", "airflow")


# --- Modelos de Datos ---
class TriggerDagPayload(BaseModel):
    conf: dict = {}


class DagRunResponse(BaseModel):
    dag_run_id: str
    dag_id: str
    execution_date: str
    state: str
    message: str


class DagRunStatusResponse(BaseModel):
    dag_id: str
    dag_run_id: str
    state: str
    logical_date: str


class DagInfo(BaseModel):
    dag_id: str
    is_paused: Optional[bool] = None
    is_active: Optional[bool] = None
    description: Optional[str] = None


class DagListResponse(BaseModel):
    dags: List[DagInfo]
    total_entries: int


# --- Aplicación FastAPI ---
app = FastAPI(
    title="Airflow Interoperability API",
    description="Una API para interactuar y disparar DAGs de Airflow.",
    version="1.0.0",
)


def get_airflow_token() -> str:
    """
    Se autentica con Airflow para obtener un token de acceso JWT.
    """
    auth_endpoint = f"{AIRFLOW_URL}/auth/token"
    print(f"Obteniendo token de: {auth_endpoint}")
    try:
        response = requests.post(
            auth_endpoint, json={"username": AIRFLOW_USER, "password": AIRFLOW_PASSWORD}
        )
        response.raise_for_status()
        token_data = response.json()
        print("Token obtenido con exito")
        return token_data["access_token"]
    except requests.exceptions.HTTPError as err:
        print(f"Error al obtener token de Airflow: {err.response.text}")
        raise HTTPException(
            status_code=err.response.status_code,
            detail="No se pudo autenticar con Airflow. Revisa las credenciales.",
        )


@app.get("/", tags=["Health Check"])
async def root():
    return {"message": "API de interoperabilidad con Airflow está activa."}


@app.get("/dags", tags=["Airflow"])
async def list_dags():
    """
    Obtiene una lista de todos los DAGs disponibles en Airflow.
    """
    print("Recibida petición para listar DAGs")
    try:
        access_token = get_airflow_token()
        # Endpoint para obtener la lista de DAGs (v2)
        dags_endpoint = f"{AIRFLOW_URL}/api/v2/dags"
        headers = {"Authorization": f"Bearer {access_token}"}

        response = requests.get(dags_endpoint, headers=headers)
        response.raise_for_status()

        data = response.json()

        # Procesamos la respuesta para devolver solo los campos que nos interesan
        dags_list = [
            DagInfo(
                dag_id=dag.get("dag_id"),
                is_paused=dag.get("is_paused"),
                is_active=dag.get("is_active"),
                description=dag.get("description"),
            )
            for dag in data.get("dags", [])
        ]

        return {"dags": dags_list, "total_entries": len(dags_list)}

    except HTTPException as http_exc:
        raise http_exc
    except Exception as e:
        print(f"Error al listar DAGs: {e}")
        raise HTTPException(
            status_code=500, detail=f"No se pudo obtener la lista de DAGs: {e}"
        )


@app.post("/trigger-dag/{dag_id}", tags=["Airflow"])
async def trigger_dag(dag_id: str, payload: TriggerDagPayload):
    """
    Dispara la ejecución de un DAG específico en Airflow usando autenticación por Token.
    """
    print(f"Recibida petición para disparar el DAG: {dag_id}")

    try:
        # --- PASO 1: Obtener el Token ---
        access_token = get_airflow_token()

        # --- PASO 2: Usar el Token para disparar el DAG ---
        # Usamos el endpoint v2 que descubriste
        trigger_endpoint = f"{AIRFLOW_URL}/api/v2/dags/{dag_id}/dagRuns"
        print(f"Disparando DAG en: {trigger_endpoint}")

        headers = {"Authorization": f"Bearer {access_token}"}

        airflow_payload = {
            "logical_date": datetime.now(timezone.utc).isoformat(),
            "conf": payload.conf,
        }

        response = requests.post(
            trigger_endpoint, headers=headers, json=airflow_payload
        )
        response.raise_for_status()

    except HTTPException as http_exc:
        raise http_exc
    except requests.exceptions.HTTPError as err:
        error_detail = err.response.json().get("detail", err.response.text)
        print(f"Error de HTTP al llamar a Airflow: {error_detail}")
        raise HTTPException(
            status_code=err.response.status_code,
            detail=f"Error al comunicarse con Airflow: {error_detail}",
        )
    except Exception as e:
        print(f"Error inesperado: {e}")
        raise HTTPException(status_code=500, detail=f"Ocurrió un error interno: {e}")

    data = response.json()
    print(f"Airflow respondió con éxito: {data}")

    return {
        "dag_run_id": data.get("dag_run_id"),
        "dag_id": data.get("dag_id"),
        "execution_date": data.get("execution_date"),
        "state": data.get("state"),
        "message": f"El DAG '{dag_id}' ha sido disparado con éxito.",
    }


@app.get("/dag-run/status/{dag_id}/{dag_run_id}", tags=["Airflow"])
async def get_dag_run_status(dag_id: str, dag_run_id: str):
    """
    Consulta el estado de una ejecución de DAG específica.
    """
    print(f"Recibida petición de estado para el DAG run: {dag_run_id}")
    try:
        access_token = get_airflow_token()
        # Endpoint para obtener detalles de un DAG Run específico (v2)
        status_endpoint = f"{AIRFLOW_URL}/api/v2/dags/{dag_id}/dagRuns/{dag_run_id}"
        headers = {"Authorization": f"Bearer {access_token}"}

        response = requests.get(status_endpoint, headers=headers)
        response.raise_for_status()

    except HTTPException as http_exc:
        raise http_exc
    except requests.exceptions.HTTPError as err:
        error_detail = err.response.json().get("detail", err.response.text)
        raise HTTPException(
            status_code=err.response.status_code,
            detail=f"Error al comunicarse con Airflow: {error_detail}",
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ocurrió un error interno: {e}")

    data = response.json()
    return {
        "dag_id": data.get("dag_id"),
        "dag_run_id": data.get("dag_run_id"),
        "state": data.get("state"),
        "logical_date": data.get("logical_date"),
    }


@app.get("/download-report/{dag_id}/{dag_run_id}", tags=["Airflow"])
async def download_report(dag_id: str, dag_run_id: str):
    """
    Descarga el archivo PDF generado por una ejecución de DAG exitosa.
    """
    print(f"Petición de descarga para el DAG run: {dag_run_id}")
    try:
        # Reutilizamos la función de status para verificar que el DAG terminó con éxito
        status_data = await get_dag_run_status(dag_id, dag_run_id)
        if status_data.get("state") != "success":
            raise HTTPException(
                status_code=400,
                detail=f"El trabajo aún no ha terminado con éxito. Estado actual: {status_data.get('state')}",
            )

        # Si tuvo éxito, obtenemos el XCom con la ruta del archivo
        access_token = get_airflow_token()
        task_id = "create_pdf_task"  # El ID de la tarea que genera el PDF
        xcom_endpoint = f"{AIRFLOW_URL}/api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/xcomEntries/return_value"
        headers = {"Authorization": f"Bearer {access_token}"}

        response = requests.get(xcom_endpoint, headers=headers)
        response.raise_for_status()

        xcom_data = response.json()
        # Lo decodificamos para obtener la ruta real
        file_path = xcom_data.get("value")

        print(f"Ruta del archivo obtenida de XCom: {file_path}")

        # Verificamos si el archivo existe en la ruta esperada DENTRO del contenedor
        if not os.path.isfile(file_path):
            raise HTTPException(
                status_code=404, detail=f"Archivo no encontrado en la ruta: {file_path}"
            )

        # Usamos FileResponse para enviar el archivo al cliente
        return FileResponse(
            path=file_path,
            media_type="application/pdf",
            filename=os.path.basename(file_path),
        )

    except HTTPException as http_exc:
        raise http_exc
    except Exception as e:
        print(f"Error en la descarga: {e}")
        raise HTTPException(
            status_code=500, detail=f"No se pudo generar la descarga: {e}"
        )
