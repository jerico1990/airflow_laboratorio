import json
import requests
import logging
import csv
import json

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook

from airflow.utils.edgemodifier import Label
from airflow.models import Variable

from datetime import datetime, timedelta
from pandas import json_normalize
from airflow.utils.dates import days_ago


def generar_archivos_gobierno(ide: str, gobierno: str):
    url = 'http://ofi5.mef.gob.pe/invierte/ConsultaPublica/traeListaProyectoConsultaAvanzadaIdeas'
    params = {
        "PageIndex": 1,
        "PageSize": 10,
        "cboDist": "0",
        "cboDivision": "0",
        "cboDpto": "0",
        "cboEsta": "*",
        "cboEstu": "*",
        "cboFunc": "0",
        "cboGLDist": "*",
        "cboGLDpto": "*",
        "cboGLManPlie": "*",
        "cboGLManUf": "",
        "cboGLProv": "*",
        "cboGLUf": "",
        "cboGNPlie": "",
        "cboGNSect": f"{ide}",
        "cboGNUF": "",
        "cboGR": "*",
        "cboGRUf": "",
        "cboGrupo": "0",
        "cboNivReqViab": "*",
        "cboNom": "1",
        "cboPais": "0",
        "cboProv": "0",
        "cboSitu": "0",
        "chkFoniprel": "",
        "chkInactivo": "0",
        "chkMonto": False,
        "chkMontoElaboracion": False,
        "filters": "",
        "ip": "",
        "isSearch": False,
        "optFecha": "I",
        "optGL": "*",
        "optUG": "DEP",
        "optUf": "*",
        "rbtnCadena": "",
        "sortField": "",
        "sortOrder": "desc",
        "tipo": "1",
        "txtFin": "09/08/2023",
        "txtIni": "01/01/2015",
        "txtMax": "",
        "txtMaxME": "",
        "txtMin": "",
        "txtMinME": "",
        "txtNom": ""
    }
    response = requests.post(url=url, data=params)
    response = json.loads(response.text)["Data"]
    df = json_normalize(response)

    df.to_csv(f'/opt/airflow/dags/input_data/{ide}.csv', index=False, header=False, na_rep='null')

def subdag_procesamiento(parent_dag_id, child_dag_id, default_args):
    with DAG(
        dag_id=f'{parent_dag_id}.{child_dag_id}',
        default_args=default_args
    ) as dag:
        registros = []
        with open(f'/opt/airflow/dags/input_data/gobiernos.csv', 'r', encoding='utf-8-sig') as archivo_csv:
            lector = csv.DictReader(archivo_csv, delimiter=';')
            registros = list(lector)

        registros_json = json.dumps(registros)  
        registros_list = json.loads(registros_json)

        for i, registro in enumerate(registros_list):
            task = PythonOperator(
                task_id=f'task_{i}',
                python_callable=generar_archivos_gobierno,
                op_args=[str(registro["IDE"]), str(registro["GOBIERNO"])]
            )
        return dag