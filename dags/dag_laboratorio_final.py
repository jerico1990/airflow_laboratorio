import json
import requests
import logging
import csv
import json
import os

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
from subdags.subdag_laboratorio_final import subdag_procesamiento
from airflow.operators.subdag import SubDagOperator
from airflow.utils.task_group import TaskGroup
from airflow.decorators import task_group


def verificando_archivo_base_gobiernos():
    if os.path.isfile(f'/opt/airflow/dags/input_data/gobiernos.csv'):
        print('Log Archivo existente')
        return True
    else:
        print('Log Archivo no existente')
        return False

    

def verficiando_servicio():
    try:
        data = {
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
            "cboGNSect": "*",
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

        response = requests.post('http://ofi5.mef.gob.pe/invierte/ConsultaPublica/traeListaProyectoConsultaAvanzadaIdeas', data=data)
        if response.status_code == 200:
            print('Log URL existe')
            return True
        else:
            print('Log URL no existe')
            return False
    except requests.ConnectionError:
        print('Log URL no existe')
        return False


def task06_migrando_bd_f(ide: str):
    file_path = f'/opt/airflow/dags/input_data/{str(registro["IDE"])}.csv'
    mysql_hook = MySqlHook(mysql_conn_id="mysql_default", local_infile=True)
    mysql_hook.bulk_load_custom('Proyecto', file_path, extra_options="FIELDS TERMINATED BY ','")
    logging.info("Data was loaded successfully!")
                
default_args = {
    'owner': 'airflow',
    'depends_on_past': True,
    'start_date': datetime(2023,8,10),
    #'retries': 3,
    #'retry_delay': timedelta(seconds=60)
}

with DAG(
    'dag_laboratorio_final',
    default_args=default_args,
    #schedule_interval='*/10 * * * *',
    catchup=False,
    max_active_tasks=3,
) as dag:

    start = DummyOperator(
        task_id='start'
    )

    task01_verificando_archivo_base_gobiernos = PythonOperator(
        task_id='task01_verificando_archivo_base_gobiernos',
        python_callable=verificando_archivo_base_gobiernos
    )

    task02_verificando_servicio = PythonOperator(
        task_id='task02_verificando_servicio',
        python_callable=verficiando_servicio
    )

    task03_verificando_conexion = MySqlOperator(
        task_id='task03_verificando_conexion',
        mysql_conn_id='mysql_default',  # Specify the MySQL connection ID defined in Airflow
        sql="SELECT 1",
    )

    task04_creando_tabla_si_no_existe = MySqlOperator(
        task_id="task04_creando_tabla_si_no_existe",
        mysql_conn_id="mysql_default",
        database="dwh",
        sql="""
            CREATE TABLE IF NOT EXISTS Proyecto (
                ID_IDEA INT PRIMARY KEY,
                ID_TIPO_FORMATO INT,
                DES_NOMBRE VARCHAR(255),
                COD_IDEA VARCHAR(255),
                DES_OBJETO_INTERV TEXT,
                UNIDAD_OPMI TEXT,
                ID_USUARIO_OPMI INT,
                UNIDAD_UF TEXT,
                ID_USUARIO_UF INT,
                UNIDAD_UEI TEXT,
                ID_USUARIO_UEI INT,
                DES_UNIDAD_UF TEXT,
                DESCRIP_SECTOR_UF TEXT,
                DESCRIP_PLIEGO_UF TEXT,
                RESPONSABLE_UF TEXT,
                DES_UNIDAD_UEI TEXT,
                DESCRIP_SECTOR_UEI TEXT,
                DESCRIP_PLIEGO_UEI TEXT,
                RESPONSABLE_UEI TEXT,
                RESPONSABLE_OPMI TEXT,
                DES_NIVEL_UF TEXT,
                DES_NIVEL_UEI TEXT,
                DES_UNIDAD_OPMI TEXT,
                ASISTENTE_UF TEXT,
                ID_SECTOR INT,
                ID_PLIEGO INT,
                SEC_EJEC INT,
                ANO_EJE INT,
                DES_EJEC TEXT,
                ID_MODALIDAD INT,
                ID_FUENTE INT,
                FLG_ESCONDIDO TEXT,
                MONTO_INVERSION DECIMAL(18, 2),
                MONTO_ESTUDIO DECIMAL(18, 2),
                MONTO_GESTION DECIMAL(18, 2),
                ID_ESTADO INT,
                ID_SUB_PROGRAMA INT,
                ID_TIPO_DOC INT,
                ID_NATURALEZA INT,
                FEC_FINALIZADO DATE,
                ID_ARC_FORMATO INT,
                ID_ARC_NOTA INT,
                ID_TIPO_IOARR INT,
                ID_ETAPA INT,
                DES_TIPO_IOARR TEXT,
                DES_MODALIDAD TEXT,
                DES_FUENTE_FTO TEXT,
                DES_NATURALEZA TEXT,
                DES_TIPO_DOC TEXT,
                DES_SUB_PROGRAMA TEXT,
                ID_PROGRAMA INT,
                ID_FUNCION INT,
                DES_PROGRAMA TEXT,
                DES_FUNCION TEXT,
                COD_PROGRAMA TEXT,
                COD_FUNCION TEXT,
                COD_SUB_PROGRAMA TEXT,
                ID_SUB_PROGRAMA_SECTOR INT,
                DES_SECTOR_RESPONSABLE TEXT,
                ID_TIPOLOGIA INT,
                ID_SERVICIO INT,
                DES_ARC_FORMATO TEXT,
                URL_ARC_FORMATO TEXT,
                DES_ARC_NOTA TEXT,
                URL_ARC_NOTA TEXT,
                DES_ESTADO TEXT,
                DES_TIPO_FORMATO TEXT,
                PERMITE_ELIMINAR TEXT,
                COD_UNICO TEXT,
                ID_ACCION INT,
                TOTAL INT,
                ID_CONVENIO INT,
                MONTO_ET DECIMAL(18, 2),
                MONTO_SUPERVISION DECIMAL(18, 2),
                MONTO_LIQUIDACION DECIMAL(18, 2),
                listaLocalizacion JSON,
                listaFuera JSON,
                listaTipoIOARR JSON,
                listaUf JSON,
                listaIndicadorBrecha JSON,
                listaProducto JSON,
                FLG_FIDT TEXT,
                TIPO_PROPUESTA_FIDT TEXT,
                CRITERIOS_PRIORIZACION JSON,
                MONTO_ETDE DECIMAL(18, 2),
                PROBLEMA_CENTRAL TEXT,
                OBJETIVO_CENTRAL TEXT,
                META TEXT,
                TIPO_POBLACION_AFECTADA TEXT,
                CANTIDAD_POBLACION_AFECTADA INT,
                FUENTE_POBLACION_AFECTADA TEXT,
                TIPO_POBLACION_OBJETIVO TEXT,
                CANTIDAD_POBLACION_OBJETIVO INT,
                FUENTE_POBLACION_OBJETIVO TEXT,
                DESC_OPE_MANT TEXT,
                DESC_RIESGO_PROYECTO TEXT,
                RESPONSABLE_EJEC_ALCALDE TEXT,
                NIVEL_FINANCIAMIENTO_BCO_MNI TEXT,
                listaNaturaleza JSON,
                listaCausa JSON,
                listaEfecto JSON,
                listaInvolucrado JSON,
                OBJETIVO_LOGRO TEXT,
                UM_OBJETIVO_LOGRO TEXT,
                SITUACION_ACTUAL TEXT,
                IMPACTO_AMB TEXT,
                ID_SECTOR_UEI INT,
                ID_PLIEGO_UEI INT,
                DIAG_TERRITORIO TEXT,
                DIAG_CARACT_POBLACION TEXT,
                DIAG_UNIDAD_PRODUCTORA TEXT,
                FEC_LIQUIDACION DATE,
                FEC_CIERRE DATE,
                listaCompetencias JSON,
                ID_OPCION INT,
                listaEjecutora JSON,
                listaConveniosIdea JSON,
                listaFase JSON,
                listaCadenasFuncionales JSON,
                listaServiciosCadenasFuncionales JSON,
                listaIdeaNaturaleza JSON,
                listaServiciosSel JSON,
                listaUnidadProductora JSON,
                listaUnidadProductoraf JSON,
                listaPoblacionAfectada JSON,
                listaAlineamiento JSON,
                listaJustificacion JSON,
                listaCostosAgregados JSON,
                listaEntidadSoste JSON,
                listaRiesgos JSON,
                listaBeneficios JSON,
                listaFuenteFinanciamiento JSON,
                listaRangoley JSON,
                listaInvOei JSON,
                listaInvAei JSON,
                listaGeoInvierte JSON,
                USO_BIM TEXT,
                ID_ENFOQUE INT,
                ENFOQUE TEXT,
                ID_CONVENIOPREG INT,
                CONVENIO_NRO TEXT,
                CONVENIO_FECHACADUCA DATE,
                ID_POLITICA_EJE INT,
                ID_POLITICA_LINEAMIENTO INT,
                ID_PLAN_ESTRATEGICO INT,
                ID_PLAN_ACCION INT,
                FLGFUERATERRITORIO TEXT,
                OPERACION_PREG1 TEXT,
                OPERACION_ENTIDAD TEXT,
                OPERACION_UEP TEXT,
                OPERACION_PREG2 TEXT,
                OPERACION_ENTPRIVADA TEXT,
                OPERACION_ENTPRIVADA_OTRO TEXT,
                MONTO_INCREMENTAL DECIMAL(18, 2),
                TEST_VALOR TEXT,
                COSTOPREINVERSION DECIMAL(18, 2),
                ID_MODALIDADE INT,
                ID_MODALIDADF INT,
                FECHA_ELABORA_INICIO DATE,
                FECHA_ELABORA_FIN DATE,
                NOMBRE_PRELIMINAR TEXT,
                ID_IDEA_TIPOLOGIA INT,
                ID_CONVENIO_PROYECTO INT,
                listaUFS JSON,
                listaUEIS JSON,
                listaUES JSON,
                AMBITO TEXT,
                NIVEL_GOB TEXT,
                ENTIDAD TEXT,
                CANT_BENEF INT,
                COSTO_ELABORACION DECIMAL(18, 2),
                SECTOR TEXT,
                NOM_DEPA TEXT,
                NOM_PROV TEXT,
                NOM_DISTRITO TEXT,
                DES_ESTILO TEXT,
                TIPOLOGIA TEXT,
                CONVENIO TEXT,
                ListaUnidadOdi JSON,
                DES_TIPOLOGIA TEXT,
                DES_SERVICIO TEXT,
                DESCRIP_SECTOR TEXT,
                CADENA_PRODUCTORA TEXT,
                listaModalidad JSON,
                listaModalidadFinanciamiento JSON,
                ESTADO JSON,
                MONTO_TENTATIVO DECIMAL(18, 2),
                listaArchivos JSON,
                DocTecnicos JSON,
                ListaFueraTerritorio JSON,
                listaEntidadPrivada JSON,
                ID_ARC_PDF_LOCA INT,
                ID_ARC_PDF_UP INT,
                DES_ARC_LOCAL TEXT,
                URL_ARC_LOCAL TEXT,
                DES_ARC_UP TEXT,
                URL_ARC_UP TEXT,
                ESRANGOLEY TEXT,
                SITUACION TEXT,
                NOMBRE_UF TEXT,
                NOMBRE_UEI TEXT,
                FLG_ACTIVO TEXT,
                FEC_CREA DATETIME,
                FEC_MODI DATETIME,
                USU_AUTORIZA TEXT,
                USU_CREA TEXT,
                USU_MODI TEXT,
                ID_VERSION INT,
                USU_VERSION TEXT,
                FEC_VERSION DATETIME,
                ID_ROL INT,
                NUM_TOTAL INT,
                FLG_MODIFICADO TEXT
            );

            """
    )

    task05_procesamiento = SubDagOperator(
        task_id='task05_procesamiento',
        subdag=subdag_procesamiento(parent_dag_id='dag_laboratorio_final', child_dag_id='task05_procesamiento', default_args=default_args)
    )

    # send_message_telegram_task = TelegramOperator(
    #     task_id="send_message_telegram",
    #     telegram_conn_id=674427392,
    #     chat_id=674427392,
    #     text="Hello from Airflow!"
    # )

    with TaskGroup('task06_migrando_bd') as task06_group_migrando_bd:
        registros = []
        with open(f'/opt/airflow/dags/input_data/gobiernos.csv', 'r', encoding='utf-8-sig') as archivo_csv:
            lector = csv.DictReader(archivo_csv, delimiter=';')
            registros = list(lector)

        registros_json = json.dumps(registros)  
        registros_list = json.loads(registros_json)

        for i, registro in enumerate(registros_list):
            task = PythonOperator(
                task_id=f'task_migrando_{i}',
                python_callable=task06_migrando_bd_f,
                op_args=[str(registro["IDE"])]
            )

    end = DummyOperator(
        task_id='end',
    )

    start >> [ task01_verificando_archivo_base_gobiernos , task02_verificando_servicio , task03_verificando_conexion ] >> task04_creando_tabla_si_no_existe >> task05_procesamiento >> task06_group_migrando_bd >> send_message_telegram_task >> end