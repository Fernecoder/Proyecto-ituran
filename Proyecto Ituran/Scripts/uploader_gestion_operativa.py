import pyodbc
import pandas as pd
import logging
import sys
import time
from datetime import datetime
from google.cloud import bigquery
from pathlib import Path

# --- CONFIGURACIÓN DE RUTAS DINÁMICAS ---
script_path = Path(__file__).resolve()
proyecto_root = script_path.parent.parent
LOGS_DIR = proyecto_root / "Logs"
LOGS_DIR.mkdir(exist_ok=True)

# --- CONFIGURACIÓN DE PARÁMETROS DEL SQL Y BQ ---
SERVER = "LDRNE-SQL4"
DATABASE = "Unificado"
PROJECT_ID = "teco-prod-ds-opereg-1f85"
DATASET_ID = "ITURAN"

# --- LOGGING CENTRALIZADO ---
log_filename = LOGS_DIR / f"log_subida_sql_gcp_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
logger = logging.getLogger()
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

file_handler = logging.FileHandler(log_filename, encoding='utf-8')
file_handler.setFormatter(formatter)
stream_handler = logging.StreamHandler(sys.stdout)
stream_handler.setFormatter(formatter)

if not logger.handlers:
    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)

def get_inputs():
    """Valida los inputs del usuario."""
    while True:
        try:
            print("\n" + "=" * 40)
            year = input("📅 Ingrese el AÑO (YYYY): ")
            month = input("📅 Ingrese el MES (M o MM): ")
            if len(year) == 4 and year.isdigit() and month.isdigit():
                return int(year), int(month)
            print("❌ Formato inválido. Ejemplo: 2025 y 8")
        except ValueError:
            print("❌ Por favor, ingrese números.")

def run_ingestion():
    year, month = get_inputs()
    table_name = f"raw_sql_gestion_operativa_{year}_{month:02d}"
    table_id = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"
    
    start_time = time.time()
    
    try:
        # 1. Conexión y Extracción
        # Trusted_Connection=yes usa las credenciales de LAN
        conn_str = f"Driver={{SQL Server}};Server={SERVER};Database={DATABASE};Trusted_Connection=yes;"
        logging.info(f"🔗 Conectando a {SERVER}...")
        conn = pyodbc.connect(conn_str)
        
        query = f"""
        SELECT * FROM [Unificado].[KPI].[Gestion_Operativa]
        WHERE año = {year} AND mes = {month}
        """
        
        logging.info(f"⏳ Extrayendo datos de {year}-{month:02d}...")
        df = pd.read_sql(query, conn)
        conn.close()
        
        rows_source = len(df)
        if rows_source == 0:
            logging.warning("⚠️ La query no devolvió registros. Proceso abortado, verificá el año y mes en el SQL.")
            return

        logging.info(f"✅ Datos recuperados: {rows_source} filas y {df.shape[1]} columnas.")

        # 2. Limpieza técnica y normalizacion de columnas
        df.columns = [
            c.replace(' ', '_').replace('.', '_').replace('-', '_').replace('/', '_').lower() 
            for c in df.columns
        ]
        
        # Opcional: Tratar nulos para evitar 'nan' strings si preferís celdas vacías
        df = df.astype(str).replace(['nan', 'None', 'None '], '')

        # 3. Carga a BigQuery
        client = bigquery.Client()
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE", # WRITE_TRUNCATE pisa la tabla si ya existe para ese mes (Idempotencia)
            autodetect=True
        )

        logging.info(f"⬆️ Subiendo a BQ: {table_id}...")
        job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result() # Esperar a que termine

        # 4. Control de calidad 
        table = client.get_table(table_id)
        rows_bq = table.num_rows
        
        logging.info("--- CONTROL DE CALIDAD ---")
        logging.info(f"📊 Registros en SQL: {rows_source}")
        logging.info(f"📊 Registros en BQ:  {rows_bq}")
        
        if rows_source == rows_bq:
            logging.info("✅ INTEGRIDAD OK: Los volúmenes coinciden.")
        else:
            logging.error(f"❌ ERROR DE INTEGRIDAD: Faltan {rows_source - rows_bq} registros.")

        duration = (time.time() - start_time) / 60
        logging.info(f"✨ Proceso terminado en {duration:.2f} minutos.")
        logging.info(f"📄 Log guardado en: {log_filename}")

    except Exception as e:
        logging.error(f"❌ FALLO CRÍTICO: {e}")

if __name__ == "__main__":
    print("--- INGESTA GESTION OPERATIVA SQL HACIA BQ ---")
    run_ingestion()
    input("\nPresione Enter para salir...")