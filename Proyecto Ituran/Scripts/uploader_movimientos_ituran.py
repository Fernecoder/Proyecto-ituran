import os
import logging
import time
import sys
import shutil # Nueva librería para mover archivos
from datetime import datetime
from google.cloud import storage
from pathlib import Path

# --- CONFIGURACIÓN DE RUTAS DINÁMICAS ---
script_path = Path(__file__).resolve()
proyecto_root = script_path.parent.parent

# Rutas relativas a la nueva estructura
LOCAL_TEMP_DIR = proyecto_root / "Data" / "Temp_Normalizados"
PROCESADOS_DIR = proyecto_root / "Data" / "Procesados"
LOGS_DIR = proyecto_root / "Logs"

# Credenciales (Si tenés el JSON, descomentá la línea de abajo)
# os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(proyecto_root / "Credentials" / "tu_key.json")

BUCKET_NAME = "sd_op_reg"
DESTINATION_FOLDER = "Import_BQ"

# Asegurar que existan las carpetas necesarias
LOGS_DIR.mkdir(exist_ok=True)
PROCESADOS_DIR.mkdir(exist_ok=True)

# --- CONFIGURACIÓN DE LOGS ---
log_file = LOGS_DIR / f"log_subida_ituran_gcp_{datetime.now().strftime('%Y%m%d')}.log"
logger = logging.getLogger()
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

# Handler Archivo y Terminal
if not logger.handlers:
    file_handler = logging.FileHandler(log_file, encoding='utf-8')
    file_handler.setFormatter(formatter)
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)

def format_destination_name(filename):
    """
    Transforma 'ituran_norm_2025_01_ENERO.csv' a 'Ituran_detallado_ENERO_2025.csv'
    """
    try:
        clean_name = filename.replace(".csv", "")
        parts = clean_name.split("_")
        year = parts[2]
        month_name = parts[4] 
        return f"Ituran_detallado_{month_name}_{year}.csv"
    except Exception:
        return filename

def main():
    try:
        logging.info("====================================================")
        logging.info("🚀 INICIANDO SUBIDA Y ARCHIVADO")
        logging.info("====================================================\n")
        
        if not LOCAL_TEMP_DIR.exists():
            logging.error(f"❌ La carpeta no existe: {LOCAL_TEMP_DIR}")
            return

        storage_client = storage.Client()
        bucket = storage_client.bucket(BUCKET_NAME)

        archivos_csv = list(LOCAL_TEMP_DIR.glob("*.csv"))
        
        if not archivos_csv:
            logging.warning("⚠️ No hay archivos nuevos para subir.")
            return

        logging.info(f"📦 Encontrados {len(archivos_csv)} archivos.")

        for csv_path in archivos_csv:
            start_time = time.time()
            dest_name = format_destination_name(csv_path.name)
            blob_path = f"{DESTINATION_FOLDER}/{dest_name}"
            
            # 1. Subida a GCP
            blob = bucket.blob(blob_path)
            logging.info(f"⬆️ Subiendo: {csv_path.name}...")
            blob.upload_from_filename(str(csv_path))
            
            duration = time.time() - start_time
            logging.info(f"✅ Subido con éxito ({duration:.2f}s)")

            # 2. Mover a Procesados (Solo si la subida no dio error)
            dest_procesado = PROCESADOS_DIR / csv_path.name
            
            # Si ya existe un archivo con el mismo nombre en Procesados, lo pisa (o podés renombrarlo)
            shutil.move(str(csv_path), str(dest_procesado))
            logging.info(f"📂 Archivo movido a: /Data/Procesados/{csv_path.name}")
            logging.info("-" * 30)

        logging.info("\n✨ PROCESO TERMINADO: Todo está en la nube y la carpeta Temp quedó limpia.")

    except Exception as e:
        logging.error(f"❌ ERROR CRÍTICO: {e}")

if __name__ == "__main__":
    main()