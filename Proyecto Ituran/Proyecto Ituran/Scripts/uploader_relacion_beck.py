import os
import shutil
import logging
import time
from datetime import datetime
from google.cloud import storage, bigquery


# Configuración del log
log_file = "subida_archivo_gcp.log"
logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# Parámetros
bucket_name = "sd_op_reg"
archivos = [
    {
        "source_file_path": "C:/Users/u622254/Desktop/Proyectos/Python/Prueba Ituran/Beck/base moviles 2025.csv",
        "destination_blob_name": "Import_BQ/Base moviles VEC 2025.csv",
        "tiene_rutina" : False,
        "esquema_rutina" : 'SDR'
    }
]

def log_and_print(message):
    logging.info(message)
    print(message)

def log_w_and_print(message):
    logging.warning(message)
    print(message)


def log_e_and_print(message):
    logging.error(message)
    print(message)

# Función para subir archivo a GCP

def upload_csv_to_gcs(bucket_name, source_file_path, destination_blob_name):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file_path)
    log_and_print(f"Archivo subido a gs://{bucket_name}/{destination_blob_name}")

# Función para mover archivo a carpeta Archivos_Ayer

def mover_archivo(source_file_path):
    carpeta_destino = os.path.join(os.path.dirname(source_file_path), "Archivos_Ayer")
    os.makedirs(carpeta_destino, exist_ok=True)
    destino = os.path.join(carpeta_destino, os.path.basename(source_file_path))
    shutil.move(source_file_path, destino)
    log_and_print(f"Archivo movido a {destino}")

# Función para ejecutar rutina en BigQuery

def ejecutar_rutina_bigquery(nombre_archivo,esquema_rutina):
    client = bigquery.Client()
    rutina = f"CALL `teco-prod-ds-opereg-1f85.{esquema_rutina}.CTLS_{nombre_archivo}`();"
    client.query(rutina).result()
    log_and_print(f"Rutina ejecutada: {rutina}")

# Procesar cada archivo

for archivo in archivos:
    source_file_path = archivo["source_file_path"]
    destination_blob_name = archivo["destination_blob_name"]
    tiene_rutina = archivo["tiene_rutina"]
    esquema_rutina = archivo["esquema_rutina"]
    try:
        if not os.path.exists(source_file_path):
            log_w_and_print(f"El archivo no existe: {source_file_path}")
            continue
        mod_time = os.path.getmtime(source_file_path)
        mod_date = datetime.fromtimestamp(mod_time).date()
        today = datetime.today().date()

        if True:
            log_and_print(f"El archivo {source_file_path} fue modificado hoy ({mod_date}). Subiendo...")
            upload_csv_to_gcs(bucket_name, source_file_path, destination_blob_name)
        
            # Extraer nombre del archivo sin extensión
            if tiene_rutina == True:
                nombre_archivo = os.path.splitext(os.path.basename(source_file_path))[0]
                ejecutar_rutina_bigquery(nombre_archivo,esquema_rutina)
        else:
            log_w_and_print(f"El archivo {source_file_path} no fue modificado hoy. Fecha: {mod_date}")
    except Exception as e:
        log_e_and_print(f"Error procesando el archivo {source_file_path}: {e}")

print(f"Proceso completado. Revisá el log en: {log_file}")
print("Script terminado. Cerrando en 10 segundos...")
time.sleep(10)
print("Fin.")