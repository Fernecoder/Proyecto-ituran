import pandas as pd
import os 
from pathlib import Path
import openpyxl
import logging 
from datetime import datetime
import sys

# --- CONFIGURACIÓN DE RUTAS DINÁMICAS ---
# Determinamos dónde está parado el script (dentro de /scripts)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Definimos las rutas subiendo un nivel hacia la raíz del proyecto
PROYECTO_ROOT = os.path.abspath(os.path.join(BASE_DIR, ".."))
LOGS_DIR = os.path.join(PROYECTO_ROOT, "Logs")
INPUT_DIR = os.path.join(PROYECTO_ROOT, "Data", "Ituran")
OUTPUT_DIR = os.path.join(PROYECTO_ROOT, "Data", "Temp_Normalizados")

# Aseguramos que existan las carpetas necesarias
os.makedirs(LOGS_DIR, exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)

# --- CONFIGURACIÓN DE LOGGING CENTRALIZADO ---
log_filename = os.path.join(LOGS_DIR, f"log_normalizacion_ituran_{datetime.now().strftime('%Y%m%d')}.txt")
logger = logging.getLogger()
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

# Handler archivo (UTF-8 para evitar errores con caracteres argentinos)
file_handler = logging.FileHandler(log_filename, encoding='utf-8')
file_handler.setFormatter(formatter)

# Handler terminal
stream_handler = logging.StreamHandler(sys.stdout)
stream_handler.setFormatter(formatter)

logger.addHandler(file_handler)
logger.addHandler(stream_handler)

# Schema que va a esperar BigQuery
GOLDEN_COLUMNS = [
    'patente', 'fecha', 'hora_inicio', 'hora_fin', 'inicio_viaje',
    'lat_inicio', 'lon_inicio', 'fin_viaje', 'lat_fin', 'lon_fin',
    'grupo', 'km_totales', 'horas_motor', 'ralenti_horas', 'ralenti_porc',
    'vel_max', 'frenos', 'giros', 'rebases', 'exceso_vel', 'aceleraciones',
    'score_seguridad', 'score_combustible', 'metadata_fuente', 'metadata_periodo'
]

# El "Traductor" para unificar el caos de nombres que me pasaste
MAPEO_COLUMNAS = {
    # Latitudes
    'latitud de inico': 'lat_inicio', 'latitud inicio del viaje': 'lat_inicio', 'lat inicio': 'lat_inicio',
    'lat inicial': 'lat_inicio', 'lat inicio de viaje': 'lat_inicio',
    'lat de inicio': 'lat_inicio', 'lat start trip': 'lat_inicio', 'lat fin de viaje': 'lat_fin', 'lat fin': 'lat_fin',
    'lat de iinicio': 'lat_inicio', 'lat inicio viaje': 'lat_inicio', 'inicio del viaje': 'inicio_viaje','inicio viaje': 'inicio_viaje','inicio de viaje': 'inicio_viaje',
    'latitud fin de viaje': 'lat_fin', 'lat de fin': 'lat_fin', 'lat end trip': 'lat_fin', 'latitud de fin': 'lat_fin',
    # Longitudes
    'longitud de inicio': 'lon_inicio', 'longitud inicio del viaje': 'lon_inicio', 'lon inicio':'lon_inicio',
    'lon inicial': 'lon_inicio', 'lon inicio de vieaje': 'lon_inicio', 'lon inicio de viaje': 'lon_inicio',
    'lon de inicio': 'lon_inicio', 'lon start trip': 'lon_inicio', 'lon fin de viaje': 'lon_fin', 'lon fin':'lon_fin',
    'long de inicio': 'lon_inicio', 'long inicio viaje': 'lon_inicio', 'fin del viaje': 'fin_viaje','fin viaje': 'fin_viaje','fin de viaje': 'fin_viaje',
    'longitud fin de viaje': 'lon_fin', 'long de fin': 'lon_fin', 'lon de fin': 'lon_fin', 'lon end trip': 'lon_fin', 'longitud de fin': 'lon_fin', 'long fin de viaje': 'lon_fin',
    # Fechas y Horas
    'date': 'fecha', 'fecha': 'fecha', 'fecha de inicio': 'hora_inicio', 
    'hora de inicio': 'hora_inicio', 'fecha de fin': 'hora_fin', 
    'hora de fin': 'hora_fin', 'fhora de fin': 'hora_fin',
    # Métricas
    'total de kilómetros recorridos': 'km_totales', 'total de horas de motor': 'horas_motor',
    'horas de velocidad cero': 'ralenti_horas', 'ralenti': 'ralenti_horas',
    'porcentaje de velocidad cero': 'ralenti_porc', '% de ralenti': 'ralenti_porc',
    'límites de velocidad excedidos': 'exceso_vel', 'velocidad máxima registrada': 'vel_max', 'promedio de seguridad ponderada': 'score_seguridad',
    'promedio de combustible ponderada': 'score_combustible', 'aceleraciones': 'aceleraciones',
    'patente': 'patente', 'grupo': 'grupo'
}


def check_quality(df, periodo, original_row_count):
    """
    Control de calidad:
    1. Compara volumen de filas.
    2. Detecta columnas críticas que quedaron vacias (NaN).
    """
    report = []

    # Check de volumen
    new_row_count = len(df)
    if new_row_count != original_row_count:
        report.append(f"❌ ERROR VOLUMEN: Origen {original_row_count} filas -> Destino {new_row_count} filas.")
    # Check de vacios (Si la columna existe pero tiene valores NaN)
    critical_cols = ['patente', 'lat_inicio', 'lon_inicio', 'fecha', 'lat_fin', 'lon_fin']
    for col in critical_cols:
        if col in df.columns:
            # Cuento cuantos no estan vacios
            non_empty = df[col].replace("",pd.NA).dropna().count()
            if non_empty == 0:
                report.append(f"⚠️ COLUMNA VACÍA: '{col}' no tiene datos. Revisar MAPEO_COLUMNAS.")
    return report

def clean_file(input_path, output_folder):
    try:
        # 1. CARGA MANUAL DE EXCEL (Cero interpretación)
        # Usamos data_only=False para que no intente calcular nada
        wb = openpyxl.load_workbook(input_path, data_only=False)
        sheet = wb.active
        
        # Extraemos los datos como una lista de listas, forzando a string
        data = []
        for row in sheet.iter_rows(min_row=2): # min_row=2 salta la fila 10
            row_data = []
            for cell in row:
                val = cell.value
                if val is None:
                    row_data.append("")
                else:
                    row_data.append(str(val).replace('.', ',')) 
            data.append(row_data)
        
        # El primer elemento de 'data' ahora son encabezados
        headers = [str(h).strip().lower() for h in data[0]]
        df = pd.DataFrame(data[1:], columns=headers)

        # 2. LIMPIEZA DE FECHAS (Si Excel mandó el " 00:00:00")
        # Como todo es string ahora, un split simple lo soluciona
        for col in df.columns:
            if any(key in col for key in ['fecha', 'date']):
                df[col] = df[col].apply(lambda x: x.split(' ')[0] if ' ' in x else x)

        # 3. RENOMBRAR
        df = df.rename(columns=MAPEO_COLUMNAS)

        # Extraigo la info del path para la metadata
        parts = Path(input_path).parts
        periodo = f"{parts[-3]}_{parts[-2]}"

        # --- DEBUG ---
        if 'patente' not in df.columns:
            print(f"⚠️ OJO: En {periodo} no encontré la columna 'patente'. Columnas: {list(df.columns[:5])}...")

         # Metadata para trazabilidad 
        df['metadata_fuente'] = os.path.basename(input_path)
        df['metadata_periodo'] = periodo
        
        # Fuerzo el Schema que espera BigQuery
        df_final = df.reindex(columns=GOLDEN_COLUMNS)
        
        original_row_count = len(df) # Cantidad antes del reindex
        quality_issues = check_quality(df_final, periodo, original_row_count)
        
        if quality_issues:
            for issue in quality_issues:
                logging.warning(f"[{periodo}] {issue}")
        else:
            logging.info(f"✅ [{periodo}] Calidad OK. Filas: {original_row_count}")

        # Normaliza la salida
        filename = f"ituran_norm_{periodo}.csv"
        out_path = os.path.join(output_folder, filename)
        
        # Exporto a CSV normalizado 
        df_final.to_csv(out_path, index=False, sep=';', encoding='utf-8-sig', quoting=1)
        return out_path
    
    except Exception as e:
        print(f"❌ Error en {input_path}: {e}")
        logging.error(f"❌ Error crítico en {input_path}: {e}")
        return None

def process_ituran_directory(base_dir, output_dir, procesados_dir):
    logging.info(f"🔍 Escaneando archivos en: {base_dir}")
    
    contador = 0
    for root, dirs, files in os.walk(base_dir):
        for file in files:
            if file.endswith(".xlsx") and not file.startswith("~$"):
                full_path = Path(os.path.join(root, file))
                
                # Lógica Robusta: 
                # .parent es '01_ENERO', .parent.parent es '2025'
                try:
                    anio = full_path.parent.parent.name
                    mes = full_path.parent.name
                    periodo = f"{anio}_{mes}"
                except Exception:
                    periodo = "desconocido"

                output_filename = f"ituran_norm_{periodo}.csv"
                # --- DOBLE CHECK DE SI EXISTE O NO EL ARCHIVO ---
                path_temp = Path(output_dir) / output_filename
                path_procesado = Path(procesados_dir) / output_filename

                if path_temp.exists():
                    logging.info(f"⏩ Saltando: Ya está en Temp (Pendiente de subida) -> {output_filename}")
                    continue
                
                if path_procesado.exists():
                    logging.info(f"✅ Saltando: Ya fue procesado y archivado -> {output_filename}")
                    continue
                # ----------------------

                logging.info(f"🚀 Procesando nuevo archivo: {file} -> Periodo: {periodo}")
                clean_file(str(full_path), output_dir)
                contador += 1
    
    if contador == 0:
        logging.info("✨ No hay archivos nuevos para procesar. Todo está al día.")

if __name__ == "__main__":
    # 1. Ubicamos la ruta del script actual
    script_path = Path(__file__).resolve() 
    
    # 2. Definimos la raíz del proyecto (sube un nivel desde /scripts)
    proyecto_root = script_path.parent.parent
    
    # 3. Construimos las rutas finales respetando tus mayúsculas/minúsculas
    # Origen: /Data/Ituran
    # Destino: /Data/Temp_Normalizados
    INPUT_DIR = proyecto_root / "Data" / "Ituran"
    OUTPUT_DIR = proyecto_root / "Data" / "Temp_Normalizados"
    PROCESADOS_DIR = proyecto_root / "Data" / "Procesados"

    # Crear carpetas si no existen
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    PROCESADOS_DIR.mkdir(parents=True, exist_ok=True)

    print("====================================================")
    print(f"🚀 INICIANDO NORMALIZACIÓN")
    print(f"📂 Origen: {INPUT_DIR}")
    print(f"📂 Destino: {OUTPUT_DIR}")
    print("====================================================\n")

    # Verificación de seguridad antes de arrancar
    if not INPUT_DIR.exists():
        logging.error(f"❌ ERROR: La carpeta de origen no existe: {INPUT_DIR}")
        print(f"❌ ERROR: No se encontró la carpeta {INPUT_DIR}. Revisá los nombres.")
    else:
        # Ejecutamos el proceso pasandole los strings de las rutas
        process_ituran_directory(str(INPUT_DIR), str(OUTPUT_DIR), str(PROCESADOS_DIR))
        
        print("\n====================================================")
        print("✅ PROCESO TERMINADO")
        print(f"📂 Chequeá los archivos CSV en: {OUTPUT_DIR}")
        print("====================================================")

