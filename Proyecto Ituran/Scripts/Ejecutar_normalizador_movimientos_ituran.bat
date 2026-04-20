@echo off
:: Se posiciona en la carpeta donde está el .bat (scripts/)
cd /d "%~dp0"
title ETL ITURAN: NORMALIZADOR DE MOVIMIENTOS

echo ====================================================
echo   INICIANDO NORMALIZACIÓN DE EXCEL (ITURAN)
echo ====================================================
echo.

:: 1. Activa el entorno virtual (sube un nivel a la raíz y entra a venv)
echo [1/3] Accediendo al entorno virtual...
if exist "..\venv\Scripts\activate" (
    call ..\venv\Scripts\activate
) else (
    echo ❌ ERROR: No se encontro la carpeta venv en la raiz.
    pause
    exit
)

:: 2. Ejecuta el script de normalización
echo [2/3] Ejecutando normalizador__movimientos_ituran.py...
echo.
python normalizador_movimientos_ituran.py

:: 3. Fin
echo.
echo [3/3] Proceso terminado.
echo Los archivos CSV estan en: /data/temp_normalizados
echo Los logs estan en: /logs
echo ====================================================
pause