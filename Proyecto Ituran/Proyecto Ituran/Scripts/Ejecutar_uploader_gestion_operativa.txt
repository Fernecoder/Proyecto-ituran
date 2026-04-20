@echo off
:: Se posiciona en la carpeta donde está el .bat (scripts/)
cd /d "%~dp0"
title INGESTA GESTION OPERATIVA - SQL4 A BQ

echo ========================================================
echo   INICIANDO PROCESO DE CARGA: GESTION OPERATIVA (SQL4)
echo ========================================================
echo.

:: 1. Activa el entorno virtual subiendo un nivel
echo [1/3] Accediendo al entorno virtual...
call ..\venv\Scripts\activate

:: 2. Ejecuta el script
echo [2/3] Ejecutando uploader_gestion_operativa.py...
echo.
python uploader_gestion_operativa.py

:: 3. Fin
echo.
echo [3/3] Proceso finalizado. Revise la carpeta /logs.
echo ====================================================
pause