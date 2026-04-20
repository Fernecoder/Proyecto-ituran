@echo off
cd /d "%~dp0"
title CLOUD UPLOADER - ITURAN

echo ====================================================
echo   SUBIENDO ARCHIVOS A GOOGLE CLOUD STORAGE
echo ====================================================
echo.

:: 1. Activar venv
call ..\venv\Scripts\activate

:: 2. Ejecutar
python uploader_movimientos_ituran.py

echo.
echo ====================================================
pause