@echo off
chcp 65001 >nul
cd /d "%~dp0"
rem ============================================================
rem  Wenku pipeline launcher
rem  Always runs with the project venv interpreter, so the
rem  "[env] re-exec" notice never appears.
rem
rem  Usage:
rem    double-click          -> interactive menu
rem    run.cmd --step all    -> run steps from command line
rem ============================================================
title Wenku Pipeline
"C:\Users\Lenovo\.workbuddy\binaries\python\envs\default\Scripts\python.exe" "%~dp0pipeline.py" %*
set RC=%ERRORLEVEL%
echo ==================== EXIT=%RC% ====================
pause
