REM ===================================
REM 4. run_load_warehouse.bat
REM ===================================
@echo off

if not exist "D:\DATAWAREHOUSE\logs" mkdir "D:\DATAWAREHOUSE\logs"

set LOG_FILE=D:\DATAWAREHOUSE\logs\load_warehouse_%date:~-4,4%%date:~-7,2%%date:~-10,2%_%time:~0,2%%time:~3,2%%time:~6,2%.log
set LOG_FILE=%LOG_FILE: =0%

echo ========================================== >> "%LOG_FILE%"
echo Load to Warehouse Started: %date% %time% >> "%LOG_FILE%"
echo ========================================== >> "%LOG_FILE%"

cd /d "D:\DATAWAREHOUSE"
java -jar target\scripts\load_scripts\LoadToDataWarehouse.jar config\config.xml >> "%LOG_FILE%" 2>&1

if %ERRORLEVEL% NEQ 0 (
    echo ERROR: Load to Warehouse failed with code %ERRORLEVEL% >> "%LOG_FILE%"
    exit /b %ERRORLEVEL%
)

echo Load to Warehouse Finished: %date% %time% >> "%LOG_FILE%"
echo ========================================== >> "%LOG_FILE%"
exit /b 0