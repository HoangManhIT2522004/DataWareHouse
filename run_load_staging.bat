REM ===================================
REM 2. run_load_staging.bat
REM ===================================
@echo off

if not exist "D:\DATAWAREHOUSE\logs" mkdir "D:\DATAWAREHOUSE\logs"

set LOG_FILE=D:\DATAWAREHOUSE\logs\load_staging_%date:~-4,4%%date:~-7,2%%date:~-10,2%_%time:~0,2%%time:~3,2%%time:~6,2%.log
set LOG_FILE=%LOG_FILE: =0%

echo ========================================== >> "%LOG_FILE%"
echo Load to Staging Started: %date% %time% >> "%LOG_FILE%"
echo ========================================== >> "%LOG_FILE%"

cd /d "D:\DATAWAREHOUSE"
java -jar target\scripts\load_scripts\LoadToStaging.jar config\config.xml >> "%LOG_FILE%" 2>&1

if %ERRORLEVEL% NEQ 0 (
    echo ERROR: Load to Staging failed with code %ERRORLEVEL% >> "%LOG_FILE%"
    exit /b %ERRORLEVEL%
)

echo Load to Staging Finished: %date% %time% >> "%LOG_FILE%"
echo ========================================== >> "%LOG_FILE%"
exit /b 0
