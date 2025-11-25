REM ===================================
REM 1. run_extract.bat
REM ===================================
@echo off

REM Tạo thư mục logs nếu chưa có
if not exist "D:\DATAWAREHOUSE\logs" mkdir "D:\DATAWAREHOUSE\logs"

REM Tạo tên file log với timestamp
set LOG_FILE=D:\DATAWAREHOUSE\logs\extract_%date:~-4,4%%date:~-7,2%%date:~-10,2%_%time:~0,2%%time:~3,2%%time:~6,2%.log
set LOG_FILE=%LOG_FILE: =0%

echo ========================================== >> "%LOG_FILE%"
echo Extract Process Started: %date% %time% >> "%LOG_FILE%"
echo ========================================== >> "%LOG_FILE%"

REM Chạy Extract script
cd /d "D:\DATAWAREHOUSE"
java -jar target\scripts\extract_scripts\ExtractToFile.jar config\config.xml config\extract_config.xml >> "%LOG_FILE%" 2>&1

if %ERRORLEVEL% NEQ 0 (
    echo ERROR: Extract process failed with code %ERRORLEVEL% >> "%LOG_FILE%"
    exit /b %ERRORLEVEL%
)

echo Extract Process Finished: %date% %time% >> "%LOG_FILE%"
echo ========================================== >> "%LOG_FILE%"
exit /b 0