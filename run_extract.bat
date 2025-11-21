@echo off
cd /d D:\DATAWAREHOUSE\TARGET
set LOG_FILE=D:\DATAWAREHOUSE\logs\extract_%date:~-4,4%%date:~-7,2%%date:~-10,2%_%time:~0,2%%time:~3,2%%time:~6,2%.log
java -jar ExtractToFile.jar >> "%LOG_FILE%" 2>&1
echo Extract completed at %date% %time% >> "%LOG_FILE%"