@echo off
setlocal
cd /d "%~dp0"
chcp 65001 >nul
title ClipFlow

set "PORT=%~1"
if "%PORT%"=="" set "PORT=8080"

echo.
echo  ClipFlow - starting...
echo  (If Windows Firewall asks, click "Allow access" for PRIVATE networks,
echo   otherwise your phone cannot reach this PC.)
echo.

where javac >nul 2>nul
if errorlevel 1 goto nojdk

if not exist build mkdir build
javac -encoding UTF-8 -d build ClipFlowHub.java
if errorlevel 1 goto failed

start "" /min powershell -NoProfile -WindowStyle Hidden -Command "Start-Sleep 3; Start-Process 'http://localhost:%PORT%'"
java -cp build ClipFlowHub %PORT%
goto end

:nojdk
where java >nul 2>nul
if errorlevel 1 goto nojava
if not exist ClipFlow.jar goto nojava
echo  No JDK found - using the prebuilt ClipFlow.jar instead.
start "" /min powershell -NoProfile -WindowStyle Hidden -Command "Start-Sleep 3; Start-Process 'http://localhost:%PORT%'"
java -jar ClipFlow.jar %PORT%
goto end

:nojava
echo  Java 11 or newer is required but was not found.
echo  Install a free JDK from https://adoptium.net and run this file again.
goto end

:failed
echo.
echo  Compilation failed - see the messages above.

:end
echo.
pause
