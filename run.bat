@echo off
setlocal EnableExtensions
cd /d "%~dp0"
chcp 65001 >nul
title ClipFlow - Setup and Start

echo.
echo  ==========================================
echo    ClipFlow - checking requirements...
echo  ==========================================
echo.

set "JMAJ=0"
set "CMAJ=0"
set "JMIN=0"
set "CMIN=0"

rem ---- 1. Check the Java RUNTIME version (java) ----
where java >nul 2>nul
if errorlevel 1 goto checkdone
set "JV="
for /f "tokens=3" %%V in ('java -version 2^>^&1 ^| findstr /i "version"') do if not defined JV set "JV=%%~V"
if not defined JV goto checkdone
for /f "tokens=1,2 delims=.-_+" %%a in ("%JV%") do (set "JMAJ=%%a" & set "JMIN=%%b")
if "%JMAJ%"=="1" set "JMAJ=%JMIN%"

rem ---- 2. Check the COMPILER version (javac) ----
where javac >nul 2>nul
if errorlevel 1 goto checkdone
set "CV="
for /f "tokens=2" %%V in ('javac -version 2^>^&1') do if not defined CV set "CV=%%V"
if not defined CV goto checkdone
for /f "tokens=1,2 delims=.-_+" %%a in ("%CV%") do (set "CMAJ=%%a" & set "CMIN=%%b")
if "%CMAJ%"=="1" set "CMAJ=%CMIN%"

:checkdone
echo  Installed Java runtime : %JMAJ%   (needs 11 or newer)
echo  Installed Java compiler: %CMAJ%   (must not be newer than the runtime)
echo.

if %JMAJ% lss 11 goto needjdk
if %CMAJ% equ 0 goto needjdk
if %CMAJ% gtr %JMAJ% goto needjdk
echo  [OK] Java setup is fine.
goto launch

:needjdk
echo  [!!] Your installed Java is missing, too old, or mismatched.
echo       A matching JDK 17 will be used instead (your PC's Java is NOT changed).
echo.

rem ---- 3. Was a portable JDK downloaded earlier? ----
set "JDKDIR="
if exist ".jdk" (
    for /d %%D in (".jdk\jdk*") do if exist "%%~fD\bin\javac.exe" set "JDKDIR=%%~fD"
)
if defined JDKDIR goto usejdk

rem ---- 4. Download a portable JDK 17 (no admin rights needed) ----
echo  [..] Downloading JDK 17 (about 180 MB), please wait...
echo.

set "ARCH=x64"
if /i "%PROCESSOR_ARCHITECTURE%"=="ARM64" set "ARCH=aarch64"

if not exist ".jdk" mkdir ".jdk"
powershell -NoProfile -ExecutionPolicy Bypass -Command "[Net.ServicePointManager]::SecurityProtocol=[Net.SecurityProtocolType]::Tls12; $ProgressPreference='SilentlyContinue'; Invoke-WebRequest -UseBasicParsing -Uri 'https://api.adoptium.net/v3/binary/latest/17/ga/windows/%ARCH%/jdk/hotspot/normal/eclipse' -OutFile '.jdk\jdk.zip'"
if errorlevel 1 goto dlfailed
if not exist ".jdk\jdk.zip" goto dlfailed

echo  [..] Extracting...
powershell -NoProfile -ExecutionPolicy Bypass -Command "Expand-Archive -Force -Path '.jdk\jdk.zip' -DestinationPath '.jdk'"
if errorlevel 1 goto dlfailed
del /q ".jdk\jdk.zip" >nul 2>nul

for /d %%D in (".jdk\jdk*") do if exist "%%~fD\bin\javac.exe" set "JDKDIR=%%~fD"
if not defined JDKDIR goto dlfailed
echo  [OK] JDK downloaded.

:usejdk
set "JAVA_HOME=%JDKDIR%"
set "PATH=%JDKDIR%\bin;%PATH%"
echo  [OK] Using portable JDK in %JDKDIR%

:launch
rem remove any old compiled files so nothing stale from another Java version is used
if exist build rmdir /s /q build >nul 2>nul
echo.
echo  Starting ClipFlow...
echo.
call start.bat %*
goto end

:dlfailed
echo.
echo  [ERROR] Could not download Java automatically.
echo          Check your internet connection and run this file again,
echo          or install a free JDK 17 manually from https://adoptium.net
echo.
pause

:end
endlocal
