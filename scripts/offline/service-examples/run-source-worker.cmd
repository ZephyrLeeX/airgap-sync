@echo off
rem Example wrapper for running the Airgap Sync Source worker on Windows.
rem
rem Copy this file into the install root (the folder that contains "current"),
rem e.g. C:\Program Files\AirgapSync\run-source-worker.cmd
rem
rem It always calls the CURRENT release, so upgrades only switch the
rem "current" junction; this wrapper never needs editing.
rem
rem MySQL password / relay token are read from the environment variables
rem named in the YAML config (set them via the service wrapper or scheduler).

setlocal
set "INSTALL_ROOT=%~dp0"
set "CONFIG=%INSTALL_ROOT%..\config\source.yaml"

if not "%AIRGAP_SYNC_SOURCE_CONFIG%"=="" set "CONFIG=%AIRGAP_SYNC_SOURCE_CONFIG%"

"%INSTALL_ROOT%current\venv\Scripts\airgap-sync.exe" source worker --config "%CONFIG%"
exit /b %ERRORLEVEL%
