@echo off
rem Windows twin of workspace_status.sh (Bazel runs the status command
rem directly, so it must be a .cmd here). Keep the two in sync.
set COMMIT=
for /f "delims=" %%i in ('git rev-parse HEAD 2^>nul') do set COMMIT=%%i
if "%COMMIT%"=="" exit /b 0
echo STABLE_GIT_COMMIT %COMMIT%
for /f "delims=" %%i in ('git rev-list --count HEAD') do echo STABLE_GIT_REVISION %%i
for /f "delims=" %%i in ('git log -1 --format^=%%cs') do echo STABLE_GIT_DATE %%i
set DIRTY=0
for /f "delims=" %%i in ('git status --porcelain --untracked-files^=no') do set DIRTY=1
echo STABLE_GIT_DIRTY %DIRTY%
