@echo off
REM Quick setup script for Windows

echo ========================================
echo Game Boy Pipeline - Quick Setup (uv)
echo ========================================
echo.

REM Check if uv is installed
where uv >nul 2>nul
if %ERRORLEVEL% NEQ 0 (
    echo [!] uv is not installed. Installing...
    pip install uv
    if %ERRORLEVEL% NEQ 0 (
        echo [ERROR] Failed to install uv
        exit /b 1
    )
)

echo [1/5] Creating virtual environment...
uv venv
if %ERRORLEVEL% NEQ 0 (
    echo [ERROR] Failed to create virtual environment
    exit /b 1
)

echo [2/5] Activating virtual environment...
call .venv\Scripts\activate.bat

echo [3/5] Installing core dependencies...
uv pip install -r requirements.txt
if %ERRORLEVEL% NEQ 0 (
    echo [ERROR] Failed to install dependencies
    exit /b 1
)

echo [4/5] Installing dev dependencies...
uv pip install -r requirements-dev.txt
if %ERRORLEVEL% NEQ 0 (
    echo [ERROR] Failed to install dev dependencies
    exit /b 1
)

echo [5/5] Setting up environment file...
if not exist .env (
    copy .env.example .env
    echo [!] Please edit .env file with your IGDB credentials
)

echo.
echo ========================================
echo Setup complete!
echo ========================================
echo.
echo Next steps:
echo 1. Edit .env file with your IGDB API credentials
echo 2. Activate virtual environment: .venv\Scripts\activate.bat
echo 3. Run tests: pytest
echo 4. Start coding!
echo.
