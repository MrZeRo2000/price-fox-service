Invoke-Expression "$PSScriptRoot/install_venv.ps1"
Invoke-Expression "$PSScriptRoot/upgrade_pip_setuptools.ps1"
Invoke-Expression "$PSScriptRoot/install_requirements.ps1"

Invoke-Expression "$PSScriptRoot/venv/Scripts/playwright install chromium"

