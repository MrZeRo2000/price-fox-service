param([string]$url)
Invoke-Expression "$PSScriptRoot/venv/Scripts/python $PSScriptRoot/src/one_time_url.py $url"