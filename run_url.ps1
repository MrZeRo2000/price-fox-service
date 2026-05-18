param([string]$url)
Invoke-Expression "$PSScriptRoot/venv/Scripts/python -X utf8 $PSScriptRoot/src/one_time_url.py $url"