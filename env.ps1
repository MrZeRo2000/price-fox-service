
$latestPython = Get-ChildItem $pythonRoot -Directory -Filter "Python*" |
    Sort-Object {
        $v = $_.Name -replace '^Python',''
        [version]"$($v[0]).$($v.Substring(1))"
    } -Descending |
    Select-Object -First 1

if ($latestPython) {
    $env:Path += ";$($latestPython.FullName)"
}

Write-Output $env:Path