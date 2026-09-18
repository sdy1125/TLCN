param(
    [switch]$Force
)

$ErrorActionPreference = "Stop"
$projectRoot = Split-Path -Parent $PSScriptRoot
$manifestPath = Join-Path $projectRoot "data\drive_manifest.csv"
$rawRoot = Join-Path $projectRoot "data\raw"
$rows = Import-Csv -LiteralPath $manifestPath

foreach ($row in $rows) {
    $relativePath = $row.relative_path.Replace("/", [IO.Path]::DirectorySeparatorChar)
    $target = Join-Path $rawRoot $relativePath
    $targetDirectory = Split-Path -Parent $target
    New-Item -ItemType Directory -Path $targetDirectory -Force | Out-Null

    if ((Test-Path -LiteralPath $target) -and -not $Force) {
        Write-Host "SKIP $($row.relative_path)"
        continue
    }

    $temporary = "$target.part"
    $url = "https://drive.usercontent.google.com/download?id=$($row.file_id)&export=download&confirm=t"
    Write-Host "GET  $($row.relative_path)"
    & curl.exe -L --fail --retry 5 --retry-all-errors --max-time 600 -o $temporary $url
    if ($LASTEXITCODE -ne 0) {
        Remove-Item -LiteralPath $temporary -Force -ErrorAction SilentlyContinue
        throw "Download failed: $($row.relative_path)"
    }
    Move-Item -LiteralPath $temporary -Destination $target -Force
}

Write-Host "Raw data is ready at $rawRoot"

