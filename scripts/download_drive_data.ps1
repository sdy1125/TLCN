param(
    [switch]$Force,
    [switch]$Prune
)

$ErrorActionPreference = "Stop"
$projectRoot = Split-Path -Parent $PSScriptRoot
$manifestPath = Join-Path $projectRoot "data\drive_manifest.csv"
$rawRoot = Join-Path $projectRoot "data\raw"
$statePath = Join-Path $projectRoot "data\.drive-download-state.json"
$legacyStatePath = Join-Path $rawRoot ".drive-download-state.json"
$rows = Import-Csv -LiteralPath $manifestPath
$rawRootFull = [IO.Path]::GetFullPath($rawRoot).TrimEnd([IO.Path]::DirectorySeparatorChar) + [IO.Path]::DirectorySeparatorChar
$manifestPaths = [Collections.Generic.HashSet[string]]::new([StringComparer]::OrdinalIgnoreCase)
$downloadState = @{}

New-Item -ItemType Directory -Path $rawRoot -Force | Out-Null

if ((Test-Path -LiteralPath $legacyStatePath) -and -not (Test-Path -LiteralPath $statePath)) {
    Move-Item -LiteralPath $legacyStatePath -Destination $statePath
}

if (Test-Path -LiteralPath $statePath) {
    $savedState = Get-Content -LiteralPath $statePath -Raw -Encoding UTF8 | ConvertFrom-Json
    foreach ($property in $savedState.PSObject.Properties) {
        $downloadState[$property.Name] = [string]$property.Value
    }
}

foreach ($row in $rows) {
    $relativePath = $row.relative_path.Trim().Replace("\", "/")
    if (-not $relativePath -or $relativePath.StartsWith("/") -or $relativePath.Split("/") -contains "..") {
        throw "Unsafe relative_path in manifest: $relativePath"
    }
    if (-not $manifestPaths.Add($relativePath)) {
        throw "Duplicate relative_path in manifest: $relativePath"
    }

    $nativeRelativePath = $relativePath.Replace("/", [IO.Path]::DirectorySeparatorChar)
    $target = [IO.Path]::GetFullPath((Join-Path $rawRoot $nativeRelativePath))
    if (-not $target.StartsWith($rawRootFull, [StringComparison]::OrdinalIgnoreCase)) {
        throw "Target is outside data/raw: $relativePath"
    }

    $targetDirectory = Split-Path -Parent $target
    New-Item -ItemType Directory -Path $targetDirectory -Force | Out-Null

    $sameFileId = $downloadState.ContainsKey($relativePath) -and $downloadState[$relativePath] -eq $row.file_id
    if ((Test-Path -LiteralPath $target) -and $sameFileId -and -not $Force) {
        Write-Host "SKIP $relativePath"
        continue
    }

    $temporary = "$target.part"
    $url = "https://drive.usercontent.google.com/download?id=$($row.file_id)&export=download&confirm=t"
    Write-Host "GET  $relativePath"
    & curl.exe -L --fail --retry 5 --retry-all-errors --max-time 600 -o $temporary $url
    if ($LASTEXITCODE -ne 0) {
        Remove-Item -LiteralPath $temporary -Force -ErrorAction SilentlyContinue
        throw "Download failed: $relativePath"
    }
    if (-not (Test-Path -LiteralPath $temporary) -or (Get-Item -LiteralPath $temporary).Length -eq 0) {
        Remove-Item -LiteralPath $temporary -Force -ErrorAction SilentlyContinue
        throw "Downloaded file is empty: $relativePath"
    }
    Move-Item -LiteralPath $temporary -Destination $target -Force
    $downloadState[$relativePath] = $row.file_id
}

if ($Prune) {
    Get-ChildItem -LiteralPath $rawRoot -Recurse -File | Where-Object { $_.Name -ne ".gitkeep" } | ForEach-Object {
        $localRelativePath = $_.FullName.Substring($rawRootFull.Length).Replace("\", "/")
        if (-not $manifestPaths.Contains($localRelativePath)) {
            Write-Host "REMOVE $localRelativePath"
            Remove-Item -LiteralPath $_.FullName -Force
        }
    }
}

$currentState = [ordered]@{}
foreach ($row in $rows) {
    $relativePath = $row.relative_path.Trim().Replace("\", "/")
    $currentState[$relativePath] = $row.file_id
}
$temporaryState = "$statePath.part"
$currentState | ConvertTo-Json | Set-Content -LiteralPath $temporaryState -Encoding UTF8
Move-Item -LiteralPath $temporaryState -Destination $statePath -Force

Write-Host "Raw data is ready at $rawRoot"
