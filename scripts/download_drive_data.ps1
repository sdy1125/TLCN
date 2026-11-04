param(
    [switch]$Force,
    [switch]$Prune
)

$ErrorActionPreference = "Stop"
$projectRoot = Split-Path -Parent $PSScriptRoot
$manifestPath = Join-Path $projectRoot "data/drive_manifest.csv"
$rawRoot = Join-Path $projectRoot "data/raw"
$archiveRoot = Join-Path $projectRoot "data/.drive-versions"
$statePath = Join-Path $rawRoot ".drive-handoff.json"
$inProgressPath = Join-Path $rawRoot ".drive-handoff.in-progress"
$rows = Import-Csv -LiteralPath $manifestPath
$rawRootFull = [IO.Path]::GetFullPath($rawRoot).TrimEnd([IO.Path]::DirectorySeparatorChar) + [IO.Path]::DirectorySeparatorChar
$manifestPaths = [Collections.Generic.HashSet[string]]::new([StringComparer]::OrdinalIgnoreCase)
$currentState = [ordered]@{}

New-Item -ItemType Directory -Path $rawRoot, $archiveRoot -Force | Out-Null
Set-Content -LiteralPath $inProgressPath -Value ((Get-Date).ToUniversalTime().ToString("o"))

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
    New-Item -ItemType Directory -Path (Split-Path -Parent $target) -Force | Out-Null

    $apiMetadata = $null
    $apiKey = [Environment]::GetEnvironmentVariable("GOOGLE_DRIVE_API_KEY")
    if ($apiKey) {
        $fields = [Uri]::EscapeDataString("id,modifiedTime,md5Checksum,version,headRevisionId")
        $safeKey = [Uri]::EscapeDataString($apiKey)
        $metadataUrl = "https://www.googleapis.com/drive/v3/files/$($row.file_id)?fields=$fields&key=$safeKey"
        $apiMetadata = Invoke-RestMethod -Uri $metadataUrl -Method Get
        if ($apiMetadata.id -ne $row.file_id) {
            throw "Drive metadata ID mismatch: $relativePath"
        }
    }

    # File IDs and timestamps are hints; downloaded bytes decide whether content changed.
    $temporary = "$target.part.$([guid]::NewGuid().ToString('N'))"
    $headersPath = "$temporary.headers"
    $url = "https://drive.usercontent.google.com/download?id=$($row.file_id)&export=download&confirm=t"
    try {
        Write-Host "GET  $relativePath"
        & curl.exe -L --fail --retry 5 --retry-all-errors --max-time 600 -D $headersPath -o $temporary $url
        if ($LASTEXITCODE -ne 0 -or -not (Test-Path -LiteralPath $temporary)) {
            throw "Download failed: $relativePath"
        }
        if ((Get-Item -LiteralPath $temporary).Length -eq 0) {
            throw "Downloaded file is empty: $relativePath"
        }
        $stream = [IO.File]::OpenRead($temporary)
        try {
            $prefix = [byte[]]::new([Math]::Min(256, [int]$stream.Length))
            [void]$stream.Read($prefix, 0, $prefix.Length)
        } finally {
            $stream.Dispose()
        }
        $textPrefix = [Text.Encoding]::UTF8.GetString($prefix) -replace '^[\uFEFF\s]+', ''
        if ($textPrefix -match '^(?i:<!doctype html|<html)') {
            throw "Drive returned an HTML page instead of source data: $relativePath"
        }
        if ($relativePath.EndsWith('.xlsx', [StringComparison]::OrdinalIgnoreCase) -and
            -not ($prefix.Length -ge 4 -and $prefix[0] -eq 80 -and $prefix[1] -eq 75)) {
            throw "Downloaded XLSX has no ZIP signature: $relativePath"
        }
        $newHash = (Get-FileHash -LiteralPath $temporary -Algorithm SHA256).Hash.ToLowerInvariant()
        if ($apiMetadata -and $apiMetadata.md5Checksum) {
            $downloadMd5 = (Get-FileHash -LiteralPath $temporary -Algorithm MD5).Hash.ToLowerInvariant()
            if ($downloadMd5 -ne $apiMetadata.md5Checksum.ToLowerInvariant()) {
                throw "Drive MD5 checksum mismatch: $relativePath"
            }
        }
        $oldHash = $null
        if (Test-Path -LiteralPath $target) {
            $oldHash = (Get-FileHash -LiteralPath $target -Algorithm SHA256).Hash.ToLowerInvariant()
        }
        if ($oldHash -eq $newHash -and -not $Force) {
            Write-Host "SKIP $relativePath (SHA256 unchanged)"
        } else {
            if ($oldHash -and $oldHash -ne $newHash) {
                $archive = Join-Path $archiveRoot ("$nativeRelativePath/$oldHash")
                New-Item -ItemType Directory -Path (Split-Path -Parent $archive) -Force | Out-Null
                if (-not (Test-Path -LiteralPath $archive)) {
                    Copy-Item -LiteralPath $target -Destination $archive
                }
                Write-Host "KEEP $relativePath previous=$oldHash"
            }
            Move-Item -LiteralPath $temporary -Destination $target -Force
            Write-Host "SAVE $relativePath sha256=$newHash"
        }
        $headerLines = @(Get-Content -LiteralPath $headersPath)
        $etag = @($headerLines | ForEach-Object {
            if ($_ -match '^ETag:\s*(.*)$') { $Matches[1].Trim() }
        }) | Select-Object -Last 1
        $lastModified = @($headerLines | ForEach-Object {
            if ($_ -match '^Last-Modified:\s*(.*)$') { $Matches[1].Trim() }
        }) | Select-Object -Last 1
        $currentState[$relativePath] = [ordered]@{
            file_id = $row.file_id
            sha256 = $newHash
            drive_version = if ($apiMetadata) { $apiMetadata.version } else { $null }
            drive_revision = if ($apiMetadata) { $apiMetadata.headRevisionId } else { $null }
            drive_modified_time = if ($apiMetadata) { $apiMetadata.modifiedTime } else { $null }
            drive_md5 = if ($apiMetadata) { $apiMetadata.md5Checksum } else { $null }
            etag = $etag
            last_modified = $lastModified
        }
    } finally {
        Remove-Item -LiteralPath $temporary, $headersPath -Force -ErrorAction SilentlyContinue
    }
}

if ($Prune) {
    $stamp = (Get-Date).ToUniversalTime().ToString("yyyyMMddTHHmmssZ")
    Get-ChildItem -LiteralPath $rawRoot -Recurse -File | Where-Object { $_.Name -notin @(".gitkeep", ".drive-handoff.json", ".drive-handoff.in-progress") } | ForEach-Object {
        $localRelativePath = $_.FullName.Substring($rawRootFull.Length).Replace("\", "/")
        if (-not $manifestPaths.Contains($localRelativePath)) {
            $archived = Join-Path $archiveRoot ("pruned/$stamp/$localRelativePath")
            New-Item -ItemType Directory -Path (Split-Path -Parent $archived) -Force | Out-Null
            Move-Item -LiteralPath $_.FullName -Destination $archived
            Write-Host "ARCHIVE $localRelativePath"
        }
    }
}

$temporaryState = "$statePath.part"
$currentState | ConvertTo-Json -Depth 5 | Set-Content -LiteralPath $temporaryState -Encoding UTF8
Move-Item -LiteralPath $temporaryState -Destination $statePath -Force
Remove-Item -LiteralPath $inProgressPath -Force
Write-Host "Raw data is ready at $rawRoot"
