# Isolated M4 comparison Grafana fixture for MVP3 verification.
# Reads the private Influx fixture credential artifact but never prints secrets.
param(
    [Parameter(Mandatory = $true)][string]$InfluxCredentials,
    [int]$Port = 3100
)
$ErrorActionPreference = 'Stop'

function New-LocalSecret {
    $bytes = New-Object byte[] 48
    $generator = [Security.Cryptography.RandomNumberGenerator]::Create()
    try { $generator.GetBytes($bytes) } finally { $generator.Dispose() }
    return [Convert]::ToBase64String($bytes)
}

function Invoke-LocalGrafana(
    [string]$Path,
    [string]$Method = 'GET',
    $Body = $null,
    [string]$Authorization = ''
) {
    $request = @{
        Uri = "$script:baseUrl$Path"
        Method = $Method
        TimeoutSec = 10
        MaximumRedirection = 0
        ErrorAction = 'Stop'
    }
    if ($Body) {
        $request.Body = $Body | ConvertTo-Json -Depth 100 -Compress
        $request.ContentType = 'application/json'
    }
    if ($Authorization) { $request.Headers = @{ Authorization = $Authorization } }
    try { return Invoke-RestMethod @request }
    catch { throw "Grafana request failed at $Method $Path (details withheld to protect credentials)." }
}

$credentialPath = (Resolve-Path -LiteralPath $InfluxCredentials).Path
$influx = Get-Content -LiteralPath $credentialPath -Raw | ConvertFrom-Json
if (-not $influx.read_token -or -not $influx.org -or -not $influx.bucket) {
    throw 'The Influx fixture credential artifact is incomplete.'
}

docker version --format '{{.Server.Version}}' | Out-Null
if ($LASTEXITCODE -ne 0) { throw 'Docker daemon is unavailable.' }
$existingContainers = @(docker ps -a --format '{{.Names}}')
if ($LASTEXITCODE -ne 0) { throw 'Docker resource inventory failed.' }
$listener = [Net.Sockets.TcpListener]::new([Net.IPAddress]::Loopback, $Port)
try { $listener.Start() } finally { $listener.Stop() }

$suffix = [Guid]::NewGuid().ToString('N').Substring(0, 8)
$container = "morelia-mvp3-grafana-$suffix"
if ($container -in $existingContainers) { throw 'Refusing to reuse an existing resource.' }
$secretDirectory = Join-Path ([IO.Path]::GetTempPath()) $container
$null = New-Item -ItemType Directory -Path $secretDirectory
$identity = [Security.Principal.WindowsIdentity]::GetCurrent().Name
icacls $secretDirectory /inheritance:r /grant:r "${identity}:(OI)(CI)F" | Out-Null
if ($LASTEXITCODE -ne 0) { throw 'Could not restrict the temporary Grafana directory.' }

$adminUser = 'morelia-mvp-admin'
$adminPassword = New-LocalSecret
$baseUrl = "http://127.0.0.1:$Port"
$fixturePath = Join-Path $secretDirectory 'grafana-fixture.json'
$fixture = @{
    base_url = $baseUrl
    container = $container
    image = 'grafana/grafana-oss:11.6.5'
    admin_user = $adminUser
    admin_password = $adminPassword
    dashboard_uid = 'morelia-m4-comparison'
    panel_id = 2
}
[IO.File]::WriteAllText($fixturePath, ($fixture | ConvertTo-Json -Depth 8))
Write-Output "Grafana fixture artifact: $fixturePath (values not displayed)"

docker run -d --name $container --label morelia.fixture=mvp3 `
    --publish "127.0.0.1:${Port}:3000" `
    --mount "type=volume,source=${container}-data,target=/var/lib/grafana" `
    --env "GF_SECURITY_ADMIN_USER=$adminUser" `
    --env "GF_SECURITY_ADMIN_PASSWORD=$adminPassword" `
    --env 'GF_SECURITY_ALLOW_EMBEDDING=true' `
    --env 'GF_AUTH_ANONYMOUS_ENABLED=true' `
    --env 'GF_AUTH_ANONYMOUS_ORG_ROLE=Viewer' `
    --env 'GF_DASHBOARDS_MIN_REFRESH_INTERVAL=500ms' `
    grafana/grafana-oss:11.6.5 | Out-Null
if ($LASTEXITCODE -ne 0) { throw 'Could not start isolated Grafana container; resources retained.' }

$healthy = $false
for ($attempt = 0; $attempt -lt 45; $attempt++) {
    try {
        $health = Invoke-LocalGrafana '/api/health'
        if ($health.database -eq 'ok') { $healthy = $true; break }
    } catch { }
    Start-Sleep -Seconds 1
}
if (-not $healthy) { throw 'Grafana readiness timed out; resources retained for diagnosis.' }

$basicBytes = [Text.Encoding]::UTF8.GetBytes("${adminUser}:${adminPassword}")
$authorization = 'Basic ' + [Convert]::ToBase64String($basicBytes)
$datasourceUrl = ([uri]$influx.url).Port
$null = Invoke-LocalGrafana '/api/datasources' 'POST' @{
    name = 'InfluxDB'
    uid = 'fepg8hwzyq9dsd'
    type = 'influxdb'
    access = 'proxy'
    url = "http://host.docker.internal:$datasourceUrl"
    isDefault = $true
    jsonData = @{
        version = 'Flux'
        organization = $influx.org
        defaultBucket = $influx.bucket
    }
    secureJsonData = @{ token = $influx.read_token }
} $authorization

$dashboardPath = Join-Path $PSScriptRoot '..\infra\grafana\dashboards\morelia_m4_comparison.json'
$dashboard = Get-Content -LiteralPath $dashboardPath -Raw | ConvertFrom-Json
$null = Invoke-LocalGrafana '/api/dashboards/db' 'POST' @{
    dashboard = $dashboard
    overwrite = $true
    folderId = 0
    message = 'Morelia MVP3 M4 comparison fixture'
} $authorization

$publicDashboard = Invoke-LocalGrafana '/api/dashboards/uid/morelia-m4-comparison'
if ($publicDashboard.dashboard.uid -ne 'morelia-m4-comparison') {
    throw 'Anonymous Viewer dashboard check failed.'
}

[pscustomobject]@{
    container = $container
    image = $fixture.image
    url = $baseUrl
    dashboard_uid = $fixture.dashboard_uid
    panel_id = $fixture.panel_id
    credential_path = $fixturePath
    influx_datasource = 'read-only token configured server-side'
    health = $health.database
    anonymous_viewer_check = 'passed'
} | ConvertTo-Json -Compress
