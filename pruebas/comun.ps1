# Funciones compartidas por las pruebas de integracion (Windows PowerShell 5.1+).
# Cada caso deja su evidencia en evidencias\<arquitectura>\<caso>.log

$ErrorActionPreference = 'Continue'
$Global:Raiz = Split-Path -Parent $PSScriptRoot
$Global:ApiUrl = 'http://localhost:3000/orders'
$Global:HealthUrl = 'http://localhost:3000/health'

function Titulo([string]$Texto) {
  Write-Host ''
  Write-Host ('=' * 90)
  Write-Host $Texto
  Write-Host ('=' * 90)
  Write-Host ('Fecha/hora de ejecucion: ' + (Get-Date -Format 'yyyy-MM-dd HH:mm:ss zzz'))
}

function Paso([string]$Texto) {
  Write-Host ''
  Write-Host ">> $Texto" -ForegroundColor Cyan
}

function Nota([string]$Texto) {
  Write-Host "   $Texto" -ForegroundColor DarkGray
}

# Momento indicado para tomar una captura de pantalla. Con -Pausar el script se detiene.
function Captura([string]$Que) {
  Write-Host ''
  Write-Host "[CAPTURA] $Que" -ForegroundColor Green
  if ($Global:Pausar) { [void](Read-Host '          Toma la captura y presiona Enter para continuar') }
  # Captura automatica: con $env:CAPTURA_DIR el script deja una solicitud NN.req y espera NN.ok.
  if ($env:CAPTURA_DIR) {
    $Global:NumCaptura++
    $id = '{0:D2}' -f $Global:NumCaptura
    [IO.File]::WriteAllText((Join-Path $env:CAPTURA_DIR "$id.req"), $Que)
    $ok = Join-Path $env:CAPTURA_DIR "$id.ok"
    $limite = (Get-Date).AddSeconds(240)
    while (-not (Test-Path $ok) -and ((Get-Date) -lt $limite)) { Start-Sleep -Milliseconds 300 }
  }
}
$Global:NumCaptura = 0

# Ejecuta un comando externo mostrando la linea exacta y su salida (queda en la evidencia).
function Run {
  $exe = $args[0]
  $resto = @($args | Select-Object -Skip 1)
  Write-Host ("PS> " + ($args -join ' ')) -ForegroundColor Yellow
  & $exe @resto 2>&1 | ForEach-Object { Write-Host "$_" }
}

# Marca de tiempo UTC para filtrar logs con "docker compose logs --since".
function Ahora {
  (Get-Date).ToUniversalTime().AddSeconds(-1).ToString('yyyy-MM-ddTHH:mm:ssZ')
}

function Logs {
  param([string]$Servicio, [string]$Desde, [string]$Patron)
  $ErrorActionPreference = 'SilentlyContinue'
  $a = @('compose', 'logs', '--no-color')
  if ($Desde) { $a += @('--since', $Desde) }
  if ($Servicio) { $a += $Servicio }
  $lineas = & docker @a 2>$null
  if ($Patron) { $lineas = $lineas | Where-Object { $_ -match $Patron } }
  @($lineas)
}

function Mostrar-Logs {
  param([string]$Servicio, [string]$Desde, [string]$Patron)
  $txt = "docker compose logs --since $Desde $Servicio"
  if ($Patron) { $txt += " | Select-String '$Patron'" }
  Write-Host "PS> $txt" -ForegroundColor Yellow
  Logs -Servicio $Servicio -Desde $Desde -Patron $Patron | ForEach-Object { Write-Host $_ }
}

# Espera hasta que aparezcan $Cantidad lineas de log que cumplan el patron.
function Esperar-Log {
  param([string]$Servicio, [string]$Patron, [string]$Desde, [int]$Cantidad = 1, [int]$Seg = 90)
  $limite = (Get-Date).AddSeconds($Seg)
  do {
    $l = Logs -Servicio $Servicio -Desde $Desde -Patron $Patron
    if ($l.Count -ge $Cantidad) { return $l }
    Start-Sleep -Seconds 1
  } while ((Get-Date) -lt $limite)
  return $l
}

function Esperar-Api([int]$Seg = 120) {
  $limite = (Get-Date).AddSeconds($Seg)
  do {
    try {
      $r = Invoke-WebRequest -Uri $Global:HealthUrl -UseBasicParsing -TimeoutSec 3
      if ($r.StatusCode -eq 200) { return $true }
    } catch { }
    Start-Sleep -Seconds 2
  } while ((Get-Date) -lt $limite)
  return $false
}

# POST /orders con curl.exe: devuelve codigo HTTP, tiempo de respuesta y cuerpo.
function Enviar-Pedido {
  param(
    [string]$Cliente = 'Pedro',
    [string]$Producto = 'Libro de arquitectura',
    [int]$Cantidad = 1,
    $PrecioUnitario = $null,
    [string]$CorrelationId,
    [string]$CuerpoCrudo,
    [switch]$Silencioso
  )
  if ($CuerpoCrudo) {
    $json = $CuerpoCrudo
  } else {
    $body = [ordered]@{ customerName = $Cliente; product = $Producto; quantity = $Cantidad }
    if ($null -ne $PrecioUnitario) { $body.unitPrice = $PrecioUnitario }
    $json = $body | ConvertTo-Json -Compress
  }
  $tmp = [IO.Path]::GetTempFileName()
  [IO.File]::WriteAllText($tmp, $json, (New-Object Text.UTF8Encoding $false))
  $a = @('-s', '-w', "`n%{http_code} %{time_total}", '-X', 'POST', $Global:ApiUrl, '-H', 'Content-Type: application/json', '--data-binary', "@$tmp")
  if ($CorrelationId) { $a += @('-H', "X-Correlation-Id: $CorrelationId") }
  $salida = @(& curl.exe @a)
  Remove-Item $tmp -ErrorAction SilentlyContinue
  $ultima = $salida[-1] -split ' '
  $cuerpo = ($salida | Select-Object -SkipLast 1) -join "`n"
  $obj = $null
  try { $obj = $cuerpo | ConvertFrom-Json } catch { }
  $r = [pscustomobject]@{
    Status        = [int]$ultima[0]
    Ms            = [math]::Round([double]::Parse($ultima[1], [Globalization.CultureInfo]::InvariantCulture) * 1000, 1)
    OrderId       = $obj.orderId
    CorrelationId = $obj.correlationId
    Cuerpo        = $cuerpo
    Peticion      = $json
  }
  if (-not $Silencioso) {
    Write-Host "PS> curl.exe -X POST $($Global:ApiUrl) -H 'Content-Type: application/json' -d '$json'$(if ($CorrelationId) { " -H 'X-Correlation-Id: $CorrelationId'" })" -ForegroundColor Yellow
    Write-Host "    HTTP $($r.Status) en $($r.Ms) ms -> $($r.Cuerpo)"
  }
  return $r
}

$Global:Resultados = @()

function Resultado {
  param([string]$Caso, [bool]$Cumple, [string]$Detalle)
  $estado = if ($Cumple) { 'CUMPLE' } else { 'NO CUMPLE' }
  $color = if ($Cumple) { 'Green' } else { 'Red' }
  Write-Host ''
  Write-Host "ESTADO $Caso : $estado" -ForegroundColor $color
  Write-Host "Detalle: $Detalle"
  $Global:Resultados += [pscustomobject]@{ Caso = $Caso; Estado = $estado; Detalle = $Detalle; Hora = (Get-Date -Format 'HH:mm:ss') }
}

function Ejecutar-Casos {
  param([string]$Arquitectura, [string[]]$Casos, [hashtable]$Mapa, [string[]]$Orden)
  $dirEvid = Join-Path $Global:Raiz "evidencias\$Arquitectura"
  New-Item -ItemType Directory -Force -Path $dirEvid | Out-Null
  if ($Casos -contains 'todos') { $Casos = $Orden }
  foreach ($c in $Casos) {
    if (-not $Mapa.ContainsKey($c)) { Write-Host "Caso desconocido: $c (disponibles: $($Orden -join ', '))" -ForegroundColor Red; continue }
    $archivo = Join-Path $dirEvid "$c.log"
    Start-Transcript -Path $archivo -Force | Out-Null
    try { & $Mapa[$c] } catch { Write-Host "ERROR ejecutando ${c}: $_" -ForegroundColor Red; Resultado $c $false "Error de ejecucion: $_" }
    finally { Stop-Transcript | Out-Null }
    Write-Host "Evidencia guardada en $archivo" -ForegroundColor DarkGray
  }
  $resumen = Join-Path $dirEvid 'resumen.txt'
  Add-Content -Path $resumen -Encoding UTF8 -Value ("Ejecucion " + (Get-Date -Format 'yyyy-MM-dd HH:mm:ss') + " - casos: " + ($Casos -join ', '))
  $Global:Resultados | Format-Table -AutoSize | Out-String -Width 200 | Add-Content -Path $resumen -Encoding UTF8
  Write-Host ''
  Write-Host "RESUMEN ($Arquitectura)"
  $Global:Resultados | Format-Table -AutoSize | Out-String -Width 200 | Write-Host
}
