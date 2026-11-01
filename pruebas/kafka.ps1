# Pruebas de integracion - Arquitectura basada en eventos (Apache Kafka)
# Uso:  powershell -ExecutionPolicy Bypass -File pruebas\kafka.ps1 [-Casos todos | ENTORNO,CP-01,...] [-Pausar]
#       -Pausar detiene el script en cada momento en que conviene tomar una captura de pantalla.
param([string[]]$Casos = @('todos'), [switch]$Pausar)

. (Join-Path $PSScriptRoot 'comun.ps1')
$Global:Pausar = [bool]$Pausar
# Puerto de Kafka UI en el host (KAFKA_UI_PORT si el 8080 esta ocupado).
$Global:KafkaUiPort = if ($env:KAFKA_UI_PORT) { $env:KAFKA_UI_PORT } else { '8080' }
Push-Location (Join-Path $Global:Raiz 'kafka')

$Topico = 'orders.events'
$Grupos = @('inventory-service-group', 'billing-service-group', 'notification-service-group')
$Consumidores = @('inventory-consumer', 'billing-consumer', 'notification-consumer')

# ---------- utilidades propias de Kafka ----------

function KExec {
  $ErrorActionPreference = 'SilentlyContinue'
  @(& docker exec kafka @args 2>$null)
}

function Total-Eventos {
  $suma = 0
  KExec kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic $Topico | ForEach-Object {
    $v = ($_ -split ':')[-1]
    if ($v -match '^\d+$') { $suma += [int]$v }
  }
  return $suma
}

# Suma el LAG del grupo en todas las particiones (-1 si el grupo no existe).
function Lag-Grupo([string]$Grupo) {
  $suma = 0; $existe = $false
  foreach ($linea in (KExec kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group $Grupo)) {
    $c = @(($linea -split '\s+') | Where-Object { $_ })
    if ($c.Count -ge 6 -and $c[0] -eq $Grupo -and $c[1] -eq $Topico) {
      $existe = $true
      if ($c[5] -match '^\d+$') { $suma += [int]$c[5] }
      elseif ($c[3] -eq '-' -and $c[4] -match '^\d+$') { $suma += [int]$c[4] }
    }
  }
  if ($existe) { return $suma } else { return -1 }
}

function Miembros-Grupo([string]$Grupo) {
  @(KExec kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group $Grupo --members |
      Where-Object { $_ -match "^$Grupo\s" }).Count
}

function Mostrar-Grupo([string]$Grupo) {
  Run docker exec kafka kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group $Grupo
}

function Mostrar-Lags([string]$Momento) {
  $r = [ordered]@{}
  foreach ($g in $Grupos) { $r[$g] = Lag-Grupo $g }
  Write-Host ("[lag por grupo | {0}] " -f $Momento + (($r.Keys | ForEach-Object { "$_=$($r[$_])" }) -join '  ')) -ForegroundColor Magenta
  return $r
}

function Esperar-Lag([string[]]$Lista = $Grupos, [int]$Seg = 120) {
  $limite = (Get-Date).AddSeconds($Seg)
  do {
    $pend = @($Lista | Where-Object { (Lag-Grupo $_) -ne 0 })
    if ($pend.Count -eq 0) { return $true }
    Start-Sleep -Seconds 2
  } while ((Get-Date) -lt $limite)
  return $false
}

# Lee el log del topico directamente en el broker (kafka-console-consumer) y filtra.
function Leer-Eventos([string]$Patron) {
  Write-Host "PS> docker exec kafka kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic $Topico --from-beginning --property print.key=true --property print.partition=true --property print.offset=true --property print.headers=true | Select-String '$Patron'" -ForegroundColor Yellow
  @(KExec kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic $Topico --from-beginning --timeout-ms 7000 `
      --property print.key=true --property print.partition=true --property print.offset=true --property print.headers=true |
      Where-Object { $_ -match $Patron })
}

function Asegurar-Entorno([int]$Inventarios = 1) {
  & docker compose up -d --scale inventory-consumer=$Inventarios 2>&1 | Out-Null
  if (-not (Esperar-Api)) { Write-Host 'La API no esta disponible' -ForegroundColor Red }
  $limite = (Get-Date).AddSeconds(90)
  while (((Miembros-Grupo 'inventory-service-group') -lt $Inventarios) -and ((Get-Date) -lt $limite)) { Start-Sleep 2 }
  [void](Esperar-Lag)
}

$PedidoBase = @{ Producto = 'Libro de arquitectura'; Cantidad = 1; PrecioUnitario = 85000 }

# ---------- casos ----------

$Mapa = @{}

$Mapa['ENTORNO'] = {
  Titulo 'ENTORNO - Montaje y evidencias del entorno Kafka'
  Paso 'Construccion y arranque de los contenedores'
  Run docker compose up -d --build
  [void](Esperar-Api)
  [void](Esperar-Lag -Seg 60)
  Paso 'Contenedores (kafka-init termina con codigo 0 tras crear el topico)'
  Run docker compose ps -a
  Paso 'Puertos expuestos'
  Run docker ps --filter 'label=com.docker.compose.project=kafka' --format 'table {{.Names}}\t{{.Status}}\t{{.Ports}}'
  Paso "Kafka UI accesible (http://localhost:$($Global:KafkaUiPort))"
  Run curl.exe -s -o NUL -w 'HTTP %{http_code}\n' http://localhost:$($Global:KafkaUiPort)/
  Run curl.exe -s http://localhost:$($Global:KafkaUiPort)/api/clusters
  Write-Host ''
  Paso 'Broker activo: topicos y descripcion de orders.events'
  Run docker exec kafka kafka-topics.sh --bootstrap-server localhost:9092 --list
  Run docker exec kafka kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic $Topico
  Paso 'Consumer groups registrados'
  Run docker exec kafka kafka-consumer-groups.sh --bootstrap-server localhost:9092 --list
  Paso 'Logs de arranque - kafka (ultimas lineas)'
  Logs -Servicio kafka | Select-Object -Last 12 | ForEach-Object { Write-Host $_ }
  Paso 'Logs - kafka-init'
  Logs -Servicio kafka-init | ForEach-Object { Write-Host $_ }
  foreach ($s in @('orders-api') + $Consumidores) {
    Paso "Logs de arranque - $s"
    Logs -Servicio $s | Where-Object { $_ -notmatch '"level":"ERROR"' } | Select-Object -First 6 | ForEach-Object { Write-Host $_ }
  }
  Captura 'Esta terminal (docker compose ps -a) y Docker Desktop con kafka, kafka-ui, orders-api y los 3 consumidores'
  Captura "http://localhost:$($Global:KafkaUiPort) (Kafka UI): Brokers (1 broker activo), Topics > orders.events > Overview (3 particiones) y Consumers (3 grupos)"
  $ui = (& curl.exe -s -o NUL -w '%{http_code}' http://localhost:$($Global:KafkaUiPort)/)
  $grupos = @($Grupos | Where-Object { (Lag-Grupo $_) -ge 0 })
  Resultado 'ENTORNO' (($ui -eq '200') -and ($grupos.Count -eq 3)) "Kafka activo, Kafka UI HTTP $ui, topico $Topico con 3 particiones y $($grupos.Count) consumer groups."
}

$Mapa['CP-01'] = {
  Titulo 'CP-01 (Kafka) - Creacion de pedido via API'
  Asegurar-Entorno
  $t = Ahora
  Paso 'Pedido valido'
  $ok = Enviar-Pedido -Cliente 'Pedro' @PedidoBase
  Paso 'Pedido invalido (sin unitPrice, quantity negativa)'
  $inv = Enviar-Pedido -CuerpoCrudo '{"customerName":"Pedro","product":"Libro","quantity":-2}'
  Paso 'JSON mal formado'
  $mal = Enviar-Pedido -CuerpoCrudo '{"customerName": "Pedro", '
  Start-Sleep 1
  Paso 'Logs de orders-api'
  Mostrar-Logs -Servicio orders-api -Desde $t
  $cuerpo = $ok.Cuerpo | ConvertFrom-Json
  $cumple = ($ok.Status -eq 202) -and ($cuerpo.status -eq 'accepted') -and ($cuerpo.message -match 'OrderCreated') -and ($ok.OrderId -match '^ORD-') -and ($inv.Status -eq 400) -and ($mal.Status -eq 400)
  Resultado 'CP-01' $cumple "Valido: HTTP $($ok.Status) status=$($cuerpo.status) orderId=$($ok.OrderId) ($($ok.Ms) ms). Invalido: HTTP $($inv.Status). Mal formado: HTTP $($mal.Status)."
}

$Mapa['CP-04'] = {
  Titulo 'CP-04 (Kafka) - Publicacion del evento OrderCreated'
  Asegurar-Entorno
  Run docker exec kafka kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic $Topico
  $antes = Total-Eventos
  $t = Ahora
  $p = Enviar-Pedido -Cliente 'Ana' -Producto 'Teclado mecanico' -Cantidad 2 -PrecioUnitario 120000
  Start-Sleep 1
  Run docker exec kafka kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic $Topico
  $despues = Total-Eventos
  Write-Host "Eventos en el topico: antes=$antes despues=$despues"
  Paso 'Log de publicacion en orders-api (particion y offset asignados por el broker)'
  Mostrar-Logs -Servicio orders-api -Desde $t -Patron $p.OrderId
  Paso 'El evento almacenado en el broker'
  $ev = @(Leer-Eventos $p.OrderId)
  $ev | ForEach-Object { Write-Host $_ }
  $json = $null
  if ($ev.Count -ge 1) { $json = (($ev[0] -split "`t")[-1]) | ConvertFrom-Json }
  Paso 'Validacion del contrato del evento'
  $campos = @('eventId', 'eventType', 'eventVersion', 'occurredAt', 'source', 'correlationId', 'data')
  $faltan = @($campos | Where-Object { -not $json -or $null -eq $json.$_ })
  foreach ($c in $campos) { Write-Host ("    {0,-14} = {1}" -f $c, $(if ($json) { if ($c -eq 'data') { $json.data | ConvertTo-Json -Compress } else { $json.$c } })) }
  Captura "Kafka UI > Topics > orders.events > Messages: el evento con key $($p.OrderId) abierto (JSON con eventType=OrderCreated), y la pestana Overview con el conteo de mensajes"
  $cumple = ($p.Status -eq 202) -and ($despues -eq $antes + 1) -and ($json.eventType -eq 'OrderCreated') -and ($json.data.orderId -eq $p.OrderId) -and ($faltan.Count -eq 0) -and ($json.data.total -eq 240000)
  Resultado 'CP-04' $cumple "Evento OrderCreated de $($p.OrderId) almacenado en $Topico (eventos $antes -> $despues); campos obligatorios presentes; total=$($json.data.total)."
}

$Mapa['CP-05'] = {
  Titulo 'CP-05 (Kafka) - Consumo del evento por cada consumidor'
  Asegurar-Entorno
  $t = Ahora
  $p = Enviar-Pedido -Cliente 'Luis' -Producto 'Monitor 27' -Cantidad 1 -PrecioUnitario 950000
  $recibidos = 0
  foreach ($s in $Consumidores) {
    Paso "Logs de $s"
    $l = Esperar-Log -Servicio $s -Desde $t -Patron "Evento procesado.*$($p.OrderId)" -Seg 60
    Mostrar-Logs -Servicio $s -Desde $t -Patron $p.OrderId
    if ($l.Count -ge 1) { $recibidos++ }
  }
  Paso 'Offsets de cada consumer group tras el consumo'
  foreach ($g in $Grupos) { Mostrar-Grupo $g }
  $lags = Mostrar-Lags 'final'
  Captura 'Kafka UI > Consumers: inventory-service-group, billing-service-group y notification-service-group con Consumer Lag = 0'
  $eventIds = @($Consumidores | ForEach-Object { Logs -Servicio $_ -Desde $t -Patron "Evento recibido.*$($p.OrderId)" } | ForEach-Object { if ($_ -match 'eventId=(\S+)') { $Matches[1] } } | Select-Object -Unique)
  $cumple = ($recibidos -eq 3) -and ($eventIds.Count -eq 1) -and (@($lags.Values | Where-Object { $_ -ne 0 }).Count -eq 0)
  Resultado 'CP-05' $cumple "Los 3 consumidores ($recibidos/3) recibieron el mismo evento eventId=$($eventIds -join ',') de $($p.OrderId); lag 0 en los 3 grupos."
}

$Mapa['CP-06'] = {
  Titulo 'CP-06 (Kafka) - Trazabilidad del flujo (orderId / correlationId)'
  Asegurar-Entorno
  $corr = 'QA-KAFKA-CP06-' + (Get-Date -Format 'HHmmss')
  $t = Ahora
  $p = Enviar-Pedido -Cliente 'Marta' -Producto 'Silla ergonomica' -Cantidad 1 -PrecioUnitario 700000 -CorrelationId $corr
  Paso '1) API: logs de orders-api'
  Mostrar-Logs -Servicio orders-api -Desde $t -Patron $corr
  Paso '2) Broker: evento en el log del topico (headers, key, particion, offset)'
  $ev = @(Leer-Eventos $corr)
  $ev | ForEach-Object { Write-Host $_ }
  $ok = 0
  foreach ($s in $Consumidores) {
    Paso "3) Consumidor: $s"
    $l = Esperar-Log -Servicio $s -Desde $t -Patron "Evento procesado.*$($p.OrderId)" -Seg 60
    Mostrar-Logs -Servicio $s -Desde $t -Patron $p.OrderId
    if ((Logs -Servicio $s -Desde $t -Patron "Evento recibido.*$corr").Count -ge 1) { $ok++ }
  }
  $api = Logs -Servicio orders-api -Desde $t -Patron "$($p.OrderId).*$corr"
  $cumple = ($api.Count -ge 2) -and ($ev.Count -ge 1) -and ($ok -eq 3)
  Resultado 'CP-06' $cumple "orderId=$($p.OrderId) correlationId=$corr seguido en orders-api, en el log de Kafka (header y payload) y en los 3 consumidores ($ok/3)."
}

$Mapa['CP-07'] = {
  Titulo 'CP-07 (Kafka) - Responsive: la API responde sin esperar a los consumidores'
  Asegurar-Entorno
  $t = Ahora
  Paso 'Se envian 5 pedidos seguidos (inventario tarda 1500 ms, facturacion 1000 ms y notificacion 800 ms por evento)'
  $inicio = Get-Date
  $r = 1..5 | ForEach-Object { Enviar-Pedido -Cliente "Cliente$_" -Producto 'Mouse' -Cantidad $_ -PrecioUnitario 50000 }
  $fin = Get-Date
  $null = Mostrar-Lags 'justo despues de las 5 respuestas'
  [void](Esperar-Lag)
  $finCons = Get-Date
  Paso 'Con los 3 consumidores detenidos la API responde igual'
  Run docker compose stop inventory-consumer billing-consumer notification-consumer
  $r2 = 1..3 | ForEach-Object { Enviar-Pedido -Cliente "SinConsumidores$_" -Producto 'Mouse' -Cantidad 1 -PrecioUnitario 50000 }
  Run docker compose start inventory-consumer billing-consumer notification-consumer
  [void](Esperar-Lag)
  $todos = @($r) + @($r2)
  $max = ($todos | Measure-Object -Property Ms -Maximum).Maximum
  $prom = [math]::Round(($todos | Measure-Object -Property Ms -Average).Average, 1)
  Write-Host ''
  Write-Host ("Tiempo de respuesta HTTP: promedio={0} ms, maximo={1} ms" -f $prom, $max)
  Write-Host ("Las 5 respuestas llegaron en {0:N1} s; los consumidores tardaron {1:N1} s en ponerse al dia" -f ($fin - $inicio).TotalSeconds, ($finCons - $inicio).TotalSeconds)
  $cumple = (@($todos | Where-Object { $_.Status -ne 202 }).Count -eq 0) -and ($max -lt 800)
  Resultado 'CP-07' $cumple "8/8 respuestas 202; maximo $max ms (< 800 ms, el procesamiento mas corto). La API no espera a los consumidores."
}

$Mapa['CP-08'] = {
  Titulo 'CP-08 (Kafka) - Message-driven: no existe comunicacion directa entre servicios'
  Asegurar-Entorno
  Paso '1) Los consumidores no publican puertos'
  Run docker compose ps --format 'table {{.Service}}\t{{.Ports}}'
  Paso '2) Ningun consumidor escucha puertos TCP dentro de su contenedor'
  foreach ($s in $Consumidores) { Run docker compose exec $s netstat -tln }
  Paso '3) Desde orders-api no es posible abrir una conexion a los consumidores'
  $resp = @()
  foreach ($s in $Consumidores) {
    $js = "require('net').connect(3000,'$s').on('connect',()=>{console.log('$s CONECTADO');process.exit(0)}).on('error',e=>{console.log('$s sin conexion directa: '+e.code);process.exit(0)})"
    Write-Host "PS> docker compose exec orders-api node -e `"$js`"" -ForegroundColor Yellow
    $o = (& docker compose exec orders-api node -e $js 2>$null) -join ''
    Write-Host $o
    $resp += $o
  }
  Paso '4) La API solo conoce al broker (variables de entorno) y su codigo no invoca consumidores'
  Run docker compose exec orders-api printenv KAFKA_BROKER KAFKA_TOPIC
  Write-Host "PS> Select-String -Path orders-api\index.js -Pattern 'inventory|billing|notification|http.request|fetch\(|axios'" -ForegroundColor Yellow
  $hallazgos = Select-String -Path 'orders-api\index.js' -Pattern 'inventory-consumer|billing-consumer|notification-consumer|http\.request|fetch\(|axios'
  if ($hallazgos) { $hallazgos | ForEach-Object { Write-Host $_ } } else { Write-Host '    (sin coincidencias)' }
  Paso '5) Los consumidores se relacionan solo con el broker (consumer groups)'
  foreach ($g in $Grupos) { Mostrar-Grupo $g }
  $cumple = (-not $hallazgos) -and (@($resp | Where-Object { $_ -match 'CONECTADO' }).Count -eq 0)
  Resultado 'CP-08' $cumple "Ningun consumidor expone puertos ni acepta conexiones; orders-api solo conoce kafka:9092; la comunicacion pasa por el topico $Topico."
}

$Mapa['CP-09'] = {
  Titulo 'CP-09 (Kafka) - Resilient: el sistema soporta la caida de un consumidor'
  Asegurar-Entorno
  Paso 'Caida abrupta de billing-consumer (SIGKILL)'
  Run docker compose kill billing-consumer
  Run docker compose ps
  $t = Ahora
  Paso 'La API sigue aceptando pedidos y los otros consumidores siguen procesando'
  $r = 1..3 | ForEach-Object { Enviar-Pedido -Cliente "Resiliencia$_" -Producto 'Cable HDMI' -Cantidad 1 -PrecioUnitario 30000 }
  [void](Esperar-Lag -Lista @('inventory-service-group', 'notification-service-group') -Seg 60)
  $null = Mostrar-Lags 'con billing caido'
  Mostrar-Grupo 'billing-service-group'
  $lagCaido = Lag-Grupo 'billing-service-group'
  Captura 'Kafka UI > Consumers: billing-service-group sin miembros activos y lag=3, mientras inventory y notification tienen lag=0'
  $otros = 0
  foreach ($s in @('inventory-consumer', 'notification-consumer')) { $otros += (Logs -Servicio $s -Desde $t -Patron 'Evento procesado').Count }
  Write-Host "Eventos procesados por inventory + notification mientras billing estaba caido: $otros"
  Paso 'Se recupera billing-consumer y se pone al dia'
  $t2 = Ahora
  Run docker compose start billing-consumer
  [void](Esperar-Lag -Seg 90)
  $null = Mostrar-Lags 'final'
  Mostrar-Logs -Servicio billing-consumer -Desde $t2 -Patron 'Factura generada'
  $fact = (Logs -Servicio billing-consumer -Desde $t2 -Patron 'Factura generada').Count
  $cumple = (@($r | Where-Object { $_.Status -eq 202 }).Count -eq 3) -and ($otros -eq 6) -and ($lagCaido -eq 3) -and ($fact -ge 3)
  Resultado 'CP-09' $cumple "Con billing caido: API 3/3 aceptados, inventory+notification procesaron $otros/6, lag de billing=$lagCaido; al volver billing genero $fact facturas."
}

$Mapa['CP-10'] = {
  Titulo 'CP-10 (Kafka) - Elastic: varias instancias del mismo consumer group'
  Paso 'Escalado de inventory-consumer a 2 instancias (mismo grupo inventory-service-group)'
  $t0 = Ahora
  Run docker compose up -d --scale inventory-consumer=2
  Asegurar-Entorno -Inventarios 2
  Start-Sleep 3
  Mostrar-Logs -Servicio inventory-consumer -Desde $t0 -Patron 'Unido al consumer group'
  Run docker exec kafka kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group inventory-service-group --members --verbose
  Captura 'Kafka UI > Consumers > inventory-service-group: 2 miembros y la asignacion de particiones de cada uno'
  $t = Ahora
  $r = 1..12 | ForEach-Object { Enviar-Pedido -Cliente "Carga$_" -Producto 'Audifonos' -Cantidad 1 -PrecioUnitario 99000 -Silencioso }
  Write-Host "12 pedidos enviados: $((@($r | Where-Object { $_.Status -eq 202 })).Count) respuestas 202"
  [void](Esperar-Lag -Seg 120)
  Paso 'Eventos procesados por cada instancia'
  $proc = Logs -Servicio inventory-consumer -Desde $t -Patron 'Evento procesado'
  $proc | ForEach-Object { Write-Host $_ }
  $porInst = $proc | ForEach-Object { ($_ -split '\s+\|')[0].Trim() } | Group-Object | Select-Object Name, Count
  $porInst | Format-Table -AutoSize | Out-String | Write-Host
  Mostrar-Grupo 'inventory-service-group'
  Paso 'Se vuelve a 1 instancia'
  Run docker compose up -d --scale inventory-consumer=1
  $cumple = (@($porInst).Count -eq 2) -and ($proc.Count -eq 12)
  Resultado 'CP-10' $cumple ("Reparto por particiones: " + (($porInst | ForEach-Object { "$($_.Name)=$($_.Count)" }) -join ', ') + " (total $($proc.Count)/12, sin duplicados entre instancias)")
}

$Mapa['CP-11'] = {
  Titulo 'CP-11 (Kafka) - Desacoplamiento temporal y consumidor posterior'
  Asegurar-Entorno

  Paso 'A) Consumidor apagado mientras se publican eventos'
  Run docker compose stop notification-consumer
  $r = 1..3 | ForEach-Object { Enviar-Pedido -Cliente "Temporal$_" -Producto 'Libro de pruebas' -Cantidad 1 -PrecioUnitario 60000 }
  $ids = $r | ForEach-Object { $_.OrderId }
  [void](Esperar-Lag -Lista @('inventory-service-group', 'billing-service-group'))
  $lagApagado = Lag-Grupo 'notification-service-group'
  Mostrar-Grupo 'notification-service-group'
  Captura 'Kafka UI > Consumers > notification-service-group: lag=3 con el consumidor apagado'
  Start-Sleep 5
  $t = Ahora
  Run docker compose start notification-consumer
  [void](Esperar-Lag -Seg 90)
  Mostrar-Logs -Servicio notification-consumer -Desde $t -Patron 'Evento recibido'
  $procA = @($ids | Where-Object { (Logs -Servicio notification-consumer -Desde $t -Patron "Evento procesado.*$_").Count -ge 1 })
  Mostrar-Grupo 'notification-service-group'

  Paso 'B) Consumidor posterior: audit-consumer se crea DESPUES de publicar los eventos'
  & docker compose --profile late rm -sf audit-consumer 2>&1 | Out-Null
  [void](KExec kafka-consumer-groups.sh --bootstrap-server localhost:9092 --delete --group audit-service-group)
  Run docker exec kafka kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic $Topico
  $total = Total-Eventos
  Write-Host "Eventos existentes en el log antes de crear audit-consumer: $total"
  Run docker compose --profile late up -d audit-consumer
  $limite = (Get-Date).AddSeconds(90)
  do { Start-Sleep 2; $leidos = @(& docker logs kafka-audit-consumer-1 2>$null | Where-Object { $_ -match 'Evento leido del log' }) } while (($leidos.Count -lt $total) -and ((Get-Date) -lt $limite))
  Write-Host 'PS> docker logs kafka-audit-consumer-1 (primeras lineas)' -ForegroundColor Yellow
  & docker logs kafka-audit-consumer-1 2>$null | Where-Object { $_ -match 'Unido|Suscrito|leido' } | Select-Object -First 12 | ForEach-Object { Write-Host $_ }
  Write-Host "    ... ($($leidos.Count) eventos leidos en total)"
  Mostrar-Grupo 'audit-service-group'
  Captura 'Kafka UI > Consumers: aparece audit-service-group, nuevo, con lag=0 tras leer todo el log'
  $desdeCero = @($leidos | Where-Object { $_ -match 'offset=0(\s|$)' }).Count
  Run docker compose --profile late stop audit-consumer

  $cumple = ($lagApagado -eq 3) -and ($procA.Count -eq 3) -and ($leidos.Count -ge $total) -and ($desdeCero -ge 1)
  Resultado 'CP-11' $cumple "A) lag de notification con el consumidor apagado=$lagApagado; al iniciarlo proceso $($procA.Count)/3. B) audit-consumer, creado despues, leyo $($leidos.Count)/$total eventos desde el offset 0."
}

$Mapa['CP-12'] = {
  Titulo 'CP-12 (Kafka) - Recuperacion tras reiniciar consumidor y broker'
  Asegurar-Entorno

  Paso 'A) Caida abrupta de inventory-consumer con eventos sin confirmar'
  $t = Ahora
  $r = 1..6 | ForEach-Object { Enviar-Pedido -Cliente "Recuperacion$_" -Producto 'Disco SSD' -Cantidad 1 -PrecioUnitario 210000 }
  [void](Esperar-Log -Servicio inventory-consumer -Desde $t -Patron 'Evento procesado' -Cantidad 2)
  Run docker compose kill inventory-consumer
  $lagKill = Lag-Grupo 'inventory-service-group'
  Mostrar-Grupo 'inventory-service-group'
  Captura 'Kafka UI > Consumers > inventory-service-group: lag pendiente tras matar el consumidor'
  $t2 = Ahora
  Run docker compose start inventory-consumer
  [void](Esperar-Lag -Seg 120)
  Mostrar-Logs -Servicio inventory-consumer -Desde $t2 -Patron 'Unido|Evento procesado'
  $faltA = @($r | Where-Object { (Logs -Servicio inventory-consumer -Desde $t -Patron "Evento procesado.*$($_.OrderId)").Count -lt 1 })
  $dup = @($r | Where-Object { (Logs -Servicio inventory-consumer -Desde $t -Patron "Evento procesado.*$($_.OrderId)").Count -gt 1 })
  Write-Host "Eventos perdidos: $($faltA.Count). Eventos reprocesados (entrega al-menos-una-vez): $($dup.Count) -> $(($dup | ForEach-Object { $_.OrderId }) -join ', ')"

  Paso 'B) Reinicio del broker Kafka'
  $antes = Total-Eventos
  Write-Host "Eventos en el log antes del reinicio: $antes"
  $tB = Ahora
  Run docker compose restart kafka
  Paso 'Pedido enviado mientras el broker arranca'
  $durante = Enviar-Pedido -Cliente 'DuranteReinicio' -Producto 'Router' -Cantidad 1 -PrecioUnitario 180000
  $limite = (Get-Date).AddSeconds(120)
  do { Start-Sleep 3; $desp = Total-Eventos } while (($desp -lt $antes) -and ((Get-Date) -lt $limite))
  Write-Host "Eventos en el log despues del reinicio: $desp"
  Mostrar-Logs -Servicio orders-api -Desde $tB -Patron 'publicado|Error|desconectado|Conectado'
  Paso 'Nuevos pedidos tras el reinicio: los consumidores se reconectan y procesan'
  $r2 = 1..3 | ForEach-Object { Enviar-Pedido -Cliente "PostReinicio$_" -Producto 'Router' -Cantidad 1 -PrecioUnitario 180000 }
  [void](Esperar-Lag -Seg 150)
  $null = Mostrar-Lags 'final'
  $faltB = 0
  foreach ($p in $r2) { foreach ($s in $Consumidores) { if ((Esperar-Log -Servicio $s -Desde $tB -Patron "Evento procesado.*$($p.OrderId)" -Seg 30).Count -lt 1) { $faltB++ } } }
  foreach ($s in $Consumidores) { Mostrar-Logs -Servicio $s -Desde $tB -Patron 'Unido|Evento procesado' }
  $okB = ($desp -ge $antes) -and ($faltB -eq 0) -and (@($r2 | Where-Object { $_.Status -eq 202 }).Count -eq 3)

  Resultado 'CP-12' (($faltA.Count -eq 0) -and ($lagKill -gt 0) -and $okB) ("A) lag tras matar inventory=$lagKill; al reiniciar se procesaron todos (perdidos=$($faltA.Count), reprocesados=$($dup.Count)). " +
    "B) broker: eventos antes=$antes despues=$desp; pedido durante el reinicio HTTP $($durante.Status) en $($durante.Ms) ms; 3 eventos posteriores procesados por los 3 consumidores (faltantes=$faltB).")
}

$Orden = @('ENTORNO', 'CP-01', 'CP-04', 'CP-05', 'CP-06', 'CP-07', 'CP-08', 'CP-09', 'CP-10', 'CP-11', 'CP-12')
try { Ejecutar-Casos -Arquitectura 'kafka' -Casos $Casos -Mapa $Mapa -Orden $Orden }
finally { Pop-Location }
