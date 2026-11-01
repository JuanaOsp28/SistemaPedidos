# Pruebas de integracion - Arquitectura basada en mensajes (RabbitMQ)
# Uso:  powershell -ExecutionPolicy Bypass -File pruebas\rabbitmq.ps1 [-Casos todos | ENTORNO,CP-01,...] [-Pausar]
#       -Pausar detiene el script en cada momento en que conviene tomar una captura de pantalla.
param([string[]]$Casos = @('todos'), [switch]$Pausar)

. (Join-Path $PSScriptRoot 'comun.ps1')
$Global:Pausar = [bool]$Pausar
Push-Location (Join-Path $Global:Raiz 'rabbitmq')

# ---------- utilidades propias de RabbitMQ ----------

function Estado-Cola {
  # Mientras el broker arranca rabbitmqctl falla: se ignora y se reporta la cola vacia.
  $ErrorActionPreference = 'SilentlyContinue'
  $json =(& docker exec rabbitmq rabbitmqctl -q list_queues --formatter json name messages_ready messages_unacknowledged messages consumers 2>$null) | Out-String
  $q = $null
  try { $q = ($json | ConvertFrom-Json) | Where-Object { $_.name -eq 'pedidos' } } catch { }
  if (-not $q) { return [pscustomobject]@{ Ready = 0; Unacked = 0; Total = 0; Consumidores = 0 } }
  [pscustomobject]@{ Ready = [int]$q.messages_ready; Unacked = [int]$q.messages_unacknowledged; Total = [int]$q.messages; Consumidores = [int]$q.consumers }
}

function Mostrar-Cola([string]$Momento) {
  $e = Estado-Cola
  Write-Host ("[cola pedidos | {0}] Ready={1} Unacked={2} Total={3} Consumidores={4}" -f $Momento, $e.Ready, $e.Unacked, $e.Total, $e.Consumidores) -ForegroundColor Magenta
  return $e
}

function Esperar-Cola([int]$Total = 0, [int]$Seg = 120) {
  $limite = (Get-Date).AddSeconds($Seg)
  do {
    if ((Estado-Cola).Total -eq $Total) { return $true }
    Start-Sleep -Seconds 1
  } while ((Get-Date) -lt $limite)
  return $false
}

# Lee mensajes de la cola via API de administracion SIN consumirlos (ack_requeue_true).
function Ver-Mensajes([int]$Cantidad = 10) {
  $tmp = [IO.Path]::GetTempFileName()
  [IO.File]::WriteAllText($tmp, "{`"count`":$Cantidad,`"ackmode`":`"ack_requeue_true`",`"encoding`":`"auto`",`"truncate`":50000}")
  Write-Host "PS> curl.exe -u admin:admin -X POST http://localhost:15672/api/queues/%2F/pedidos/get -d '{count:$Cantidad, ackmode:ack_requeue_true}'" -ForegroundColor Yellow
  $r = (& curl.exe -s -u admin:admin -H 'content-type: application/json' -X POST --data-binary "@$tmp" 'http://localhost:15672/api/queues/%2F/pedidos/get') | Out-String
  Remove-Item $tmp -ErrorAction SilentlyContinue
  try { return @($r | ConvertFrom-Json) } catch { Write-Host $r; return @() }
}

function Mostrar-Mensaje($m) {
  Write-Host ("    routing_key={0} redelivered={1} delivery_mode={2} (2=persistente) correlation_id={3} message_id={4} type={5}" -f `
      $m.routing_key, $m.redelivered, $m.properties.delivery_mode, $m.properties.correlation_id, $m.properties.message_id, $m.properties.type)
  Write-Host "    payload=$($m.payload)"
}

function Asegurar-Entorno([int]$Workers = 1) {
  & docker compose up -d --scale notification-worker=$Workers 2>&1 | Out-Null
  if (-not (Esperar-Api)) { Write-Host 'La API no esta disponible' -ForegroundColor Red }
  # Espera a que los workers esten conectados y la cola vacia antes de cada caso.
  $limite = (Get-Date).AddSeconds(60)
  while (((Estado-Cola).Consumidores -lt $Workers) -and ((Get-Date) -lt $limite)) { Start-Sleep 1 }
  [void](Esperar-Cola -Total 0 -Seg 120)
}

# ---------- casos ----------

$Mapa = @{}

$Mapa['ENTORNO'] = {
  Titulo 'ENTORNO - Montaje y evidencias del entorno RabbitMQ'
  Paso 'Construccion y arranque de los contenedores'
  Run docker compose up -d --build
  [void](Esperar-Api)
  Paso 'Contenedores en ejecucion'
  Run docker compose ps
  Paso 'Puertos expuestos'
  Run docker ps --filter 'name=rabbitmq' --filter 'name=orders-api' --format 'table {{.Names}}\t{{.Status}}\t{{.Ports}}'
  Paso 'Consola de RabbitMQ Management accesible (http://localhost:15672)'
  Run curl.exe -s -o NUL -w 'HTTP %{http_code}\n' http://localhost:15672/
  Run curl.exe -s -u admin:admin http://localhost:15672/api/overview -o NUL -w 'API de administracion: HTTP %{http_code}\n'
  Paso 'Cola pedidos declarada (durable)'
  Run docker exec rabbitmq rabbitmqctl list_queues name durable messages_ready messages_unacknowledged messages consumers
  Paso 'Conexiones AMQP abiertas en el broker'
  Run docker exec rabbitmq rabbitmqctl list_connections name user state
  Paso 'Logs de arranque - rabbitmq (ultimas lineas)'
  Logs -Servicio rabbitmq | Select-Object -Last 15 | ForEach-Object { Write-Host $_ }
  Paso 'Logs de arranque - orders-api'
  Logs -Servicio orders-api | Select-Object -First 15 | ForEach-Object { Write-Host $_ }
  Paso 'Logs de arranque - notification-worker'
  Logs -Servicio notification-worker | Select-Object -First 15 | ForEach-Object { Write-Host $_ }
  Captura 'Esta terminal (docker compose ps) y Docker Desktop con los 3 contenedores en ejecucion'
  Captura 'http://localhost:15672 (admin/admin): Overview, pestana Queues con "pedidos" (D = durable) y pestana Connections con orders-api y notification-worker'
  $e = Estado-Cola
  Resultado 'ENTORNO' ($e.Consumidores -ge 1) "rabbitmq, orders-api y notification-worker en ejecucion; consumidores en la cola: $($e.Consumidores)"
}

$Mapa['CP-01'] = {
  Titulo 'CP-01 (RabbitMQ) - Creacion de pedido via API'
  Asegurar-Entorno
  $t = Ahora
  Paso 'Pedido valido'
  $ok = Enviar-Pedido -Cliente 'Pedro' -Producto 'Libro de arquitectura' -Cantidad 1
  Paso 'Pedido invalido (quantity = 0, sin product)'
  $inv = Enviar-Pedido -CuerpoCrudo '{"customerName":"Pedro","quantity":0}'
  Paso 'JSON mal formado'
  $mal = Enviar-Pedido -CuerpoCrudo '{"customerName": "Pedro", '
  Start-Sleep 1
  Paso 'Logs de orders-api'
  Mostrar-Logs -Servicio orders-api -Desde $t
  $cuerpo = $ok.Cuerpo | ConvertFrom-Json
  $cumple = ($ok.Status -eq 202) -and ($cuerpo.status -eq 'accepted') -and ($ok.OrderId -match '^ORD-') -and ($inv.Status -eq 400) -and ($mal.Status -eq 400)
  Resultado 'CP-01' $cumple "Valido: HTTP $($ok.Status) status=$($cuerpo.status) orderId=$($ok.OrderId) ($($ok.Ms) ms). Invalido: HTTP $($inv.Status). Mal formado: HTTP $($mal.Status)."
}

$Mapa['CP-02'] = {
  Titulo 'CP-02 (RabbitMQ) - Envio de mensaje a RabbitMQ'
  Asegurar-Entorno
  Paso 'Se detiene el worker para que el mensaje quede visible en la cola'
  Run docker compose stop notification-worker
  $antes = Mostrar-Cola 'antes del POST'
  $t = Ahora
  $p = Enviar-Pedido -Cliente 'Ana' -Producto 'Teclado' -Cantidad 2
  Start-Sleep 1
  $despues = Mostrar-Cola 'despues del POST'
  Paso 'Mensaje visible en la cola (lectura sin consumir)'
  $msgs = Ver-Mensajes
  $m = $msgs | Where-Object { $_.payload -match $p.OrderId } | Select-Object -First 1
  if ($m) { Mostrar-Mensaje $m }
  Captura "RabbitMQ > Queues > pedidos: Ready=1 y, en 'Get messages' (Ack mode: Nack message requeue true), el payload con $($p.OrderId)"
  Paso 'Log de publicacion en orders-api'
  Mostrar-Logs -Servicio orders-api -Desde $t -Patron $p.OrderId
  Paso 'Se reinicia el worker (procesa el mensaje pendiente)'
  Run docker compose start notification-worker
  [void](Esperar-Cola -Total 0)
  $null = Mostrar-Cola 'tras reiniciar el worker'
  $cumple = ($p.Status -eq 202) -and ($despues.Ready -eq $antes.Ready + 1) -and ($null -ne $m) -and ($m.properties.delivery_mode -eq 2)
  Resultado 'CP-02' $cumple "Ready paso de $($antes.Ready) a $($despues.Ready); mensaje con orderId=$($p.OrderId) visible en la cola pedidos, persistente (delivery_mode=2)."
}

$Mapa['CP-03'] = {
  Titulo 'CP-03 (RabbitMQ) - Consumo de mensaje'
  Asegurar-Entorno
  $t = Ahora
  $p = Enviar-Pedido -Cliente 'Luis' -Producto 'Monitor' -Cantidad 1
  Start-Sleep -Milliseconds 700
  $durante = Mostrar-Cola 'durante el procesamiento'
  $l = Esperar-Log -Servicio notification-worker -Desde $t -Patron "confirmado \(ack\).*$($p.OrderId)"
  $fin = Mostrar-Cola 'despues del ack'
  Paso 'Logs del worker para el pedido'
  Mostrar-Logs -Servicio notification-worker -Desde $t -Patron $p.OrderId
  $cumple = ($l.Count -ge 1) -and ($durante.Unacked -eq 1) -and ($fin.Total -eq 0)
  Resultado 'CP-03' $cumple "El worker recibio, proceso y confirmo (ack) $($p.OrderId). Unacked durante el procesamiento=$($durante.Unacked); Total al final=$($fin.Total)."
}

$Mapa['CP-06'] = {
  Titulo 'CP-06 (RabbitMQ) - Trazabilidad del flujo (orderId / correlationId)'
  Asegurar-Entorno
  $corr = 'QA-RMQ-CP06-' + (Get-Date -Format 'HHmmss')
  Run docker compose stop notification-worker
  $t = Ahora
  $p = Enviar-Pedido -Cliente 'Marta' -Producto 'Silla ergonomica' -Cantidad 1 -CorrelationId $corr
  Paso '1) API: logs de orders-api'
  Mostrar-Logs -Servicio orders-api -Desde $t -Patron $corr
  Paso '2) Broker: mensaje almacenado en la cola pedidos'
  $m = Ver-Mensajes | Where-Object { $_.properties.correlation_id -eq $corr } | Select-Object -First 1
  if ($m) { Mostrar-Mensaje $m }
  Paso '3) Consumidor: se inicia el worker y se sigue el mismo pedido'
  Run docker compose start notification-worker
  $l = Esperar-Log -Servicio notification-worker -Desde $t -Patron "confirmado \(ack\).*$corr"
  Mostrar-Logs -Servicio notification-worker -Desde $t -Patron $p.OrderId
  $api = Logs -Servicio orders-api -Desde $t -Patron "$($p.OrderId).*$corr"
  $cumple = ($api.Count -ge 2) -and ($null -ne $m) -and ($m.payload -match $p.OrderId) -and ($l.Count -ge 1)
  Resultado 'CP-06' $cumple "orderId=$($p.OrderId) correlationId=$corr aparece en orders-api, en el mensaje de la cola (correlation_id) y en notification-worker."
}

$Mapa['CP-07'] = {
  Titulo 'CP-07 (RabbitMQ) - Responsive: la API responde sin esperar al consumidor'
  Asegurar-Entorno
  $t = Ahora
  Paso 'Se envian 5 pedidos seguidos con el worker activo (procesamiento simulado de 2 a 5 s por mensaje)'
  $inicio = Get-Date
  $r = 1..5 | ForEach-Object { Enviar-Pedido -Cliente "Cliente$_" -Producto 'Mouse' -Cantidad $_ }
  $fin = Get-Date
  $null = Mostrar-Cola 'justo despues de las 5 respuestas'
  [void](Esperar-Cola -Total 0 -Seg 60)
  $finWorker = Get-Date
  Paso 'Tiempos de procesamiento reportados por el worker'
  Mostrar-Logs -Servicio notification-worker -Desde $t -Patron 'duracionMs'
  Paso 'Con el worker detenido la API responde igual'
  Run docker compose stop notification-worker
  $r2 = 1..3 | ForEach-Object { Enviar-Pedido -Cliente "SinWorker$_" -Producto 'Mouse' -Cantidad 1 }
  Run docker compose start notification-worker
  [void](Esperar-Cola -Total 0 -Seg 60)
  $todos = @($r) + @($r2)
  $max = ($todos | Measure-Object -Property Ms -Maximum).Maximum
  $prom = [math]::Round(($todos | Measure-Object -Property Ms -Average).Average, 1)
  Write-Host ''
  Write-Host ("Tiempo de respuesta HTTP: promedio={0} ms, maximo={1} ms" -f $prom, $max)
  Write-Host ("Las 5 respuestas llegaron en {0:N1} s; el worker tardo {1:N1} s en vaciar la cola" -f ($fin - $inicio).TotalSeconds, ($finWorker - $inicio).TotalSeconds)
  $cumple = (@($todos | Where-Object { $_.Status -ne 202 }).Count -eq 0) -and ($max -lt 2000)
  Resultado 'CP-07' $cumple "8/8 respuestas 202; maximo $max ms (< 2000 ms, minimo del procesamiento del worker). La API no espera al consumidor."
}

$Mapa['CP-08'] = {
  Titulo 'CP-08 (RabbitMQ) - Message-driven: no existe comunicacion directa entre servicios'
  Asegurar-Entorno
  Paso '1) El worker no publica puertos'
  Run docker compose ps --format 'table {{.Service}}\t{{.Ports}}'
  Paso '2) El worker no escucha ningun puerto TCP dentro de su contenedor'
  Run docker compose exec notification-worker netstat -tln
  Paso '3) Desde orders-api no es posible abrir una conexion al worker'
  Run docker compose exec orders-api node -e "require('net').connect(3000,'notification-worker').on('connect',()=>{console.log('CONECTADO');process.exit(0)}).on('error',e=>{console.log('Sin conexion directa al worker: '+e.code);process.exit(0)})"
  Paso '4) El codigo de la API no contiene llamadas HTTP al worker'
  Write-Host "PS> Select-String -Path orders-api\index.js -Pattern 'notification-worker|http.request|fetch\(|axios'" -ForegroundColor Yellow
  $hallazgos = Select-String -Path 'orders-api\index.js' -Pattern 'notification-worker|http\.request|fetch\(|axios'
  if ($hallazgos) { $hallazgos | ForEach-Object { Write-Host $_ } } else { Write-Host '    (sin coincidencias)' }
  Paso '5) Ambos servicios solo se conectan al broker (conexiones AMQP en RabbitMQ)'
  $con = (& curl.exe -s -u admin:admin http://localhost:15672/api/connections) | Out-String | ConvertFrom-Json
  $con | ForEach-Object { Write-Host ("    {0,-40} peer={1}:{2} -> rabbitmq:{3} protocolo={4}" -f $_.user_provided_name, $_.peer_host, $_.peer_port, $_.port, $_.protocol) }
  $netstat = (& docker compose exec notification-worker netstat -tln 2>$null) -join "`n"
  $directo = (& docker compose exec orders-api node -e "require('net').connect(3000,'notification-worker').on('connect',()=>{console.log('CONECTADO');process.exit(0)}).on('error',e=>{console.log(e.code);process.exit(0)})" 2>$null) -join ''
  $cumple = (-not $hallazgos) -and ($netstat -notmatch ':3000|0\.0\.0\.0:\d+\s') -and ($directo -ne 'CONECTADO') -and (@($con).Count -ge 2)
  Resultado 'CP-08' $cumple "El worker no expone puertos ni acepta conexiones ($directo); la API no lo invoca; ambos solo tienen conexion AMQP con rabbitmq."
}

$Mapa['CP-09'] = {
  Titulo 'CP-09 (RabbitMQ) - Resilient: el sistema soporta la caida del consumidor'
  Asegurar-Entorno
  Paso 'Caida abrupta del worker (SIGKILL)'
  Run docker compose kill notification-worker
  Run docker compose ps
  $antes = Mostrar-Cola 'worker caido'
  Paso 'La API sigue aceptando pedidos'
  $r = 1..5 | ForEach-Object { Enviar-Pedido -Cliente "Resiliencia$_" -Producto 'Cable HDMI' -Cantidad 1 }
  Run curl.exe -s http://localhost:3000/health
  $despues = Mostrar-Cola 'tras 5 pedidos con el worker caido'
  Captura 'RabbitMQ > Queues: pedidos con Ready=5, Consumers=0 (worker caido) mientras la API sigue respondiendo 202'
  Paso 'Se recupera el worker y procesa lo acumulado'
  $t = Ahora
  Run docker compose start notification-worker
  [void](Esperar-Cola -Total 0 -Seg 60)
  $null = Mostrar-Cola 'final'
  $ack = Logs -Servicio notification-worker -Desde $t -Patron 'confirmado \(ack\)'
  $acept = @($r | Where-Object { $_.Status -eq 202 }).Count
  $cumple = ($acept -eq 5) -and ($despues.Ready -eq 5) -and ($despues.Consumidores -eq 0) -and ($ack.Count -ge 5)
  Resultado 'CP-09' $cumple "Con el worker caido la API acepto $acept/5 pedidos y la cola retuvo Ready=$($despues.Ready) sin consumidores; al volver se procesaron $($ack.Count)."
}

$Mapa['CP-10'] = {
  Titulo 'CP-10 (RabbitMQ) - Elastic: distribucion de carga con multiples consumidores'
  Paso 'Escalado a 2 workers'
  Run docker compose up -d --scale notification-worker=2
  Asegurar-Entorno -Workers 2
  Run docker compose ps notification-worker
  $null = Mostrar-Cola 'con 2 consumidores'
  $t = Ahora
  $r = 1..10 | ForEach-Object { Enviar-Pedido -Cliente "Carga$_" -Producto 'Audifonos' -Cantidad 1 -Silencioso }
  Write-Host "10 pedidos enviados: $((@($r | Where-Object { $_.Status -eq 202 })).Count) respuestas 202"
  Start-Sleep 1
  $null = Mostrar-Cola 'inmediatamente despues de publicar'
  Captura 'RAPIDO (los workers siguen procesando): RabbitMQ > Queues > pedidos con Consumers=2 y Unacked=2 (un mensaje en curso por worker)'
  [void](Esperar-Cola -Total 0 -Seg 90)
  Paso 'Mensajes confirmados por cada worker'
  $ack = Logs -Servicio notification-worker -Desde $t -Patron 'confirmado \(ack\)'
  $ack | ForEach-Object { Write-Host $_ }
  $porWorker = $ack | ForEach-Object { ($_ -split '\s+\|')[0].Trim() } | Group-Object | Select-Object Name, Count
  Write-Host ''
  $porWorker | Format-Table -AutoSize | Out-String | Write-Host
  Paso 'Se vuelve a 1 worker'
  Run docker compose up -d --scale notification-worker=1
  $cumple = (@($porWorker).Count -eq 2) -and (@($porWorker | Where-Object { $_.Count -gt 0 }).Count -eq 2) -and ($ack.Count -eq 10)
  Resultado 'CP-10' $cumple ("Reparto: " + (($porWorker | ForEach-Object { "$($_.Name)=$($_.Count)" }) -join ', '))
}

$Mapa['CP-11'] = {
  Titulo 'CP-11 (RabbitMQ) - Desacoplamiento temporal'
  Asegurar-Entorno
  Run docker compose stop notification-worker
  $tPub = Get-Date
  $r = 1..4 | ForEach-Object { Enviar-Pedido -Cliente "Temporal$_" -Producto 'Libro de pruebas' -Cantidad 1 }
  $ids = $r | ForEach-Object { $_.OrderId }
  $acum = Mostrar-Cola 'consumidor apagado'
  Captura 'RabbitMQ > Queues > pedidos: Ready=4 acumulados con el consumidor apagado'
  Paso 'Se espera 10 s con el consumidor apagado: los mensajes permanecen en la cola'
  Start-Sleep 10
  $null = Mostrar-Cola '10 s despues'
  $t = Ahora
  Paso 'Se inicia el consumidor'
  Run docker compose start notification-worker
  [void](Esperar-Cola -Total 0 -Seg 60)
  $null = Mostrar-Cola 'final'
  Mostrar-Logs -Servicio notification-worker -Desde $t -Patron 'confirmado \(ack\)'
  $procesados = @($ids | Where-Object { (Logs -Servicio notification-worker -Desde $t -Patron "confirmado \(ack\).*$_").Count -ge 1 })
  $cumple = ($acum.Ready -eq 4) -and ($procesados.Count -eq 4)
  Resultado 'CP-11' $cumple "4 mensajes publicados con el consumidor apagado (Ready=$($acum.Ready)); al iniciarlo se procesaron $($procesados.Count)/4: $($ids -join ', ')."
}

$Mapa['CP-12'] = {
  Titulo 'CP-12 (RabbitMQ) - Recuperacion tras reiniciar consumidor y broker'
  Asegurar-Entorno

  Paso 'A) Caida del worker con un mensaje sin confirmar (Unacked)'
  $t = Ahora
  $r = 1..3 | ForEach-Object { Enviar-Pedido -Cliente "Recuperacion$_" -Producto 'Disco SSD' -Cantidad 1 }
  [void](Esperar-Log -Servicio notification-worker -Desde $t -Patron 'Simulando envio')
  $enProceso = Mostrar-Cola 'worker procesando'
  Run docker compose kill notification-worker
  Start-Sleep 2
  $trasKill = Mostrar-Cola 'worker eliminado con SIGKILL'
  $t2 = Ahora
  Run docker compose start notification-worker
  [void](Esperar-Cola -Total 0 -Seg 60)
  $null = Mostrar-Cola 'worker reiniciado'
  Mostrar-Logs -Servicio notification-worker -Desde $t2 -Patron 'Mensaje recibido|confirmado'
  $redeliv = Logs -Servicio notification-worker -Desde $t2 -Patron 'redelivered=true'
  $okA = ($enProceso.Unacked -eq 1) -and ($trasKill.Unacked -eq 0) -and ($trasKill.Ready -ge 2) -and ($redeliv.Count -ge 1)
  $faltA = @($r | Where-Object { (Logs -Servicio notification-worker -Desde $t -Patron "confirmado \(ack\).*$($_.OrderId)").Count -lt 1 })

  Paso 'B) Reinicio del broker con mensajes pendientes (cola durable + mensajes persistentes)'
  Run docker compose stop notification-worker
  $r2 = 1..3 | ForEach-Object { Enviar-Pedido -Cliente "Broker$_" -Producto 'Memoria RAM' -Cantidad 1 }
  $antes = Mostrar-Cola 'antes de reiniciar rabbitmq'
  $tB = Ahora
  Run docker compose restart rabbitmq
  $limite = (Get-Date).AddSeconds(90)
  do { Start-Sleep 2; $desp = Estado-Cola } while (($desp.Ready -eq 0) -and ((Get-Date) -lt $limite))
  $desp = Mostrar-Cola 'despues de reiniciar rabbitmq'
  Captura 'RabbitMQ (recargar la consola tras el reinicio): pedidos conserva Ready=3 despues de reiniciar el broker'
  Paso 'La API se reconecta sola al broker'
  [void](Esperar-Api 60)
  Mostrar-Logs -Servicio orders-api -Desde $tB -Patron 'Conectado|cerrada|no disponible'
  $t3 = Ahora
  Run docker compose start notification-worker
  [void](Esperar-Cola -Total 0 -Seg 60)
  $null = Mostrar-Cola 'final'
  Mostrar-Logs -Servicio notification-worker -Desde $t3 -Patron 'confirmado'
  $faltB = @($r2 | Where-Object { (Logs -Servicio notification-worker -Desde $t3 -Patron "confirmado \(ack\).*$($_.OrderId)").Count -lt 1 })
  $okB = ($antes.Ready -eq 3) -and ($desp.Ready -eq 3) -and ($faltB.Count -eq 0)

  Resultado 'CP-12' ($okA -and $okB -and $faltA.Count -eq 0) ("A) worker: Unacked $($enProceso.Unacked)->0, mensaje reentregado (redelivered=true), perdidos=$($faltA.Count). " +
    "B) broker: Ready $($antes.Ready) antes y $($desp.Ready) despues del reinicio, perdidos=$($faltB.Count).")
}

$Orden = @('ENTORNO', 'CP-01', 'CP-02', 'CP-03', 'CP-06', 'CP-07', 'CP-08', 'CP-09', 'CP-10', 'CP-11', 'CP-12')
try { Ejecutar-Casos -Arquitectura 'rabbitmq' -Casos $Casos -Mapa $Mapa -Orden $Orden }
finally { Pop-Location }
