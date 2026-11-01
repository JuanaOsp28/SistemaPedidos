# Sistema de Pedidos: pruebas de integración con RabbitMQ y Kafka

Taller de Aseguramiento de la Calidad (ITM). Contiene dos arquitecturas reactivas e independientes para el mismo caso de estudio (procesamiento de pedidos) y las pruebas de integración automatizadas CP-01 a CP-12.

```
SistemaPedidos/
├── rabbitmq/                    Arquitectura basada en mensajes (Anexo 1)
│   ├── docker-compose.yml       rabbitmq + orders-api + notification-worker
│   ├── orders-api/              API REST productora (Node.js + Express + amqplib)
│   └── notification-worker/     Consumidor de la cola "pedidos" (ack manual, prefetch 1)
├── kafka/                       Arquitectura basada en eventos (Anexo 2)
│   ├── docker-compose.yml       kafka + kafka-init + kafka-ui + orders-api + 3 consumidores (+ audit)
│   ├── orders-api/              API REST productora del evento OrderCreated (kafkajs)
│   ├── inventory-consumer/      grupo inventory-service-group
│   ├── billing-consumer/        grupo billing-service-group
│   ├── notification-consumer/   grupo notification-service-group
│   └── audit-consumer/          consumidor "posterior" (perfil late), grupo audit-service-group
├── pruebas/                     Scripts PowerShell que ejecutan y evidencian los casos
│   ├── comun.ps1
│   ├── rabbitmq.ps1             ENTORNO, CP-01, 02, 03, 06, 07, 08, 09, 10, 11, 12
│   └── kafka.ps1                ENTORNO, CP-01, 04, 05, 06, 07, 08, 09, 10, 11, 12
├── postman/                     Colección Postman (CP-01 en ambas arquitecturas)
├── evidencias/                  Salida de cada caso (.log) + resumen.txt
└── INFORME.md                   Informe de pruebas (base para el PDF)
```

## Requisitos

- Docker Desktop con Docker Compose v2.
- Windows PowerShell 5.1 o superior (para los scripts de prueba). `curl.exe` viene incluido en Windows 10/11.
- No hace falta tener Node.js instalado: todo corre en contenedores.

> Las dos arquitecturas publican la API en el puerto **3000**, así que solo puede estar levantada una a la vez. Antes de cambiar de arquitectura, ejecuta `docker compose down` en la carpeta de la otra.

## Levantar cada arquitectura

### RabbitMQ

```powershell
cd rabbitmq
docker compose up -d --build
docker compose ps
```

| Servicio | URL / puerto |
|---|---|
| API | `POST http://localhost:3000/orders` |
| RabbitMQ Management | http://localhost:15672 (admin / admin) |
| AMQP | `localhost:5672` (dentro de Docker: `amqp://admin:admin@rabbitmq:5672`) |

Escalado: `docker compose up -d --scale notification-worker=2`

### Kafka

```powershell
cd kafka
docker compose up -d --build
docker compose ps -a
```

| Servicio | URL / puerto |
|---|---|
| API | `POST http://localhost:3000/orders` |
| Kafka UI | http://localhost:8080 (si el puerto está ocupado: `$env:KAFKA_UI_PORT='8081'` antes de `docker compose up` y de `pruebas\kafka.ps1`) |
| Kafka | `kafka:9092` (interno), `localhost:29092` (desde el anfitrión) |

- `kafka-init` crea el tópico `orders.events` con 3 particiones y termina con código 0.
- Escalado de un consumidor: `docker compose up -d --scale inventory-consumer=2`
- Consumidor posterior: `docker compose --profile late up -d audit-consumer`

## Ejecutar las pruebas

Desde la raíz del proyecto:

```powershell
# Todos los casos de una arquitectura
powershell -ExecutionPolicy Bypass -File pruebas\rabbitmq.ps1
powershell -ExecutionPolicy Bypass -File pruebas\kafka.ps1

# Casos puntuales
powershell -ExecutionPolicy Bypass -File pruebas\kafka.ps1 -Casos CP-04,CP-05

# Pausar en cada momento en que conviene tomar una captura de la consola web
powershell -ExecutionPolicy Bypass -File pruebas\rabbitmq.ps1 -Pausar
```

Cada caso guarda su salida completa en `evidencias\<arquitectura>\<caso>.log`: los comandos ejecutados, las respuestas HTTP, el estado de la cola o de los consumer groups y los logs filtrados de cada contenedor. Al final aparece `ESTADO: CUMPLE / NO CUMPLE`. En `evidencias\<arquitectura>\resumen.txt` queda la tabla de cada ejecución.

Los scripts dejan el sistema en estado limpio después de cada caso: reinician los consumidores detenidos y vuelven a una sola instancia.

## Decisiones de implementación relevantes para QA

- **Respuesta 202 Accepted** inmediata. La API solo espera la confirmación del broker (publisher confirms en RabbitMQ, `acks=all` en Kafka), nunca al consumidor.
- **Trazabilidad**: la API acepta el header `X-Correlation-Id` (o genera uno). Ese valor viaja en las propiedades AMQP (`correlation_id`), en los headers Kafka y en el cuerpo del evento, y aparece en todos los logs junto al `orderId`.
- **Reintentos de conexión** en la API y en los consumidores (`depends_on` no garantiza que el broker esté listo). Además hay healthchecks en `rabbitmq` y `kafka`.
- **Durabilidad**: cola `pedidos` durable, mensajes persistentes (`delivery_mode=2`) y volúmenes Docker para los datos de RabbitMQ y del log de Kafka.
- **Consumidores sin puertos**: ningún worker o consumidor declara `ports` ni levanta un servidor HTTP.
