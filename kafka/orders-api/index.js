// orders-api (Kafka): recibe pedidos por HTTP y publica el evento de dominio
// OrderCreated en el topico orders.events. No conoce a ningun consumidor.
const express = require('express');
const { Kafka, Partitioners, logLevel } = require('kafkajs');
const { randomUUID } = require('crypto');

const PORT = Number(process.env.PORT || 3000);
const BROKER = process.env.KAFKA_BROKER || 'kafka:9092';
const TOPIC = process.env.KAFKA_TOPIC || 'orders.events';
const RETRY_MS = 3000;

function log(text, fields = {}) {
  const extra = Object.entries(fields).map(([k, v]) => `${k}=${v}`).join(' ');
  console.log(`${new Date().toISOString()} [orders-api] ${text}${extra ? ' | ' + extra : ''}`);
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

const kafka = new Kafka({ clientId: 'orders-api', brokers: [BROKER], logLevel: logLevel.WARN });
const producer = kafka.producer({ createPartitioner: Partitioners.DefaultPartitioner });
let connected = false;

producer.on(producer.events.DISCONNECT, () => {
  connected = false;
  log('Productor desconectado de Kafka');
});

// depends_on no garantiza que Kafka acepte conexiones: se reintenta hasta lograrlo.
async function connect() {
  for (let attempt = 1; ; attempt++) {
    try {
      await producer.connect();
      connected = true;
      log('Conectado a Kafka', { broker: BROKER, topico: TOPIC });
      return;
    } catch (err) {
      log(`Kafka no disponible (intento ${attempt})`, { error: err.message });
      await sleep(RETRY_MS);
    }
  }
}

function validate(body) {
  const errors = [];
  if (!body || typeof body !== 'object') return ['El cuerpo debe ser un JSON'];
  if (typeof body.customerName !== 'string' || !body.customerName.trim()) errors.push('customerName es obligatorio');
  if (typeof body.product !== 'string' || !body.product.trim()) errors.push('product es obligatorio');
  if (!Number.isInteger(body.quantity) || body.quantity <= 0) errors.push('quantity debe ser un entero mayor que 0');
  if (typeof body.unitPrice !== 'number' || body.unitPrice < 0) errors.push('unitPrice debe ser un numero mayor o igual a 0');
  return errors;
}

const app = express();
app.use(express.json());

app.post('/orders', async (req, res) => {
  const started = process.hrtime.bigint();
  const correlationId = req.get('X-Correlation-Id') || randomUUID();
  res.set('X-Correlation-Id', correlationId);

  const errors = validate(req.body);
  if (errors.length) {
    log('Pedido rechazado por validacion', { correlationId, errores: errors.join('; ') });
    return res.status(400).json({ status: 'rejected', errors, correlationId });
  }

  const orderId = 'ORD-' + randomUUID().split('-')[0].toUpperCase();
  const { customerName, product, quantity, unitPrice } = req.body;
  const event = {
    eventId: randomUUID(),
    eventType: 'OrderCreated',
    eventVersion: '1.0',
    occurredAt: new Date().toISOString(),
    source: 'orders-api',
    correlationId,
    data: {
      orderId,
      customerName: customerName.trim(),
      product: product.trim(),
      quantity,
      unitPrice,
      total: quantity * unitPrice,
    },
  };

  log('Pedido recibido', { orderId, correlationId, cliente: event.data.customerName });
  let metadata;
  try {
    if (!connected) {
      await producer.connect();
      connected = true;
    }
    [metadata] = await producer.send({
      topic: TOPIC,
      messages: [{
        key: orderId, // mismo pedido -> misma particion
        value: JSON.stringify(event),
        headers: { eventType: event.eventType, eventId: event.eventId, correlationId },
      }],
    });
  } catch (err) {
    log('Error publicando evento', { orderId, correlationId, error: err.message });
    return res.status(503).json({ status: 'unavailable', message: 'Kafka no disponible, intente de nuevo', correlationId });
  }

  const elapsedMs = Number(process.hrtime.bigint() - started) / 1e6;
  log('Evento OrderCreated publicado en Kafka', {
    orderId,
    eventId: event.eventId,
    correlationId,
    topico: TOPIC,
    particion: metadata.partition,
    offset: metadata.baseOffset,
    tiempoMs: elapsedMs.toFixed(1),
  });

  // Se responde de inmediato: los consumidores reaccionan despues, cada uno a su ritmo.
  return res.status(202).json({
    status: 'accepted',
    message: 'Pedido recibido y evento OrderCreated publicado en Kafka',
    orderId,
    eventId: event.eventId,
    correlationId,
  });
});

app.get('/health', (req, res) => {
  res.status(connected ? 200 : 503).json({ service: 'orders-api', broker: connected ? 'connected' : 'disconnected' });
});

app.use((err, req, res, next) => {
  if (err.type === 'entity.parse.failed') {
    return res.status(400).json({ status: 'rejected', errors: ['JSON mal formado'] });
  }
  return next(err);
});

app.listen(PORT, () => log(`API escuchando en el puerto ${PORT}`, { endpoint: 'POST /orders' }));
connect();

for (const signal of ['SIGTERM', 'SIGINT']) {
  process.on(signal, async () => {
    log(`Senal ${signal} recibida, deteniendo`);
    try { await producer.disconnect(); } finally { process.exit(0); }
  });
}
