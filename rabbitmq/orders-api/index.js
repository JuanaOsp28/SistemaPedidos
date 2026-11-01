// orders-api (RabbitMQ): recibe pedidos por HTTP y publica un mensaje "ProcessOrder"
// en la cola durable "pedidos". No conoce ni llama al worker.
const express = require('express');
const amqp = require('amqplib');
const { randomUUID } = require('crypto');

const PORT = Number(process.env.PORT || 3000);
const RABBITMQ_URL = process.env.RABBITMQ_URL || 'amqp://admin:admin@rabbitmq:5672';
const QUEUE = process.env.QUEUE_NAME || 'pedidos';
const RETRY_MS = 3000;

let channel = null;

function log(text, fields = {}) {
  const extra = Object.entries(fields).map(([k, v]) => `${k}=${v}`).join(' ');
  console.log(`${new Date().toISOString()} [orders-api] ${text}${extra ? ' | ' + extra : ''}`);
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// depends_on no garantiza que RabbitMQ acepte conexiones: se reintenta hasta lograrlo,
// y si la conexion se pierde (p. ej. reinicio del broker) se vuelve a conectar.
async function connect() {
  for (let attempt = 1; ; attempt++) {
    try {
      const connection = await amqp.connect(RABBITMQ_URL, {
        clientProperties: { connection_name: 'orders-api' },
      });
      connection.on('error', (err) => log('Error en la conexion AMQP', { error: err.message }));
      connection.on('close', () => {
        channel = null;
        log('Conexion con RabbitMQ cerrada; reintentando', { enMs: RETRY_MS });
        setTimeout(connect, RETRY_MS);
      });
      // Canal con confirmaciones del publicador: el broker confirma que recibio el mensaje.
      const ch = await connection.createConfirmChannel();
      await ch.assertQueue(QUEUE, { durable: true });
      channel = ch;
      log('Conectado a RabbitMQ', { broker: 'rabbitmq:5672', cola: QUEUE, durable: true });
      return;
    } catch (err) {
      log(`RabbitMQ no disponible (intento ${attempt})`, { error: err.message });
      await sleep(RETRY_MS);
    }
  }
}

function publish(message, correlationId) {
  return new Promise((resolve, reject) => {
    channel.sendToQueue(
      QUEUE,
      Buffer.from(JSON.stringify(message)),
      {
        persistent: true,
        contentType: 'application/json',
        type: message.type,
        messageId: randomUUID(),
        correlationId,
        appId: 'orders-api',
        timestamp: Math.floor(Date.now() / 1000),
      },
      (err) => (err ? reject(err) : resolve()),
    );
  });
}

function validate(body) {
  const errors = [];
  if (!body || typeof body !== 'object') return ['El cuerpo debe ser un JSON'];
  if (typeof body.customerName !== 'string' || !body.customerName.trim()) errors.push('customerName es obligatorio');
  if (typeof body.product !== 'string' || !body.product.trim()) errors.push('product es obligatorio');
  if (!Number.isInteger(body.quantity) || body.quantity <= 0) errors.push('quantity debe ser un entero mayor que 0');
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
  if (!channel) {
    log('Pedido no aceptado: broker no disponible', { correlationId });
    return res.status(503).json({ status: 'unavailable', message: 'RabbitMQ no disponible, intente de nuevo', correlationId });
  }

  const orderId = 'ORD-' + randomUUID().split('-')[0].toUpperCase();
  const message = {
    type: 'ProcessOrder',
    orderId,
    correlationId,
    customerName: req.body.customerName.trim(),
    product: req.body.product.trim(),
    quantity: req.body.quantity,
    createdAt: new Date().toISOString(),
  };

  log('Pedido recibido', { orderId, correlationId, cliente: message.customerName });
  try {
    await publish(message, correlationId);
  } catch (err) {
    log('Error publicando mensaje', { orderId, correlationId, error: err.message });
    return res.status(503).json({ status: 'unavailable', message: 'No se pudo publicar en RabbitMQ', correlationId });
  }

  const elapsedMs = Number(process.hrtime.bigint() - started) / 1e6;
  log('Mensaje ProcessOrder publicado en RabbitMQ', { orderId, correlationId, cola: QUEUE, tiempoMs: elapsedMs.toFixed(1) });

  // Se responde de inmediato: el worker procesa despues, de forma asincrona.
  return res.status(202).json({
    status: 'accepted',
    message: 'Pedido recibido y enviado a RabbitMQ',
    orderId,
    correlationId,
  });
});

app.get('/health', (req, res) => {
  res.status(channel ? 200 : 503).json({ service: 'orders-api', broker: channel ? 'connected' : 'disconnected' });
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
  process.on(signal, () => {
    log(`Senal ${signal} recibida, deteniendo`);
    process.exit(0);
  });
}
