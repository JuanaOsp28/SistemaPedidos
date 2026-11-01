// notification-worker: consume la cola "pedidos", simula el envio de una notificacion
// y confirma cada mensaje con ack manual. No expone HTTP ni conoce a la API.
const amqp = require('amqplib');
const os = require('os');

const RABBITMQ_URL = process.env.RABBITMQ_URL || 'amqp://admin:admin@rabbitmq:5672';
const QUEUE = process.env.QUEUE_NAME || 'pedidos';
const MIN_MS = Number(process.env.PROCESSING_MIN_MS || 2000);
const MAX_MS = Number(process.env.PROCESSING_MAX_MS || 5000);
const WORKER_ID = process.env.WORKER_ID || os.hostname();
const RETRY_MS = 3000;

let connection = null;
let processed = 0;

function log(text, fields = {}) {
  const extra = Object.entries(fields).map(([k, v]) => `${k}=${v}`).join(' ');
  console.log(`${new Date().toISOString()} [notification-worker ${WORKER_ID}] ${text}${extra ? ' | ' + extra : ''}`);
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function start() {
  for (let attempt = 1; ; attempt++) {
    try {
      connection = await amqp.connect(RABBITMQ_URL, {
        clientProperties: { connection_name: `notification-worker-${WORKER_ID}` },
      });
      break;
    } catch (err) {
      log(`RabbitMQ no disponible (intento ${attempt})`, { error: err.message });
      await sleep(RETRY_MS);
    }
  }
  connection.on('error', (err) => log('Error en la conexion AMQP', { error: err.message }));
  connection.on('close', () => {
    connection = null;
    log('Conexion con RabbitMQ cerrada; reintentando', { enMs: RETRY_MS });
    setTimeout(start, RETRY_MS);
  });

  const channel = await connection.createChannel();
  await channel.assertQueue(QUEUE, { durable: true });
  // Un mensaje a la vez por worker: RabbitMQ reparte la carga entre instancias.
  await channel.prefetch(1);
  log('Conectado a RabbitMQ y escuchando la cola', { cola: QUEUE, prefetch: 1, ack: 'manual' });

  await channel.consume(QUEUE, async (msg) => {
    if (!msg) return; // el broker cancelo el consumidor

    let data;
    try {
      data = JSON.parse(msg.content.toString());
    } catch (err) {
      log('Mensaje invalido descartado', { deliveryTag: msg.fields.deliveryTag });
      channel.nack(msg, false, false);
      return;
    }

    const duration = MIN_MS + Math.floor(Math.random() * (MAX_MS - MIN_MS + 1));
    log('Mensaje recibido', {
      tipo: data.type,
      orderId: data.orderId,
      correlationId: msg.properties.correlationId,
      redelivered: msg.fields.redelivered,
      deliveryTag: msg.fields.deliveryTag,
    });
    log('Simulando envio de notificacion...', { orderId: data.orderId, cliente: data.customerName, duracionMs: duration });
    await sleep(duration);

    channel.ack(msg);
    processed += 1;
    log('Notificacion enviada; mensaje confirmado (ack)', {
      orderId: data.orderId,
      correlationId: msg.properties.correlationId,
      procesadosPorEsteWorker: processed,
    });
  }, { noAck: false });
}

// Al detener el contenedor se cierra la conexion: los mensajes sin ack vuelven a la cola.
for (const signal of ['SIGTERM', 'SIGINT']) {
  process.on(signal, async () => {
    log(`Senal ${signal} recibida, cerrando conexion`, { procesados: processed });
    try {
      if (connection) {
        connection.removeAllListeners('close');
        await connection.close();
      }
    } finally {
      process.exit(0);
    }
  });
}

start();
