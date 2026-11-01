// inventory-consumer: reacciona a OrderCreated simulando la reserva de inventario.
// Grupo propio (inventory-service-group): recibe todos los eventos del topico.
const { Kafka, logLevel } = require('kafkajs');
const os = require('os');

const SERVICE = 'inventory-consumer';
const BROKER = process.env.KAFKA_BROKER || 'kafka:9092';
const TOPIC = process.env.KAFKA_TOPIC || 'orders.events';
const GROUP_ID = process.env.KAFKA_GROUP_ID || 'inventory-service-group';
const PROCESSING_MS = Number(process.env.PROCESSING_MS || 1500);
const INSTANCE = os.hostname();
const RETRY_MS = 3000;

function log(text, fields = {}) {
  const extra = Object.entries(fields).map(([k, v]) => `${k}=${v}`).join(' ');
  console.log(`${new Date().toISOString()} [${SERVICE} ${INSTANCE}] ${text}${extra ? ' | ' + extra : ''}`);
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// Accion simulada de negocio propia de este consumidor.
async function handle(event) {
  const { orderId, product, quantity } = event.data;
  log('Simulando reserva de inventario...', { orderId, producto: product, cantidad: quantity });
  await sleep(PROCESSING_MS);
  log('Inventario reservado', { orderId, producto: product, unidadesReservadas: quantity });
}

const kafka = new Kafka({ clientId: `${SERVICE}-${INSTANCE}`, brokers: [BROKER], logLevel: logLevel.WARN });
const consumer = kafka.consumer({ groupId: GROUP_ID, sessionTimeout: 10000, heartbeatInterval: 3000 });
let processed = 0;

consumer.on(consumer.events.GROUP_JOIN, ({ payload }) => {
  const partitions = (payload.memberAssignment[TOPIC] || []).join(',') || 'ninguna';
  log('Unido al consumer group', { grupo: GROUP_ID, particionesAsignadas: partitions });
});
consumer.on(consumer.events.CRASH, ({ payload }) => log('Consumidor caido; kafkajs reintentara', { error: payload.error.message }));

async function start() {
  for (let attempt = 1; ; attempt++) {
    try {
      await consumer.connect();
      await consumer.subscribe({ topic: TOPIC, fromBeginning: true });
      break;
    } catch (err) {
      log(`Kafka no disponible (intento ${attempt})`, { error: err.message });
      await sleep(RETRY_MS);
    }
  }
  log('Suscrito al topico', { topico: TOPIC, grupo: GROUP_ID });

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const event = JSON.parse(message.value.toString());
      if (event.eventType !== 'OrderCreated') {
        log('Evento ignorado', { eventType: event.eventType, offset: message.offset });
        return;
      }
      log('Evento recibido', {
        eventType: event.eventType,
        eventId: event.eventId,
        orderId: event.data.orderId,
        correlationId: event.correlationId,
        topic,
        partition,
        offset: message.offset,
      });
      await handle(event);
      processed += 1;
      log('Evento procesado', { orderId: event.data.orderId, partition, offset: message.offset, procesadosPorEstaInstancia: processed });
    },
  });
}

// Desconexion ordenada: se confirman los offsets procesados y el grupo se rebalancea de inmediato.
for (const signal of ['SIGTERM', 'SIGINT']) {
  process.on(signal, async () => {
    log(`Senal ${signal} recibida, saliendo del grupo`, { procesados: processed });
    try { await consumer.disconnect(); } finally { process.exit(0); }
  });
}

start();
