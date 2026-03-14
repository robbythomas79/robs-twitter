const express = require('express');
const { Kafka, logLevel } = require('kafkajs');
const WebSocket = require('ws');
const path = require('path');

const app = express();
const PORT = process.env.PORT || 3000;
const KAFKA_BROKER = process.env.KAFKA_BROKER || 'localhost:9092';
const KAFKA_TOPIC = process.env.KAFKA_TOPIC || 'twitterTopic';
const KAFKA_GROUP_ID = process.env.KAFKA_GROUP_ID || 'robs-twitter-frontend';
const MAX_BUFFER = 100;

// --- Static files ---
app.use(express.static(path.join(__dirname, 'public')));

app.get('/health', (_req, res) => res.json({ status: 'ok', topic: KAFKA_TOPIC, broker: KAFKA_BROKER }));

// --- HTTP + WebSocket server ---
const server = app.listen(PORT, () => {
  console.log(`[server] Listening on http://localhost:${PORT}`);
});

const wss = new WebSocket.Server({ server, path: '/ws' });

// Ring-buffer of recent tweets sent to clients that connect late
const tweetBuffer = [];

function broadcast(payload) {
  const msg = JSON.stringify(payload);
  wss.clients.forEach((client) => {
    if (client.readyState === WebSocket.OPEN) client.send(msg);
  });
}

wss.on('connection', (ws) => {
  console.log(`[ws] Client connected (${wss.clients.size} total)`);

  // Send recent history so the feed isn't blank on first load
  if (tweetBuffer.length > 0) {
    ws.send(JSON.stringify({ type: 'history', tweets: [...tweetBuffer] }));
  }

  ws.on('close', () =>
    console.log(`[ws] Client disconnected (${wss.clients.size} total)`)
  );
});

// --- Kafka consumer ---
async function startConsumer() {
  const kafka = new Kafka({
    clientId: 'robs-twitter',
    brokers: [KAFKA_BROKER],
    logLevel: logLevel.WARN,
    retry: {
      initialRetryTime: 3000,
      retries: 20,
    },
  });

  const consumer = kafka.consumer({ groupId: KAFKA_GROUP_ID });

  // Graceful shutdown
  const shutdown = async () => {
    console.log('[kafka] Disconnecting...');
    await consumer.disconnect();
    process.exit(0);
  };
  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);

  try {
    await consumer.connect();
    console.log(`[kafka] Connected to ${KAFKA_BROKER}`);

    await consumer.subscribe({ topic: KAFKA_TOPIC, fromBeginning: false });
    console.log(`[kafka] Subscribed to topic "${KAFKA_TOPIC}"`);

    await consumer.run({
      eachMessage: async ({ message }) => {
        try {
          const raw = JSON.parse(message.value.toString('utf8'));

          // Add to ring-buffer
          tweetBuffer.unshift(raw);
          if (tweetBuffer.length > MAX_BUFFER) tweetBuffer.pop();

          // Push to all connected browsers
          broadcast({ type: 'tweet', tweet: raw });
        } catch (err) {
          console.error('[kafka] Failed to parse message:', err.message);
        }
      },
    });
  } catch (err) {
    console.error('[kafka] Connection error:', err.message);
    console.log('[kafka] Retrying in 5s...');
    setTimeout(startConsumer, 5000);
  }
}

startConsumer();
