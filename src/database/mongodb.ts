import mongoose from 'mongoose';

const MONGODB_URI = process.env.MONGODB_URI || 'mongodb+srv://assistantsupdev_db_user:rCp0BrUushhwIKR8@binance.bjmukc0.mongodb.net/?appName=Binance';

const RECONNECT_INTERVAL_MS = 5000;
const MAX_RECONNECT_ATTEMPTS = 10;

let reconnectAttempts = 0;

export async function connectMongoDB(): Promise<void> {
  mongoose.connection.on('connected', () => {
    console.log('[MongoDB] Connected');
    reconnectAttempts = 0;
  });

  mongoose.connection.on('disconnected', () => {
    console.warn('[MongoDB] Disconnected');
    handleReconnect();
  });

  mongoose.connection.on('error', (err) => {
    console.error('[MongoDB] Error:', err.message);
  });

  try {
    await mongoose.connect(MONGODB_URI, {
      serverSelectionTimeoutMS: 5000,
      heartbeatFrequencyMS: 10000,
    });
  } catch (err: unknown) {
    const message = err instanceof Error ? err.message : String(err);
    console.error('[MongoDB] Initial connection failed:', message);
    handleReconnect();
  }
}

function handleReconnect(): void {
  if (reconnectAttempts >= MAX_RECONNECT_ATTEMPTS) {
    console.error(`[MongoDB] Max reconnect attempts (${MAX_RECONNECT_ATTEMPTS}) reached. Giving up.`);
    return;
  }

  reconnectAttempts++;
  console.log(`[MongoDB] Reconnecting attempt ${reconnectAttempts}/${MAX_RECONNECT_ATTEMPTS} in ${RECONNECT_INTERVAL_MS / 1000}s...`);

  setTimeout(async () => {
    try {
      await mongoose.connect(MONGODB_URI, {
        serverSelectionTimeoutMS: 5000,
        heartbeatFrequencyMS: 10000,
      });
    } catch (err: unknown) {
      const message = err instanceof Error ? err.message : String(err);
      console.error('[MongoDB] Reconnect failed:', message);
      handleReconnect();
    }
  }, RECONNECT_INTERVAL_MS);
}

export default mongoose;
