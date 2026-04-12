import Redis from 'ioredis';

const redis = new Redis(process.env.REDIS_URL || 'redis://default:sk6XJNqzVZC5Hs6jkJJbgDRbp7zzH1Xv@redis-19471.c294.ap-northeast-1-2.ec2.cloud.redislabs.com:19471');

redis.on('connect', () => console.log('[Redis] Connected'));
redis.on('error', (err) => console.error('[Redis] Error:', err.message));

export async function setRealtimePrice(symbol: string, price: number): Promise<void> {
  await redis.set(`price:${symbol}`, price.toString());
}

export async function getRealtimePrice(symbol: string): Promise<number | null> {
  const val = await redis.get(`price:${symbol}`);
  return val ? parseFloat(val) : null;
}

export default redis;
