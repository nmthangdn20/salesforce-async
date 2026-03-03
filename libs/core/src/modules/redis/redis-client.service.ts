import { TypeConfigService } from '@app/core/modules/type-config';
import {
  Injectable,
  Logger,
  OnModuleDestroy,
  OnModuleInit,
} from '@nestjs/common';
import Redis from 'ioredis';

/**
 * Centralised Redis connection manager.
 *
 * Exposes a shared `client` for general-purpose Redis operations (reads, writes,
 * sorted sets, TTL, etc.) and a `createClient()` factory for cases that need a
 * dedicated connection — most notably subscribe-mode clients (ioredis connections
 * in subscribe mode cannot issue regular commands on the same socket).
 *
 * Callers that obtain a client via `createClient()` are responsible for its
 * lifecycle (i.e. they must call `.quit()` themselves).
 * The shared `client` is managed exclusively by this service.
 *
 * Convenience methods (set, get, del, exists, sadd, srem, sismember, expire, pipeline)
 * delegate to the shared client so callers can avoid using `client` directly.
 */
@Injectable()
export class RedisClientService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(RedisClientService.name);

  client!: Redis;

  constructor(private readonly config: TypeConfigService) {}

  onModuleInit(): void {
    this.client = this.createClient();
    this.client.on('error', (err: Error) => {
      this.logger.error(`Redis client error: ${err.message}`, err.stack);
    });
  }

  async onModuleDestroy(): Promise<void> {
    await this.client?.quit();
  }

  /**
   * Creates a **new** ioredis connection using the same configuration.
   * Use this when you need a dedicated connection, e.g. for subscribe mode.
   * The caller owns the returned client and must call `.quit()` when done.
   */
  createClient(): Redis {
    const r = this.config.getObject('redis');
    return new Redis({
      host: r.host,
      port: r.port,
      ...(r.password && { password: r.password }),
      ...(r.db !== undefined && { db: r.db }),
    });
  }

  // ---------------------------------------------------------------------------
  // String
  // ---------------------------------------------------------------------------

  set(key: string, value: string, ttlSeconds?: number): Promise<'OK' | null> {
    if (ttlSeconds != null) {
      return this.client.set(key, value, 'EX', ttlSeconds);
    }
    return this.client.set(key, value);
  }

  get(key: string): Promise<string | null> {
    return this.client.get(key);
  }

  del(...keys: string[]): Promise<number> {
    return this.client.del(...keys);
  }

  exists(...keys: string[]): Promise<number> {
    return this.client.exists(...keys);
  }

  expire(key: string, seconds: number): Promise<number> {
    return this.client.expire(key, seconds);
  }

  // ---------------------------------------------------------------------------
  // Set
  // ---------------------------------------------------------------------------

  sadd(key: string, ...members: string[]): Promise<number> {
    return this.client.sadd(key, ...members);
  }

  srem(key: string, ...members: string[]): Promise<number> {
    return this.client.srem(key, ...members);
  }

  sismember(key: string, member: string): Promise<number> {
    return this.client.sismember(key, member);
  }

  pipeline(): ReturnType<Redis['pipeline']> {
    return this.client.pipeline();
  }
}
