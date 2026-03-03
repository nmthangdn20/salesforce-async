import {
  Injectable,
  Logger,
  OnModuleDestroy,
  OnModuleInit,
} from '@nestjs/common';
import Redis from 'ioredis';

import { RedisClientService } from './redis-client.service';

@Injectable()
export class RedisPubSubService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(RedisPubSubService.name);
  private subClient: Redis | null = null;
  private readonly handlers = new Map<
    string,
    Set<(channel: string, message: string) => void>
  >();

  constructor(private readonly redisClient: RedisClientService) {}

  onModuleInit(): void {
    // pubClient is the shared client from RedisClientService — lifecycle managed there.
    // subClient must be a dedicated connection: ioredis connections in subscribe mode
    // cannot issue regular commands, so we create a separate one here.
    this.subClient = this.redisClient.createClient();

    this.subClient.on('message', (channel: string, message: string) => {
      this.handlers.get(channel)?.forEach((fn) => fn(channel, message));
    });
  }

  async onModuleDestroy(): Promise<void> {
    await this.subClient?.quit();
    this.subClient = null;
  }

  publish(channel: string, message: string): Promise<number> {
    return this.redisClient.client.publish(channel, message);
  }

  subscribe(
    channel: string,
    onMessage: (channel: string, message: string) => void,
  ): void {
    if (!this.subClient) throw new Error('RedisPubSubService not initialized');

    if (!this.handlers.has(channel)) {
      this.handlers.set(channel, new Set());
      this.subClient
        .subscribe(channel)
        .catch((err: Error) =>
          this.logger.error(
            `Failed to subscribe to ${channel}: ${err.message}`,
          ),
        );
    }

    this.handlers.get(channel)!.add(onMessage);
  }

  unsubscribe(
    channel: string,
    onMessage: (channel: string, message: string) => void,
  ): void {
    const set = this.handlers.get(channel);
    if (!set) return;

    set.delete(onMessage);

    if (set.size === 0) {
      this.handlers.delete(channel);
      this.subClient
        ?.unsubscribe(channel)
        .catch((err: Error) =>
          this.logger.error(
            `Failed to unsubscribe from ${channel}: ${err.message}`,
          ),
        );
    }
  }
}
