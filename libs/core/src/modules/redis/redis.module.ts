import { Global, Module } from '@nestjs/common';

import { RedisClientService } from './redis-client.service';
import { RedisPubSubService } from './redis-pubsub.service';

@Global()
@Module({
  providers: [RedisClientService, RedisPubSubService],
  exports: [RedisClientService, RedisPubSubService],
})
export class RedisModule {}
