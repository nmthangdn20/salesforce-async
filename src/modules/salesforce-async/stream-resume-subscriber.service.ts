import { RedisPubSubService } from '@app/core/modules/redis';
import { SalesforceGrpcService } from '@app/helper/modules/salesforce/salesforce-grpc.service';
import { Injectable, Logger, OnModuleInit } from '@nestjs/common';

import { STREAM_RESUME_CHANNEL } from './constants/redis-channel.constant';

/**
 * Main process: subscribes to Redis channel for stream-resume after FULL_RESYNC.
 * When worker (GapProcessor) publishes, calls SalesforceGrpcService.resumeStream().
 */
@Injectable()
export class StreamResumeSubscriberService implements OnModuleInit {
  private readonly logger = new Logger(StreamResumeSubscriberService.name);

  constructor(
    private readonly redisPubSub: RedisPubSubService,
    private readonly grpcService: SalesforceGrpcService,
  ) {}

  onModuleInit(): void {
    this.redisPubSub.subscribe(
      STREAM_RESUME_CHANNEL,
      (_channel: string, message: string) => {
        try {
          const { tenantId, topicName } = JSON.parse(message) as {
            tenantId: string;
            topicName: string;
          };
          void this.grpcService.resumeStream(tenantId, topicName);
        } catch (err) {
          this.logger.error(
            `Failed to handle stream-resume message: ${err instanceof Error ? err.message : String(err)}`,
            err instanceof Error ? err.stack : undefined,
          );
        }
      },
    );
  }
}
