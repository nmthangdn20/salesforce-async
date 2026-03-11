import { RedisPubSubService } from '@app/core/modules/redis';
import { SalesforceGrpcService } from '@app/helper/modules/salesforce/salesforce-grpc.service';
import {
  GAP_JOB_NAMES,
  QUEUE_NAMES,
} from '@app/shared/constants/queue-name.constant';
import { Processor, WorkerHost } from '@nestjs/bullmq';
import { Injectable, Logger } from '@nestjs/common';
import { Job } from 'bullmq';
import { STREAM_RESUME_CHANNEL } from 'src/modules/salesforce-async/constants/redis-channel.constant';
import type { GapJobPayload } from 'src/modules/salesforce-async/types/queue-job-payload.type';

import { CdcGapHandlerService } from '../cdc-gap-handler.service';

const PUBLISH_MAX_RETRIES = 5;
const PUBLISH_RETRY_DELAY_MS = 1000;

@Injectable()
@Processor(QUEUE_NAMES.GAP)
export class GapProcessor extends WorkerHost {
  private readonly logger = new Logger(GapProcessor.name);

  constructor(
    private readonly handler: CdcGapHandlerService,
    private readonly redisPubSub: RedisPubSubService,
    private readonly grpcService: SalesforceGrpcService,
  ) {
    super();
  }

  async process(job: Job<GapJobPayload, unknown, string>): Promise<unknown> {
    const { gapInfo, filename, tenantId, topicName } = job.data;

    switch (job.name) {
      case GAP_JOB_NAMES.NORMAL_WITH_IDS:
        await this.handler.handleNormalGapWithIds(gapInfo, filename);
        break;

      case GAP_JOB_NAMES.NORMAL_WITHOUT_IDS:
        await this.handler.handleNormalGapWithoutIds(gapInfo, filename);
        break;

      case GAP_JOB_NAMES.FULL_RESYNC:
        await this.handler.handleFullResync(gapInfo, filename, {
          tenantId,
          topicName,
          onResume: async (tid, tname) => {
            await this.publishOrResumeDirect(tid, tname);
          },
        });
        break;

      default:
        this.logger.warn(`Unknown gap job name: ${job.name}`);
    }

    return { processed: true, gapType: gapInfo.type };
  }

  /**
   * Try Redis publish first; if it fails after all retries, try to resume stream
   * directly in this process (works when worker runs in same process as gRPC subscriber).
   */
  private async publishOrResumeDirect(
    tenantId: string,
    topicName: string,
  ): Promise<void> {
    try {
      await this.publishWithRetry(
        STREAM_RESUME_CHANNEL,
        JSON.stringify({ tenantId, topicName }),
      );
    } catch (err) {
      this.logger.warn(
        `Redis publish failed, attempting direct stream resume for ${tenantId}/${topicName}: ${err instanceof Error ? err.message : String(err)}`,
      );
      try {
        await this.grpcService.resumeStream(tenantId, topicName);
        this.logger.log(
          `Stream resumed directly (Redis publish had failed for ${tenantId}/${topicName})`,
        );
      } catch (resumeErr) {
        this.logger.error(
          `Direct resume also failed: ${resumeErr instanceof Error ? resumeErr.message : String(resumeErr)}`,
          resumeErr instanceof Error ? resumeErr.stack : undefined,
        );
        throw resumeErr;
      }
    }
  }

  private async publishWithRetry(
    channel: string,
    message: string,
    attempt = 1,
  ): Promise<void> {
    try {
      await this.redisPubSub.publish(channel, message);
      if (attempt > 1) {
        this.logger.log(
          `Stream resume published successfully on attempt ${attempt}`,
        );
      }
    } catch (err) {
      const isConnectionError =
        err instanceof Error &&
        (err.message.includes('Connection is closed') ||
          err.message.includes('Stream isn') ||
          err.message.includes('ECONNREFUSED') ||
          err.message.includes('ECONNRESET'));

      if (isConnectionError && attempt < PUBLISH_MAX_RETRIES) {
        const delay = PUBLISH_RETRY_DELAY_MS * attempt;
        this.logger.warn(
          `Failed to publish stream resume (attempt ${attempt}/${PUBLISH_MAX_RETRIES}), retrying in ${delay}ms: ${err.message}`,
        );
        await new Promise((resolve) => setTimeout(resolve, delay));
        return this.publishWithRetry(channel, message, attempt + 1);
      }

      const errorMessage = `Failed to publish stream resume after ${attempt} attempt(s): ${err instanceof Error ? err.message : String(err)}`;
      this.logger.error(
        errorMessage,
        err instanceof Error ? err.stack : undefined,
      );
      throw new Error(errorMessage);
    }
  }
}
