import {
  QUEUE_NAMES,
  SYNC_JOB_NAMES,
} from '@app/shared/constants/queue-name.constant';
import { Processor, WorkerHost } from '@nestjs/bullmq';
import { Injectable, Logger } from '@nestjs/common';
import { Job } from 'bullmq';
import type { SyncJobPayload } from 'src/modules/salesforce-async/types/queue-job-payload.type';

import { SalesforceAsyncService } from '../salesforce-async.service';

@Injectable()
@Processor(QUEUE_NAMES.SALESFORCE_SYNC)
export class SalesforceSyncProcessor extends WorkerHost {
  private readonly logger = new Logger(SalesforceSyncProcessor.name);

  constructor(private readonly service: SalesforceAsyncService) {
    super();
  }

  async process(job: Job<SyncJobPayload, unknown, string>): Promise<unknown> {
    const { filename } = job.data;

    switch (job.name) {
      case SYNC_JOB_NAMES.SYNC_DATA:
        this.logger.log(`Starting sync for "${filename}"`);
        await this.service.syncData(filename);
        this.logger.log(`Sync completed for "${filename}"`);
        break;

      default:
        this.logger.warn(`Unknown sync job name: ${job.name}`);
    }

    return { synced: true, filename };
  }
}
