import { QUEUE_NAMES } from '@app/shared/constants/queue-name.constant';
import { Processor, WorkerHost } from '@nestjs/bullmq';
import { Injectable } from '@nestjs/common';
import { Job } from 'bullmq';

@Injectable()
@Processor(QUEUE_NAMES.SALESFORCE_SYNC)
export class SalesforceSyncProcessor extends WorkerHost {
  async process(job: Job<unknown, unknown, string>): Promise<unknown> {
    console.log('job', job);
    return job;
  }
}
