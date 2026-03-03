import { QUEUE_NAMES } from '@app/shared/constants/queue-name.constant';
import { Processor, WorkerHost } from '@nestjs/bullmq';
import { Injectable } from '@nestjs/common';
import { Job } from 'bullmq';
import type { CdcJobPayload } from 'src/modules/salesforce-async/types/queue-job-payload.type';
import { deserializeParseEvents } from 'src/modules/salesforce-async/utils/event-serializer.util';

import { CdcGapHandlerService } from '../cdc-gap-handler.service';

@Injectable()
@Processor(QUEUE_NAMES.CDC)
export class CdcProcessor extends WorkerHost {
  constructor(private readonly handler: CdcGapHandlerService) {
    super();
  }

  async process(job: Job<CdcJobPayload, unknown, string>): Promise<unknown> {
    const { events, object, filename } = job.data;
    const parsed = deserializeParseEvents(events);
    await this.handler.handleCdc(parsed, object, filename);
    return { processed: true, count: parsed.length };
  }
}
