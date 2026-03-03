import { SalesforceModule } from '@app/helper';
import { QUEUE_NAMES } from '@app/shared/constants/queue-name.constant';
import { BullModule } from '@nestjs/bullmq';
import { Module } from '@nestjs/common';
import { DbModule } from 'src/modules/db/db.module';
import { DirtyRecordService } from 'src/modules/salesforce-async/dirty-record.service';

import { CdcGapHandlerService } from './cdc-gap-handler.service';
import { CdcProcessor } from './processors/cdc.processor';
import { GapProcessor } from './processors/gap.processor';
import { SalesforceSyncProcessor } from './processors/salesforce-sync.processor';
import { SalesforceAsyncController } from './salesforce-async.controller';
import { SalesforceAsyncService } from './salesforce-async.service';
import { StreamResumeSubscriberService } from './stream-resume-subscriber.service';

@Module({
  imports: [
    DbModule,
    SalesforceModule,
    BullModule.registerQueue({ name: QUEUE_NAMES.SALESFORCE_SYNC }),
    BullModule.registerQueue({ name: QUEUE_NAMES.CDC }),
    BullModule.registerQueue({ name: QUEUE_NAMES.GAP }),
  ],
  controllers: [SalesforceAsyncController],
  providers: [
    DirtyRecordService,
    StreamResumeSubscriberService,
    CdcGapHandlerService,
    SalesforceAsyncService,
    CdcProcessor,
    GapProcessor,
    SalesforceSyncProcessor,
  ],
  exports: [BullModule],
})
export class SalesforceAsyncModule {}
