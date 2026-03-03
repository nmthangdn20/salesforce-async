import { Body, Controller, Post } from '@nestjs/common';
import { CdcDto } from 'src/modules/salesforce-async/dto/cdc.dto';
import { SyncDataDto } from 'src/modules/salesforce-async/dto/sync-data.dto';

import { SalesforceAsyncService } from './salesforce-async.service';

@Controller('salesforce-async')
export class SalesforceAsyncController {
  constructor(
    private readonly salesforceAsyncService: SalesforceAsyncService,
  ) {}

  @Post('sync-data')
  async syncData(@Body() body: SyncDataDto) {
    return this.salesforceAsyncService.syncData(body.filename);
  }

  @Post('cdc')
  async cdc(@Body() body: CdcDto) {
    return this.salesforceAsyncService.cdc(body.filename);
  }
}
