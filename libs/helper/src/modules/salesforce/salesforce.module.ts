import { SalesforceGrpcService } from '@app/helper/modules/salesforce/salesforce-grpc.service';
import { Module } from '@nestjs/common';

import { SalesforceService } from './salesforce.service';

@Module({
  providers: [SalesforceService, SalesforceGrpcService],
  exports: [SalesforceService, SalesforceGrpcService],
})
export class SalesforceModule {}
