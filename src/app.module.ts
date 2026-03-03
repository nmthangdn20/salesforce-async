import { CoreModule } from '@app/core';
import { RedisModule } from '@app/core/modules/redis';
import { Module } from '@nestjs/common';

import { AuthModule } from './modules/auth/auth.module';
import { DbModule } from './modules/db/db.module';
import { SalesforceAsyncModule } from './modules/salesforce-async/salesforce-async.module';

@Module({
  imports: [
    CoreModule,
    RedisModule,
    AuthModule,
    SalesforceAsyncModule,
    DbModule,
  ],
})
export class AppModule {}
