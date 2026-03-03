import { DatabaseModule } from '@app/core/modules/database/database.module';
import { TypeConfigModule } from '@app/core/modules/type-config/type-config.module';
import { TypeConfigService } from '@app/core/modules/type-config/type-config.service';
import { I18nConfigModule } from '@app/helper';
import { BullModule } from '@nestjs/bullmq';
import { Module } from '@nestjs/common';

@Module({
  imports: [
    TypeConfigModule,
    I18nConfigModule,
    DatabaseModule,
    BullModule.forRootAsync({
      imports: [TypeConfigModule],
      useFactory: (config: TypeConfigService) => {
        const redis = config.getObject('redis');
        return {
          connection: {
            host: redis.host,
            port: redis.port,
            maxRetriesPerRequest: null, // required for BullMQ Worker blocking commands
            ...(redis.password && { password: redis.password }),
            ...(redis.db !== undefined && { db: redis.db }),
          },
        };
      },
      inject: [TypeConfigService],
    }),
  ],
})
export class CoreModule {}
