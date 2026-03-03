import { TypeConfigService } from '@app/core/modules/type-config';
import {
  registerAppConfig,
  registerDatabaseConfig,
  registerJwtConfig,
  registerRedisConfig,
} from '@app/core/modules/type-config/configs';
import { Global, Module } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';

@Global()
@Module({
  imports: [
    ConfigModule.forRoot({
      isGlobal: true,
      load: [
        registerAppConfig,
        registerJwtConfig,
        registerDatabaseConfig,
        registerRedisConfig,
      ],
      envFilePath: ['.env'],
    }),
  ],
  providers: [TypeConfigService],
  exports: [TypeConfigService],
})
export class TypeConfigModule {}
