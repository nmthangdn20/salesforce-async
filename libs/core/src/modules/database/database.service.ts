import { DrizzleConnectionFactory } from '@app/core/modules/database/database.factory';
import { DbConnectionConfig } from '@app/core/modules/database/types';
import { Injectable } from '@nestjs/common';

@Injectable()
export class DatabaseService {
  async connectTemporary(config: DbConnectionConfig) {
    return await DrizzleConnectionFactory.create(config);
  }
}
