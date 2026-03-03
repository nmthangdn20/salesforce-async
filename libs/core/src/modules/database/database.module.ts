import { DatabaseService } from '@app/core/modules/database/database.service';
import { Module } from '@nestjs/common';

@Module({
  imports: [],
  providers: [DatabaseService],
  exports: [DatabaseService],
})
export class DatabaseModule {}
