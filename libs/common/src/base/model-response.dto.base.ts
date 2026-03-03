import { TBaseModelResponseDto } from '@app/shared/dtos/response.dto';
import { ApiResponseProperty } from '@nestjs/swagger';
import { Expose } from 'class-transformer';

export class BaseModelResponseDto implements TBaseModelResponseDto {
  @Expose()
  @ApiResponseProperty({ type: 'string', format: 'uuid' })
  id!: string;

  @Expose()
  @ApiResponseProperty({
    example: new Date().getTime(),
  })
  createdAt!: Date;

  @Expose()
  @ApiResponseProperty({
    example: new Date().getTime(),
  })
  updatedAt!: Date;
}
