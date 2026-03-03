import {
  TBasePaginationMetadataDto,
  TBaseResponseDto,
  TBaseResponsePaginationDto,
} from '@app/shared/dtos/response.dto';
import { ApiResponseProperty } from '@nestjs/swagger';
import { Expose } from 'class-transformer';

export class BaseResponseDto<T = any> implements TBaseResponseDto<T> {
  @Expose()
  data?: T;

  @ApiResponseProperty({
    type: String,
    example: 'success',
  })
  @Expose()
  message!: string;

  @ApiResponseProperty({
    type: Number,
    example: 200,
  })
  @Expose()
  statusCode?: number;
}

export class PaginationMetadataDto implements TBasePaginationMetadataDto {
  @ApiResponseProperty({
    type: Number,
    example: 1,
  })
  @Expose()
  page!: number;

  @ApiResponseProperty({
    type: Number,
    example: 10,
  })
  @Expose()
  take!: number;

  @ApiResponseProperty({
    type: Number,
    example: 100,
  })
  @Expose()
  totalItems!: number;

  @ApiResponseProperty({
    type: Number,
    example: 10,
  })
  @Expose()
  totalPages!: number;

  @ApiResponseProperty({
    type: Number,
    example: 10,
  })
  @Expose()
  itemCount!: number;
}

export class BaseResponsePaginationDto<T = any>
  implements TBaseResponsePaginationDto<T>
{
  @Expose()
  items!: T[];

  @ApiResponseProperty({
    type: PaginationMetadataDto,
  })
  @Expose()
  meta!: PaginationMetadataDto;
}
