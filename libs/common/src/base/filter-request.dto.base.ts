import { TBaseFilterRequestDto } from '@app/shared/dtos/response.dto';
import { ApiProperty } from '@nestjs/swagger';
import { Type } from 'class-transformer';
import { IsNumber, IsOptional, Min } from 'class-validator';

export class BaseFilterRequestDto implements TBaseFilterRequestDto {
  @ApiProperty({
    type: Number,
    required: false,
    default: 1,
  })
  @IsNumber()
  @IsOptional()
  @Min(1)
  @Type(() => Number)
  page: number = 1;

  @ApiProperty({
    type: Number,
    required: false,
    default: 10,
  })
  @IsNumber()
  @IsOptional()
  @Min(1)
  @Type(() => Number)
  take: number = 10;
}
