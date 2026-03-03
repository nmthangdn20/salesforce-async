import { TResponseErrorDto } from '@app/shared/dtos/response.dto';
import { ApiResponseProperty } from '@nestjs/swagger';
import { Expose } from 'class-transformer';

export class ErrorResponseBase implements TResponseErrorDto {
  @ApiResponseProperty({
    type: String,
    example: 'error',
  })
  @Expose()
  errorCode?: string | number | undefined;

  @ApiResponseProperty({
    type: String,
    example: 'error',
  })
  @Expose()
  errorMessage?: string;
}
