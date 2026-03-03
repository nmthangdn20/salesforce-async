import { CustomHttpException } from '@app/common/exceptions/custom-http.exception';
import { camelToSnake } from '@app/common/utils/key-transform.util';
import { TResponseErrorDto } from '@app/shared/dtos/response.dto';
import {
  ArgumentsHost,
  Catch,
  HttpException,
  HttpStatus,
  Logger,
} from '@nestjs/common';
import { BaseExceptionFilter } from '@nestjs/core';
import { Response } from 'express';
import { I18nService } from 'nestjs-i18n';

@Catch()
export class AllExceptionFilter extends BaseExceptionFilter {
  private readonly logger = new Logger(AllExceptionFilter.name);

  constructor(
    private readonly i18n: I18nService,
    private readonly transFormKeys: boolean,
  ) {
    super();
  }

  catch(exception: unknown, host: ArgumentsHost) {
    console.log(exception);

    this.logger.error(exception);

    const ctx = host.switchToHttp();
    const response = ctx.getResponse<Response>();

    const status =
      exception instanceof HttpException
        ? exception.getStatus()
        : HttpStatus.INTERNAL_SERVER_ERROR;

    let message =
      exception instanceof HttpException
        ? (exception.getResponse() as { message: string }).message
        : this.i18n.translate('error.500');
    let errorCode;

    if (exception instanceof CustomHttpException) {
      errorCode = (
        exception.getResponse() as {
          errorCode: number;
        }
      ).errorCode;
      message = this.i18n.translate(`error.${errorCode}`);
    }

    const res: TResponseErrorDto<any> = {
      statusCode: status,
      errorCode: errorCode,
      errorMessage: message,
    };

    response.status(status).json(this.transFormKeys ? camelToSnake(res) : res);
  }
}
