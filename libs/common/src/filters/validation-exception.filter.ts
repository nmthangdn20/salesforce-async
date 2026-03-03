import { ValidatorException } from '@app/common/exceptions/validation.exception';
import { camelToSnake } from '@app/common/utils/key-transform.util';
import { TResponseErrorDto } from '@app/shared/dtos/response.dto';
import { TValidatorError } from '@app/shared/types/validation.type';
import {
  ArgumentsHost,
  Catch,
  ExceptionFilter,
  HttpException,
} from '@nestjs/common';
import { ValidationError } from 'class-validator';
import { Response } from 'express';

@Catch(ValidatorException)
export class ClassValidatorFilter implements ExceptionFilter {
  constructor(private readonly transFormKeys: boolean) {}

  catch(exception: HttpException, host: ArgumentsHost) {
    const ctx = host.switchToHttp();
    const response = ctx.getResponse<Response>();

    const statusCode = exception.getStatus();
    const res = exception.getResponse();
    if (typeof res === 'object' && 'message' in res && 'error' in res) {
      const validationErrors = res['message'] as ValidationError[];
      const data: TValidatorError[] =
        this.mapValidationErrors(validationErrors);

      const errorResponse: TResponseErrorDto<TValidatorError[]> = {
        statusCode,
        data,
        errorMessage: res['error'] as string,
      };

      response
        .status(statusCode)
        .json(this.transFormKeys ? camelToSnake(errorResponse) : errorResponse);
    }

    return;
  }

  private mapValidationErrors(errors: ValidationError[]): TValidatorError[] {
    return errors.map((error) => {
      const validatorError: TValidatorError = {
        property: error.property,
        constraints: Object.values(error.constraints || {}).map(
          (constraint) => constraint,
        ),
      };

      if (error.children && error.children.length > 0) {
        validatorError.children = this.mapValidationErrors(error.children);
      }

      return validatorError;
    });
  }
}
