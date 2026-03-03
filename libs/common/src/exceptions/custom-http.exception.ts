import { ERROR_MESSAGES } from '@app/shared/constants/error.constant';
import { HttpException, HttpStatus } from '@nestjs/common';

export class CustomHttpException extends HttpException {
  constructor(
    errorCode: ERROR_MESSAGES,
    status: HttpStatus = HttpStatus.BAD_REQUEST,
  ) {
    super(
      {
        errorCode,
        statusCode: HttpStatus.BAD_REQUEST,
        message: Object.keys(ERROR_MESSAGES).find(
          (key) =>
            ERROR_MESSAGES[key as keyof typeof ERROR_MESSAGES] === errorCode,
        ) as string,
      },
      status,
    );
  }
}
