import { camelToSnake } from '@app/common/utils/key-transform.util';
import {
  CallHandler,
  ExecutionContext,
  Injectable,
  NestInterceptor,
} from '@nestjs/common';
import { map, Observable } from 'rxjs';

interface ResponseData {
  statusCode: number;
  data: any;
  message: string;
}

@Injectable()
export class SnakeCaseInterceptor implements NestInterceptor {
  constructor(private readonly transFormKeys: boolean) {}

  intercept(_: ExecutionContext, next: CallHandler): Observable<any> {
    return next.handle().pipe(
      map((data: Record<string, any>) => {
        if (
          data &&
          typeof data === 'object' &&
          'statusCode' in data &&
          'data' in data
        ) {
          const responseData = data as ResponseData;

          if (
            responseData.data &&
            (responseData.data.pipe ||
              responseData.data.constructor?.name === 'ServerResponse')
          ) {
            return responseData;
          }
        }

        if (
          data === null ||
          data === undefined ||
          typeof data === 'string' ||
          typeof data === 'number' ||
          typeof data === 'boolean' ||
          data instanceof Buffer ||
          data?.pipe || // Check if it's a stream
          data?.constructor?.name === 'ReadStream' ||
          data?.constructor?.name === 'WriteStream' ||
          data?.constructor?.name === 'ServerResponse'
        ) {
          return data;
        }

        return this.transFormKeys ? camelToSnake(data) : data;
      }),
    );
  }
}
