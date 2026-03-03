import { TBaseResponseDto } from '@app/shared/dtos/response.dto';
import {
  CallHandler,
  ExecutionContext,
  Injectable,
  NestInterceptor,
} from '@nestjs/common';
import { I18nService } from 'nestjs-i18n';
import { Observable } from 'rxjs';
import { catchError, map } from 'rxjs/operators';

@Injectable()
export class TransformInterceptor<T>
  implements NestInterceptor<T, TBaseResponseDto<T>>
{
  constructor(private readonly i18nService: I18nService) {}

  intercept(
    context: ExecutionContext,
    next: CallHandler,
  ): Observable<TBaseResponseDto<T>> {
    return next.handle().pipe(
      map((data: T) => {
        const message =
          typeof data === 'object' && data && 'message' in data
            ? this.getMessage(data.message as string)
            : this.i18nService.translate('message.SUCCESS');

        if (typeof data === 'object' && data) {
          delete (data as any).message;
        }

        const isEmpty =
          typeof data === 'object' && data && Object.keys(data).length === 0;

        return {
          statusCode: context.switchToHttp().getResponse().statusCode as number,
          data: isEmpty ? undefined : data,
          message,
        };
      }),
      catchError((err) => {
        throw err;
      }),
    );
  }

  private getMessage(message: string): string {
    return message.includes('message.')
      ? this.i18nService.translate(message)
      : message;
  }
}
