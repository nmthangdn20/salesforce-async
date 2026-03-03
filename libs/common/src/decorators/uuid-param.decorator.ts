import {
  BadRequestException,
  Injectable,
  Param,
  PipeTransform,
} from '@nestjs/common';
import { ApiParam } from '@nestjs/swagger';
import { isUUID } from 'class-validator';

@Injectable()
export class UUIDValidationPipe implements PipeTransform {
  constructor(private readonly paramName?: string) {}

  transform(value: any) {
    if (!isUUID(value)) {
      throw new BadRequestException([
        {
          property: this.paramName ?? 'id',
          constraints: [`${this.paramName ?? 'id'} must be a valid UUID`],
        },
      ]);
    }
    return value;
  }
}

export function UUIDParam(name: string): ParameterDecorator {
  return (
    target: object,
    propertyKey: string | symbol | undefined,
    parameterIndex: number,
  ) => {
    Param(name, new UUIDValidationPipe(name))(
      target,
      propertyKey,
      parameterIndex,
    );
    if (propertyKey) {
      const descriptor = Object.getOwnPropertyDescriptor(target, propertyKey);
      ApiParam({ name, type: 'string', format: 'uuid' })(
        target,
        propertyKey,
        descriptor!,
      );
    }
  };
}
