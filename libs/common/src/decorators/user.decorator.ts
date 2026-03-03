import { TUser } from '@app/shared/models/user.model';
import { NestedKeyOf } from '@app/shared/types/nested-keyof.type';
import { createParamDecorator, ExecutionContext } from '@nestjs/common';
import { Request } from 'express';

function getByPath<T extends object>(obj: T, path: string): any {
  return path.split('.').reduce((acc: any, key) => acc?.[key], obj);
}

export const User = createParamDecorator(
  (k: NestedKeyOf<TUser>, ctx: ExecutionContext): TUser => {
    const request = ctx.switchToHttp().getRequest<Request>();
    const user = request.user as TUser;

    return k ? getByPath(user, k) : user;
  },
);
