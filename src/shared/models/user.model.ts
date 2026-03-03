import { TBase } from '@app/shared/models/base.model';

export enum USER_STATUS {
  ACTIVE = 'active',
  INACTIVE = 'inactive',
  PENDING = 'pending',
  BLOCKED = 'blocked',
}

export type TUser = TBase & {
  email: string;
  password: string;
  status: USER_STATUS;
};
