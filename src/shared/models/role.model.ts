import { TBase } from '@app/shared/models/base.model';
import { TUser } from '@app/shared/models/user.model';

import { TPermission } from './permission.model';

export type TRole = TBase & {
  name: string;
  description: string;
  users: TUser[];
  permissions: TPermission[];
};
