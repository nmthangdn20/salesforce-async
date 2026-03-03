import { TBase } from '@app/shared/models/base.model';
import { TPermission } from '@app/shared/models/permission.model';
import { TProfile } from '@app/shared/models/profile.model';
import { TRole } from '@app/shared/models/role.model';

export enum USER_STATUS {
  ACTIVE = 'active',
  INACTIVE = 'inactive',
  PENDING = 'pending',
  BLOCKED = 'blocked',
}

export type TUser = TBase & {
  email: string;
  password: string;
  profile: TProfile;
  roles: TRole[];
  permissions: TPermission[];
  status: USER_STATUS;
};
