import { TRole } from '@app/shared/models/role.model';
import { TUser } from '@app/shared/models/user.model';

export type TPermission = {
  code: number;
  key: string;
  name: string;
  description: string;
  module: string;
  roles?: TRole[];
  users?: TUser[];
};
