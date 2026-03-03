import { TBase } from '@app/shared/models/base.model';

export type TProfile = TBase & {
  userId: string;
  firstName: string;
  lastName?: string;
  phone?: string;
  avatar?: string;
  gender?: string;
  dob?: Date;
  bio?: string;
  address?: string;
};
