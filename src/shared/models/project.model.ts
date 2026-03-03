import { TBase } from '@app/shared/models/base.model';
import { TUser } from '@app/shared/models/user.model';

export enum PROJECT_MEMBER_ROLE {
  OWNER = 'owner',
  ADMIN = 'admin',
  EDITOR = 'editor',
  VIEWER = 'viewer',
}

export type TProject = TBase & {
  name: string;
  slug?: string;
  projectMembers: TProjectMember[];
  sources: any[]; // TSource - forward reference
  targets: any[]; // TTarget - forward reference
};

export type TProjectMember = TBase & {
  projectId: string;
  userId: string;
  role: PROJECT_MEMBER_ROLE;
  project: TProject;
  user: TUser;
};
