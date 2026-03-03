import { TBase } from '@app/shared/models/base.model';

export type TFile = TBase & {
  userId: string;
  name: string;
  path: string;
  size: number;
  type: string;
  url: string;
};
