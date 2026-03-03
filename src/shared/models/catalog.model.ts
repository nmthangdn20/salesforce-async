import { TBase } from '@app/shared/models/base.model';
import { TSource } from '@app/shared/models/source.model';

export type TSfObjectsCatalog = TBase & {
  sourceId: string;
  apiName: string;
  label?: string;
  isSelected: boolean;
  selectedBy?: string;
  selectedAt?: Date;
  source: TSource;
  sfFieldsCatalog: TSfFieldsCatalog[];
};

export type TSfFieldsCatalog = TBase & {
  objectId: string;
  apiName: string;
  label?: string;
  sfType: string;
  isRequired?: boolean;
  length?: number;
  precision?: number;
  scale?: number;
  isSelected: boolean;
  selectedBy?: string;
  selectedAt?: Date;
  object: TSfObjectsCatalog;
};
