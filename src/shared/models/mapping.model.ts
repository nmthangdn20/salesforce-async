import { TBase } from '@app/shared/models/base.model';
import { TSource } from '@app/shared/models/source.model';
import { TTarget } from '@app/shared/models/target.model';

export enum PK_STRATEGY {
  SF_ID = 'sf_id',
  CUSTOM = 'custom',
}

export type TObjectMapping = TBase & {
  sourceId: string;
  objectApiName: string;
  targetId: string;
  targetTable: string;
  pkStrategy: PK_STRATEGY;
  source: TSource;
  target: TTarget;
  fieldMappings: TFieldMapping[];
};

export type TFieldMapping = TBase & {
  objectMappingId: string;
  sfFieldApiName: string;
  targetColumn: string;
  logicalType: string;
  targetTypeOverride?: string;
  objectMapping: TObjectMapping;
};
