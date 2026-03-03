import { TBase } from '@app/shared/models/base.model';

export type TTypeDictionary = TBase & {
  sfType: string;
  logicalType: string;
  pgType?: string;
  mysqlType?: string;
  sqlserverType?: string;
  bigqueryType?: string;
  snowflakeType?: string;
  clickhouseType?: string;
};
