import { TBase } from '@app/shared/models/base.model';
import { TProject } from '@app/shared/models/project.model';
import { TSource } from '@app/shared/models/source.model';

export enum TARGET_KIND {
  POSTGRES = 'postgres',
  MYSQL = 'mysql',
  SQLSERVER = 'sqlserver',
  BIGQUERY = 'bigquery',
  SNOWFLAKE = 'snowflake',
  CLICKHOUSE = 'clickhouse',
  MONGODB = 'mongodb',
}

export enum CONNECTION_TYPE {
  HOST = 'host',
  URL = 'url',
}

export type TTarget = TBase & {
  projectId: string;
  sourceId: string;
  kind: TARGET_KIND;
  name: string;
  connectionType: CONNECTION_TYPE;
  // Database connection fields
  host?: string;
  port?: number;
  database?: string;
  username?: string;
  schema?: string;
  ssl: boolean;
  sslMode?: string;
  connectionString?: string;
  project: TProject;
  source: TSource;
  objectMappings: any[]; // TObjectMapping - forward reference
  syncJobs: any[]; // TSyncJob - forward reference
};
