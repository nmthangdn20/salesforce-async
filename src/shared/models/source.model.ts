import { TBase } from '@app/shared/models/base.model';
import { TProject } from '@app/shared/models/project.model';
import { TTarget } from './target.model';

export enum SOURCE_PROVIDER {
  SALESFORCE = 'salesforce',
  HUBSPOT = 'hubspot',
  CUSTOM = 'custom',
}

export enum SOURCE_ENVIRONMENT {
  PROD = 'prod',
  SANDBOX = 'sandbox',
  DEV = 'dev',
}

export enum SOURCE_STATUS {
  ACTIVE = 'active',
  DISABLED = 'disabled',
}

export enum AUTH_TYPE {
  OAUTH2 = 'oauth2',
  API_KEY = 'api_key',
  BASIC = 'basic',
}

export type TSource = TBase & {
  projectId: string;
  provider: SOURCE_PROVIDER;
  name: string;
  environment: SOURCE_ENVIRONMENT;
  status: SOURCE_STATUS;
  project: TProject;
  sourceSetting?: TSourceSetting;
  target: TTarget; // TTarget - forward reference
  sfObjectsCatalog: any[]; // TSfObjectsCatalog - forward reference
  objectMappings: any[]; // TObjectMapping - forward reference
  syncJobs: any[]; // TSyncJob - forward reference
};

export type TSourceSetting = TBase & {
  sourceId: string;
  instanceUrl: string;
  authType: AUTH_TYPE;
  scopes?: string[];
  secretsRef?: string;
  clientId?: string;
  clientSecret?: string;
  refreshToken?: string;
  accessToken?: string;
  expiresAt?: Date;
  source: TSource;
};
