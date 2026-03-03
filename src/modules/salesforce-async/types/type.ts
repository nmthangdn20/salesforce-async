import { DbConnectionConfig } from '@app/core/modules/database/types';

export type TObject = {
  name: string;
  apiName: string;
  cdcTopic: string;
  fieldNames: string[];
};

export type TSalesforceConfig = {
  filename: string;
  instanceUrl: string;
  clientId: string;
  clientSecret: string;
  scopes: string[];
  accessToken: string;
  refreshToken: string;
  expiresAt: Date;
  databaseConfig: DbConnectionConfig;
  /** Salesforce Org/Tenant ID (required for gRPC Pub/Sub API) */
  tenantId?: string;
  objects: TObject[];
};
