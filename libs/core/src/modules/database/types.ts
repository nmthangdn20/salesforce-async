export type SupportedDbType = 'pg' | 'mysql';

export interface DbConnectionConfig {
  type: SupportedDbType;
  host: string;
  port: number;
  username: string;
  password: string;
  database: string;
  schema: string;
}
