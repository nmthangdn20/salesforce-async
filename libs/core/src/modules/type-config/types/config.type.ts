export const CONFIG_TOKEN = {
  APP: 'app',
  JWT: 'jwt',
  DATABASE: 'database',
  REDIS: 'redis',
} as const;

export type TConfig = {
  [CONFIG_TOKEN.APP]: TAppConfig;
  [CONFIG_TOKEN.JWT]: TJwtConfig;
  [CONFIG_TOKEN.DATABASE]: TDatabaseConfig;
  [CONFIG_TOKEN.REDIS]: TRedisConfig;
};

export enum ENVIRONMENT {
  Development = 'development',
  Production = 'production',
  Test = 'test',
  Provision = 'provision',
}

export type TAppConfig = {
  nodeEnv: ENVIRONMENT;
  appName: string;
  appVersion: string;
  appDescription: string;
  apiDocPath: string;
  port: number;
  backendUrl: string;
  frontendUrl?: string;
  apiPrefix: string;
  corsOrigins: string[];
  transFormKeys: boolean;
};

export type TJwtConfig = {
  secret: string;
  expiresIn: string;
};

export type TDatabaseConfig = {
  host: string;
  port: number;
  username: string;
  password: string;
  database: string;
  schema: string;
};

export type TRedisConfig = {
  host: string;
  port: number;
  password?: string;
  db?: number;
};

export type IsPlainObject<T> = T extends object
  ? T extends any[]
    ? false
    : T extends (...args: any[]) => any
      ? false
      : true
  : false;

export type Leaves<T> = T extends object
  ? {
      [K in keyof T]: IsPlainObject<T[K]> extends true
        ? `${Exclude<K, symbol>}${Leaves<T[K]> extends never ? '' : `.${Leaves<T[K]>}`}`
        : `${Exclude<K, symbol>}`;
    }[keyof T]
  : never;

export type LeafTypes<T, S extends string> = S extends `${infer T1}.${infer T2}`
  ? T1 extends keyof T
    ? LeafTypes<T[T1], T2>
    : never
  : S extends keyof T
    ? T[S]
    : never;

export type Paths<T> = T extends object
  ? {
      [K in keyof T]: IsPlainObject<T[K]> extends true
        ? `${Exclude<K, symbol>}` | `${Exclude<K, symbol>}.${Paths<T[K]>}`
        : `${Exclude<K, symbol>}`;
    }[keyof T]
  : never;

export type PathTypes<T, S extends string> = S extends `${infer T1}.${infer T2}`
  ? T1 extends keyof T
    ? PathTypes<T[T1], T2>
    : never
  : S extends keyof T
    ? T[S]
    : never;
