// src/db/drizzle.factory.ts
import { DbConnectionConfig } from '@app/core/modules/database/types';
import { MySql2Database } from 'drizzle-orm/mysql2';
import {
  drizzle as drizzlePg,
  NodePgDatabase,
} from 'drizzle-orm/node-postgres';
import { Pool as PgPool } from 'pg';
// import mysql from 'mysql2/promise';
// import { drizzle as drizzleMysql } from 'drizzle-orm/mysql2';
// import * as mysqlSchema from './mysql-schema'; // ví dụ, tuỳ bạn

export type AnyDrizzleDb =
  | (NodePgDatabase<Record<string, never>> & {
      $client: PgPool;
    })
  | MySql2Database<any>;

export class DrizzleConnectionFactory {
  // eslint-disable-next-line @typescript-eslint/require-await
  static async create(config: DbConnectionConfig): Promise<{
    db: AnyDrizzleDb;
    dispose: () => Promise<void>;
  }> {
    switch (config.type) {
      case 'pg':
        return this.createPgConnection(config);

      // case 'mysql':
      //   return this.createMysqlConnection(config);

      default:
        throw new Error(`Unsupported DB type: ${config.type}`);
    }
  }

  // --------------------------------------------------
  // PostgreSQL FACTORY
  // --------------------------------------------------
  private static createPgConnection(config: DbConnectionConfig): {
    db: AnyDrizzleDb;
    dispose: () => Promise<void>;
  } {
    const pool = new PgPool({
      host: config.host,
      port: config.port,
      user: config.username,
      password: config.password,
      database: config.database,
    });

    const db = drizzlePg(pool) as NodePgDatabase<Record<string, never>> & {
      $client: PgPool;
    };

    const dispose = async () => {
      await pool.end();
    };

    return { db, dispose };
  }

  // --------------------------------------------------
  // MySQL FACTORY (đang comment theo yêu cầu)
  // --------------------------------------------------
  // private static async createMysqlConnection(config: DbConnectionConfig): Promise<{
  //   db: AnyDrizzleDb;
  //   dispose: () => Promise<void>;
  // }> {
  //   const connection = await mysql.createConnection({
  //     host: config.host,
  //     port: config.port,
  //     user: config.user,
  //     password: config.password,
  //     database: config.database,
  //   });
  //
  //   const db = drizzleMysql(connection, { schema: mysqlSchema }) as MySql2Database<any>;
  //
  //   const dispose = async () => {
  //     await connection.end();
  //   };
  //
  //   return { db, dispose };
  // }
}
