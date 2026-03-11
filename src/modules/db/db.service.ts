import { TDatabaseConfig } from '@app/core/modules/type-config/types/config.type';
import { Injectable, OnModuleDestroy } from '@nestjs/common';
import { Pool } from 'pg';

import { DbRepository } from './db.repository';
import { IDbRepository } from './interfaces/db-repository.interface';
import { DesiredColumn, SyncPolicy } from './types';
import {
  buildCreateTableSql,
  getDbColumnTypeSig,
  normalizeType,
  quoteIdentifier,
} from './utils';

const poolMap = new Map<string, Pool>();
@Injectable()
export class DbService implements OnModuleDestroy {
  getRepo(config: TDatabaseConfig, key: string): IDbRepository {
    const pool = this.getOrCreatePool(key, config);
    return new DbRepository(pool, key, (k: string) => this.close(k));
  }

  async close(key: string): Promise<void> {
    const pool = poolMap.get(key);
    if (!pool) return;
    await pool.end();
    poolMap.delete(key);
  }

  async onModuleDestroy(): Promise<void> {
    await Promise.all([...poolMap.keys()].map((k) => this.close(k)));
  }

  async ensureTableSchema(params: {
    repo: IDbRepository;
    key: string;
    table: string;
    columns: DesiredColumn[];
    policy?: SyncPolicy;
  }): Promise<void> {
    const { repo, key, table, columns } = params;

    const policy: Required<SyncPolicy> = {
      allowCreateTable: true,
      allowAddColumn: true,
      allowDropColumn: true,
      allowTypeChange: true,
      allowSetNotNull: false,
      allowDropNotNull: false,
      schema: 'public',
      ...(params.policy ?? {}),
    };

    const schema = policy.schema ?? 'public';
    const lockKey = `${key}:${schema}.${table}`;

    const desiredMap = new Map(columns.map((c) => [c.name, c]));

    await repo.withTransaction(async (tx) => {
      await tx.acquireAdvisoryLock({
        lockKey,
        scope: 'transaction',
        wait: true,
      });

      const exists = await tx.tableExists({ schema, table });

      if (!exists) {
        if (!policy.allowCreateTable) {
          throw new Error(
            `Table not found and allowCreateTable=false: ${schema}.${table}`,
          );
        }

        const query = buildCreateTableSql(schema, table, columns, false);
        await tx.execute(query);
        return;
      }

      const currentCols = await tx.getTableColumns({ schema, table });
      const currentMap = new Map(currentCols.map((c) => [c.columnName, c]));

      // Add missing columns (do not apply NOT NULL for now)
      for (const col of columns) {
        if (currentMap.has(col.name)) continue;
        if (!policy.allowAddColumn) throw new Error(`allowAddColumn=false`);

        await tx.addColumn({
          schema,
          table,
          column: col.name,
          typeSql: col.typeSql,
          notNull: false,
          ifNotExists: true,
        });
      }

      // Update existing columns (type only)
      for (const col of columns) {
        const cur = currentMap.get(col.name);
        if (!cur) continue;

        if (!policy.allowTypeChange) continue;

        const curType = normalizeType(getDbColumnTypeSig(cur));
        const wantType = normalizeType(col.typeSql);

        if (curType !== wantType) {
          await tx.alterColumnType({
            schema,
            table,
            column: col.name,
            newTypeSql: col.typeSql,
            usingSql: col.usingSql,
          });
        }
      }

      // Drop extra columns (optional)
      if (policy.allowDropColumn) {
        for (const cur of currentCols) {
          if (!desiredMap.has(cur.columnName)) {
            await tx.dropColumn({
              schema,
              table,
              column: cur.columnName,
              ifExists: true,
              cascade: false,
            });
          }
        }
      }
    });
  }

  async updateRecord(
    repo: IDbRepository,
    table: string,
    id: string,
    data: Record<string, any>,
  ): Promise<void> {
    const keys = Object.keys(data);
    if (keys.length === 0) return;

    const setClause = keys.map((k, i) => `"${k}" = $${i + 1}`).join(', ');
    const query = `
      UPDATE ${quoteIdentifier(table)}
      SET ${setClause}
      WHERE "Id" = $${keys.length + 1}
    `;

    const params = keys.map((k) => data[k]).concat(id);
    await repo.execute(query, params);
  }

  async insertMany<T extends Record<string, unknown>>(
    repo: IDbRepository,
    table: string,
    data: T[],
  ): Promise<void> {
    if (!data?.length) return;

    const keys = Object.keys(data[0]) as (keyof T)[];
    const values: unknown[] = [];

    let paramIndex = 1;

    const placeholders = data
      .map((row) => {
        const rowPlaceholders = keys.map(() => `$${paramIndex++}`).join(', ');
        values.push(...keys.map((k) => row[k]));
        return `(${rowPlaceholders})`;
      })
      .join(', ');

    const query = `
      INSERT INTO ${quoteIdentifier(table)} (${keys.map((k) => `"${String(k)}"`).join(', ')})
      VALUES ${placeholders}
    `;

    await repo.execute(query, values);
  }

  /**
   * Upsert by Id: INSERT with ON CONFLICT ("Id") DO UPDATE.
   * Used for CDC CREATE to be idempotent on replay.
   */
  async upsertMany<T extends Record<string, unknown>>(
    repo: IDbRepository,
    table: string,
    data: T[],
  ): Promise<void> {
    if (!data?.length) return;

    const keys = Object.keys(data[0]) as (keyof T)[];
    const values: unknown[] = [];

    let paramIndex = 1;

    const placeholders = data
      .map((row) => {
        const rowPlaceholders = keys.map(() => `$${paramIndex++}`).join(', ');
        values.push(...keys.map((k) => row[k]));
        return `(${rowPlaceholders})`;
      })
      .join(', ');

    const updateCols = keys.filter((k) => k !== 'Id');
    const setClause = updateCols
      .map((k) => `"${String(k)}" = EXCLUDED."${String(k)}"`)
      .join(', ');

    const onConflictClause =
      setClause === ''
        ? 'ON CONFLICT ("Id") DO NOTHING'
        : `ON CONFLICT ("Id") DO UPDATE SET ${setClause}`;

    const query = `
      INSERT INTO ${quoteIdentifier(table)} (${keys.map((k) => `"${String(k)}"`).join(', ')})
      VALUES ${placeholders}
      ${onConflictClause}
    `;

    await repo.execute(query, values);
  }

  async softDeleteMany(
    repo: IDbRepository,
    table: string,
    ids: string[],
  ): Promise<void> {
    const query = `
      UPDATE ${quoteIdentifier(table)}
      SET "IsDeleted" = true
      WHERE "Id" = ANY($1)
    `;

    await repo.execute(query, [ids]);
  }

  async undeleteMany(
    repo: IDbRepository,
    table: string,
    ids: string[],
  ): Promise<void> {
    const query = `
      UPDATE ${quoteIdentifier(table)}
      SET "IsDeleted" = false
      WHERE "Id" = ANY($1)
    `;

    await repo.execute(query, [ids]);
  }

  /**
   * Returns IDs that exist in the table
   */
  async getExistingIds(
    repo: IDbRepository,
    table: string,
    ids: string[],
  ): Promise<string[]> {
    if (!ids?.length) return [];
    const query = `
      SELECT "Id" FROM ${quoteIdentifier(table)}
      WHERE "Id" = ANY($1)
    `;
    const rows = await repo.query<{ Id: string }>(query, [ids]);
    return rows.map((r) => r.Id);
  }

  async updateMany(
    repo: IDbRepository,
    table: string,
    ids: string[],
    data: Record<string, any>,
  ): Promise<void> {
    if (!ids?.length) return;

    const keys = Object.keys(data);
    if (keys.length === 0) return;

    const setClause = keys.map((k, i) => `"${k}" = $${i + 1}`).join(', ');

    const idsParamIndex = keys.length + 1;

    const query = `
      UPDATE ${quoteIdentifier(table)}
      SET ${setClause}
      WHERE "Id" = ANY($${idsParamIndex})
    `;

    const params = keys.map((k) => data[k]).concat([ids]);
    await repo.execute(query, params);
  }

  private getOrCreatePool(key: string, config: TDatabaseConfig): Pool {
    const existed = poolMap.get(key);
    if (existed) return existed;

    const pool = new Pool({
      host: config.host,
      port: config.port,
      user: config.username,
      password: config.password,
      database: config.database,
    });

    poolMap.set(key, pool);
    return pool;
  }
}
