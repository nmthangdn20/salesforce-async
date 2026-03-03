import { PoolClient } from 'pg';

import { DEFAULT_SCHEMA } from './constants/db.constant';
import { IDbRepository } from './interfaces/db-repository.interface';
import { ColumnRef, DbColumn, TableRef } from './types';
import { quoteIdentifier, quoteTable } from './utils';

export class TxDbRepository implements IDbRepository {
  constructor(private readonly client: PoolClient) {}

  async execute(sql: string, params: any[] = []): Promise<void> {
    await this.client.query(sql, params);
  }

  async query<T = any>(sql: string, params: any[] = []): Promise<T[]> {
    const res = await this.client.query(sql, params);
    return res.rows as T[];
  }

  async withTransaction<T>(fn: (tx: IDbRepository) => Promise<T>): Promise<T> {
    // already in a transaction
    return fn(this);
  }

  async acquireAdvisoryLock(params: {
    lockKey: string | number;
    wait?: boolean;
    scope?: 'transaction' | 'session';
  }): Promise<void> {
    const wait = params.wait ?? true;
    const scope = params.scope ?? 'transaction';

    const fn =
      scope === 'transaction'
        ? wait
          ? 'pg_advisory_xact_lock'
          : 'pg_try_advisory_xact_lock'
        : wait
          ? 'pg_advisory_lock'
          : 'pg_try_advisory_lock';

    const res = await this.client.query(
      `SELECT ${fn}(${this.lockKeySql(params.lockKey)}) AS ok`,
    );

    if (!wait && !res.rows[0]?.ok) {
      throw new Error(
        `Failed to acquire advisory lock: ${String(params.lockKey)}`,
      );
    }
  }

  async releaseAdvisoryLock(params: {
    lockKey: string | number;
    scope?: 'transaction' | 'session';
  }): Promise<void> {
    if ((params.scope ?? 'transaction') === 'transaction') return;

    const res = await this.client.query(
      `SELECT pg_advisory_unlock(${this.lockKeySql(params.lockKey)}) AS ok`,
    );

    if (!res.rows[0]?.ok) {
      throw new Error(
        `Failed to release advisory lock: ${String(params.lockKey)}`,
      );
    }
  }

  async tableExists(params: TableRef): Promise<boolean> {
    const schema = params.schema ?? DEFAULT_SCHEMA;
    const res = await this.client.query(
      `
      SELECT 1
      FROM information_schema.tables
      WHERE table_schema = $1 AND table_name = $2
      LIMIT 1
      `,
      [schema, params.table],
    );
    return (res.rowCount ?? 0) > 0;
  }

  async getTableColumns(params: TableRef): Promise<DbColumn[]> {
    const schema = params.schema ?? DEFAULT_SCHEMA;

    const res = await this.client.query(
      `
      SELECT
        table_schema,
        table_name,
        column_name,
        data_type,
        udt_name,
        is_nullable,
        character_maximum_length,
        numeric_precision,
        numeric_scale,
        column_default
      FROM information_schema.columns
      WHERE table_schema = $1
        AND table_name = $2
      ORDER BY ordinal_position
      `,
      [schema, params.table],
    );

    return res.rows.map((r) => ({
      schema: r.table_schema as string,
      table: r.table_name as string,
      columnName: r.column_name as string,
      dataType: r.data_type as string,
      udtName: r.udt_name as string,
      isNullable: (r.is_nullable as string) === 'YES',
      characterMaximumLength: (r.character_maximum_length as number) ?? null,
      numericPrecision: (r.numeric_precision as number) ?? null,
      numericScale: (r.numeric_scale as number) ?? null,
      columnDefault: (r.column_default as string) ?? null,
    }));
  }

  async addColumn(
    params: ColumnRef & {
      typeSql: string;
      notNull?: boolean;
      defaultSql?: string;
      ifNotExists?: boolean;
    },
  ): Promise<void> {
    const schema = params.schema ?? DEFAULT_SCHEMA;
    const ifNotExists = params.ifNotExists ?? true;

    const ddl =
      `ALTER TABLE ${quoteTable(schema, params.table)} ` +
      `ADD COLUMN ${ifNotExists ? 'IF NOT EXISTS ' : ''}${quoteIdentifier(params.column)} ${params.typeSql}` +
      (params.defaultSql !== undefined ? ` DEFAULT ${params.defaultSql}` : '') +
      (params.notNull ? ` NOT NULL` : '');

    await this.client.query(ddl);
  }

  async dropColumn(
    params: ColumnRef & { ifExists?: boolean; cascade?: boolean },
  ): Promise<void> {
    const schema = params.schema ?? DEFAULT_SCHEMA;
    const ifExists = params.ifExists ?? true;
    const cascade = params.cascade ?? false;

    const ddl =
      `ALTER TABLE ${quoteTable(schema, params.table)} ` +
      `DROP COLUMN ${ifExists ? 'IF EXISTS ' : ''}${quoteIdentifier(params.column)}` +
      (cascade ? ' CASCADE' : '');

    await this.client.query(ddl);
  }

  async alterColumnType(
    params: ColumnRef & { newTypeSql: string; usingSql?: string },
  ): Promise<void> {
    const schema = params.schema ?? DEFAULT_SCHEMA;

    const ddl =
      `ALTER TABLE ${quoteTable(schema, params.table)} ` +
      `ALTER COLUMN ${quoteIdentifier(params.column)} TYPE ${params.newTypeSql}` +
      (params.usingSql ? ` USING ${params.usingSql}` : '');

    await this.client.query(ddl);
  }

  async setColumnNotNull(
    params: ColumnRef & { precheck?: boolean },
  ): Promise<void> {
    const schema = params.schema ?? DEFAULT_SCHEMA;
    const precheck = params.precheck ?? true;

    if (precheck) {
      const res = await this.client.query(
        `SELECT COUNT(*)::bigint AS cnt FROM ${quoteTable(schema, params.table)} WHERE ${quoteIdentifier(params.column)} IS NULL`,
      );
      const cnt = BigInt((res.rows?.[0]?.cnt as string) ?? '0');
      if (cnt > 0n) {
        throw new Error(
          `Cannot set NOT NULL on ${schema}.${params.table}.${params.column}: found ${cnt.toString()} NULL rows`,
        );
      }
    }

    await this.client.query(
      `ALTER TABLE ${quoteTable(schema, params.table)} ALTER COLUMN ${quoteIdentifier(params.column)} SET NOT NULL`,
    );
  }

  async dropColumnNotNull(params: ColumnRef): Promise<void> {
    const schema = params.schema ?? DEFAULT_SCHEMA;
    await this.client.query(
      `ALTER TABLE ${quoteTable(schema, params.table)} ALTER COLUMN ${quoteIdentifier(params.column)} DROP NOT NULL`,
    );
  }

  async setColumnDefault(
    params: ColumnRef & { defaultSql: string },
  ): Promise<void> {
    const schema = params.schema ?? DEFAULT_SCHEMA;
    await this.client.query(
      `ALTER TABLE ${quoteTable(schema, params.table)} ALTER COLUMN ${quoteIdentifier(params.column)} SET DEFAULT ${params.defaultSql}`,
    );
  }

  async dropColumnDefault(params: ColumnRef): Promise<void> {
    const schema = params.schema ?? DEFAULT_SCHEMA;
    await this.client.query(
      `ALTER TABLE ${quoteTable(schema, params.table)} ALTER COLUMN ${quoteIdentifier(params.column)} DROP DEFAULT`,
    );
  }

  async backfillColumn(
    params: ColumnRef & {
      valueSql: string;
      whereSql?: string;
      updateOnlyNull?: boolean;
    },
  ): Promise<number> {
    const schema = params.schema ?? DEFAULT_SCHEMA;
    const updateOnlyNull = params.updateOnlyNull ?? true;

    const cond: string[] = [];
    if (updateOnlyNull) cond.push(`${quoteIdentifier(params.column)} IS NULL`);
    if (params.whereSql) cond.push(`(${params.whereSql})`);
    const where = cond.length ? ` WHERE ${cond.join(' AND ')}` : '';

    const res = await this.client.query(
      `UPDATE ${quoteTable(schema, params.table)} SET ${quoteIdentifier(params.column)} = ${params.valueSql}${where}`,
    );
    return res.rowCount ?? 0;
  }

  private lockKeySql(key: string | number): string {
    return typeof key === 'number'
      ? `${key}`
      : `hashtext('${String(key).replace(/'/g, "''")}')`;
  }
}
