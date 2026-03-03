import { Pool } from 'pg';

import { DEFAULT_SCHEMA } from './constants/db.constant';
import { IDbRepository } from './interfaces/db-repository.interface';
import { TxDbRepository } from './tx-db.repository';
import { ColumnRef, DbColumn, TableRef } from './types';
import { quoteIdentifier, quoteTable } from './utils';

export class DbRepository implements IDbRepository {
  constructor(
    private readonly pool: Pool,
    public readonly key: string,
    private readonly closer: (key: string) => Promise<void>,
  ) {}

  async close(): Promise<void> {
    await this.closer(this.key);
  }

  async execute(sql: string, params: any[] = []): Promise<void> {
    await this.pool.query(sql, params);
  }

  async query<T = any>(sql: string, params: any[] = []): Promise<T[]> {
    const res = await this.pool.query(sql, params);
    return res.rows as T[];
  }

  async withTransaction<T>(fn: (tx: IDbRepository) => Promise<T>): Promise<T> {
    const client = await this.pool.connect();
    try {
      await client.query('BEGIN');
      const txRepo = new TxDbRepository(client);
      const result = await fn(txRepo as unknown as IDbRepository);
      await client.query('COMMIT');
      return result;
    } catch (e) {
      await client.query('ROLLBACK');
      throw e;
    } finally {
      client.release();
    }
  }

  async acquireAdvisoryLock(params: {
    lockKey: string | number;
    wait?: boolean;
    scope?: 'transaction' | 'session';
  }): Promise<void> {
    const scope = params.scope ?? 'transaction';

    if (scope === 'session') {
      // session lock can be taken directly using the pool (any connection will do,
      // but note: it will be held by that connection, not the whole pool).
      const client = await this.pool.connect();
      try {
        const tx = new TxDbRepository(client);
        await tx.acquireAdvisoryLock({ ...params, scope: 'session' });
      } finally {
        client.release();
      }
      return;
    }

    await this.withTransaction(async (tx) => {
      await tx.acquireAdvisoryLock({ ...params, scope: 'transaction' });
    });
  }

  async releaseAdvisoryLock(params: {
    lockKey: string | number;
    scope?: 'transaction' | 'session';
  }): Promise<void> {
    const scope = params.scope ?? 'transaction';

    if (scope === 'transaction') {
      // transaction locks are auto-released on commit/rollback
      return;
    }

    const client = await this.pool.connect();
    try {
      const tx = new TxDbRepository(client);
      await tx.releaseAdvisoryLock({ ...params, scope: 'session' });
    } finally {
      client.release();
    }
  }

  async tableExists(params: TableRef): Promise<boolean> {
    const schema = params.schema ?? DEFAULT_SCHEMA;
    const res = await this.pool.query(
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
    const res = await this.pool.query(
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
      WHERE table_schema = $1 AND table_name = $2
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

    await this.pool.query(ddl);
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

    await this.pool.query(ddl);
  }

  async alterColumnType(
    params: ColumnRef & { newTypeSql: string; usingSql?: string },
  ): Promise<void> {
    const schema = params.schema ?? DEFAULT_SCHEMA;

    const ddl =
      `ALTER TABLE ${quoteTable(schema, params.table)} ` +
      `ALTER COLUMN ${quoteIdentifier(params.column)} TYPE ${params.newTypeSql}` +
      (params.usingSql ? ` USING ${params.usingSql}` : '');

    await this.pool.query(ddl);
  }

  async setColumnNotNull(
    params: ColumnRef & { precheck?: boolean },
  ): Promise<void> {
    const schema = params.schema ?? DEFAULT_SCHEMA;
    const precheck = params.precheck ?? true;

    if (precheck) {
      const res = await this.pool.query(
        `SELECT COUNT(*)::bigint AS cnt FROM ${quoteTable(schema, params.table)} WHERE ${quoteIdentifier(params.column)} IS NULL`,
      );
      const cnt = BigInt((res.rows?.[0]?.cnt as string) ?? '0');
      if (cnt > 0n) {
        throw new Error(
          `Cannot set NOT NULL on ${schema}.${params.table}.${params.column}: found ${cnt.toString()} NULL rows`,
        );
      }
    }

    await this.pool.query(
      `ALTER TABLE ${quoteTable(schema, params.table)} ALTER COLUMN ${quoteIdentifier(params.column)} SET NOT NULL`,
    );
  }

  async dropColumnNotNull(params: ColumnRef): Promise<void> {
    const schema = params.schema ?? DEFAULT_SCHEMA;
    await this.pool.query(
      `ALTER TABLE ${quoteTable(schema, params.table)} ALTER COLUMN ${quoteIdentifier(params.column)} DROP NOT NULL`,
    );
  }

  async setColumnDefault(
    params: ColumnRef & { defaultSql: string },
  ): Promise<void> {
    const schema = params.schema ?? DEFAULT_SCHEMA;
    await this.pool.query(
      `ALTER TABLE ${quoteTable(schema, params.table)} ALTER COLUMN ${quoteIdentifier(params.column)} SET DEFAULT ${params.defaultSql}`,
    );
  }

  async dropColumnDefault(params: ColumnRef): Promise<void> {
    const schema = params.schema ?? DEFAULT_SCHEMA;
    await this.pool.query(
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

    const res = await this.pool.query(
      `UPDATE ${quoteTable(schema, params.table)} SET ${quoteIdentifier(params.column)} = ${params.valueSql}${where}`,
    );
    return res.rowCount ?? 0;
  }
}
