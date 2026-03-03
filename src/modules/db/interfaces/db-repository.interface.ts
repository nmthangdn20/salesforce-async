import { DbColumn } from '../types/column.type';
import { ColumnRef, TableRef } from '../types/table-ref.type';

export interface IDbRepository {
  execute(sql: string, params?: any[]): Promise<void>;
  query<T = any>(sql: string, params?: any[]): Promise<T[]>;

  withTransaction<T>(fn: (tx: IDbRepository) => Promise<T>): Promise<T>;

  acquireAdvisoryLock(params: {
    lockKey: string | number;
    wait?: boolean;
    scope?: 'transaction' | 'session';
  }): Promise<void>;

  releaseAdvisoryLock(params: {
    lockKey: string | number;
    scope?: 'transaction' | 'session';
  }): Promise<void>;

  tableExists(params: TableRef): Promise<boolean>;
  getTableColumns(params: TableRef): Promise<DbColumn[]>;

  addColumn(
    params: ColumnRef & {
      typeSql: string;
      notNull?: boolean;
      defaultSql?: string;
      ifNotExists?: boolean;
    },
  ): Promise<void>;

  dropColumn(
    params: ColumnRef & {
      ifExists?: boolean;
      cascade?: boolean;
    },
  ): Promise<void>;

  alterColumnType(
    params: ColumnRef & {
      newTypeSql: string;
      usingSql?: string;
    },
  ): Promise<void>;

  setColumnNotNull(
    params: ColumnRef & {
      precheck?: boolean;
    },
  ): Promise<void>;

  dropColumnNotNull(params: ColumnRef): Promise<void>;

  setColumnDefault(
    params: ColumnRef & {
      defaultSql: string;
    },
  ): Promise<void>;

  dropColumnDefault(params: ColumnRef): Promise<void>;

  backfillColumn(
    params: ColumnRef & {
      valueSql: string;
      whereSql?: string;
      updateOnlyNull?: boolean;
    },
  ): Promise<number>;

  close?(): Promise<void>;
}
