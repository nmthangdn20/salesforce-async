export type DesiredColumn = {
  name: string;
  typeSql: string;
  primaryKey?: boolean;
  required: boolean;
  defaultSql?: string;
  usingSql?: string;
  backfillValueSql?: string;
};

export type DbColumn = {
  schema: string;
  table: string;
  columnName: string;

  dataType: string;
  udtName: string;

  isNullable: boolean;

  characterMaximumLength: number | null;
  numericPrecision: number | null;
  numericScale: number | null;

  columnDefault: string | null;
};
