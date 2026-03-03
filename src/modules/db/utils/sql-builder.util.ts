import { DesiredColumn } from '../types/column.type';
import { quoteIdentifier } from './sql-identifier.util';

/**
 * Build CREATE TABLE SQL statement
 */
export function buildCreateTableSql(
  schema: string,
  table: string,
  cols: DesiredColumn[],
  includeNotNull: boolean,
): string {
  const colsSql = cols.map(
    (c) =>
      `${quoteIdentifier(c.name)} ${c.typeSql}${includeNotNull && c.required ? ' NOT NULL' : ''}`,
  );

  const pkCols = cols.filter((c) => c.primaryKey);
  const pkConstraint =
    pkCols.length > 0
      ? `PRIMARY KEY (${pkCols.map((c) => quoteIdentifier(c.name)).join(', ')})`
      : null;

  const allParts = [...colsSql, ...(pkConstraint ? [pkConstraint] : [])];
  return `CREATE TABLE ${quoteIdentifier(schema)}.${quoteIdentifier(table)} (\n  ${allParts.join(',\n  ')}\n)`;
}
