/**
 * Quote a SQL identifier (column name, table name, etc.)
 */
export function quoteIdentifier(name: string): string {
  return `"${String(name).replace(/"/g, '""')}"`;
}

/**
 * Quote a schema.table reference
 */
export function quoteTable(schema: string, table: string): string {
  return `${quoteIdentifier(schema)}.${quoteIdentifier(table)}`;
}
