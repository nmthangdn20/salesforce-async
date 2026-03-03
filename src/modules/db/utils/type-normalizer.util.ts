import { DbColumn } from '../types/column.type';

/**
 * Normalize SQL type string for comparison
 */
export function normalizeType(s: string): string {
  return s.trim().toLowerCase().replace(/\s+/g, ' ');
}

/**
 * Get type signature from DbColumn (from information_schema)
 */
export function getDbColumnTypeSig(c: DbColumn): string {
  const udt = (c.udtName || '').toLowerCase();

  if (udt === 'varchar' || udt === 'bpchar') {
    return c.characterMaximumLength
      ? `varchar(${c.characterMaximumLength})`
      : 'varchar';
  }

  if (udt === 'numeric') {
    const p = c.numericPrecision;
    const s = c.numericScale;
    if (p != null && s != null) return `decimal(${p}, ${s})`;
    return 'decimal';
  }

  if (udt === 'timestamptz') return 'timestamp with time zone';
  if (udt === 'timestamp') return 'timestamp without time zone';

  return udt || (c.dataType || '').toLowerCase();
}
