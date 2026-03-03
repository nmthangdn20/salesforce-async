/**
 * Salesforce to PostgreSQL Type Mapper
 */

const SF_TO_PG_TYPE_MAP: Record<
  string,
  (length?: number, precision?: number, scale?: number) => string
> = {
  id: () => 'VARCHAR(18)',
  reference: () => 'VARCHAR(18)',
  string: (len) => `VARCHAR(${len || 255})`,
  textarea: (len) => (len && len > 10000 ? 'TEXT' : `VARCHAR(${len || 32000})`),
  boolean: () => 'BOOLEAN',
  int: () => 'INTEGER',
  long: () => 'BIGINT',
  double: (_, p, s) => `DECIMAL(${p || 18}, ${s || 2})`,
  currency: (_, p, s) => `DECIMAL(${p || 18}, ${s || 2})`,
  percent: (_, p, s) => `DECIMAL(${p || 18}, ${s || 2})`,
  date: () => 'DATE',
  datetime: () => 'TIMESTAMP WITH TIME ZONE',
  time: () => 'TIME',
  email: (len) => `VARCHAR(${len || 255})`,
  phone: (len) => `VARCHAR(${len || 40})`,
  url: (len) => `VARCHAR(${len || 255})`,
  picklist: (len) => `VARCHAR(${len || 255})`,
  multipicklist: (len) => `VARCHAR(${len || 4099})`,
  address: () => 'JSONB',
  location: () => 'POINT',
  base64: () => 'BYTEA',
  encryptedstring: (len) => `VARCHAR(${len || 255})`,
  combobox: (len) => `VARCHAR(${len || 255})`,
  anytype: () => 'JSONB',
};

export function sfTypeToPgType(
  sfType: string,
  length?: number,
  precision?: number,
  scale?: number,
): string {
  const mapper = SF_TO_PG_TYPE_MAP[sfType.toLowerCase()];
  if (!mapper) {
    return `VARCHAR(${length || 255})`;
  }
  return mapper(length, precision, scale);
}
