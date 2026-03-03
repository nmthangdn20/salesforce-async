import { Type, types } from 'avsc';

type AvroField = {
  getName(): string;
  getType(): AvroType;
};

type AvroType = {
  getFields?(): AvroField[];
  getTypes?(): AvroType[];
};

/**
 * Long type is used to decode long values from Avro.
 */
export const longType = types.LongType.__with({
  fromBuffer: (buf: Buffer) => buf.readBigInt64LE(),
  toBuffer: (n: bigint) => {
    const buf = Buffer.alloc(8);
    buf.writeBigInt64LE(n);
    return buf;
  },
  fromJSON: BigInt,
  toJSON: Number,
  isValid: (n: unknown) => typeof n === 'bigint',
  compare: (n1: bigint, n2: bigint) => (n1 === n2 ? 0 : n1 < n2 ? -1 : 1),
});

const bitmapToFieldNames = (hex: string, fieldNames: string[]): string[] => {
  const bits = BigInt(hex);
  const out: string[] = [];

  for (let i = 0; i < fieldNames.length; i++) {
    if ((bits & (1n << BigInt(i))) !== 0n) out.push(fieldNames[i]);
  }
  return out;
};

export const decodeChangedFieldsFromAvroType = (
  avroType: Type,
  changedFields: string[] | null | undefined,
): string[] => {
  if (!changedFields?.length) return [];

  const topFields = (avroType as unknown as AvroType).getFields?.() ?? [];
  const topNames = topFields.map((f) => f.getName());

  const out: string[] = [];

  const first = changedFields[0];
  if (first?.startsWith('0x')) {
    out.push(...bitmapToFieldNames(first, topNames));
  }

  for (const entry of changedFields.slice(1)) {
    const m = entry.match(/^(\d+)-0x([0-9a-fA-F]+)$/);
    if (!m) continue;

    const parentPos = Number(m[1]);
    const nestedHex = '0x' + m[2];

    const parentField = topFields[parentPos];
    if (!parentField) continue;

    const parentName = parentField.getName();

    const nestedNames = getNestedFieldNames(parentField);
    if (!nestedNames.length) continue;

    const nested = bitmapToFieldNames(nestedHex, nestedNames);
    for (const n of nested) out.push(`${parentName}.${n}`);
  }

  return out;
};

const getNestedFieldNames = (parentField: AvroField): string[] => {
  const t: AvroType | undefined = parentField.getType?.();
  const union = t?.getTypes?.();
  const candidates = union?.length ? union : [t];

  for (const c of candidates) {
    const nested = c?.getFields?.();
    if (nested?.length) return nested.map((f) => f.getName());
  }
  return [];
};
