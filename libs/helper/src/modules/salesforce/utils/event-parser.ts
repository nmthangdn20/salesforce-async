import { Schema, Type } from 'avsc';

import {
  ConsumerEvent,
  ParseEvent,
  PayloadWithHeader,
  TChangeEventHeader,
} from '../types/grpc.types';
import { decodeChangedFieldsFromAvroType, longType } from './avro-decoder';

/**
 * Salesforce CDC date/datetime fields that come as bigint (epoch milliseconds)
 */
const DATE_FIELDS = new Set([
  'LastModifiedDate',
  'CreatedDate',
  'SystemModstamp',
  'LastActivityDate',
  'LastViewedDate',
  'LastReferencedDate',
]);

type AvroTypeObject = { type: string; logicalType?: string };
type AvroFieldType = string | (string | AvroTypeObject)[] | AvroTypeObject;

type AvroSchemaField = {
  name: string;
  type: AvroFieldType;
  doc?: string;
};

type AvroSchema = Schema & {
  fields?: AvroSchemaField[];
};

/**
 * Check if field type contains 'long'
 */
function fieldTypeIncludesLong(type: AvroFieldType): boolean {
  if (typeof type === 'string') {
    return type === 'long';
  }
  if (Array.isArray(type)) {
    return type.some(
      (t) => t === 'long' || (typeof t === 'object' && t?.type === 'long'),
    );
  }
  if (typeof type === 'object' && type !== null) {
    return type.type === 'long';
  }
  return false;
}

/**
 * Convert bigint timestamps to ISO string for PostgreSQL compatibility
 */
function convertBigintTimestamps(
  payload: PayloadWithHeader,
  schema: AvroSchema,
): PayloadWithHeader {
  const result: PayloadWithHeader = {
    ChangeEventHeader: payload.ChangeEventHeader,
  };

  for (const [key, value] of Object.entries(payload)) {
    if (key === 'ChangeEventHeader') continue;

    const field = schema.fields?.find((f) => f.name === key);

    if (
      field &&
      fieldTypeIncludesLong(field.type) &&
      field.doc?.includes('Date')
    ) {
      result[key] = new Date(Number(value)).toISOString();
    } else if (DATE_FIELDS.has(key) && typeof value === 'bigint') {
      result[key] = new Date(Number(value)).toISOString();
    } else if (typeof value === 'bigint') {
      result[key] = Number(value);
    } else {
      result[key] = value;
    }
  }

  return result;
}

export const parseEvent = (
  event: ConsumerEvent,
  schemaJson: string,
  avroTypeCache: Map<string, Type>,
  _compoundFieldMapping: Record<string, string[]>,
): ParseEvent => {
  const schemaId = event.event.schema_id;
  const schema = JSON.parse(schemaJson) as AvroSchema;

  let avroType = avroTypeCache.get(schemaId);
  if (!avroType) {
    avroType = Type.forSchema(schema, {
      registry: { long: longType },
    });
    avroTypeCache.set(schemaId, avroType);
  }

  const data = avroType.decode(event.event.payload) as unknown as {
    value: Record<string, unknown>;
  };

  const payload = data.value as unknown as {
    ChangeEventHeader: TChangeEventHeader;
  };

  if (payload?.ChangeEventHeader) {
    try {
      payload.ChangeEventHeader.changedFields = decodeChangedFieldsFromAvroType(
        avroType,
        payload.ChangeEventHeader?.changedFields,
      );
    } catch (error) {
      throw new Error(`Failed to decode changed fields`, { cause: error });
    }

    try {
      payload.ChangeEventHeader.nulledFields = decodeChangedFieldsFromAvroType(
        avroType,
        payload.ChangeEventHeader?.nulledFields,
      );
    } catch (error) {
      throw new Error(`Failed to decode changed fields`, { cause: error });
    }

    try {
      payload.ChangeEventHeader.diffFields = decodeChangedFieldsFromAvroType(
        avroType,
        payload.ChangeEventHeader?.diffFields,
      );
    } catch (error) {
      throw new Error(`Failed to decode changed fields`, { cause: error });
    }
  }

  const filteredPayload =
    payload.ChangeEventHeader.changeType === 'UPDATE'
      ? filterEventPayloadByFieldPaths(payload)
      : payload;

  // Convert bigint timestamps to ISO strings for PostgreSQL
  const convertedPayload = convertBigintTimestamps(filteredPayload, schema);

  return {
    id: event.event.id,
    replayId: decodeReplayId(event.replay_id),
    payload: convertedPayload,
    schema: schemaJson,
  };
};

export const decodeReplayId = (encodedReplayId: Buffer) => {
  return Number(encodedReplayId.readBigUInt64BE());
};

function filterEventPayloadByFieldPaths(
  payload: PayloadWithHeader,
): PayloadWithHeader {
  const header = payload?.ChangeEventHeader;
  const fieldPaths: string[] = [
    ...(header?.changedFields ?? []),
    ...(header?.nulledFields ?? []),
    ...(header?.diffFields ?? []),
  ];

  const keep = new Set(fieldPaths);

  const out: PayloadWithHeader = { ChangeEventHeader: header };

  for (const path of keep) {
    if (!path || path === 'ChangeEventHeader') continue;

    if (!path.includes('.')) {
      if (path in payload) out[path] = payload[path];
      continue;
    }

    const [root, child] = path.split('.', 2);
    const rootVal = payload[root] as Record<string, unknown> | undefined;

    if (rootVal && typeof rootVal === 'object') {
      if (!(root in out)) out[root] = {};
      (out[root] as Record<string, unknown>)[child] = rootVal[child];
    }
  }

  return out;
}
