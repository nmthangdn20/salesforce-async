import type {
  ParseEvent,
  PayloadWithHeader,
} from '@app/helper/modules/salesforce/types/grpc.types';

/** Alias for ParseEvent after deserialization (used by CdcGapHandlerService). */
export type DeserializedParseEvent = ParseEvent;

/**
 * JSON-safe representation of ParseEvent (bigint fields as string for Redis/BullMQ job storage).
 */
export type SerializedParseEvent = {
  id: string;
  replayId: number;
  payload: SerializedPayloadWithHeader;
  schema: string;
};

type SerializedPayloadWithHeader = Omit<
  PayloadWithHeader,
  'ChangeEventHeader'
> & {
  ChangeEventHeader: Omit<
    PayloadWithHeader['ChangeEventHeader'],
    'sequenceNumber' | 'commitTimestamp' | 'commitNumber'
  > & {
    sequenceNumber: string;
    commitTimestamp: string;
    commitNumber: string;
  };
};

const BIGINT_KEYS = [
  'sequenceNumber',
  'commitTimestamp',
  'commitNumber',
] as const;

export function serializeParseEvents(
  events: ParseEvent[],
): SerializedParseEvent[] {
  return events.map((event) => {
    const header = event.payload?.ChangeEventHeader;
    if (!header) {
      return event as unknown as SerializedParseEvent;
    }
    const serializedHeader = { ...header } as Record<string, unknown>;
    for (const key of BIGINT_KEYS) {
      const v = (header as Record<string, unknown>)[key];
      if (typeof v === 'bigint') {
        serializedHeader[key] = String(v);
      }
    }
    return {
      ...event,
      payload: {
        ...event.payload,
        ChangeEventHeader: serializedHeader,
      },
    } as unknown as SerializedParseEvent;
  });
}

export function deserializeParseEvents(
  serialized: SerializedParseEvent[],
): ParseEvent[] {
  return serialized.map((event) => {
    const header = event.payload?.ChangeEventHeader as
      | Record<string, unknown>
      | undefined;
    if (!header) {
      return event as unknown as ParseEvent;
    }
    const restoredHeader = { ...header };
    for (const key of BIGINT_KEYS) {
      const v = header[key];
      if (typeof v === 'string') {
        try {
          (restoredHeader as Record<string, unknown>)[key] = BigInt(v);
        } catch {
          (restoredHeader as Record<string, unknown>)[key] = v;
        }
      }
    }
    return {
      ...event,
      payload: {
        ...event.payload,
        ChangeEventHeader: restoredHeader,
      },
    } as unknown as ParseEvent;
  });
}
