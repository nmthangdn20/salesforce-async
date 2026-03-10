import type { GapInfo } from '@app/helper/modules/salesforce/types/grpc.types';

import type { SerializedParseEvent } from '../utils/event-serializer.util';
import type { TObject } from './type';

export interface CdcJobPayload {
  events: SerializedParseEvent[];
  object: TObject;
  filename: string;
}

export interface GapJobPayload {
  gapInfo: GapInfo;
  filename: string;
  tenantId: string;
  topicName: string;
}
