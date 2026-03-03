import type * as grpc from '@grpc/grpc-js';

// ============ Enums ============
export enum ReplayPreset {
  LATEST = 0, // Receive only NEW events from subscribe point onwards
  EARLIEST = 1, // Receive all events from the beginning of the topic
  CUSTOM = 2, // Receive events from a specific replay ID
}

// ============ Messages ============
export type TopicInfo = {
  topic_name: string;
  tenant_guid: string;
  can_publish: boolean;
  can_subscribe: boolean;
  schema_id: string;
  rpc_id: string;
};

export type TopicRequest = {
  topic_name: string;
};

export type EventHeader = {
  key: string;
  value: Buffer;
};

export type ProducerEvent = {
  id: string;
  schema_id: string;
  payload: Buffer;
  headers: EventHeader[];
};

export type ConsumerEvent = {
  event: ProducerEvent;
  replay_id: Buffer;
};

export type FetchRequest = {
  topic_name: string;
  replay_preset?: ReplayPreset;
  replay_id?: Buffer;
  num_requested: number;
  auth_refresh?: string;
};

export type FetchResponse = {
  events: ConsumerEvent[];
  latest_replay_id: Buffer;
  rpc_id: string;
  pending_num_requested: number;
};

export type SchemaRequest = {
  schema_id: string;
};

export type SchemaInfo = {
  schema_json: string;
  schema_id: string;
  rpc_id: string;
};

// ============ Service Client Types ============
export type PubSubClient = grpc.Client & {
  // Metadata is passed when creating the stream (required for auth)
  Subscribe(
    metadata?: grpc.Metadata,
  ): grpc.ClientDuplexStream<FetchRequest, FetchResponse>;

  GetSchema(
    request: SchemaRequest,
    metadata: grpc.Metadata,
    callback: (error: grpc.ServiceError | null, response: SchemaInfo) => void,
  ): grpc.ClientUnaryCall;

  GetTopic(
    request: TopicRequest,
    metadata: grpc.Metadata,
    callback: (error: grpc.ServiceError | null, response: TopicInfo) => void,
  ): grpc.ClientUnaryCall;
};

export type EventBusV1Package = {
  PubSub: grpc.ServiceClientConstructor;
};

// ============ Config & Decoded Event ============
export type SalesforceGrpcConfig = {
  accessToken: string;
  instanceUrl: string;
  tenantId: string;
};

export type ChangeType =
  | 'UPDATE'
  | 'DELETE'
  | 'CREATE'
  | 'UNDELETE'
  | 'GAP_CREATE'
  | 'GAP_UPDATE'
  | 'GAP_DELETE'
  | 'GAP_UNDELETE'
  | 'GAP_OVERFLOW';

export type PayloadWithHeader = {
  ChangeEventHeader: TChangeEventHeader;
} & Record<string, unknown>;

export type ParseEvent = {
  id: string;
  replayId: number;
  payload: PayloadWithHeader;
  schema: string;
};

export type TChangeEventHeader = {
  entityName: string;
  recordIds: string[];
  changeType: ChangeType;
  changeOrigin: string;
  transactionKey: string;
  sequenceNumber: bigint;
  commitTimestamp: bigint;
  commitNumber: bigint;
  commitUser: string;
  nulledFields: string[];
  diffFields: string[];
  changedFields: string[];
};

// ============ Subscription Types ============
export interface SubscriptionOptions {
  numRequested?: number;
  replayPreset?: ReplayPreset;
  replayId?: Buffer;
}

export type GapInfo = {
  type: 'NORMAL' | 'FULL_RESYNC';
  topic: string;
  objectName: string;
  fromReplayId: string;
  toReplayId: string;
  fromTimestamp: number;
  toTimestamp: number;
  /** Record IDs from gap event payload (NORMAL only). Used to reconcile by ID per Salesforce doc. */
  recordIds?: string[];
};

export interface SubscriptionConfig {
  config: SalesforceGrpcConfig;
  topicName: string;
  objectName: string;
  compoundFieldMapping: Record<string, string[]>;
  onEvent: (events: ParseEvent[], gapInfo?: GapInfo) => void | Promise<void>;
  options: Required<SubscriptionOptions>;
  retryCount: number;
  lastReplayId?: Buffer;
  // gap event tracking
  gapEventCount?: number;
  lastGapEventTime?: Date;
  /** Thời điểm resume; dùng để cooldown (không pause GAP ngay sau resume) */
  resumedAt?: Date;
}

export type StreamKey = `${string}:${string}`; // tenantId:topicName
export type StreamMap = Map<
  StreamKey,
  grpc.ClientDuplexStream<FetchRequest, FetchResponse>
>;

export enum StreamState {
  ACTIVE = 'ACTIVE',
  PAUSED = 'PAUSED',
  RESYNCING = 'RESYNCING',
}

export type PauseReason = {
  type: 'GAP_THRESHOLD' | 'GAP_OVERFLOW';
  metadata: {
    gapCount?: number;
    eventCount?: number;
    threshold: number;
    lastReplayId?: Buffer;
    timestamp: Date;
  };
};

export type StreamStateInfo = {
  state: StreamState;
  pauseReason?: PauseReason;
};
