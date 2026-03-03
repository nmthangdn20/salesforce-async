export const SALESFORCE_GRPC_ENDPOINT = 'api.pubsub.salesforce.com:7443';

export const RECONNECT_CONFIG = {
  maxRetries: 10,
  initialDelayMs: 1000,
  maxDelayMs: 60000,
  backoffMultiplier: 2,
} as const;

export const DEFAULT_NUM_REQUESTED = 100;
export const FLOW_CONTROL_THRESHOLD = 0.5;

export const GAP_CONFIG = {
  maxGapEventsBeforeResync: 5,
  gapEventResetWindowMs: 1000 * 60 * 5,
  resumeCooldownMs: 1000 * 30,
} as const;
