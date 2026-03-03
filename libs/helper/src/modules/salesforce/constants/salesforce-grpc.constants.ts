// ============ Constants ============
export const SALESFORCE_GRPC_ENDPOINT = 'api.pubsub.salesforce.com:7443';

export const RECONNECT_CONFIG = {
  maxRetries: 10,
  initialDelayMs: 1000,
  maxDelayMs: 60000,
  backoffMultiplier: 2,
} as const;

export const DEFAULT_NUM_REQUESTED = 100;
export const FLOW_CONTROL_THRESHOLD = 0.5; // Request more when < 50% pending

export const GAP_CONFIG = {
  maxGapEventsBeforeResync: 5,
  gapEventResetWindowMs: 1000 * 60 * 5, // 5 minutes
  /** Sau resume, trong N ms không auto-pause khi gặp GAP_OVERFLOW/GAP_THRESHOLD (chỉ deliver) → tránh loop pause-resume */
  resumeCooldownMs: 1000 * 30, // 30 giây
} as const;
