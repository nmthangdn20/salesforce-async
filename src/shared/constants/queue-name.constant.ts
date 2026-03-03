export const QUEUE_NAMES = {
  SALESFORCE_SYNC: 'salesforce-sync',
  CDC: 'salesforce-cdc',
  GAP: 'salesforce-gap',
} as const;

export type QueueName = (typeof QUEUE_NAMES)[keyof typeof QUEUE_NAMES];

export const GAP_JOB_NAMES = {
  NORMAL_WITH_IDS: 'normal-with-ids',
  NORMAL_WITHOUT_IDS: 'normal-without-ids',
  FULL_RESYNC: 'full-resync',
} as const;

export type GapJobName = (typeof GAP_JOB_NAMES)[keyof typeof GAP_JOB_NAMES];
