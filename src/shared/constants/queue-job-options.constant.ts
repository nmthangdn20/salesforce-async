import type { JobsOptions } from 'bullmq';

export const CDC_JOB_OPTIONS: JobsOptions = {
  attempts: 3,
  backoff: { type: 'exponential', delay: 1000 },
  removeOnComplete: { count: 500 },
  removeOnFail: { count: 200 },
};

export const GAP_NORMAL_JOB_OPTIONS: JobsOptions = {
  attempts: 3,
  backoff: { type: 'exponential', delay: 2000 },
  removeOnComplete: { count: 100 },
  removeOnFail: { count: 100 },
};

export const GAP_FULL_RESYNC_JOB_OPTIONS: JobsOptions = {
  attempts: 2,
  backoff: { type: 'fixed', delay: 10000 },
  removeOnComplete: { count: 50 },
  removeOnFail: { count: 50 },
};

export const SYNC_DATA_JOB_OPTIONS: JobsOptions = {
  attempts: 2,
  backoff: { type: 'fixed', delay: 10000 },
  removeOnComplete: { count: 50 },
  removeOnFail: { count: 50 },
};
