import { TBase } from '@app/shared/models/base.model';
import { TSource } from '@app/shared/models/source.model';
import { TTarget } from '@app/shared/models/target.model';

export enum SYNC_JOB_STATUS {
  IDLE = 'idle',
  RUNNING = 'running',
  PAUSED = 'paused',
  ERROR = 'error',
}

export enum SYNC_RUN_STATUS {
  RUNNING = 'running',
  SUCCESS = 'success',
  FAILED = 'failed',
  CANCELLED = 'cancelled',
}

export type TSyncJob = TBase & {
  sourceId: string;
  targetId: string;
  type: string;
  scheduleCron?: string;
  status: SYNC_JOB_STATUS;
  lastRunAt?: Date;
  source: TSource;
  target: TTarget;
  syncRuns: TSyncRun[];
};

export type TSyncRun = TBase & {
  jobId: string;
  startedAt: Date;
  finishedAt?: Date;
  status?: SYNC_RUN_STATUS;
  metrics?: Record<string, any>;
  job: TSyncJob;
};
