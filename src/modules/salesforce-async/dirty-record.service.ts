import { RedisClientService } from '@app/core/modules/redis';
import { Injectable } from '@nestjs/common';

/**
 * Tracks records marked as "dirty" after a gap event (per Salesforce "How to Handle a Gap Event").
 * Change events for dirty records are not processed until the record is reconciled.
 *
 * Uses Redis for persistence across process restarts and for sharing state between
 * the main process (gRPC subscriber) and worker processes (BullMQ processors).
 *
 * Two dirty granularities:
 *  - Record-level: specific record IDs (NORMAL gap with recordIds)
 *  - Object-level: entire object type (NORMAL gap without recordIds — fallback)
 *
 * Key patterns:
 *  - Record-level: `sf:dirty:{filename}:{objectName}` → Redis Set of record IDs
 *  - Object-level: `sf:dirty-all:{filename}:{objectName}` → Redis String "1"
 *
 * Both key types carry a 1-hour TTL as a safety net in case reconciliation
 * fails and the process crashes before clearing the flag.
 */
@Injectable()
export class DirtyRecordService {
  /** Safety-net TTL: dirty flags are auto-expired after 1 hour. */
  private static readonly TTL_SECONDS = 3600;

  constructor(private readonly redisClient: RedisClientService) {}

  private recordKey(filename: string, objectName: string): string {
    return `sf:dirty:${filename}:${objectName}`;
  }

  private allKey(filename: string, objectName: string): string {
    return `sf:dirty-all:${filename}:${objectName}`;
  }

  async addDirty(
    filename: string,
    objectName: string,
    recordIds: string[],
  ): Promise<void> {
    const ids = recordIds.filter(Boolean);
    if (ids.length === 0) return;
    const k = this.recordKey(filename, objectName);
    await this.redisClient.sadd(k, ...ids);
    await this.redisClient.expire(k, DirtyRecordService.TTL_SECONDS);
  }

  async removeDirty(
    filename: string,
    objectName: string,
    recordIds: string[],
  ): Promise<void> {
    const ids = recordIds.filter(Boolean);
    if (ids.length === 0) return;
    const k = this.recordKey(filename, objectName);
    await this.redisClient.srem(k, ...ids);
  }

  async addDirtyAll(filename: string, objectName: string): Promise<void> {
    const k = this.allKey(filename, objectName);
    await this.redisClient.set(k, '1', DirtyRecordService.TTL_SECONDS);
  }

  async removeDirtyAll(filename: string, objectName: string): Promise<void> {
    await this.redisClient.del(this.allKey(filename, objectName));
  }

  /**
   * Returns only the IDs from recordIds that are NOT dirty.
   * If the entire object is dirty (object-level flag), returns an empty array.
   * Used to partially process CDC events that contain a mix of dirty and clean records.
   */
  async filterDirtyIds(
    filename: string,
    objectName: string,
    recordIds: string[],
  ): Promise<string[]> {
    const ids = recordIds.filter(Boolean);
    if (!ids.length) return [];

    const allDirty = await this.redisClient.exists(
      this.allKey(filename, objectName),
    );
    if (allDirty) return [];

    const k = this.recordKey(filename, objectName);

    const pipeline = this.redisClient.pipeline();
    for (const id of ids) {
      pipeline.sismember(k, id);
    }

    const results = await pipeline.exec();
    return ids.filter((_, i) => {
      const [err, val] = (results ?? [])[i] as [Error | null, unknown];
      return !!err || val !== 1;
    });
  }
}
