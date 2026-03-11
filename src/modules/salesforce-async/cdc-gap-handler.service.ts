import { GapInfo } from '@app/helper';
import type { PayloadWithHeader } from '@app/helper/modules/salesforce/types/grpc.types';
import { Injectable, Logger } from '@nestjs/common';
import { Connection } from 'jsforce';
import { DbService, IDbRepository } from 'src/modules/db';
import { isRequiredField } from 'src/modules/salesforce-async/constants/required-fields.constant';
import { DirtyRecordService } from 'src/modules/salesforce-async/dirty-record.service';
import { handleAccountMapping } from 'src/modules/salesforce-async/map-objects/account';
import { SalesforceAsyncService } from 'src/modules/salesforce-async/salesforce-async.service';
import { TObject } from 'src/modules/salesforce-async/types/type';
import type { DeserializedParseEvent } from 'src/modules/salesforce-async/utils/event-serializer.util';

export type GapHandleOptions = {
  tenantId: string;
  topicName: string;
  /** Called when FULL_RESYNC completes so main process can resume stream (e.g. Redis publish) */
  onResume?: (tenantId: string, topicName: string) => Promise<void>;
};

@Injectable()
export class CdcGapHandlerService {
  private readonly logger = new Logger(CdcGapHandlerService.name);

  private static readonly SOQL_IN_BATCH_SIZE = 200;
  private static readonly GAP_TIMESTAMP_BUFFER_MS = 2 * 60 * 1000;

  constructor(
    private readonly dbService: DbService,
    private readonly salesforceAsyncService: SalesforceAsyncService,
    private readonly dirtyRecordService: DirtyRecordService,
  ) {}

  async handleCdc(
    events: DeserializedParseEvent[],
    object: TObject,
    filename: string,
  ): Promise<void> {
    if (events.length === 0) return;

    const salesforceConfig =
      await this.salesforceAsyncService.getSalesforceConfigValidated(filename);
    const repo = this.dbService.getRepo(
      salesforceConfig.databaseConfig,
      'salesforce-async',
    );
    const connection =
      this.salesforceAsyncService.createConnection(salesforceConfig);
    const fields = await this.salesforceAsyncService.getFields(
      connection,
      object,
    );

    const records = events.map(async (event) => {
      const recordIds = this.extractRecordIds(
        event.payload?.ChangeEventHeader?.recordIds,
      );

      if (recordIds.length === 0) {
        this.logger.warn(
          `CDC event for ${object.apiName} has no valid recordIds - skipping`,
          {
            rawRecordIds: event.payload?.ChangeEventHeader
              ?.recordIds as unknown as string[],
          },
        );
        return;
      }

      const cleanIds = await this.dirtyRecordService.filterDirtyIds(
        filename,
        object.apiName,
        recordIds,
      );

      if (cleanIds.length === 0) {
        this.logger.debug(
          `Skipping CDC event for dirty record(s) ${recordIds.join(',')} (${object.apiName}) until reconciled`,
        );
        return;
      }

      if (cleanIds.length < recordIds.length) {
        const dirtyIds = recordIds.filter((id) => !cleanIds.includes(id));
        this.logger.debug(
          `Partial dirty skip: dropping ${dirtyIds.join(',')} (${object.apiName}), processing ${cleanIds.join(',')}`,
        );
      }

      const rawPayload = this.mappingObjectToFlat(object, event.payload);
      const payload =
        cleanIds.length < recordIds.length
          ? {
              ...rawPayload,
              ChangeEventHeader: {
                ...rawPayload.ChangeEventHeader,
                recordIds: cleanIds,
              },
            }
          : rawPayload;

      if (!payload?.ChangeEventHeader?.changeType) {
        this.logger.warn(
          `CDC event for ${object.apiName} missing changeType - skipping`,
          { payload: payload as unknown as Record<string, unknown> },
        );
        return;
      }

      this.logger.debug(
        `Processing ${payload.ChangeEventHeader.changeType} for ${object.apiName}:`,
        payload,
      );

      switch (payload.ChangeEventHeader.changeType) {
        case 'CREATE':
          return this.insertRecord(repo, object, payload);
        case 'UPDATE':
          return this.updateRecord(repo, object, payload, connection, fields);
        case 'DELETE':
          return this.deleteRecord(repo, object, payload);
        case 'UNDELETE':
          return this.undeleteRecord(repo, object, payload, connection, fields);
        default:
          this.logger.warn(
            `Unknown changeType: ${payload.ChangeEventHeader.changeType} for ${object.apiName}`,
          );
          return;
      }
    });

    const results = await Promise.allSettled(records);
    results.forEach((result, index) => {
      if (result.status === 'rejected') {
        this.logger.error(
          `CDC event ${index + 1}/${events.length} failed for ${object.apiName}: ${
            result.reason instanceof Error
              ? result.reason.message
              : String(result.reason)
          }`,
          result.reason instanceof Error ? result.reason.stack : undefined,
        );
      }
    });
  }

  async handleNormalGapWithIds(
    gapInfo: GapInfo,
    filename: string,
  ): Promise<void> {
    const ctx = await this.resolveSetup(filename, gapInfo);
    if (!ctx) return;

    const { repo, connection, fields } = ctx;
    const recordIds = this.extractRecordIds(gapInfo.recordIds);

    this.logger.log(
      `Processing NORMAL gap event for ${gapInfo.objectName} with ${recordIds.length} record(s)`,
    );

    await this.dirtyRecordService.addDirty(
      filename,
      gapInfo.objectName,
      recordIds,
    );

    try {
      const maxLmd = await this.syncRecordByIdBatches(
        repo,
        connection,
        gapInfo,
        fields,
        recordIds,
      );

      // Catch-up sync: fetch records modified AFTER the initial SF query returned.
      // This closes the reconciliation window where CDC events were skipped but
      // the reconciled snapshot didn't yet include those changes.
      // Per Salesforce docs: "compare the LastModifiedDate fields on the change
      // event and the record retrieved" to ensure the change is covered.
      if (maxLmd) {
        this.logger.debug(
          `Catch-up sync for ${gapInfo.objectName} with LastModifiedDate > ${maxLmd}`,
        );
        for (
          let i = 0;
          i < recordIds.length;
          i += CdcGapHandlerService.SOQL_IN_BATCH_SIZE
        ) {
          const chunk = recordIds.slice(
            i,
            i + CdcGapHandlerService.SOQL_IN_BATCH_SIZE,
          );
          const catchupWhere = `${this.buildIdInWhereClause(chunk)} AND LastModifiedDate > ${maxLmd}`;
          await this.salesforceAsyncService.syncRecord(
            repo,
            connection,
            gapInfo.objectName,
            fields,
            catchupWhere,
          );
        }
      }

      await this.dirtyRecordService.removeDirty(
        filename,
        gapInfo.objectName,
        recordIds,
      );

      this.logger.log(
        `Successfully reconciled ${recordIds.length} record(s) for ${gapInfo.objectName}`,
      );
    } catch (err) {
      this.logger.error(
        `Gap reconciliation failed for ${gapInfo.objectName}, records remain dirty: ${
          err instanceof Error ? err.message : String(err)
        }`,
        err instanceof Error ? err.stack : undefined,
      );
      throw err;
    }
  }

  async handleNormalGapWithoutIds(
    gapInfo: GapInfo,
    filename: string,
  ): Promise<void> {
    const ctx = await this.resolveSetup(filename, gapInfo);
    if (!ctx) return;

    const { repo, connection, fields } = ctx;

    this.logger.warn(
      `NORMAL gap event for ${gapInfo.objectName} has no recordIds - falling back to timestamp query`,
    );

    const where = this.buildGapWhereClause(gapInfo);
    this.logger.log(`Syncing ${gapInfo.objectName} with query: ${where}`);

    await this.dirtyRecordService.addDirtyAll(filename, gapInfo.objectName);

    try {
      await this.salesforceAsyncService.syncRecord(
        repo,
        connection,
        gapInfo.objectName,
        fields,
        where,
      );

      await this.dirtyRecordService.removeDirtyAll(
        filename,
        gapInfo.objectName,
      );

      this.logger.log(
        `Successfully reconciled ${gapInfo.objectName} via timestamp query`,
      );
    } catch (err) {
      this.logger.error(
        `Timestamp reconciliation failed for ${gapInfo.objectName}, object remains dirty: ${
          err instanceof Error ? err.message : String(err)
        }`,
        err instanceof Error ? err.stack : undefined,
      );
      throw err;
    }
  }

  async handleFullResync(
    gapInfo: GapInfo,
    filename: string,
    opts: GapHandleOptions,
  ): Promise<void> {
    const ctx = await this.resolveSetup(filename, gapInfo);
    if (!ctx) return;

    const { repo, connection, object, fields } = ctx;

    const where = this.buildGapWhereClause(gapInfo);
    this.logger.log(`Syncing ${gapInfo.objectName} with query: ${where}`);

    await this.salesforceAsyncService.syncRecord(
      repo,
      connection,
      gapInfo.objectName,
      fields,
      where,
    );

    const fullResyncSince = this.getFullResyncSinceDate();
    const deletedIds = await this.salesforceAsyncService.getDeletedRecordIds(
      connection,
      gapInfo.objectName,
      fullResyncSince,
    );

    if (deletedIds.length > 0) {
      const existingDeletedIds = await this.dbService.getExistingIds(
        repo,
        object.name,
        deletedIds,
      );
      if (existingDeletedIds.length > 0) {
        this.logger.log(
          `Reconcile deleted: soft-deleting ${existingDeletedIds.length} record(s) for ${gapInfo.objectName}`,
        );
        await this.dbService.softDeleteMany(
          repo,
          object.name,
          existingDeletedIds,
        );
      }
    }

    await this.dirtyRecordService.removeDirtyAll(filename, gapInfo.objectName);

    this.logger.log(`Full resync completed for ${gapInfo.objectName}`);

    await opts.onResume?.(opts.tenantId, opts.topicName);
  }

  private async resolveSetup(
    filename: string,
    gapInfo: GapInfo,
  ): Promise<{
    repo: IDbRepository;
    connection: Connection;
    object: TObject;
    fields: { name: string }[];
  } | null> {
    const salesforceConfig =
      await this.salesforceAsyncService.getSalesforceConfigValidated(filename);
    const connection =
      this.salesforceAsyncService.createConnection(salesforceConfig);
    const repo = this.dbService.getRepo(
      salesforceConfig.databaseConfig,
      'salesforce-async',
    );

    const object = salesforceConfig.objects.find(
      (o) => o.apiName === gapInfo.objectName,
    );

    if (!object) {
      this.logger.warn(
        `Object ${gapInfo.objectName} not found in configuration`,
      );
      return null;
    }

    const fields = await this.salesforceAsyncService.getFields(
      connection,
      object,
    );

    return { repo, connection, object, fields };
  }

  private getFullResyncSinceDate(): Date {
    const since = new Date();
    since.setUTCHours(0, 0, 0, 0);
    since.setUTCDate(since.getUTCDate() - 1);
    return since;
  }

  private extractRecordIds(rawRecordIds: unknown): string[] {
    if (!rawRecordIds) {
      return [];
    }
    if (Array.isArray(rawRecordIds)) {
      return rawRecordIds.filter(
        (id): id is string => typeof id === 'string' && id.trim().length > 0,
      );
    }
    if (typeof rawRecordIds === 'string' && rawRecordIds.trim().length > 0) {
      return [rawRecordIds.trim()];
    }
    this.logger.warn(`Invalid recordIds format:`, { rawRecordIds });
    return [];
  }

  /**
   * Syncs records from Salesforce in batches of SOQL_IN_BATCH_SIZE.
   * Returns the maximum LastModifiedDate seen across all fetched records so the
   * caller can perform a catch-up sync for changes that landed during the SF API
   * call window (see "How to Handle a Gap Event" — LastModifiedDate comparison).
   */
  private async syncRecordByIdBatches(
    repo: IDbRepository,
    connection: Connection,
    gapInfo: GapInfo,
    fields: { name: string }[],
    recordIds: string[],
  ): Promise<string | null> {
    const totalBatches = Math.ceil(
      recordIds.length / CdcGapHandlerService.SOQL_IN_BATCH_SIZE,
    );
    let maxLastModifiedDate: string | null = null;

    for (
      let i = 0;
      i < recordIds.length;
      i += CdcGapHandlerService.SOQL_IN_BATCH_SIZE
    ) {
      const chunk = recordIds.slice(
        i,
        i + CdcGapHandlerService.SOQL_IN_BATCH_SIZE,
      );
      const batchNumber =
        Math.floor(i / CdcGapHandlerService.SOQL_IN_BATCH_SIZE) + 1;

      this.logger.debug(
        `Syncing batch ${batchNumber}/${totalBatches} (${chunk.length} records) for ${gapInfo.objectName}`,
      );

      const where = this.buildIdInWhereClause(chunk);
      const rows = await this.salesforceAsyncService.syncRecord(
        repo,
        connection,
        gapInfo.objectName,
        fields,
        where,
      );

      for (const row of rows) {
        const lmd = row['LastModifiedDate'];
        if (lmd && (!maxLastModifiedDate || lmd > maxLastModifiedDate)) {
          maxLastModifiedDate = lmd;
        }
      }
    }

    return maxLastModifiedDate;
  }

  private buildIdInWhereClause(recordIds: string[]): string {
    if (recordIds.length === 0) {
      throw new Error('buildIdInWhereClause requires at least one ID');
    }
    const escaped = recordIds.map(
      (id) => `'${String(id).replace(/'/g, "''")}'`,
    );
    return `Id IN (${escaped.join(',')})`;
  }

  private buildGapWhereClause(gapInfo: GapInfo): string {
    const { type, fromTimestamp, toTimestamp, recordIds } = gapInfo;

    if (type === 'FULL_RESYNC') {
      return `LastModifiedDate >= ${this.getFullResyncSinceDate().toISOString()}`;
    }

    const ids = this.extractRecordIds(recordIds);
    if (type === 'NORMAL' && ids.length > 0) {
      const escaped = ids.map((id) => `'${String(id).replace(/'/g, "''")}'`);
      return `Id IN (${escaped.join(',')})`;
    }

    if (!fromTimestamp || !toTimestamp) {
      throw new Error(
        `Gap event for ${gapInfo.objectName} missing both recordIds and timestamps - cannot build query`,
      );
    }

    const from = new Date(
      fromTimestamp - CdcGapHandlerService.GAP_TIMESTAMP_BUFFER_MS,
    ).toISOString();
    const to = new Date(
      toTimestamp + CdcGapHandlerService.GAP_TIMESTAMP_BUFFER_MS,
    ).toISOString();

    return `LastModifiedDate >= ${from} AND LastModifiedDate <= ${to}`;
  }

  private async insertRecord(
    repo: IDbRepository,
    object: TObject,
    payload: PayloadWithHeader,
  ): Promise<void> {
    const { ChangeEventHeader, ...data } = payload;
    const recordIds = this.extractRecordIds(ChangeEventHeader?.recordIds);

    if (recordIds.length === 0) {
      this.logger.warn(
        `CREATE event for ${object.apiName} has no valid recordIds`,
      );
      return;
    }

    const validData = Object.fromEntries(
      Object.entries(data).filter(
        ([field]) =>
          object.fieldNames.includes(field) || isRequiredField(field),
      ),
    );

    if (Object.keys(validData).length === 0) {
      this.logger.warn(
        `CREATE event for ${object.apiName} has no valid fields to insert`,
      );
      return;
    }

    const records = recordIds.map((id) => ({
      ...validData,
      Id: id,
    }));

    this.logger.debug(
      `Inserting ${records.length} record(s) for ${object.apiName}`,
    );
    await this.dbService.upsertMany(repo, object.name, records);
  }

  /**
   * Resolves which of the given record IDs exist in DB and which are missing.
   * @returns { existingIds, missingIds }
   */
  private async resolveExistingAndMissingIds(
    repo: IDbRepository,
    object: TObject,
    recordIds: string[],
    changeType: string,
  ): Promise<{ existingIds: string[]; missingIds: string[] }> {
    const existingIds = await this.dbService.getExistingIds(
      repo,
      object.name,
      recordIds,
    );
    const missingIds: string[] = recordIds.filter(
      (id) => !existingIds.includes(id),
    );
    if (missingIds.length > 0) {
      this.logger.debug(
        `${changeType} event for ${object.apiName}: ${existingIds.length} in DB, ${missingIds.length} missing (${missingIds.join(',')})`,
      );
    }
    return { existingIds, missingIds };
  }

  /**
   * Fetches missing records from Salesforce and upserts into DB.
   * Uses batches to stay within SOQL/API limits.
   */
  private async fetchMissingRecordsAndSync(
    repo: IDbRepository,
    connection: Connection,
    object: TObject,
    fields: { name: string }[],
    missingIds: string[],
  ): Promise<void> {
    if (missingIds.length === 0) return;
    this.logger.debug(
      `Fetching ${missingIds.length} missing record(s) for ${object.apiName} from Salesforce`,
    );
    for (
      let i = 0;
      i < missingIds.length;
      i += CdcGapHandlerService.SOQL_IN_BATCH_SIZE
    ) {
      const chunk = missingIds.slice(
        i,
        i + CdcGapHandlerService.SOQL_IN_BATCH_SIZE,
      );
      const where = this.buildIdInWhereClause(chunk);
      await this.salesforceAsyncService.syncRecord(
        repo,
        connection,
        object.apiName,
        fields,
        where,
      );
    }
  }

  private async updateRecord(
    repo: IDbRepository,
    object: TObject,
    payload: PayloadWithHeader,
    connection?: Connection,
    fields?: { name: string }[],
  ): Promise<void> {
    const { ChangeEventHeader, ...data } = payload;
    const recordIds = this.extractRecordIds(ChangeEventHeader?.recordIds);

    if (recordIds.length === 0) {
      this.logger.warn(
        `UPDATE event for ${object.apiName} has no valid recordIds`,
      );
      return;
    }

    const { existingIds, missingIds } = await this.resolveExistingAndMissingIds(
      repo,
      object,
      recordIds,
      'UPDATE',
    );

    if (missingIds.length > 0 && connection && fields) {
      await this.fetchMissingRecordsAndSync(
        repo,
        connection,
        object,
        fields,
        missingIds,
      );
    }

    const validData = Object.fromEntries(
      Object.entries(data).filter(
        ([field]) =>
          object.fieldNames.includes(field) || isRequiredField(field),
      ),
    );

    if (Object.keys(validData).length === 0) {
      this.logger.warn(
        `UPDATE event for ${object.apiName} has no valid fields to update`,
      );
      return;
    }

    const idsToUpdate =
      missingIds.length > 0 && connection && fields ? recordIds : existingIds;
    if (idsToUpdate.length === 0) {
      this.logger.debug(
        `UPDATE event for ${object.apiName}: no record(s) in DB - skipping`,
      );
      return;
    }

    this.logger.debug(
      `Updating ${idsToUpdate.length} record(s) for ${object.apiName} with ${Object.keys(validData).length} field(s)`,
    );

    await this.dbService.updateMany(repo, object.name, idsToUpdate, validData);
  }

  private async deleteRecord(
    repo: IDbRepository,
    object: TObject,
    payload: PayloadWithHeader,
  ): Promise<void> {
    const { ChangeEventHeader } = payload;
    const recordIds = this.extractRecordIds(ChangeEventHeader?.recordIds);

    if (recordIds.length === 0) {
      this.logger.warn(
        `DELETE event for ${object.apiName} has no valid recordIds`,
      );
      return;
    }

    const { existingIds } = await this.resolveExistingAndMissingIds(
      repo,
      object,
      recordIds,
      'DELETE',
    );
    if (existingIds.length === 0) {
      this.logger.debug(
        `DELETE event for ${object.apiName}: no record(s) in DB - skipping`,
      );
      return;
    }

    this.logger.debug(
      `Soft deleting ${existingIds.length} record(s) for ${object.apiName}`,
    );
    await this.dbService.softDeleteMany(repo, object.name, existingIds);
  }

  private async undeleteRecord(
    repo: IDbRepository,
    object: TObject,
    payload: PayloadWithHeader,
    connection?: Connection,
    fields?: { name: string }[],
  ): Promise<void> {
    const { ChangeEventHeader } = payload;
    const recordIds = this.extractRecordIds(ChangeEventHeader?.recordIds);

    if (recordIds.length === 0) {
      this.logger.warn(
        `UNDELETE event for ${object.apiName} has no valid recordIds`,
      );
      return;
    }

    const { existingIds, missingIds } = await this.resolveExistingAndMissingIds(
      repo,
      object,
      recordIds,
      'UNDELETE',
    );

    if (missingIds.length > 0 && connection && fields) {
      await this.fetchMissingRecordsAndSync(
        repo,
        connection,
        object,
        fields,
        missingIds,
      );
    }

    const idsToUndelete =
      missingIds.length > 0 && connection && fields ? recordIds : existingIds;
    if (idsToUndelete.length === 0) {
      this.logger.debug(
        `UNDELETE event for ${object.apiName}: no record(s) in DB - skipping`,
      );
      return;
    }

    this.logger.debug(
      `Undeleting ${idsToUndelete.length} record(s) for ${object.apiName}`,
    );
    await this.dbService.undeleteMany(repo, object.name, idsToUndelete);
  }

  private mappingObjectToFlat(
    object: TObject,
    data: PayloadWithHeader,
  ): PayloadWithHeader {
    switch (object.apiName) {
      case 'Account':
        return handleAccountMapping(
          data as PayloadWithHeader & {
            Name: { LastName: string; FirstName: string; Salutation: string };
          },
        ) as unknown as PayloadWithHeader;
      default:
        return data;
    }
  }
}
