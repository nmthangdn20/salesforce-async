import { CustomHttpException } from '@app/common/exceptions/custom-http.exception';
import { TypeConfigService } from '@app/core/modules/type-config';
import { SalesforceService } from '@app/helper';
import { SalesforceGrpcService } from '@app/helper/modules/salesforce/salesforce-grpc.service';
import {
  GapInfo,
  ReplayPreset,
} from '@app/helper/modules/salesforce/types/grpc.types';
import { ERROR_MESSAGES } from '@app/shared/constants/error.constant';
import {
  CDC_JOB_OPTIONS,
  GAP_FULL_RESYNC_JOB_OPTIONS,
  GAP_NORMAL_JOB_OPTIONS,
  SYNC_DATA_JOB_OPTIONS,
} from '@app/shared/constants/queue-job-options.constant';
import {
  GAP_JOB_NAMES,
  QUEUE_NAMES,
  SYNC_JOB_NAMES,
} from '@app/shared/constants/queue-name.constant';
import { InjectQueue } from '@nestjs/bullmq';
import { Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { Queue } from 'bullmq';
import { parse as parseCsv } from 'fast-csv';
import { readFileSync } from 'fs';
import { Connection } from 'jsforce';
import { join } from 'path';
import { IDbRepository } from 'src/modules/db';
import { DbService } from 'src/modules/db/db.service';
import { isRequiredField } from 'src/modules/salesforce-async/constants/required-fields.constant';
import { DirtyRecordService } from 'src/modules/salesforce-async/dirty-record.service';
import type {
  CdcJobPayload,
  GapJobPayload,
  SyncJobPayload,
} from 'src/modules/salesforce-async/types/queue-job-payload.type';
import {
  TObject,
  TSalesforceConfig,
} from 'src/modules/salesforce-async/types/type';
import { serializeParseEvents } from 'src/modules/salesforce-async/utils/event-serializer.util';
import { sfTypeToPgType } from 'src/modules/salesforce-async/utils/type-mapper.util';
import { chunkArray } from 'src/utils/utils';
import { Readable } from 'stream';

@Injectable()
export class SalesforceAsyncService implements OnModuleInit {
  private readonly logger = new Logger(SalesforceAsyncService.name);
  private readonly compoundFieldMappingMap: Map<
    string,
    Record<string, string[]>
  > = new Map();
  /** Cache config by filename to avoid reading file on every CDC/Gap call. */
  private readonly configCache = new Map<string, TSalesforceConfig>();
  /** Cache describe fields per object API name, expires after 10 minutes. */
  private readonly fieldsCache = new Map<
    string,
    {
      fields: {
        name: string;
        typeSql: string;
        required: boolean;
        primaryKey: boolean;
      }[];
      expiresAt: number;
    }
  >();
  private static readonly FIELDS_CACHE_TTL_MS = 10 * 60 * 1000;

  constructor(
    private readonly salesforceService: SalesforceService,
    private readonly typeConfigService: TypeConfigService,
    private readonly dbService: DbService,
    private readonly grpcService: SalesforceGrpcService,
    private readonly dirtyRecordService: DirtyRecordService,
    @InjectQueue(QUEUE_NAMES.CDC) private readonly cdcQueue: Queue,
    @InjectQueue(QUEUE_NAMES.GAP) private readonly gapQueue: Queue,
    @InjectQueue(QUEUE_NAMES.SALESFORCE_SYNC) private readonly syncQueue: Queue,
  ) {}

  onModuleInit(): void {
    this.cdc('salesforce.config.json').catch((err: unknown) => {
      this.logger.error(
        `Failed to initialize CDC subscription: ${err instanceof Error ? err.message : String(err)}`,
        err instanceof Error ? err.stack : undefined,
      );
    });
  }

  /**
   * Returns Salesforce config for the given filename. Result is cached in memory;
   * change config on disk requires process restart.
   */
  getSalesforceConfig(filename: string): TSalesforceConfig {
    const cached = this.configCache.get(filename);
    if (cached) return cached;
    const config = JSON.parse(
      readFileSync(join(process.cwd(), 'salesforce-configs', filename), 'utf8'),
    ) as TSalesforceConfig;
    this.configCache.set(filename, config);
    return config;
  }

  /**
   * Returns config and ensures token is valid (refreshes if expired).
   * Use this before using config for API calls so both CDC and Gap paths stay consistent.
   */
  async getSalesforceConfigValidated(
    filename: string,
  ): Promise<TSalesforceConfig> {
    const config = this.getSalesforceConfig(filename);
    await this.salesforceService.verifyConnection(
      config,
      `${this.typeConfigService.get('app.backendUrl')}/api/auth/oauth/callback`,
    );
    return config;
  }

  createConnection(salesforceConfig: TSalesforceConfig) {
    return this.salesforceService.createConnection({
      instanceUrl: salesforceConfig.instanceUrl,
      accessToken: salesforceConfig.accessToken,
    });
  }

  async syncData(filename: string): Promise<void> {
    try {
      const salesforceConfig =
        await this.getSalesforceConfigValidated(filename);
      const connection = this.createConnection(salesforceConfig);

      const dbRepo = this.dbService.getRepo(
        salesforceConfig.databaseConfig,
        'salesforce-async',
      );

      for (const object of salesforceConfig.objects) {
        const fields = await this.getFields(connection, object);

        await this.syncObject(dbRepo, connection, object);

        await this.syncRecord(dbRepo, connection, object.apiName, fields);

        // Vacuum to reclaim dead tuples space after upsert
        // this.logger.log(`Running VACUUM on ${object.name} table...`);
        // await dbRepo.execute(`VACUUM "${object.name}"`);
        // this.logger.log(`VACUUM completed for ${object.name}`);
      }
    } catch (error) {
      this.logger.error(
        `Failed to sync data for ${filename}: ${error instanceof Error ? error.message : String(error)}`,
        error instanceof Error ? error.stack : undefined,
      );
      throw error;
    }
  }

  async enqueueSyncData(filename: string): Promise<void> {
    const payload: SyncJobPayload = { filename };
    await this.syncQueue.add(SYNC_JOB_NAMES.SYNC_DATA, payload, SYNC_DATA_JOB_OPTIONS);
  }

  async getFields(connection: Connection, object: TObject) {
    const cached = this.fieldsCache.get(object.apiName);
    if (cached && Date.now() < cached.expiresAt) return cached.fields;

    const data = await this.salesforceService.describe(
      connection,
      object.apiName,
    );

    const sfFields = data.fields;

    const fields = sfFields
      .filter((field) => field.type !== 'address' && field.type !== 'location')
      .filter(
        (field) =>
          object.fieldNames.includes(field.name) || isRequiredField(field.name),
      )
      .map((field) => ({
        name: field.name,
        typeSql: sfTypeToPgType(
          field.type,
          field.length,
          field.precision,
          field.scale,
        ),
        required: field.nillable === false,
        primaryKey: field.type === 'id',
      }));

    this.fieldsCache.set(object.apiName, {
      fields,
      expiresAt: Date.now() + SalesforceAsyncService.FIELDS_CACHE_TTL_MS,
    });

    return fields;
  }

  private async syncObject(
    dbRepo: IDbRepository,
    connection: Connection,
    object: TObject,
  ) {
    const fields = await this.getFields(connection, object);

    await this.dbService.ensureTableSchema({
      repo: dbRepo,
      key: 'salesforce-async',
      table: object.name,
      columns: fields,
    });
  }

  async syncRecord(
    dbRepo: IDbRepository,
    connection: Connection,
    objectName: string,
    fields: { name: string }[],
    where?: string,
  ) {
    this.logger.log(
      `Fetching records from Salesforce for ${objectName}${where ? ` (where: ${where})` : ''}...`,
    );
    const recordsStream = await this.salesforceService.getRecords(
      connection,
      objectName,
      fields.map((field) => field.name),
      where,
    );

    const rows = await this.processBulkCsvStreamToArray(recordsStream.stream());
    this.logger.log(
      `Fetched ${rows.length} record(s) for ${objectName}, upserting to DB...`,
    );

    const batchSize = 200;

    const quotedTable = `"${objectName}"`;
    const quotedCols = fields.map((f) => `"${f.name}"`).join(',');
    const updateCols = fields
      .filter((f) => f.name !== 'Id')
      .map((f) => `"${f.name}" = EXCLUDED."${f.name}"`)
      .join(',');

    const batches = chunkArray(rows, batchSize);
    const totalBatches = batches.length;

    for (let batchIndex = 0; batchIndex < batches.length; batchIndex++) {
      const batch = batches[batchIndex];
      if (batchIndex > 0 && batchIndex % 10 === 0) {
        this.logger.log(
          `Upserting ${objectName}: batch ${batchIndex + 1}/${totalBatches}`,
        );
      }
      const values: any[] = [];
      const valuesSql = batch
        .map((row, rowIndex) => {
          const base = rowIndex * fields.length;
          const placeholders = fields.map(
            (_, colIndex) => `$${base + colIndex + 1}`,
          );

          for (const field of fields) {
            const v = row[field.name];
            values.push(v === '' ? null : v);
          }

          return `(${placeholders.join(',')})`;
        })
        .join(',');

      const query = `
        INSERT INTO ${quotedTable} (${quotedCols})
        VALUES ${valuesSql}
        ON CONFLICT ("Id")
        DO UPDATE SET
          ${updateCols}
      `;

      await dbRepo.execute(query, values);
    }

    return rows;
  }

  /**
   * Get IDs of records deleted (Recycle Bin) since the given date.
   * Uses queryAll (scanAll) so deleted records are included. Used for overflow FULL_RESYNC reconcile-deleted.
   * Returns [] if the object does not support IsDeleted or on query error.
   */
  async getDeletedRecordIds(
    connection: Connection,
    objectName: string,
    sinceDate: Date,
  ): Promise<string[]> {
    try {
      const where = `IsDeleted = true AND LastModifiedDate >= ${sinceDate.toISOString()}`;
      const recordsStream = await this.salesforceService.getRecords(
        connection,
        objectName,
        ['Id'],
        where,
        { scanAll: true },
      );
      const rows = await this.processBulkCsvStreamToArray(
        recordsStream.stream(),
      );
      return rows.map((r) => r.Id).filter(Boolean);
    } catch (err) {
      this.logger.warn(
        `getDeletedRecordIds failed for ${objectName} (object may not support IsDeleted): ${
          err instanceof Error ? err.message : String(err)
        }`,
      );
      return [];
    }
  }

  private processBulkCsvStreamToArray(
    readable: Readable,
  ): Promise<Record<string, string>[]> {
    return new Promise<Record<string, string>[]>(
      (
        resolve: (value: Record<string, string>[]) => void,
        reject: (reason?: unknown) => void,
      ) => {
        const rows: Record<string, string>[] = [];

        const csvStream = parseCsv({
          headers: true,
          ignoreEmpty: true,
          trim: true,
        });

        csvStream
          .on('error', (err) => reject(err))
          .on('data', (row) => {
            rows.push(row as Record<string, string>);
          })
          .on('end', () => {
            resolve(rows);
          });

        readable.on('error', (err) => reject(err));
        readable.pipe(csvStream);
      },
    );
  }

  /**
   * Subscribe to Change Data Capture events for specified objects
   */
  async cdc(filename: string) {
    const salesforceConfig = await this.getSalesforceConfigValidated(filename);

    if (!salesforceConfig.tenantId) {
      throw new CustomHttpException(ERROR_MESSAGES.ConnectionNotFound);
    }

    const grpcConfig = {
      accessToken: salesforceConfig.accessToken,
      instanceUrl: salesforceConfig.instanceUrl,
      tenantId: salesforceConfig.tenantId,
    };

    const connection = this.createConnection(salesforceConfig);

    for (const object of salesforceConfig.objects) {
      const topicName = object.cdcTopic;

      this.logger.log(`Subscribing to CDC topic: ${topicName}`);

      const compoundFieldMapping = await this.getCompoundFieldMapping(
        connection,
        object,
      );

      try {
        await this.grpcService.subscribe(
          grpcConfig,
          topicName,
          object.apiName,
          compoundFieldMapping,
          (events, gapInfo?: GapInfo) => {
            const serialized = serializeParseEvents(events);

            if (!gapInfo) {
              const payload: CdcJobPayload = {
                events: serialized,
                object,
                filename,
              };
              return void this.cdcQueue.add('cdc', payload, CDC_JOB_OPTIONS).catch((err) => {
                this.logger.error(
                  `Failed to add CDC job for ${object.apiName}: ${err instanceof Error ? err.message : String(err)}`,
                );
              });
            }

            // Mark records dirty as soon as we receive the gap (before job runs) so any
            // CDC event for these records is skipped until reconciliation completes.
            if (gapInfo.type === 'FULL_RESYNC') {
              this.dirtyRecordService
                .addDirtyAll(filename, gapInfo.objectName)
                .catch((err: unknown) => {
                  this.logger.error(
                    `Failed to mark dirty-all for FULL_RESYNC ${gapInfo.objectName}: ${err instanceof Error ? err.message : String(err)}`,
                  );
                });
            } else if (gapInfo.type === 'NORMAL') {
              if (gapInfo.recordIds && gapInfo.recordIds.length > 0) {
                this.dirtyRecordService
                  .addDirty(filename, gapInfo.objectName, gapInfo.recordIds)
                  .catch((err: unknown) => {
                    this.logger.error(
                      `Failed to mark dirty for ${gapInfo.objectName}: ${err instanceof Error ? err.message : String(err)}`,
                    );
                  });
              } else {
                this.dirtyRecordService
                  .addDirtyAll(filename, gapInfo.objectName)
                  .catch((err: unknown) => {
                    this.logger.error(
                      `Failed to mark dirty-all for ${gapInfo.objectName}: ${err instanceof Error ? err.message : String(err)}`,
                    );
                  });
              }
            }

            const payload: GapJobPayload = {
              gapInfo,
              filename,
              tenantId: salesforceConfig.tenantId ?? '',
              topicName,
            };

            const jobName =
              gapInfo.type === 'FULL_RESYNC'
                ? GAP_JOB_NAMES.FULL_RESYNC
                : gapInfo.recordIds && gapInfo.recordIds.length > 0
                  ? GAP_JOB_NAMES.NORMAL_WITH_IDS
                  : GAP_JOB_NAMES.NORMAL_WITHOUT_IDS;

            const jobOptions =
              gapInfo.type === 'FULL_RESYNC'
                ? GAP_FULL_RESYNC_JOB_OPTIONS
                : GAP_NORMAL_JOB_OPTIONS;

            return void this.gapQueue.add(jobName, payload, jobOptions).catch((err) => {
              this.logger.error(
                `Failed to add GAP job for ${object.apiName}: ${err instanceof Error ? err.message : String(err)}`,
              );
            });
          },
          {
            numRequested: 100,
            replayPreset: ReplayPreset.LATEST,
          },
        );
      } catch (error) {
        this.logger.error(`Failed to subscribe to ${topicName}: ${error}`);
        throw error;
      }
    }

    return {
      message: `Subscribed to CDC for: ${salesforceConfig.objects.map((o) => o.apiName).join(', ')}`,
    };
  }

  /**
   * Subscribe to all Change Data Capture events
   */
  // async cdcAll(filename: string) {
  //   const salesforceConfig = this.getSalesforceConfig(filename);

  //   await this.salesforceService.verifyConnection(
  //     salesforceConfig,
  //     `${this.typeConfigService.get('app.backendUrl')}/api/auth/oauth/callback`,
  //   );

  //   if (!salesforceConfig.tenantId) {
  //     throw new CustomHttpException(ERROR_MESSAGES.ConnectionNotFound);
  //   }

  //   const grpcConfig = {
  //     accessToken: salesforceConfig.accessToken,
  //     instanceUrl: salesforceConfig.instanceUrl,
  //     tenantId: salesforceConfig.tenantId,
  //   };

  //   // Subscribe to all change events
  //   const topicName = '/data/ChangeEvents';

  //   this.logger.log(`Subscribing to all CDC events: ${topicName}`);

  //   try {
  //     await this.grpcService.subscribe(
  //       grpcConfig,
  //       topicName,
  //       object.apiName,
  //       {}, // Empty mapping for generic subscription - no field flattening
  //       (events) => {
  //         this.logger.log(`Received ${events.length} CDC events`);

  //         for (const event of events) {
  //           this.logger.debug(
  //             `Event payload: ${JSON.stringify(event.payload)}`,
  //           );
  //           // TODO: Process CDC events - sync changes to database
  //         }
  //       },
  //       {
  //         numRequested: 100,
  //         replayPreset: ReplayPreset.LATEST,
  //       },
  //     );

  //     return { message: 'Subscribed to all CDC events' };
  //   } catch (error) {
  //     this.logger.error(`Failed to subscribe to ${topicName}: ${error}`);
  //     throw error;
  //   }
  // }

  private async getCompoundFieldMapping(
    connection: Connection,
    object: TObject,
  ): Promise<Record<string, string[]>> {
    if (this.compoundFieldMappingMap.has(object.apiName)) {
      return this.compoundFieldMappingMap.get(object.apiName)!;
    }

    const data = await this.salesforceService.describe(
      connection,
      object.apiName,
    );

    const sfFields = data.fields;
    const compoundFields: Record<string, string[]> = {};

    for (const field of sfFields) {
      if (!field.compoundFieldName) continue;

      const compoundName = field.compoundFieldName;

      if (!compoundFields[compoundName]) {
        compoundFields[compoundName] = [];
      }

      compoundFields[compoundName].push(field.name);
    }

    this.compoundFieldMappingMap.set(object.apiName, compoundFields);

    return compoundFields;
  }
}
