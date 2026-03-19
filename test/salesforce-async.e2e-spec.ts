/**
 * E2E tests — Salesforce async processing pipeline
 *
 * Tests the full flow from event reception through to database operations:
 *   gRPC event → onEvent callback → BullMQ job payload → processor → CdcGapHandlerService → DB
 *
 * External I/O boundaries are mocked:
 *   - SalesforceGrpcService  (gRPC pub/sub stream)
 *   - SalesforceService      (Salesforce REST API)
 *   - DbService              (PostgreSQL)
 *   - DirtyRecordService     (Redis-backed dirty-record tracking)
 *   - BullMQ queues
 *   - RedisPubSubService     (Redis pub/sub for stream resume signal)
 *   - fs.readFileSync        (config file)
 */

import * as fs from 'fs';
import { Test, TestingModule } from '@nestjs/testing';
import { getQueueToken } from '@nestjs/bullmq';
import { Job } from 'bullmq';

import { RedisPubSubService } from '@app/core/modules/redis';
import { SalesforceService } from '@app/helper';
import { SalesforceGrpcService } from '@app/helper/modules/salesforce/salesforce-grpc.service';
import {
  GapInfo,
  ParseEvent,
  SalesforceGrpcConfig,
  SubscriptionOptions,
} from '@app/helper/modules/salesforce/types/grpc.types';
import { TypeConfigService } from '@app/core/modules/type-config';
import { GAP_JOB_NAMES, QUEUE_NAMES } from '@app/shared/constants/queue-name.constant';

import { CdcGapHandlerService } from '../src/modules/salesforce-async/cdc-gap-handler.service';
import { CdcProcessor } from '../src/modules/salesforce-async/processors/cdc.processor';
import { GapProcessor } from '../src/modules/salesforce-async/processors/gap.processor';
import { SalesforceAsyncService } from '../src/modules/salesforce-async/salesforce-async.service';
import { DirtyRecordService } from '../src/modules/salesforce-async/dirty-record.service';
import { DbService } from '../src/modules/db/db.service';
import { STREAM_RESUME_CHANNEL } from '../src/modules/salesforce-async/constants/redis-channel.constant';
import type { CdcJobPayload, GapJobPayload } from '../src/modules/salesforce-async/types/queue-job-payload.type';
import type { TObject, TSalesforceConfig } from '../src/modules/salesforce-async/types/type';
import {
  deserializeParseEvents,
  serializeParseEvents,
} from '../src/modules/salesforce-async/utils/event-serializer.util';

// ── Fixtures ──────────────────────────────────────────────────────────────────

const MOCK_OBJECT: TObject = {
  name: 'Contact',
  apiName: 'Contact',
  cdcTopic: '/data/ContactChangeEvent',
  fieldNames: ['Id', 'FirstName', 'LastName', 'Email', 'AccountId'],
};

const MOCK_SF_CONFIG: TSalesforceConfig = {
  filename: 'salesforce.config.json',
  instanceUrl: 'https://test.salesforce.com',
  clientId: 'client-id',
  clientSecret: 'client-secret',
  scopes: [],
  accessToken: 'access-token',
  refreshToken: 'refresh-token',
  expiresAt: new Date(Date.now() + 3600_000),
  tenantId: 'tenant-1',
  databaseConfig: {
    type: 'pg' as const,
    host: 'localhost',
    port: 5432,
    username: 'postgres',
    password: 'postgres',
    database: 'test',
    schema: 'public',
  },
  objects: [MOCK_OBJECT],
};

const MOCK_FIELDS = [
  { name: 'Id', typeSql: 'TEXT', required: true, primaryKey: true },
  { name: 'FirstName', typeSql: 'TEXT', required: false, primaryKey: false },
  { name: 'LastName', typeSql: 'TEXT', required: false, primaryKey: false },
  { name: 'Email', typeSql: 'TEXT', required: false, primaryKey: false },
];

function makeParseEvent(
  changeType: string,
  recordIds: string[] = ['003xx001'],
  extraPayload: Record<string, unknown> = {},
): ParseEvent {
  return {
    id: 'evt-1',
    replayId: 100,
    schema: '{}',
    payload: {
      ChangeEventHeader: {
        changeType: changeType as any,
        entityName: 'Contact',
        recordIds,
        changeOrigin: 'com/salesforce/api/rest',
        transactionKey: 'tx-key-1',
        sequenceNumber: 1n,
        commitTimestamp: 1700000000000n,
        commitNumber: 1n,
        commitUser: 'user@test.com',
        nulledFields: [],
        diffFields: [],
        changedFields: ['FirstName'],
      },
      ...extraPayload,
    },
  };
}

function makeGapInfo(type: 'NORMAL' | 'FULL_RESYNC', overrides: Partial<GapInfo> = {}): GapInfo {
  return {
    type,
    topic: '/data/ContactChangeEvent',
    objectName: 'Contact',
    fromReplayId: '100',
    toReplayId: '110',
    fromTimestamp: 1700000000000,
    toTimestamp: 1700000010000,
    recordIds: ['003xx001', '003xx002'],
    ...overrides,
  };
}

function makeMockJob<T>(name: string, data: T): Job<T> {
  return { name, data } as unknown as Job<T>;
}

// ── Module setup ──────────────────────────────────────────────────────────────

describe('Salesforce Async Processing Pipeline (e2e)', () => {
  let module: TestingModule;

  // Services under test
  let salesforceAsyncService: SalesforceAsyncService;
  let handler: CdcGapHandlerService;
  let cdcProcessor: CdcProcessor;
  let gapProcessor: GapProcessor;

  // Mocks — typed as `any` on the mock variables so jest.fn() methods are accessible
  let mockRepo: { execute: jest.Mock };
  let mockDbService: {
    getRepo: jest.Mock;
    upsertMany: jest.Mock;
    updateMany: jest.Mock;
    softDeleteMany: jest.Mock;
    undeleteMany: jest.Mock;
    getExistingIds: jest.Mock;
  };
  let mockDirtyService: {
    filterDirtyIds: jest.Mock;
    addDirty: jest.Mock;
    removeDirty: jest.Mock;
    addDirtyAll: jest.Mock;
    removeDirtyAll: jest.Mock;
  };
  let mockSalesforceService: {
    verifyConnection: jest.Mock;
    describe: jest.Mock;
    createConnection: jest.Mock;
  };
  let mockGrpcService: { subscribe: jest.Mock; resumeStream: jest.Mock };
  let mockCdcQueue: { add: jest.Mock };
  let mockGapQueue: { add: jest.Mock };
  let mockRedisPubSub: { publish: jest.Mock };

  // Captured gRPC onEvent callback (set when grpcService.subscribe() is called)
  let capturedOnEvent: (events: ParseEvent[], gapInfo?: GapInfo) => void | Promise<void>;

  beforeEach(async () => {
    mockRepo = { execute: jest.fn().mockResolvedValue(undefined) };

    mockDbService = {
      getRepo: jest.fn().mockReturnValue(mockRepo),
      upsertMany: jest.fn().mockResolvedValue(undefined),
      updateMany: jest.fn().mockResolvedValue(undefined),
      softDeleteMany: jest.fn().mockResolvedValue(undefined),
      undeleteMany: jest.fn().mockResolvedValue(undefined),
      getExistingIds: jest.fn().mockResolvedValue([]),
    };

    // By default: no records are dirty → all IDs pass through
    mockDirtyService = {
      filterDirtyIds: jest.fn().mockImplementation((_f, _o, ids: string[]) => Promise.resolve(ids)),
      addDirty: jest.fn().mockResolvedValue(undefined),
      removeDirty: jest.fn().mockResolvedValue(undefined),
      addDirtyAll: jest.fn().mockResolvedValue(undefined),
      removeDirtyAll: jest.fn().mockResolvedValue(undefined),
    };

    mockSalesforceService = {
      verifyConnection: jest.fn().mockResolvedValue(undefined),
      describe: jest.fn().mockResolvedValue({ fields: [] }),
      createConnection: jest.fn().mockReturnValue({}),
    };

    mockGrpcService = {
      subscribe: jest.fn().mockImplementation(
        (
          _config: SalesforceGrpcConfig,
          _topic: string,
          _obj: string,
          _mapping: Record<string, string[]>,
          onEvent: (events: ParseEvent[], gapInfo?: GapInfo) => void | Promise<void>,
          _opts: SubscriptionOptions,
        ) => {
          capturedOnEvent = onEvent;
          return Promise.resolve();
        },
      ),
      resumeStream: jest.fn().mockResolvedValue(undefined),
    };

    mockCdcQueue = { add: jest.fn().mockResolvedValue({ id: 'job-1' }) };
    mockGapQueue = { add: jest.fn().mockResolvedValue({ id: 'job-2' }) };

    mockRedisPubSub = {
      publish: jest.fn().mockResolvedValue(undefined),
    };

    // Mock readFileSync so SalesforceAsyncService can load config without a real file
    jest.spyOn(fs, 'readFileSync').mockReturnValue(JSON.stringify(MOCK_SF_CONFIG) as any);

    module = await Test.createTestingModule({
      providers: [
        SalesforceAsyncService,
        CdcGapHandlerService,
        CdcProcessor,
        GapProcessor,
        { provide: DbService, useValue: mockDbService },
        { provide: DirtyRecordService, useValue: mockDirtyService },
        { provide: SalesforceService, useValue: mockSalesforceService },
        { provide: SalesforceGrpcService, useValue: mockGrpcService },
        { provide: RedisPubSubService, useValue: mockRedisPubSub },
        { provide: TypeConfigService, useValue: { get: jest.fn().mockReturnValue('http://localhost:3000') } },
        { provide: getQueueToken(QUEUE_NAMES.CDC), useValue: mockCdcQueue },
        { provide: getQueueToken(QUEUE_NAMES.GAP), useValue: mockGapQueue },
        { provide: getQueueToken(QUEUE_NAMES.SALESFORCE_SYNC), useValue: { add: jest.fn() } },
      ],
    }).compile();

    salesforceAsyncService = module.get(SalesforceAsyncService);
    handler = module.get(CdcGapHandlerService);
    cdcProcessor = module.get(CdcProcessor);
    gapProcessor = module.get(GapProcessor);
  });

  afterEach(async () => {
    jest.restoreAllMocks();
    await module.close();
  });

  // ── 1. Event routing: cdc() → onEvent → BullMQ ───────────────────────────────

  describe('Event routing — cdc() setup', () => {
    beforeEach(async () => {
      // getFields is called via getCompoundFieldMapping → describe() → returns []
      // We also need getFields to work for the handler tests later
      jest.spyOn(salesforceAsyncService, 'getSalesforceConfigValidated').mockResolvedValue(MOCK_SF_CONFIG);
      jest.spyOn(salesforceAsyncService, 'getFields').mockResolvedValue(MOCK_FIELDS);
      await salesforceAsyncService.cdc('salesforce.config.json');
    });

    it('should subscribe to gRPC topic on cdc()', () => {
      expect(mockGrpcService.subscribe).toHaveBeenCalledWith(
        expect.objectContaining({ tenantId: 'tenant-1' }),
        '/data/ContactChangeEvent',
        'Contact',
        expect.any(Object),
        expect.any(Function),
        expect.any(Object),
      );
    });

    it('should enqueue CDC job when normal event arrives', async () => {
      const event = makeParseEvent('CREATE', ['003xx001'], { FirstName: 'John', LastName: 'Doe' });

      await capturedOnEvent([event]);

      expect(mockCdcQueue.add).toHaveBeenCalledWith(
        'cdc',
        expect.objectContaining({
          object: MOCK_OBJECT,
          filename: 'salesforce.config.json',
          events: expect.any(Array),
        }),
        expect.any(Object),
      );
    });

    it('should enqueue NORMAL_WITH_IDS gap job when gap event with IDs arrives', async () => {
      const event = makeParseEvent('GAP_CREATE', ['003xx001', '003xx002']);
      const gapInfo = makeGapInfo('NORMAL', { recordIds: ['003xx001', '003xx002'] });

      await capturedOnEvent([event], gapInfo);

      expect(mockGapQueue.add).toHaveBeenCalledWith(
        GAP_JOB_NAMES.NORMAL_WITH_IDS,
        expect.objectContaining({
          gapInfo,
          topicName: '/data/ContactChangeEvent',
          tenantId: 'tenant-1',
        }),
        expect.any(Object),
      );
    });

    it('should enqueue NORMAL_WITHOUT_IDS gap job when gap has no IDs', async () => {
      const event = makeParseEvent('GAP_CREATE', []);
      const gapInfo = makeGapInfo('NORMAL', { recordIds: undefined });

      await capturedOnEvent([event], gapInfo);

      expect(mockGapQueue.add).toHaveBeenCalledWith(
        GAP_JOB_NAMES.NORMAL_WITHOUT_IDS,
        expect.any(Object),
        expect.any(Object),
      );
    });

    it('should enqueue FULL_RESYNC job when gap type is FULL_RESYNC', async () => {
      const event = makeParseEvent('GAP_OVERFLOW');
      const gapInfo = makeGapInfo('FULL_RESYNC');

      await capturedOnEvent([event], gapInfo);

      expect(mockGapQueue.add).toHaveBeenCalledWith(
        GAP_JOB_NAMES.FULL_RESYNC,
        expect.objectContaining({ gapInfo }),
        expect.any(Object),
      );
    });

    it('should mark dirty-all when FULL_RESYNC gap arrives', async () => {
      const gapInfo = makeGapInfo('FULL_RESYNC');

      await capturedOnEvent([makeParseEvent('GAP_OVERFLOW')], gapInfo);

      expect(mockDirtyService.addDirtyAll).toHaveBeenCalledWith(
        'salesforce.config.json',
        gapInfo.objectName,
      );
    });

    it('should mark specific records dirty when NORMAL gap with IDs arrives', async () => {
      const recordIds = ['003xx001', '003xx002'];
      const gapInfo = makeGapInfo('NORMAL', { recordIds });

      await capturedOnEvent([makeParseEvent('GAP_CREATE', recordIds)], gapInfo);

      expect(mockDirtyService.addDirty).toHaveBeenCalledWith(
        'salesforce.config.json',
        gapInfo.objectName,
        recordIds,
      );
    });
  });

  // ── 2. CDC processor → CdcGapHandlerService → DB ─────────────────────────────

  describe('CDC job processing', () => {
    beforeEach(() => {
      jest.spyOn(salesforceAsyncService, 'getSalesforceConfigValidated').mockResolvedValue(MOCK_SF_CONFIG);
      jest.spyOn(salesforceAsyncService, 'getFields').mockResolvedValue(MOCK_FIELDS);
      jest.spyOn(salesforceAsyncService, 'createConnection').mockReturnValue({} as any);
    });

    async function runCdcJob(events: ParseEvent[]): Promise<void> {
      const payload: CdcJobPayload = {
        events: serializeParseEvents(events),
        object: MOCK_OBJECT,
        filename: 'salesforce.config.json',
      };
      await cdcProcessor.process(makeMockJob('cdc', payload));
    }

    describe('CREATE event', () => {
      it('should upsert the new record into DB', async () => {
        const event = makeParseEvent('CREATE', ['003xx001'], { FirstName: 'John', LastName: 'Doe' });

        await runCdcJob([event]);

        expect(mockDbService.upsertMany).toHaveBeenCalledWith(
          mockRepo,
          MOCK_OBJECT.name,
          expect.arrayContaining([expect.objectContaining({ Id: '003xx001' })]),
        );
      });

      it('should skip record when it is dirty', async () => {
        mockDirtyService.filterDirtyIds!.mockResolvedValue([]); // all dirty
        const event = makeParseEvent('CREATE', ['003xx001'], { FirstName: 'John' });

        await runCdcJob([event]);

        expect(mockDbService.upsertMany).not.toHaveBeenCalled();
      });

      it('should process only clean IDs when batch is partially dirty', async () => {
        mockDirtyService.filterDirtyIds!.mockResolvedValue(['003xx002']); // 003xx001 is dirty
        const event = makeParseEvent('CREATE', ['003xx001', '003xx002'], { FirstName: 'John' });

        await runCdcJob([event]);

        expect(mockDbService.upsertMany).toHaveBeenCalledWith(
          mockRepo,
          MOCK_OBJECT.name,
          expect.arrayContaining([expect.objectContaining({ Id: '003xx002' })]),
        );
        const [[, , records]] = (mockDbService.upsertMany as jest.Mock).mock.calls;
        expect(records.map((r: any) => r.Id)).not.toContain('003xx001');
      });
    });

    describe('UPDATE event', () => {
      it('should update existing record in DB', async () => {
        mockDbService.getExistingIds!.mockResolvedValue(['003xx001']);
        const event = makeParseEvent('UPDATE', ['003xx001'], { LastName: 'Smith' });

        await runCdcJob([event]);

        expect(mockDbService.updateMany).toHaveBeenCalledWith(
          mockRepo,
          MOCK_OBJECT.name,
          ['003xx001'],
          expect.objectContaining({ LastName: 'Smith' }),
        );
      });

      it('should fetch missing record from Salesforce and upsert when not in DB', async () => {
        mockDbService.getExistingIds!.mockResolvedValue([]); // not in DB
        jest.spyOn(salesforceAsyncService, 'syncRecord').mockResolvedValue([]);
        const event = makeParseEvent('UPDATE', ['003xx001'], { LastName: 'New' });

        await runCdcJob([event]);

        expect(salesforceAsyncService.syncRecord).toHaveBeenCalledWith(
          mockRepo,
          expect.any(Object),
          'Contact',
          MOCK_FIELDS,
          expect.stringContaining('003xx001'),
        );
        expect(mockDbService.updateMany).toHaveBeenCalled();
      });

      it('should skip update when record is dirty', async () => {
        mockDirtyService.filterDirtyIds!.mockResolvedValue([]);
        const event = makeParseEvent('UPDATE', ['003xx001'], { LastName: 'Smith' });

        await runCdcJob([event]);

        expect(mockDbService.updateMany).not.toHaveBeenCalled();
      });
    });

    describe('DELETE event', () => {
      it('should soft-delete record when it exists in DB', async () => {
        mockDbService.getExistingIds!.mockResolvedValue(['003xx001']);
        const event = makeParseEvent('DELETE', ['003xx001']);

        await runCdcJob([event]);

        expect(mockDbService.softDeleteMany).toHaveBeenCalledWith(
          mockRepo,
          MOCK_OBJECT.name,
          ['003xx001'],
        );
      });

      it('should skip soft-delete when record is not in DB', async () => {
        mockDbService.getExistingIds!.mockResolvedValue([]); // not in DB
        const event = makeParseEvent('DELETE', ['003xx001']);

        await runCdcJob([event]);

        expect(mockDbService.softDeleteMany).not.toHaveBeenCalled();
      });
    });

    describe('UNDELETE event', () => {
      it('should restore record when it exists in DB', async () => {
        mockDbService.getExistingIds!.mockResolvedValue(['003xx001']);
        const event = makeParseEvent('UNDELETE', ['003xx001']);

        await runCdcJob([event]);

        expect(mockDbService.undeleteMany).toHaveBeenCalledWith(
          mockRepo,
          MOCK_OBJECT.name,
          ['003xx001'],
        );
      });

      it('should fetch record from Salesforce and restore when not in DB', async () => {
        mockDbService.getExistingIds!.mockResolvedValue([]);
        jest.spyOn(salesforceAsyncService, 'syncRecord').mockResolvedValue([]);
        const event = makeParseEvent('UNDELETE', ['003xx001']);

        await runCdcJob([event]);

        expect(salesforceAsyncService.syncRecord).toHaveBeenCalled();
        expect(mockDbService.undeleteMany).toHaveBeenCalled();
      });
    });

    describe('batch of events', () => {
      it('should process each event independently in one job', async () => {
        // CREATE does not call getExistingIds; UPDATE does
        mockDbService.getExistingIds.mockResolvedValue(['003xx002']);

        const events = [
          makeParseEvent('CREATE', ['003xx001'], { FirstName: 'Alice' }),
          makeParseEvent('UPDATE', ['003xx002'], { LastName: 'Smith' }),
        ];

        await runCdcJob(events);

        expect(mockDbService.upsertMany).toHaveBeenCalledTimes(1);
        expect(mockDbService.updateMany).toHaveBeenCalledTimes(1);
      });
    });
  });

  // ── 3. GAP processor → CdcGapHandlerService → DB + SF sync ───────────────────

  describe('GAP job processing', () => {
    beforeEach(() => {
      jest.spyOn(salesforceAsyncService, 'getSalesforceConfigValidated').mockResolvedValue(MOCK_SF_CONFIG);
      jest.spyOn(salesforceAsyncService, 'getFields').mockResolvedValue(MOCK_FIELDS);
      jest.spyOn(salesforceAsyncService, 'createConnection').mockReturnValue({} as any);
      jest.spyOn(salesforceAsyncService, 'syncRecord').mockResolvedValue([]);
      jest.spyOn(salesforceAsyncService, 'getDeletedRecordIds').mockResolvedValue([]);
    });

    describe('NORMAL gap with record IDs', () => {
      it('should sync records from Salesforce by ID', async () => {
        const gapInfo = makeGapInfo('NORMAL', { recordIds: ['003xx001', '003xx002'] });
        const payload: GapJobPayload = {
          gapInfo,
          filename: 'salesforce.config.json',
          tenantId: 'tenant-1',
          topicName: '/data/ContactChangeEvent',
        };

        await gapProcessor.process(makeMockJob(GAP_JOB_NAMES.NORMAL_WITH_IDS, payload));

        expect(salesforceAsyncService.syncRecord).toHaveBeenCalledWith(
          mockRepo,
          expect.any(Object),
          'Contact',
          MOCK_FIELDS,
          expect.stringContaining('003xx001'),
        );
      });

      it('should mark records dirty before sync and clear after success', async () => {
        const recordIds = ['003xx001', '003xx002'];
        const gapInfo = makeGapInfo('NORMAL', { recordIds });
        const payload: GapJobPayload = {
          gapInfo,
          filename: 'salesforce.config.json',
          tenantId: 'tenant-1',
          topicName: '/data/ContactChangeEvent',
        };

        await gapProcessor.process(makeMockJob(GAP_JOB_NAMES.NORMAL_WITH_IDS, payload));

        expect(mockDirtyService.addDirty).toHaveBeenCalledWith(
          'salesforce.config.json',
          'Contact',
          recordIds,
        );
        expect(mockDirtyService.removeDirty).toHaveBeenCalledWith(
          'salesforce.config.json',
          'Contact',
          recordIds,
        );
      });

      it('should perform catch-up sync when syncRecord returns records with LastModifiedDate', async () => {
        jest.spyOn(salesforceAsyncService, 'syncRecord')
          .mockResolvedValueOnce([{ LastModifiedDate: '2024-01-15T10:00:00Z' }])
          .mockResolvedValue([]); // catch-up sync

        const gapInfo = makeGapInfo('NORMAL', { recordIds: ['003xx001'] });
        const payload: GapJobPayload = {
          gapInfo,
          filename: 'salesforce.config.json',
          tenantId: 'tenant-1',
          topicName: '/data/ContactChangeEvent',
        };

        await gapProcessor.process(makeMockJob(GAP_JOB_NAMES.NORMAL_WITH_IDS, payload));

        // syncRecord should be called at least twice: initial sync + catch-up
        expect(salesforceAsyncService.syncRecord).toHaveBeenCalledTimes(2);
        const catchUpCall = (salesforceAsyncService.syncRecord as jest.Mock).mock.calls[1];
        expect(catchUpCall[4]).toContain('LastModifiedDate >');
      });
    });

    describe('NORMAL gap without record IDs', () => {
      it('should sync by timestamp range and clear dirty-all flag', async () => {
        const gapInfo = makeGapInfo('NORMAL', { recordIds: undefined });
        const payload: GapJobPayload = {
          gapInfo,
          filename: 'salesforce.config.json',
          tenantId: 'tenant-1',
          topicName: '/data/ContactChangeEvent',
        };

        await gapProcessor.process(makeMockJob(GAP_JOB_NAMES.NORMAL_WITHOUT_IDS, payload));

        expect(salesforceAsyncService.syncRecord).toHaveBeenCalledWith(
          mockRepo,
          expect.any(Object),
          'Contact',
          MOCK_FIELDS,
          expect.stringMatching(/LastModifiedDate/),
        );
        expect(mockDirtyService.removeDirtyAll).toHaveBeenCalledWith(
          'salesforce.config.json',
          'Contact',
        );
      });
    });

    describe('FULL_RESYNC gap', () => {
      it('should sync all recent records from Salesforce', async () => {
        const gapInfo = makeGapInfo('FULL_RESYNC');
        const payload: GapJobPayload = {
          gapInfo,
          filename: 'salesforce.config.json',
          tenantId: 'tenant-1',
          topicName: '/data/ContactChangeEvent',
        };

        await gapProcessor.process(makeMockJob(GAP_JOB_NAMES.FULL_RESYNC, payload));

        expect(salesforceAsyncService.syncRecord).toHaveBeenCalledWith(
          mockRepo,
          expect.any(Object),
          'Contact',
          MOCK_FIELDS,
          expect.stringMatching(/LastModifiedDate >= /),
        );
      });

      it('should soft-delete records that were deleted in Salesforce', async () => {
        jest.spyOn(salesforceAsyncService, 'getDeletedRecordIds').mockResolvedValue(['003xx099']);
        mockDbService.getExistingIds!.mockResolvedValue(['003xx099']); // exists in our DB

        const gapInfo = makeGapInfo('FULL_RESYNC');
        const payload: GapJobPayload = {
          gapInfo,
          filename: 'salesforce.config.json',
          tenantId: 'tenant-1',
          topicName: '/data/ContactChangeEvent',
        };

        await gapProcessor.process(makeMockJob(GAP_JOB_NAMES.FULL_RESYNC, payload));

        expect(mockDbService.softDeleteMany).toHaveBeenCalledWith(
          mockRepo,
          MOCK_OBJECT.name,
          ['003xx099'],
        );
      });

      it('should publish stream resume signal via Redis after full resync', async () => {
        const gapInfo = makeGapInfo('FULL_RESYNC');
        const payload: GapJobPayload = {
          gapInfo,
          filename: 'salesforce.config.json',
          tenantId: 'tenant-1',
          topicName: '/data/ContactChangeEvent',
        };

        await gapProcessor.process(makeMockJob(GAP_JOB_NAMES.FULL_RESYNC, payload));

        expect(mockRedisPubSub.publish).toHaveBeenCalledWith(
          STREAM_RESUME_CHANNEL,
          JSON.stringify({ tenantId: 'tenant-1', topicName: '/data/ContactChangeEvent' }),
        );
      });

      it('should resume stream directly when Redis publish fails', async () => {
        // Use a non-connection error so publishWithRetry throws immediately (no retry delays)
        mockRedisPubSub.publish.mockRejectedValue(new Error('publish failed: queue full'));

        const gapInfo = makeGapInfo('FULL_RESYNC');
        const payload: GapJobPayload = {
          gapInfo,
          filename: 'salesforce.config.json',
          tenantId: 'tenant-1',
          topicName: '/data/ContactChangeEvent',
        };

        await gapProcessor.process(makeMockJob(GAP_JOB_NAMES.FULL_RESYNC, payload));

        expect(mockGrpcService.resumeStream).toHaveBeenCalledWith(
          'tenant-1',
          '/data/ContactChangeEvent',
        );
      });

      it('should clear dirty-all flag after successful resync', async () => {
        const gapInfo = makeGapInfo('FULL_RESYNC');
        const payload: GapJobPayload = {
          gapInfo,
          filename: 'salesforce.config.json',
          tenantId: 'tenant-1',
          topicName: '/data/ContactChangeEvent',
        };

        await gapProcessor.process(makeMockJob(GAP_JOB_NAMES.FULL_RESYNC, payload));

        expect(mockDirtyService.removeDirtyAll).toHaveBeenCalledWith(
          'salesforce.config.json',
          'Contact',
        );
      });
    });
  });
});
