import { parseEvent } from '@app/helper/modules/salesforce/utils/event-parser';
import * as grpc from '@grpc/grpc-js';
import * as protoLoader from '@grpc/proto-loader';
import {
  Injectable,
  Logger,
  OnModuleDestroy,
  OnModuleInit,
} from '@nestjs/common';
import { Type } from 'avsc';
import { join } from 'path';

import {
  DEFAULT_NUM_REQUESTED,
  FLOW_CONTROL_THRESHOLD,
  GAP_CONFIG,
  RECONNECT_CONFIG,
  SALESFORCE_GRPC_ENDPOINT,
} from './constants/salesforce-grpc.constants';
import {
  EventBusV1Package,
  FetchRequest,
  FetchResponse,
  GapInfo,
  ParseEvent,
  PauseReason,
  PubSubClient,
  ReplayPreset,
  SalesforceGrpcConfig,
  SchemaInfo,
  StreamKey,
  StreamMap,
  StreamState,
  StreamStateInfo,
  SubscriptionConfig,
  SubscriptionOptions,
  TopicInfo,
} from './types/grpc.types';

@Injectable()
export class SalesforceGrpcService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(SalesforceGrpcService.name);

  // gRPC Infrastructure
  private packageDef!: grpc.GrpcObject;
  private readonly clients = new Map<string, PubSubClient>();

  // Schema Caching
  private readonly schemaCache = new Map<string, SchemaInfo>();
  private readonly avroTypeCache = new Map<string, Type>();

  // Stream Management
  private readonly activeStreams: StreamMap = new Map();
  private readonly subscriptionConfigs = new Map<
    StreamKey,
    SubscriptionConfig
  >();
  private readonly streamStates = new Map<StreamKey, StreamStateInfo>();
  private readonly reconnectTimers = new Map<StreamKey, NodeJS.Timeout>();

  // Lifecycle
  private isShuttingDown = false;

  constructor() {}

  // LIFECYCLE HOOKS
  onModuleInit(): void {
    this.initializeProtoDefinition();
  }

  onModuleDestroy(): void {
    this.cleanup();
  }

  async subscribe(
    config: SalesforceGrpcConfig,
    topicName: string,
    objectName: string,
    compoundFieldMapping: Record<string, string[]>,
    onEvent: (events: ParseEvent[], gapInfo?: GapInfo) => void | Promise<void>,
    options: SubscriptionOptions = {},
  ): Promise<void> {
    const topicInfo = await this.getTopic(config, topicName);
    await this.getSchema(config, topicInfo.schema_id);
    const streamKey = this.createStreamKey(
      config.tenantId,
      topicInfo.topic_name,
    );

    const subscriptionConfig = {
      config,
      topicName,
      objectName,
      compoundFieldMapping,
      onEvent,
      options: {
        numRequested: options.numRequested ?? DEFAULT_NUM_REQUESTED,
        replayPreset: options.replayPreset ?? ReplayPreset.LATEST,
        replayId: options.replayId,
      } as Required<SubscriptionOptions>,
      retryCount: 0,
    };

    this.subscriptionConfigs.set(streamKey, subscriptionConfig);
    this.closeStream(streamKey);
    this.clearReconnectTimer(streamKey);

    const stream = this.createStream(
      config,
      topicInfo,
      streamKey,
      subscriptionConfig,
    );

    this.setupStreamHandlers(stream, topicInfo, streamKey, subscriptionConfig);
  }

  unsubscribe(tenantId: string, topicName: string): void {
    const streamKey = this.createStreamKey(tenantId, topicName);
    this.clearReconnectTimer(streamKey);
    this.subscriptionConfigs.delete(streamKey);
    this.closeStream(streamKey);
    this.streamStates.delete(streamKey);
  }

  getActiveSubscriptions(): string[] {
    return Array.from(this.activeStreams.keys());
  }

  // INITIALIZATION
  private initializeProtoDefinition(): void {
    const protoPath = join(__dirname, './proto/pubsub_api.proto');

    const packageDefinition = protoLoader.loadSync(protoPath, {
      keepCase: true,
      longs: String,
      enums: String,
      defaults: true,
      oneofs: true,
    });

    this.packageDef = grpc.loadPackageDefinition(packageDefinition);
    this.logger.log('Proto file loaded successfully');
  }

  private createStream(
    config: SalesforceGrpcConfig,
    topicInfo: TopicInfo,
    streamKey: StreamKey,
    subscriptionConfig: SubscriptionConfig,
  ): grpc.ClientDuplexStream<FetchRequest, FetchResponse> {
    const client = this.getClient(config.tenantId);
    const metadata = this.createMetadata(config);
    const stream = client.Subscribe(metadata);

    this.activeStreams.set(streamKey, stream);

    const fetchRequest = this.createFetchRequest(
      topicInfo.topic_name,
      subscriptionConfig.options,
    );

    stream.write(fetchRequest);
    this.logger.log(`Subscribed to topic: ${topicInfo.topic_name}`);

    return stream;
  }

  private createFetchRequest(
    topicName: string,
    options: Required<SubscriptionOptions>,
  ): FetchRequest {
    return {
      topic_name: topicName,
      num_requested: options.numRequested,
      replay_preset: options.replayPreset,
      ...(options.replayId && { replay_id: options.replayId }),
    };
  }

  // STREAM HANDLERS
  private setupStreamHandlers(
    stream: grpc.ClientDuplexStream<FetchRequest, FetchResponse>,
    topicInfo: TopicInfo,
    streamKey: StreamKey,
    subscriptionConfig: SubscriptionConfig,
  ): void {
    stream.on(
      'data',
      (response: FetchResponse) =>
        void this.handleStreamData(
          response,
          topicInfo,
          streamKey,
          subscriptionConfig,
        ),
    );

    stream.on('error', (error) =>
      this.handleStreamError(error, topicInfo, streamKey),
    );

    stream.on('end', () => this.handleStreamEnd(topicInfo, streamKey));
  }

  private async handleStreamData(
    response: FetchResponse,
    topicInfo: TopicInfo,
    streamKey: StreamKey,
    subscriptionConfig: SubscriptionConfig,
  ): Promise<void> {
    if (this.isKeepaliveMessage(response)) {
      this.updateReplayId(streamKey, response.latest_replay_id);
      this.logKeepalive(response.latest_replay_id);
      return;
    }

    try {
      await this.processEvents(
        response,
        topicInfo,
        subscriptionConfig,
        streamKey,
      );
      this.handleFlowControl(response, topicInfo, subscriptionConfig);
      this.updateReplayId(streamKey, response.latest_replay_id);
    } catch (error) {
      this.logger.error(
        `Failed to process events for ${topicInfo.topic_name}: ${error instanceof Error ? error.message : String(error)}`,
        error instanceof Error ? error.stack : undefined,
      );
      // Do not advance replayId so replay can retry from same point if supported
    }
  }

  private updateReplayId(streamKey: StreamKey, replayId?: Buffer): void {
    const subConfig = this.subscriptionConfigs.get(streamKey);
    if (subConfig && replayId) {
      subConfig.lastReplayId = replayId;
      subConfig.retryCount = 0;
    }
  }

  private isKeepaliveMessage(response: FetchResponse): boolean {
    return !response.events || response.events.length === 0;
  }

  private logKeepalive(replayId?: Buffer): void {
    this.logger.debug(
      `Keepalive received. Latest replay ID: ${replayId?.toString('hex')}`,
    );
  }

  private async processEvents(
    response: FetchResponse,
    topicInfo: TopicInfo,
    subscriptionConfig: SubscriptionConfig,
    streamKey: StreamKey,
  ): Promise<void> {
    const streamState = this.streamStates.get(streamKey);
    if (streamState?.state === StreamState.PAUSED) {
      this.logger.warn(`Stream ${streamKey} is PAUSED, ignoring data`);
      return;
    }

    this.logger.log(`Received ${response.events.length} events`);

    const schema = this.schemaCache.get(topicInfo.schema_id);
    if (!schema) {
      throw new Error(`Schema not found for topic: ${topicInfo.topic_name}`);
    }

    const parsedEvents = response.events.map((event) =>
      parseEvent(
        event,
        schema.schema_json,
        this.avroTypeCache,
        subscriptionConfig.compoundFieldMapping,
      ),
    );

    const { gapEvents, continuousEvents } = this.categorizeEvents(parsedEvents);

    if (gapEvents.length > 0) {
      await this.handleGapEvents(gapEvents, subscriptionConfig, streamKey);
    }

    if (continuousEvents.length > 0) {
      await subscriptionConfig.onEvent(continuousEvents);
    }
  }

  private async handleGapEvents(
    events: ParseEvent[],
    subscriptionConfig: SubscriptionConfig,
    streamKey: StreamKey,
  ) {
    if (events.length === 0) return;

    const now = new Date();
    const lastGapTime = subscriptionConfig.lastGapEventTime;

    if (
      lastGapTime &&
      now.getTime() - lastGapTime.getTime() > GAP_CONFIG.gapEventResetWindowMs
    ) {
      subscriptionConfig.gapEventCount = 0;
      this.logger.log('GAP event counter reset due to time window');
    }

    const currentCount = subscriptionConfig.gapEventCount || 0;
    const totalCount = currentCount + events.length;

    const gapInfo: GapInfo = {
      type: 'NORMAL',
      topic: subscriptionConfig.topicName,
      objectName: events[0].payload?.ChangeEventHeader?.entityName,
      fromReplayId: events[0].replayId as unknown as string,
      toReplayId: events[events.length - 1].replayId as unknown as string,
      fromTimestamp: Number(
        events[0].payload?.ChangeEventHeader
          ?.commitTimestamp as unknown as number,
      ),
      toTimestamp: Number(
        events[events.length - 1].payload?.ChangeEventHeader
          ?.commitTimestamp as unknown as number,
      ),
    };

    const inResumeCooldown: boolean = Boolean(
      subscriptionConfig.resumedAt &&
      now.getTime() - subscriptionConfig.resumedAt.getTime() <
        GAP_CONFIG.resumeCooldownMs,
    );

    if (
      events.some(
        (event) =>
          event.payload?.ChangeEventHeader?.changeType === 'GAP_OVERFLOW',
      )
    ) {
      gapInfo.type = 'FULL_RESYNC';
      subscriptionConfig.gapEventCount = 0;
      subscriptionConfig.lastGapEventTime = undefined;

      if (inResumeCooldown) {
        this.logger.log(
          `GAP_OVERFLOW after resume (cooldown ${GAP_CONFIG.resumeCooldownMs}ms), delivered without pause to avoid loop`,
        );
        await subscriptionConfig.onEvent(events, gapInfo);
        return;
      }

      this.logger.warn(`GAP overflow detected. Triggering full resync.`);
      this.pauseStream(streamKey, {
        type: 'GAP_OVERFLOW',
        metadata: {
          gapCount: totalCount,
          eventCount: events.length,
          threshold: GAP_CONFIG.maxGapEventsBeforeResync,
          timestamp: now,
          lastReplayId: subscriptionConfig.lastReplayId,
        },
      });
      await subscriptionConfig.onEvent(events, gapInfo);
      return;
    }

    if (totalCount >= GAP_CONFIG.maxGapEventsBeforeResync) {
      gapInfo.type = 'FULL_RESYNC';
      subscriptionConfig.gapEventCount = 0;
      subscriptionConfig.lastGapEventTime = undefined;

      if (inResumeCooldown) {
        this.logger.log(
          `GAP threshold exceeded after resume (cooldown ${GAP_CONFIG.resumeCooldownMs}ms), delivered without pause to avoid loop`,
        );
        await subscriptionConfig.onEvent(events, gapInfo);
        return;
      }

      this.logger.warn(
        `GAP threshold exceeded: ${totalCount}/${GAP_CONFIG.maxGapEventsBeforeResync}. Triggering full resync.`,
      );
      this.pauseStream(streamKey, {
        type: 'GAP_THRESHOLD',
        metadata: {
          gapCount: totalCount,
          eventCount: events.length,
          threshold: GAP_CONFIG.maxGapEventsBeforeResync,
          timestamp: now,
          lastReplayId: subscriptionConfig.lastReplayId,
        },
      });
      await subscriptionConfig.onEvent(events, gapInfo);
      return;
    }

    subscriptionConfig.gapEventCount = totalCount;
    subscriptionConfig.lastGapEventTime = now;

    // NORMAL gap: pass record IDs from gap event payload for reconcile-by-ID (Salesforce doc)
    const recordIds = Array.from(
      new Set(
        events.flatMap(
          (e) =>
            (e.payload?.ChangeEventHeader?.recordIds as string[] | undefined) ??
            [],
        ),
      ),
    ).filter(Boolean);
    if (recordIds.length > 0) {
      gapInfo.recordIds = recordIds;
    }

    this.logger.log(
      `Processing ${events.length} GAP events. Total count: ${totalCount}/${GAP_CONFIG.maxGapEventsBeforeResync}`,
    );

    await subscriptionConfig.onEvent(events, gapInfo);
  }

  private pauseStream(streamKey: StreamKey, reason: PauseReason): void {
    const stream = this.activeStreams.get(streamKey);
    if (!stream) {
      this.logger.warn(`Cannot pause stream ${streamKey} - stream not found`);
      return;
    }

    this.streamStates.set(streamKey, {
      state: StreamState.PAUSED,
      pauseReason: reason,
    });

    this.activeStreams.delete(streamKey);

    stream.end();

    this.logger.warn(
      `Stream ${streamKey} PAUSED - ${reason?.type}`,
      reason?.metadata,
    );
  }

  async resumeStream(tenantId: string, topicName: string): Promise<void> {
    const streamKey = this.createStreamKey(tenantId, topicName);

    this.clearReconnectTimer(streamKey);

    const subConfig = this.subscriptionConfigs.get(streamKey);

    if (!subConfig) {
      const error = `No subscription config found for ${streamKey}`;
      this.logger.error(error);
      throw new Error(error);
    }

    const currentState = this.streamStates.get(streamKey);
    if (currentState?.state !== StreamState.PAUSED) {
      this.logger.warn(
        `Cannot resume stream ${streamKey} - current state: ${currentState?.state || 'UNKNOWN'}`,
      );
      return;
    }

    subConfig.gapEventCount = 0;
    subConfig.lastGapEventTime = undefined;
    subConfig.resumedAt = new Date();

    this.logger.log(`Resuming stream ${streamKey}`);

    this.closeStream(streamKey);

    try {
      await this.subscribe(
        subConfig.config,
        subConfig.topicName,
        subConfig.objectName,
        subConfig.compoundFieldMapping,
        subConfig.onEvent,
        this.prepareReconnectOptions(subConfig),
      );

      this.streamStates.set(streamKey, {
        state: StreamState.ACTIVE,
      });

      this.logger.log(`Stream ${streamKey} RESUMED successfully`);
    } catch (error) {
      this.logger.error(
        `Failed to resume stream ${streamKey}:`,
        error instanceof Error ? error.message : String(error),
      );

      throw error;
    }
  }

  private handleFlowControl(
    response: FetchResponse,
    topicInfo: TopicInfo,
    subscriptionConfig: SubscriptionConfig,
  ): void {
    const pendingRequested = response.pending_num_requested ?? 0;
    const threshold =
      subscriptionConfig.options.numRequested * FLOW_CONTROL_THRESHOLD;

    if (pendingRequested < threshold) {
      this.requestMoreEvents(topicInfo, subscriptionConfig);
    }
  }

  private categorizeEvents(parsedEvents: ParseEvent[]) {
    const gapEvents: ParseEvent[] = [];
    const continuousEvents: ParseEvent[] = [];

    for (const event of parsedEvents) {
      (event.payload?.ChangeEventHeader as any).changeType = 'GAP_CREATE';
      const changeType = event.payload?.ChangeEventHeader?.changeType;
      if (changeType?.startsWith('GAP_')) {
        gapEvents.push(event);
      } else {
        continuousEvents.push(event);
      }
    }

    return { gapEvents, continuousEvents };
  }

  private requestMoreEvents(
    topicInfo: TopicInfo,
    subscriptionConfig: SubscriptionConfig,
  ): void {
    const streamKey = this.createStreamKey(
      subscriptionConfig.config.tenantId,
      topicInfo.topic_name,
    );
    const stream = this.activeStreams.get(streamKey);

    if (!stream) return;

    const additionalRequest: FetchRequest = {
      topic_name: topicInfo.topic_name,
      num_requested: subscriptionConfig.options.numRequested,
    };

    stream.write(additionalRequest);
    this.logger.debug(
      `Flow control: Requested ${subscriptionConfig.options.numRequested} more events for ${topicInfo.topic_name}`,
    );
  }

  private handleStreamError(
    error: Error,
    topicInfo: TopicInfo,
    streamKey: StreamKey,
  ): void {
    this.logger.error(
      `Stream error for ${topicInfo.topic_name}: ${error.message}`,
    );
    this.activeStreams.delete(streamKey);

    // this.eventEmitter.emit('salesforce.stream.error', {
    //   topic: topicInfo.topic_name,
    //   error,
    // });

    this.scheduleReconnect(streamKey);
  }

  private handleStreamEnd(topicInfo: TopicInfo, streamKey: StreamKey): void {
    this.logger.log(`Stream ended for topic: ${topicInfo.topic_name}`);
    this.activeStreams.delete(streamKey);

    // this.eventEmitter.emit('salesforce.stream.end', {
    //   topic: topicInfo.topic_name,
    // });

    const streamState = this.streamStates.get(streamKey);
    if (streamState?.state === StreamState.PAUSED) {
      this.logger.log(
        `Stream ${streamKey} ended while PAUSED (expected), skipping reconnect until resume`,
      );
      return;
    }

    this.scheduleReconnect(streamKey);
  }

  // RECONNECTION LOGIC
  private scheduleReconnect(streamKey: StreamKey): void {
    if (this.isShuttingDown) {
      this.logger.log(
        `Skipping reconnect for ${streamKey} - service is shutting down`,
      );
      return;
    }

    const subConfig = this.subscriptionConfigs.get(streamKey);
    if (!subConfig) {
      this.logger.warn(
        `No subscription config found for ${streamKey}, cannot reconnect`,
      );
      return;
    }

    if (subConfig.retryCount >= RECONNECT_CONFIG.maxRetries) {
      this.logger.error(
        `Max reconnection attempts (${RECONNECT_CONFIG.maxRetries}) reached for ${streamKey}`,
      );
      return;
    }

    const delay = this.calculateReconnectDelay(subConfig.retryCount);
    subConfig.retryCount++;

    this.logger.log(
      `Scheduling reconnection for ${streamKey} in ${delay}ms ` +
        `(attempt ${subConfig.retryCount}/${RECONNECT_CONFIG.maxRetries})`,
    );

    this.setReconnectTimer(streamKey, delay);
  }

  private calculateReconnectDelay(retryCount: number): number {
    const exponentialDelay =
      RECONNECT_CONFIG.initialDelayMs *
      Math.pow(RECONNECT_CONFIG.backoffMultiplier, retryCount);

    return Math.min(exponentialDelay, RECONNECT_CONFIG.maxDelayMs);
  }

  private setReconnectTimer(streamKey: StreamKey, delay: number): void {
    this.clearReconnectTimer(streamKey);

    const timer = setTimeout(() => {
      this.reconnectTimers.delete(streamKey);
      void this.reconnect(streamKey);
    }, delay);

    this.reconnectTimers.set(streamKey, timer);
  }

  private async reconnect(streamKey: StreamKey): Promise<void> {
    if (this.isShuttingDown) return;

    const subConfig = this.subscriptionConfigs.get(streamKey);
    if (!subConfig) {
      this.logger.warn(`No subscription config found for ${streamKey}`);
      return;
    }

    this.logger.log(`Attempting to reconnect ${streamKey}...`);

    try {
      const reconnectOptions = this.prepareReconnectOptions(subConfig);

      await this.subscribe(
        subConfig.config,
        subConfig.topicName,
        subConfig.objectName,
        subConfig.compoundFieldMapping,
        subConfig.onEvent,
        reconnectOptions,
      );

      this.streamStates.set(streamKey, {
        state: StreamState.ACTIVE,
      });

      this.logger.log(`Stream ${streamKey} RECONNECTED successfully`);
    } catch (error) {
      this.handleFailedReconnect(streamKey, error);
    }
  }

  private prepareReconnectOptions(
    subConfig: SubscriptionConfig,
  ): SubscriptionOptions {
    const options: SubscriptionOptions = { ...subConfig.options };

    if (subConfig.lastReplayId) {
      options.replayPreset = ReplayPreset.CUSTOM;
      options.replayId = subConfig.lastReplayId;
      this.logger.log(
        `Reconnecting from replay ID: ${subConfig.lastReplayId.toString('hex')}`,
      );
    }

    return options;
  }

  private handleFailedReconnect(streamKey: StreamKey, error: unknown): void {
    this.logger.error(
      `Reconnection failed for ${streamKey}: ${error instanceof Error ? error.message : String(error)}`,
    );
    this.scheduleReconnect(streamKey);
  }

  private clearReconnectTimer(streamKey: StreamKey): void {
    const timer = this.reconnectTimers.get(streamKey);
    if (timer) {
      clearTimeout(timer);
      this.reconnectTimers.delete(streamKey);
    }
  }

  // GRPC OPERATIONS
  private async getTopic(
    config: SalesforceGrpcConfig,
    topicName: string,
  ): Promise<TopicInfo> {
    const client = this.getClient(config.tenantId);
    const metadata = this.createMetadata(config);

    return new Promise((resolve, reject) => {
      client.GetTopic(
        { topic_name: topicName },
        metadata,
        (error, response) => {
          if (error) {
            this.logger.error(`GetTopic error: ${error.message}`);
            reject(error);
          } else {
            this.logger.log(`GetTopic success for: ${topicName}`);
            resolve(response);
          }
        },
      );
    });
  }

  private async getSchema(
    config: SalesforceGrpcConfig,
    schemaId: string,
  ): Promise<SchemaInfo> {
    if (this.schemaCache.has(schemaId)) {
      return this.schemaCache.get(schemaId)!;
    }

    const client = this.getClient(config.tenantId);
    const metadata = this.createMetadata(config);

    return new Promise((resolve, reject) => {
      client.GetSchema({ schema_id: schemaId }, metadata, (error, response) => {
        if (error) {
          this.logger.error(`GetSchema error: ${error.message}`);
          reject(error);
        } else {
          this.schemaCache.set(schemaId, response);
          this.logger.log(`GetSchema success for: ${schemaId}`);
          resolve(response);
        }
      });
    });
  }

  // CLIENT MANAGEMENT
  private getClient(tenantId: string): PubSubClient {
    const existingClient = this.clients.get(tenantId);
    if (existingClient) {
      return existingClient;
    }

    const newClient = this.createClient();
    this.clients.set(tenantId, newClient);
    this.logger.log(`Created new gRPC client for tenant: ${tenantId}`);

    return newClient;
  }

  private createClient(): PubSubClient {
    const eventBus = this.packageDef.eventbus as unknown as {
      v1: EventBusV1Package;
    };
    const PubSubService = eventBus.v1.PubSub;

    return new PubSubService(
      SALESFORCE_GRPC_ENDPOINT,
      grpc.credentials.createSsl(),
    ) as unknown as PubSubClient;
  }

  private createMetadata(config: SalesforceGrpcConfig): grpc.Metadata {
    const metadata = new grpc.Metadata();
    metadata.add('accesstoken', config.accessToken);
    metadata.add('instanceurl', config.instanceUrl);
    metadata.add('tenantid', config.tenantId);
    return metadata;
  }

  // UTILITIES
  private createStreamKey(tenantId: string, topicName: string): StreamKey {
    return `${tenantId}:${topicName}`;
  }

  private closeStream(streamKey: StreamKey): void {
    const stream = this.activeStreams.get(streamKey);
    if (stream) {
      stream.end();
      this.activeStreams.delete(streamKey);
      this.logger.log(`Closed stream: ${streamKey}`);
    }
  }

  // CLEANUP
  private cleanup(): void {
    this.logger.log('Cleaning up gRPC resources...');
    this.isShuttingDown = true;

    this.cleanupReconnectTimers();
    this.cleanupStreams();
    this.cleanupClients();
    this.cleanupCaches();
  }

  private cleanupReconnectTimers(): void {
    for (const [key] of this.reconnectTimers) {
      this.clearReconnectTimer(key);
    }
  }

  private cleanupStreams(): void {
    for (const [key] of this.activeStreams) {
      this.closeStream(key);
    }
  }

  private cleanupClients(): void {
    for (const [tenantId, client] of this.clients) {
      client.close();
      this.logger.log(`Closed client for tenant: ${tenantId}`);
    }
    this.clients.clear();
  }

  private cleanupCaches(): void {
    this.schemaCache.clear();
    this.avroTypeCache.clear();
    this.subscriptionConfigs.clear();
  }
}
