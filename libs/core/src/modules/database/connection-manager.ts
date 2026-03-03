import { DrizzleConnectionFactory } from '@app/core/modules/database/database.factory';
import { DbConnectionConfig } from '@app/core/modules/database/types';

export class ConnectionManager {
  private static instance: ConnectionManager;
  private connections = new Map<
    string,
    { db: any; dispose: () => Promise<void> }
  >();

  static getInstance(): ConnectionManager {
    if (!this.instance) this.instance = new ConnectionManager();
    return this.instance;
  }

  async getConnection(tenantId: string, config: DbConnectionConfig) {
    const existing = this.connections.get(tenantId);
    if (existing) return existing.db;

    const { db, dispose } = await DrizzleConnectionFactory.create(config);
    this.connections.set(tenantId, { db, dispose });
    return db;
  }

  async closeConnection(tenantId: string) {
    const conn = this.connections.get(tenantId);
    if (!conn) return;
    await conn.dispose();
    this.connections.delete(tenantId);
  }
}
