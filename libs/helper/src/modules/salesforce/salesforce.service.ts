import { CustomHttpException } from '@app/common/exceptions/custom-http.exception';
import { ERROR_MESSAGES } from '@app/shared/constants/error.constant';
import { Injectable, Logger } from '@nestjs/common';
import { writeFileSync } from 'fs';
import * as jsforce from 'jsforce';
import { Parsable } from 'jsforce/lib/record-stream';
import { join } from 'path';
import { TSalesforceConfig } from 'src/modules/salesforce-async/types/type';

@Injectable()
export class SalesforceService {
  private readonly logger = new Logger(SalesforceService.name);

  /**
   * Tạo OAuth2 connection (chưa login)
   */
  createOAuth2(config: {
    loginUrl: string;
    clientId: string;
    clientSecret: string;
    redirectUri: string;
  }) {
    return new jsforce.Connection({
      oauth2: {
        loginUrl: config.loginUrl,
        clientId: config.clientId,
        clientSecret: config.clientSecret,
        redirectUri: config.redirectUri,
      },
    });
  }

  /**
   * Login bằng username + password (không phải OAuth)
   */
  async loginWithPassword(
    username: string,
    password: string,
    securityToken?: string,
    loginUrl = 'https://login.salesforce.com',
  ) {
    const conn = new jsforce.Connection({ loginUrl });

    const userInfo = await conn.login(
      username,
      password + (securityToken || ''),
    );

    this.logger.log(`Logged in as ${userInfo.id}`);
    return userInfo;
  }

  createConnection(config: { instanceUrl: string; accessToken: string }) {
    return new jsforce.Connection({
      instanceUrl: config.instanceUrl,
      accessToken: config.accessToken,
    });
  }

  async describe(connection: jsforce.Connection, objectName: string) {
    return connection.sobject(objectName).describe();
  }

  /**
   * @param options.scanAll - If true, use queryAll to include deleted records (Recycle Bin). Use for reconcile-deleted (overflow).
   */
  async getRecords(
    connection: jsforce.Connection,
    objectName: string,
    fields: string[],
    where?: string,
    options?: { scanAll?: boolean },
  ): Promise<Parsable<jsforce.Record>> {
    let query = `SELECT ${fields.join(', ')} FROM ${objectName}`;

    if (where) {
      query += ` WHERE ${where}`;
    }

    return connection.bulk2.query(
      query,
      options?.scanAll ? { scanAll: true } : undefined,
    );
  }

  async logout(connection: jsforce.Connection) {
    await connection.logout();
  }

  async verifyConnection(
    salesforceConfig: TSalesforceConfig,
    redirectUri: string,
  ) {
    if (!salesforceConfig.accessToken && !salesforceConfig.refreshToken) {
      throw new CustomHttpException(ERROR_MESSAGES.ConnectionNotFound);
    }

    if (
      new Date(salesforceConfig.expiresAt) < new Date() ||
      !salesforceConfig.accessToken
    ) {
      const connection = this.createOAuth2({
        loginUrl: salesforceConfig.instanceUrl,
        clientId: salesforceConfig.clientId,
        clientSecret: salesforceConfig.clientSecret,
        redirectUri,
      });

      const tokenResponse = await connection.oauth2.refreshToken(
        salesforceConfig.refreshToken,
      );

      salesforceConfig.accessToken = tokenResponse.access_token ?? '';
      salesforceConfig.expiresAt = new Date(
        new Date().getTime() + 10 * 60 * 1000,
      );

      writeFileSync(
        join(process.cwd(), 'salesforce-configs', salesforceConfig.filename),
        JSON.stringify(salesforceConfig, null, 2),
      );
    }
  }
}
