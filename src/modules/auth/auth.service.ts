import { CustomHttpException } from '@app/common/exceptions/custom-http.exception';
import { TypeConfigService } from '@app/core/modules/type-config';
import { SalesforceService } from '@app/helper';
import { ERROR_MESSAGES } from '@app/shared/constants/error.constant';
import { Injectable, Logger } from '@nestjs/common';
import { readdirSync, readFileSync, writeFileSync } from 'fs';
import * as path from 'path';
import { TSalesforceConfig } from 'src/modules/salesforce-async/types/type';

@Injectable()
export class AuthService {
  private readonly logger = new Logger(AuthService.name);

  constructor(
    private readonly typeConfigService: TypeConfigService,
    private readonly salesforceService: SalesforceService,
  ) {}

  connection() {
    const files = readdirSync(path.join(process.cwd(), 'credentials'));

    const urls: string[] = [];

    files.forEach((file) => {
      const credentials = JSON.parse(
        readFileSync(path.join(process.cwd(), 'credentials', file), 'utf8'),
      ) as TSalesforceConfig;

      const connection = this.salesforceService.createOAuth2({
        loginUrl: credentials.instanceUrl,
        clientId: credentials.clientId,
        clientSecret: credentials.clientSecret,
        redirectUri: `${this.typeConfigService.get('app.backendUrl')}/api/auth/oauth/callback`,
      });

      const url = connection.oauth2.getAuthorizationUrl({
        scope: credentials.scopes.join(' '),
        state: credentials.filename,
      });

      if (url) {
        urls.push(url);
      }
    });

    return urls;
  }

  async oauthCallback(code: string, state: string) {
    try {
      const salesforceConfig = JSON.parse(
        readFileSync(
          path.join(process.cwd(), 'salesforce-configs', state),
          'utf8',
        ),
      ) as TSalesforceConfig;

      if (!salesforceConfig) {
        throw new CustomHttpException(ERROR_MESSAGES.SalesforceConfigNotFound);
      }

      const connection = this.salesforceService.createOAuth2({
        loginUrl: salesforceConfig.instanceUrl,
        clientId: salesforceConfig.clientId,
        clientSecret: salesforceConfig.clientSecret,
        redirectUri: `${this.typeConfigService.get('app.backendUrl')}/api/auth/oauth/callback`,
      });

      await connection.authorize(code);

      const expiresAt = new Date().getTime() + 10 * 60 * 1000;

      const credential = {
        ...salesforceConfig,
        accessToken: connection.accessToken,
        refreshToken: connection.refreshToken,
        expiresAt: new Date(expiresAt),
      };

      writeFileSync(
        path.join(process.cwd(), 'salesforce-configs', state),
        JSON.stringify(credential, null, 2),
      );

      return {
        message: 'OAuth token generated successfully',
      };
    } catch (error) {
      this.logger.error((error as Error).message);
      throw new CustomHttpException(ERROR_MESSAGES.OAuthTokenFailed);
    }
  }
}
