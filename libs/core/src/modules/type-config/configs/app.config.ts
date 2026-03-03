import { validateConfig } from '@app/core/modules/type-config/type-config.validator';
import {
  CONFIG_TOKEN,
  ENVIRONMENT,
  TAppConfig,
} from '@app/core/modules/type-config/types/config.type';
import { registerAs } from '@nestjs/config';
import { Transform, Type } from 'class-transformer';
import {
  IsArray,
  IsBoolean,
  IsEnum,
  IsNotEmpty,
  IsNumber,
  IsOptional,
  IsString,
  IsUrl,
  Matches,
  Max,
  Min,
} from 'class-validator';

class EnvironmentVariables {
  @IsString()
  @IsNotEmpty()
  APP_NAME!: string;

  @IsString()
  @IsNotEmpty()
  @Matches(/^\d+\.\d+\.\d+$/, {
    message: 'APP_VERSION must be in format x.y.z (e.g., 1.0.0)',
  })
  APP_VERSION!: string;

  @IsString()
  @IsNotEmpty()
  APP_DESCRIPTION!: string;

  @IsString()
  API_DOC_PATH!: string;

  @IsEnum(ENVIRONMENT)
  NODE_ENV!: ENVIRONMENT;

  @IsNumber()
  @Min(0)
  @Max(65535)
  @Type(() => Number)
  PORT!: number;

  // isUrl not pass when using localhost:port
  @IsUrl({ require_tld: false }, { message: 'BACKEND_URL must be a valid URL' })
  BACKEND_URL!: string;

  @IsUrl(
    { require_tld: false },
    { message: 'FRONTEND_URL must be a valid URL' },
  )
  @IsOptional()
  FRONTEND_URL?: string;

  @IsString()
  @IsNotEmpty()
  @Matches(/^\/[a-zA-Z0-9/_-]*$/, {
    message: 'API_PREFIX must start with a slash and contain valid characters',
  })
  API_PREFIX!: string;

  @Transform(
    ({ value }) =>
      (typeof value === 'string'
        ? value.split(',').map((v) => v.trim())
        : value) as string[],
  )
  @IsArray()
  @IsString({ each: true })
  @IsOptional()
  CORS_ORIGINS!: string[];

  @Transform(({ value }) => {
    if (typeof value === 'string') {
      return value.toLowerCase() === 'true';
    }
    return Boolean(value);
  })
  @IsBoolean()
  TRANS_FORM_KEYS!: boolean;
}

export const registerAppConfig = registerAs<TAppConfig>(
  CONFIG_TOKEN.APP,
  () => {
    const env = validateConfig(process.env, EnvironmentVariables);

    return {
      appName: env.APP_NAME,
      appVersion: env.APP_VERSION,
      appDescription: env.APP_DESCRIPTION,
      apiDocPath: env.API_DOC_PATH,
      nodeEnv: env.NODE_ENV,
      port: env.PORT,
      backendUrl: env.BACKEND_URL,
      frontendUrl: env.FRONTEND_URL,
      apiPrefix: env.API_PREFIX,
      corsOrigins: env.CORS_ORIGINS || ['*'],
      transFormKeys: false,
    };
  },
);
