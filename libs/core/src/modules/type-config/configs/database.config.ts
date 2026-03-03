import { validateConfig } from '@app/core/modules/type-config/type-config.validator';
import {
  CONFIG_TOKEN,
  TDatabaseConfig,
} from '@app/core/modules/type-config/types/config.type';
import { registerAs } from '@nestjs/config';
import { IsNotEmpty, IsNumber, IsString } from 'class-validator';

class DatabaseVariables {
  @IsString()
  @IsNotEmpty()
  DB_HOST!: string;

  @IsNumber()
  @IsNotEmpty()
  DB_PORT!: number;

  @IsString()
  @IsNotEmpty()
  DB_USERNAME!: string;

  @IsString()
  @IsNotEmpty()
  DB_PASSWORD!: string;

  @IsString()
  @IsNotEmpty()
  DB_NAME!: string;

  @IsString()
  @IsNotEmpty()
  DB_SCHEMA!: string;
}

export const registerDatabaseConfig = registerAs<TDatabaseConfig>(
  CONFIG_TOKEN.DATABASE,
  () => {
    const env = validateConfig(process.env, DatabaseVariables);

    return {
      host: env.DB_HOST,
      port: Number(env.DB_PORT),
      username: env.DB_USERNAME,
      password: env.DB_PASSWORD,
      database: env.DB_NAME,
      schema: env.DB_SCHEMA,
    };
  },
);
