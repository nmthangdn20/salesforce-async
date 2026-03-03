import { validateConfig } from '@app/core/modules/type-config/type-config.validator';
import {
  CONFIG_TOKEN,
  TRedisConfig,
} from '@app/core/modules/type-config/types/config.type';
import { registerAs } from '@nestjs/config';
import { IsNotEmpty, IsNumber, IsOptional, IsString } from 'class-validator';

class RedisVariables {
  @IsString()
  @IsNotEmpty()
  REDIS_HOST!: string;

  @IsNumber()
  @IsNotEmpty()
  REDIS_PORT!: number;

  @IsString()
  @IsOptional()
  REDIS_PASSWORD?: string;

  @IsNumber()
  @IsOptional()
  REDIS_DB?: number;
}

export const registerRedisConfig = registerAs<TRedisConfig>(
  CONFIG_TOKEN.REDIS,
  () => {
    const env = validateConfig(process.env, RedisVariables);

    return {
      host: env.REDIS_HOST,
      port: Number(env.REDIS_PORT),
      ...(env.REDIS_PASSWORD && { password: env.REDIS_PASSWORD }),
      ...(env.REDIS_DB !== undefined && { db: Number(env.REDIS_DB) }),
    };
  },
);
