import { validateConfig } from '@app/core/modules/type-config/type-config.validator';
import {
  CONFIG_TOKEN,
  TJwtConfig,
} from '@app/core/modules/type-config/types/config.type';
import { registerAs } from '@nestjs/config';
import { IsNotEmpty, IsString } from 'class-validator';

class JwtVariables {
  @IsString()
  @IsNotEmpty()
  JWT_SECRET!: string;

  @IsString()
  @IsNotEmpty()
  JWT_EXPIRES_IN!: string;
}

export const registerJwtConfig = registerAs<TJwtConfig>(
  CONFIG_TOKEN.JWT,
  () => {
    const env = validateConfig(process.env, JwtVariables);

    return {
      secret: env.JWT_SECRET,
      expiresIn: env.JWT_EXPIRES_IN,
    };
  },
);
