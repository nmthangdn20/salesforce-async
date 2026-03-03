import { Controller, Get, Query } from '@nestjs/common';
import { ApiTags } from '@nestjs/swagger';

import { AuthService } from './auth.service';

@ApiTags('Auth')
@Controller('auth')
export class AuthController {
  constructor(private readonly authService: AuthService) {}

  @Get('connection')
  connection() {
    return this.authService.connection();
  }

  @Get('/oauth/callback')
  oauthCallback(@Query('code') code: string, @Query('state') state: string) {
    return this.authService.oauthCallback(code, state);
  }
}
