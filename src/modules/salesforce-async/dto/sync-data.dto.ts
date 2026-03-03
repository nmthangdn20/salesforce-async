import { ApiProperty } from '@nestjs/swagger';
import { IsNotEmpty, IsString } from 'class-validator';

export class SyncDataDto {
  @ApiProperty({
    description: 'The filename of the credentials',
    example: 'salesforce.config.json',
    required: true,
  })
  @IsString()
  @IsNotEmpty()
  filename!: string;
}
