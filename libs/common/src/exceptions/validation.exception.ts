import { BadRequestException } from '@nestjs/common';
import { ValidationError } from 'class-validator';

export class ValidatorException extends BadRequestException {
  constructor(errors: ValidationError[]) {
    super(errors);
    this.name = 'ValidatorException';
  }
}
