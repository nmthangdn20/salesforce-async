import { Injectable } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';

import {
  LeafTypes,
  Leaves,
  Paths,
  PathTypes,
  TConfig,
} from './types/config.type';

@Injectable()
export class TypeConfigService {
  constructor(private configService: ConfigService) {}

  // Get values of leaves
  get<T extends Leaves<TConfig>>(propertyPath: T): LeafTypes<TConfig, T> {
    return this.configService.get(propertyPath) as LeafTypes<TConfig, T>;
  }

  // Get values of all paths
  getObject<T extends Paths<TConfig>>(propertyPath: T): PathTypes<TConfig, T> {
    return this.configService.get(propertyPath) as PathTypes<TConfig, T>;
  }
}
