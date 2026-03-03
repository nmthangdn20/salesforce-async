import { Test, TestingModule } from '@nestjs/testing';

import { TypeConfigService } from './type-config.service';

describe('TypeConfigService', () => {
  let service: TypeConfigService;

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      providers: [TypeConfigService],
    }).compile();

    service = module.get<TypeConfigService>(TypeConfigService);
  });

  it('should be defined', () => {
    expect(service).toBeDefined();
  });
});
