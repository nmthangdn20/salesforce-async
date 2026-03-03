import {
  ClassValidatorFilter,
  CustomValidationPipe,
  TransformInterceptor,
} from '@app/common';
import { AllExceptionFilter } from '@app/common/filters/all-exception.filter';
import { TypeConfigService } from '@app/core/modules/type-config/type-config.service';
import { LANG } from '@app/shared/constants/lang.constant';
import { RequestMethod } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { DocumentBuilder, SwaggerModule } from '@nestjs/swagger';
import { I18nService } from 'nestjs-i18n';

import { AppModule } from './app.module';

async function bootstrap() {
  const app = await NestFactory.create(AppModule, {
    rawBody: true,
  });

  const configService = app.get(TypeConfigService);
  const transFormKeys = configService.get('app.transFormKeys');

  app.setGlobalPrefix('api', {
    exclude: [{ path: 'files/:filename', method: RequestMethod.GET }],
  });
  app.enableShutdownHooks();
  app.enableCors({
    origin: configService.get('app.corsOrigins'),
  });

  app.useGlobalInterceptors(new TransformInterceptor(app.get(I18nService)));

  app.useGlobalFilters(
    new AllExceptionFilter(app.get(I18nService), transFormKeys),
    new ClassValidatorFilter(transFormKeys),
  );

  app.useGlobalPipes(new CustomValidationPipe());

  const options = new DocumentBuilder()
    .setTitle(configService.get('app.appName'))
    .setDescription(configService.get('app.appDescription'))
    .setVersion(configService.get('app.appVersion'))
    .addBearerAuth()
    .addServer(configService.get('app.backendUrl'))
    .addGlobalParameters({
      name: 'x-lang',
      in: 'header',
      required: false,
      description: 'Language preference',
      schema: {
        type: 'string',
        enum: Object.values(LANG),
        default: LANG.EN,
      },
    })
    .build();

  const document = SwaggerModule.createDocument(app, options);

  SwaggerModule.setup(configService.get('app.apiDocPath'), app, document, {
    swaggerOptions: {
      filter: true,
      displayOperationId: false,
      displayRequestDuration: true,
      operationsSorter: 'alpha',
      tagsSorter: 'alpha',
    },
  });

  await app.listen(process.env.PORT ?? 3000);

  console.log(
    `URL Swagger ${configService.get('app.backendUrl')}/${configService.get(
      'app.apiDocPath',
    )}`,
  );
  console.log(`Starting on ${configService.get('app.backendUrl')}`);
}

void bootstrap();
