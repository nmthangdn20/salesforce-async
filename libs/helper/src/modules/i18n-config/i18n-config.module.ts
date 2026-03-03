import { Global, Module } from '@nestjs/common';
import {
  AcceptLanguageResolver,
  HeaderResolver,
  I18nModule,
  QueryResolver,
} from 'nestjs-i18n';
import * as path from 'path';

@Global()
@Module({
  imports: [
    I18nModule.forRootAsync({
      useFactory: () => ({
        fallbackLanguage: 'en',
        loaderOptions: {
          path: path.join(
            process.cwd(),
            'dist/libs/helper/src/modules/i18n-config/i18n/',
          ),
          watch: true,
        },
        typesOutputPath: path.join(
          process.cwd(),
          'dist/libs/helper/src/modules/i18n-config/i18n/i18n.generated.ts',
        ),
      }),
      resolvers: [
        new HeaderResolver(['x-lang']),
        { use: QueryResolver, options: ['lang'] },
        AcceptLanguageResolver,
      ],
    }),
  ],
})
export class I18nConfigModule {}
