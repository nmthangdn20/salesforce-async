import { CustomHttpException } from '@app/common/exceptions/custom-http.exception';
import { ERROR_MESSAGES } from '@app/shared/constants/error.constant';
import { Request } from 'express';
import { existsSync, mkdirSync } from 'fs';
import { diskStorage, memoryStorage } from 'multer';

const storageDisk = diskStorage({
  destination: function (
    _req: Request,
    _: Express.Multer.File,
    cb: (error: Error | null, destination: string) => void,
  ) {
    if (!existsSync('./public/uploads')) {
      mkdirSync('./public/uploads', { recursive: true });
    }

    return cb(null, './public/uploads');
  },
  filename: function (
    _req: Request,
    file: Express.Multer.File,
    cb: (error: Error | null, filename: string) => void,
  ) {
    const uniqueSuffix = Date.now() + '-' + Math.round(Math.random() * 1e9);
    cb(null, uniqueSuffix + '.' + file.originalname.split('.').pop());
  },
});

const storageMemory = memoryStorage();

const fileFilter = (
  _req: Request,
  file: Express.Multer.File,
  cb: (error: Error | null, acceptFile: boolean) => void,
): void => {
  if (
    file.mimetype === 'image/png' ||
    file.mimetype === 'image/jpg' ||
    file.mimetype === 'image/jpeg' ||
    file.mimetype === 'video/mp4'
  ) {
    cb(null, true);
  } else {
    cb(new CustomHttpException(ERROR_MESSAGES.FileFormatNotSupport), false);
  }
};

export const multerDiskOption = {
  storage: storageDisk,
  fileFilter,
};

export const multerMemoryOption = {
  storage: storageMemory,
  fileFilter,
};
