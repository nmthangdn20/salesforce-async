export type TBaseModelResponseDto = {
  id: string;
  createdAt: Date;
  updatedAt: Date;
};

export type TBaseResponseDto<T = any> = {
  data?: T;
  message: string | string[];
  errorMessage?: string;
  errorCode?: number | string;
  statusCode?: number;
  eventName?: string; // for socket
};

export type TResponseErrorDto<T = any> = Partial<TBaseResponseDto<T>>;

export type TBasePaginationMetadataDto = {
  page: number;
  take: number;
  totalItems: number;
  totalPages: number;
  itemCount: number;
};

export type TBaseResponsePaginationDto<T> = {
  items: T[];
  meta: TBasePaginationMetadataDto;
};

export type TBaseFilterRequestDto = {
  page: number;
  take: number;
};
