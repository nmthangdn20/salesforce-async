export type TApiResponseOptions = {
  operation: string;
  successStatus?: number;
  successType: any;
  errorType?: any;
  includeAuth?: boolean;
  extraResponses?: Array<{
    status: number;
    description: string;
    type?: any;
  }>;
};
