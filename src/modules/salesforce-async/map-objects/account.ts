import { PayloadWithHeader } from '@app/helper';

export const handleAccountMapping = (
  dataCdc: PayloadWithHeader & {
    Name: {
      LastName: string;
      FirstName: string;
      Salutation: string;
    };
  },
) => {
  const { Name, ...data } = dataCdc;

  return {
    ...data,
    ...Name,
  };
};
