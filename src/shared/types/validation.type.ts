export type TValidatorError = {
  property: string;
  constraints: string[];
  children?: TValidatorError[];
};
