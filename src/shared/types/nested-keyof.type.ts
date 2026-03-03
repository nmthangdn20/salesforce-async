type IsPlainObject<T> = T extends object
  ? T extends (...args: any[]) => any
    ? false
    : T extends Date
      ? false
      : T extends any[]
        ? false
        : true
  : false;

type Join<K, P> = K extends string | number
  ? P extends string | number
    ? `${K}.${P}`
    : never
  : never;

export type NestedKeyOf<T> = {
  [K in keyof T & (string | number)]: IsPlainObject<T[K]> extends true
    ? K | Join<K, keyof T[K] & (string | number)>
    : K;
}[keyof T & (string | number)];
