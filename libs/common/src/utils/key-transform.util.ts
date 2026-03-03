import { instanceToPlain } from 'class-transformer';

const camelToSnakePreserveSpecial = (key: string): string => {
  return key.replace(/([a-z0-9])([A-Z])/g, '$1_$2').toLowerCase();
};

const isPlainObject = (val: any): val is Record<string, any> => {
  return (
    Object.prototype.toString.call(val) === '[object Object]' &&
    (Object.getPrototypeOf(val) === Object.prototype ||
      Object.getPrototypeOf(val) === null)
  );
};

/**
 * Convert camel case to snake case and preserve special characters
 * @param obj - The object to convert
 * @returns The converted object
 * @example
 * camelToSnake({ camelCase: 'value' }) // { camel_case: 'value' }
 * camelToSnake({ camelCase_with_special_characters: 'value' }) // { camel_case_with_special_characters: 'value' }
 */
export const camelToSnake = (obj: any): any => {
  const objPlain = instanceToPlain(obj);

  if (Array.isArray(objPlain)) {
    return objPlain.map(camelToSnake);
  }

  if (isPlainObject(objPlain)) {
    return Object.entries(objPlain).reduce((acc, [key, value]) => {
      const newKey = /^[a-zA-Z0-9]+$/.test(key)
        ? camelToSnakePreserveSpecial(key)
        : key;
      acc[newKey] = camelToSnake(value) as Record<string, any>;
      return acc;
    }, {} as any);
  }

  return objPlain;
};
