import {
  ClassConstructor,
  ClassTransformOptions,
  plainToInstance,
} from 'class-transformer';

/**
 * Convert plain object(s) to class instance(s) using class-transformer.
 *
 * Supports both single object and array input.
 * Automatically applies default transform options:
 * - excludeExtraneousValues: true → only keeps properties with @Expose()
 * - exposeUnsetFields: false → skips unset fields in output
 *
 * @param classType - The target class constructor (DTO class)
 * @param plain - Plain object or array of plain objects
 * @param options - Optional transform options (overrides default)
 * @returns Transformed class instance(s)
 *
 * @example
 * const dto = customPlainToInstance(UserDto, req.body);
 * const list = customPlainToInstance(UserDto, userList);
 */
export function customPlainToInstance<T, V>(
  classType: ClassConstructor<T>,
  plain: V[],
  options?: ClassTransformOptions,
): T[];

export function customPlainToInstance<T, V>(
  classType: ClassConstructor<T>,
  plain: V,
  options?: ClassTransformOptions,
): T;

export function customPlainToInstance<T, V>(
  classType: ClassConstructor<T>,
  plain: V | V[],
  options?: ClassTransformOptions,
): T | T[] {
  const optionsDefault: ClassTransformOptions = {
    excludeExtraneousValues: true,
    exposeUnsetFields: false,
  };

  const optionsMerged = {
    ...optionsDefault,
    ...options,
  };

  return plainToInstance(classType, plain, optionsMerged);
}
