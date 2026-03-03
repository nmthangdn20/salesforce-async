/**
 * Mapping của compound fields
 * Key: Tên compound field trong CDC (vd: "BillingAddress")
 * Value: Array các flat field names trong Bulk API (vd: ["BillingStreet", "BillingCity"])
 */
type CompoundFieldMapping = {
  [compoundField: string]: string[];
};

/**
 * Custom mapping cho sub-fields
 * Key: Compound field name
 * Value: Object map từ flat field name -> CDC sub-field name
 */
type CustomSubFieldMapping = {
  [compoundField: string]: {
    [flatField: string]: string;
  };
};

/**
 * Generic data object từ Salesforce
 */
type SalesforceData = {
  [key: string]: any;
};

// ============================================
// PRIVATE HELPER FUNCTIONS
// ============================================

/**
 * Tìm prefix chung trong danh sách flat fields
 * @param flatFields - Danh sách tất cả các flat fields
 * @param compoundField - Tên compound field
 * @returns Prefix chung
 * @example
 * ```typescript
 * const flatFields = ['billingStreet', 'billingCity', 'billingState'];
 * const compoundField = 'billingAddress';
 * const prefix = findCommonPrefix(flatFields, compoundField);
 * // 'billing'
 */
const findCommonPrefix = (
  flatFields: string[],
  compoundField: string,
): string => {
  const fieldsToCheck = flatFields.filter((f) => f !== compoundField);

  if (fieldsToCheck.length === 0) return '';

  let prefix = '';
  const firstField = fieldsToCheck[0];

  for (let i = 0; i < firstField.length; i++) {
    const char = firstField[i];
    if (char === char.toUpperCase() && i > 0) {
      prefix = firstField.substring(0, i);
      break;
    }
  }

  const allHavePrefix = fieldsToCheck.every((f) => f.startsWith(prefix));
  return allHavePrefix ? prefix : '';
};

/**
 * Tự động suy ra sub-field name từ flat field name
 * @param flatField - Tên flat field
 * @param compoundField - Tên compound field
 * @param allFlatFields - Danh sách tất cả các flat fields
 * @returns Tên sub-field
 * @example
 * ```typescript
 * const flatField = 'billingStreet';
 * const compoundField = 'billingAddress';
 * const allFlatFields = ['billingStreet', 'billingCity', 'billingState'];
 * const subFieldName = inferSubFieldName(flatField, compoundField, allFlatFields);
 * // 'street'
 * ```
 */
const inferSubFieldName = (
  flatField: string,
  compoundField: string,
  allFlatFields: string[],
): string => {
  if (flatField === compoundField) {
    return flatField;
  }

  const prefix = findCommonPrefix(allFlatFields, compoundField);

  if (prefix && flatField.startsWith(prefix)) {
    const subField = flatField.substring(prefix.length);
    return subField.charAt(0).toLowerCase() + subField.slice(1);
  }

  return flatField.charAt(0).toLowerCase() + flatField.slice(1);
};

/**
 * Tìm matching key trong object với nhiều variations
 * @param obj - Object cần tìm
 * @param targetKey - Tên key cần tìm
 * @returns Object chứa value và flag exists
 * @example
 * ```typescript
 * const obj = { billingStreet: '123 Main St', billingCity: 'San Francisco' };
 * const targetKey = 'street';
 * const result = findMatchingKey(obj, targetKey);
 * // { value: null, exists: false }
 * ```
 */
const findMatchingKey = (
  obj: Record<string, any>,
  targetKey: string,
): { value: any; exists: boolean } => {
  // 1. Exact match
  if (targetKey in obj) {
    return { value: obj[targetKey] as string, exists: true };
  }

  // 2. Case-insensitive match
  const lowerTarget = targetKey.toLowerCase();
  for (const key in obj) {
    if (key.toLowerCase() === lowerTarget) {
      return { value: obj[key] as string, exists: true };
    }
  }

  // 3. Try variations
  const variations = [
    targetKey,
    targetKey.charAt(0).toLowerCase() + targetKey.slice(1), // camelCase
    targetKey.charAt(0).toUpperCase() + targetKey.slice(1), // PascalCase
    targetKey.toLowerCase(),
    targetKey.toUpperCase(),
  ];

  for (const variation of variations) {
    if (variation in obj) {
      return { value: obj[variation] as string, exists: true };
    }
  }

  return { value: null, exists: false };
};

/**
 * Build reverse mapping: flatField -> { compoundField, subField }
 * @param compoundFieldMapping - Mapping của compound fields
 * @returns Reverse mapping
 * @example
 * ```typescript
 * const compoundFieldMapping = { billingAddress: ['billingStreet', 'billingCity', 'billingState'] };
 * const reverseMapping = buildReverseMapping(compoundFieldMapping);
 * // { billingStreet: { compoundField: 'billingAddress', subField: 'street' }, ... }
 * ```
 */
const buildReverseMapping = (
  compoundFieldMapping: CompoundFieldMapping,
): Record<string, { compoundField: string; subField: string }> => {
  const reverse: Record<string, { compoundField: string; subField: string }> =
    {};

  for (const [compoundField, flatFields] of Object.entries(
    compoundFieldMapping,
  )) {
    flatFields.forEach((flatField) => {
      reverse[flatField] = {
        compoundField,
        subField: inferSubFieldName(flatField, compoundField, flatFields),
      };
    });
  }

  return reverse;
};

// ============================================
// MAIN FUNCTION
// ============================================

/**
 * Flatten CDC data (compound fields) sang Bulk API format (flat fields)
 *
 * @param cdcData - Dữ liệu từ CDC với compound fields
 * @param compoundFieldMapping - Mapping của compound fields
 * @param customSubFieldMapping - (Optional) Manual mapping cho custom fields
 * @returns Dữ liệu dạng flat fields (CHỈ chứa các field thực sự có trong CDC data)
 *
 * @example
 * ```typescript
 * const cdcEvent = {
 *   Id: "001xxx",
 *   BillingAddress: {
 *     street: "123 Main St",
 *     city: "San Francisco"
 *   }
 * };
 *
 * const mapping = {
 *   BillingAddress: ["BillingStreet", "BillingCity", "BillingState"]
 * };
 *
 * const result = flattenCDCData(cdcEvent, mapping);
 * // { Id: "001xxx", BillingStreet: "123 Main St", BillingCity: "San Francisco" }
 * // Lưu ý: BillingState KHÔNG có vì không tồn tại trong CDC data
 * ```
 *
 * @example
 * ```typescript
 * const cdcEvent = {
 *   Name: {
 *     LastName: "qbc",
 *     FirstName: null
 *   }
 * };
 *
 * const mapping = {
 *   Name: ["Name", "LastName", "FirstName", "Salutation"]
 * };
 *
 * const result = flattenCDCData(cdcEvent, mapping);
 * // { LastName: "qbc", FirstName: null }
 * // Lưu ý: Name và Salutation KHÔNG có vì không tồn tại trong CDC data
 * ```
 */
export const flattenCDCData = (
  cdcData: SalesforceData,
  compoundFieldMapping: CompoundFieldMapping,
  customSubFieldMapping: CustomSubFieldMapping = {},
): SalesforceData => {
  const flatData: SalesforceData = {};
  const reverseMapping = buildReverseMapping(compoundFieldMapping);

  for (const [key, value] of Object.entries(cdcData)) {
    // Nếu không phải compound field, copy trực tiếp
    if (!compoundFieldMapping[key]) {
      flatData[key] = value as string;
      continue;
    }

    // Nếu là compound field và value là object
    if (value && typeof value === 'object' && !Array.isArray(value)) {
      const flatFields = compoundFieldMapping[key];

      flatFields.forEach((flatField) => {
        let subFieldValue: any = null;
        let shouldInclude = false;

        // Kiểm tra có custom mapping không
        if (customSubFieldMapping[key]?.[flatField]) {
          const cdcFieldName = customSubFieldMapping[key][flatField];
          // Chỉ include nếu field thực sự tồn tại trong CDC data
          if (cdcFieldName in value) {
            subFieldValue = (value[cdcFieldName] as string) ?? null;
            shouldInclude = true;
          }
        } else {
          // Dùng auto-inference
          const { subField } = reverseMapping[flatField];
          const result = findMatchingKey(
            value as Record<string, any>,
            subField,
          );

          // Chỉ include nếu field thực sự tồn tại trong CDC data
          if (result.exists) {
            subFieldValue = result.value as string;
            shouldInclude = true;
          }
        }

        // CHỈ thêm vào flatData nếu field tồn tại trong CDC
        if (shouldInclude) {
          flatData[flatField] = subFieldValue as string;
        }
      });
    }
    // Nếu compound field = null/undefined, KHÔNG tạo ra các flat fields
    // Bỏ qua hoàn toàn
  }

  return flatData;
};
