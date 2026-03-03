/**
 * Salesforce system fields that are always included in sync
 * regardless of user-specified field list
 */
export const REQUIRED_FIELDS = ['Id', 'IsDeleted', 'LastModifiedDate'] as const;

export type RequiredField = (typeof REQUIRED_FIELDS)[number];

/**
 * Check if a field is a required system field
 */
export function isRequiredField(fieldName: string): boolean {
  return REQUIRED_FIELDS.includes(fieldName as RequiredField);
}
