/**
 * Remove duplicate items from an array
 * @param arr - The array to remove duplicates from
 * @returns The array with duplicates removed
 * @example
 * const myArray = [1, 2, 3, 1, 2, 3];
 * const myArrayWithoutDuplicates = removeDuplicates(myArray);
 * console.log(myArrayWithoutDuplicates); // Output: [1, 2, 3]
 */
export const removeDuplicates = <T>(arr: T[]): T[] => {
  return [...new Set(arr)];
};
