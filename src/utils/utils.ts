export const chunkArray = <T>(array: T[], size: number): T[][] => {
  return array.reduce<T[][]>((result, item, index) => {
    const chunkIndex = Math.floor(index / size);
    if (!result[chunkIndex]) {
      result[chunkIndex] = [] as T[];
    }
    result[chunkIndex].push(item);
    return result;
  }, []);
};
