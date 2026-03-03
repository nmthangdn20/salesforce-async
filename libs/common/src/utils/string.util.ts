import slugify from 'slugify';

/**

Convert a string to a slug.
@param str - The string to convert.
@returns The converted slug string.
@example
const myString = 'Welcome to OpenAI';
const mySlug = stringToSlug(myString);
console.log(mySlug); // Output: welcome-to-openai
*/
export const stringToSlug = (str: string) => {
  return slugify(str, {
    replacement: '-', // replace spaces with replacement character, defaults to `-`
    remove: undefined, // remove characters that match regex, defaults to `undefined`
    lower: true, // convert to lower case, defaults to `false`
    strict: false, // strip special characters except replacement, defaults to `false`
    locale: 'vi', // language code of the locale to use
    trim: true, // trim leading and trailing replacement chars, defaults to `true`
  });
};
