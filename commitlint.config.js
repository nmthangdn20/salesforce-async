// commitlint.config.js
module.exports = {
  parserPreset: {
    parserOpts: {
      // Supports:
      // "✨ feat: task-123 - description"
      // "feat: task-123 - description"
      // "feat: description"
      headerPattern: /^(\p{Extended_Pictographic}|\p{Emoji_Presentation}|\p{Emoji}\uFE0F?\s*)?(\w+): (?:(task-\d+) - )?(.+)$/u,
      headerCorrespondence: ['emoji', 'type', 'task', 'subject'],
    },
  },
  rules: {
    'type-enum': [
      2,
      'always',
      [
        'feat',
        'fix',
        'docs',
        'style',
        'refactor',
        'perf',
        'test',
        'build',
        'ci',
        'chore',
        'revert',
      ],
    ],
    'type-empty': [2, 'never'],
    'subject-empty': [2, 'never'],
    'header-match-team-format': [2, 'always'],
  },
  plugins: [
    {
      rules: {
        'header-match-team-format': ({ header }) => {
          const pattern = /^(\p{Extended_Pictographic}|\p{Emoji_Presentation}|\p{Emoji}\uFE0F?\s*)?(\w+): (?:(task-\d+) - )?(.+)$/u;
          const valid = pattern.test(header);
          return [
            valid,
            `❌ Invalid commit message format.

✅ Valid format: "[emoji ]<type>: [task-123 - ]description"

🔹 Allowed <type> values:
  - feat:     A new feature
  - fix:      A bug fix
  - docs:     Documentation changes only
  - style:    Code style changes (e.g., formatting, whitespace)
  - refactor: Refactoring code without changing behavior
  - perf:     Performance improvements
  - test:     Adding or modifying test cases
  - build:    Changes to build system or dependencies
  - ci:       Continuous Integration configuration changes
  - chore:    Routine tasks and maintenance
  - revert:   Reverts a previous commit

📌 Examples:
  - ✨ feat: task-456 - add dark mode
  - fix: resolve login issue
  - 📝 docs: update API reference

(Note: emoji at the start is optional but encouraged for clarity)`,
          ];
        },
      },
    },
  ],
};
