module.exports = {
  env: {
    es6: true,
    node: true,
    mocha: true
  },
  extends: 'eslint:recommended',
  parserOptions: {
    sourceType: 'module'
  },
  rules: {
    'prefer-const': ['error'],
    'linebreak-style': ['error', 'unix'],
    semi: ['error', 'always'],
    'no-console': ['off']
  }
};
