import js from '@eslint/js';
import svelte from 'eslint-plugin-svelte';
import globals from 'globals';
import ts from 'typescript-eslint';
import prettier from 'eslint-config-prettier';
import svelteConfig from './svelte.config.js';

export default ts.config(
	{
		ignores: ['.svelte-kit/', 'build/', 'dist/', 'node_modules/', 'src/lib/server/db/schema.d.ts']
	},
	js.configs.recommended,
	...ts.configs.recommended,
	...svelte.configs.recommended,
	prettier,
	...svelte.configs.prettier,
	{
		languageOptions: {
			globals: { ...globals.browser, ...globals.node }
		},
		rules: {
			// TypeScript's own checker handles undefined symbols; the lint rule misfires on globals.
			'no-undef': 'off',
			'svelte/no-navigation-without-resolve': 'off' // not relevant for this project
		}
	},
	{
		files: ['**/*.svelte', '**/*.svelte.ts', '**/*.svelte.js'],
		languageOptions: {
			parserOptions: {
				projectService: true,
				extraFileExtensions: ['.svelte'],
				parser: ts.parser,
				svelteConfig
			}
		}
	}
);
