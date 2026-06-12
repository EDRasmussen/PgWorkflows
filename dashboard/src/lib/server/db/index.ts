import { env } from '$env/dynamic/private';
import { Kysely, PostgresDialect } from 'kysely';
import pg from 'pg';
import type { DB } from './schema';

let instance: Kysely<DB> | undefined;

// Lazy so the module can be imported without a database, e.g. during `vite build`.
export function getDb(): Kysely<DB> {
	if (!instance) {
		if (!env.DATABASE_URL) {
			throw new Error('DATABASE_URL is not set');
		}

		instance = new Kysely<DB>({
			dialect: new PostgresDialect({
				pool: new pg.Pool({ connectionString: env.DATABASE_URL })
			})
		});
	}

	return instance;
}

export type { DB } from './schema';
