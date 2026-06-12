import { env } from '$env/dynamic/private';
import { Kysely, PostgresDialect } from 'kysely';
import pg from 'pg';
import type { DB } from './schema';

if (!env.DATABASE_URL) {
	throw new Error('DATABASE_URL is not set');
}

export const db = new Kysely<DB>({
	dialect: new PostgresDialect({
		pool: new pg.Pool({ connectionString: env.DATABASE_URL })
	})
});

export type { DB } from './schema';
