import dotenv from "dotenv";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";

dotenv.config({ path: process.env.ENV_FILE || join(dirname(fileURLToPath(import.meta.url)), "../../../.env") });
import { readdir, readFile } from "node:fs/promises";
import pg from "pg";

const { Client } = pg;

async function main() {
  const databaseUrl = process.env.DATABASE_URL;
  if (!databaseUrl) throw new Error("DATABASE_URL is required");

  const client = new Client({ connectionString: databaseUrl });
  await client.connect();
  try {
    // 0003 回填媒体对象台账时需要桶名；通过会话参数传入，缺省值与 infra/minio 保持一致。
    await client.query(
      "SELECT set_config('app.s3_quarantine_bucket', $1, false), set_config('app.s3_public_bucket', $2, false)",
      [process.env.S3_QUARANTINE_BUCKET || "map-quarantine", process.env.S3_PUBLIC_BUCKET || "map-public"]
    );

    await client.query(`
      CREATE TABLE IF NOT EXISTS schema_migrations (
        filename text PRIMARY KEY,
        applied_at timestamptz NOT NULL DEFAULT now()
      )
    `);

    const directory = join(dirname(fileURLToPath(import.meta.url)), "../migrations");
    const files = (await readdir(directory)).filter((file) => file.endsWith(".sql")).sort();

    for (const filename of files) {
      const existing = await client.query("SELECT 1 FROM schema_migrations WHERE filename = $1", [filename]);
      if (existing.rowCount) continue;

      const sql = await readFile(join(directory, filename), "utf8");
      await client.query("BEGIN");
      try {
        await client.query(sql);
        await client.query("INSERT INTO schema_migrations(filename) VALUES ($1)", [filename]);
        await client.query("COMMIT");
        console.log(`applied ${filename}`);
      } catch (error) {
        await client.query("ROLLBACK");
        throw error;
      }
    }
  } finally {
    await client.end();
  }
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
