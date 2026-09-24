import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";
import { describe, expect, it } from "vitest";

const migration = readFileSync(
  join(dirname(fileURLToPath(import.meta.url)), "../migrations/0001_init.sql"),
  "utf8"
);

describe("initial migration", () => {
  it("contains the core audited entities", () => {
    for (const table of [
      "users", "sessions", "auth_tokens", "categories", "map_features",
      "feature_revisions", "media_assets", "comments", "reports",
      "moderation_actions", "outbox_events", "audit_logs", "notifications"
    ]) {
      expect(migration).toContain(`CREATE TABLE ${table}`);
    }
  });

  it("adds public thumbnail and outbox recovery fields in migration 0002", () => {
    const followup = readFileSync(
      join(dirname(fileURLToPath(import.meta.url)), "../migrations/0002_media_public_thumb.sql"),
      "utf8"
    );
    expect(followup).toContain("public_thumbnail_object_key");
    expect(followup).toContain("updated_at timestamptz");
  });

  it("introduces the media object ledger, processing attempts and reconcile cursors in migration 0003", () => {
    const followup = readFileSync(
      join(dirname(fileURLToPath(import.meta.url)), "../migrations/0003_media_object_ledger.sql"),
      "utf8"
    );
    expect(followup).toContain("CREATE TABLE media_object_ledger");
    expect(followup).toContain("CREATE TABLE media_processing_attempts");
    expect(followup).toContain("media_reconcile_cursors");
    expect(followup).toContain("current_attempt_id");
    // 存量对象必须通过现有列回填台账，避免迁移后全部误判为孤儿。
    expect(followup).toContain("INSERT INTO media_object_ledger");
    expect(followup).toContain("ON CONFLICT (bucket, object_key) DO NOTHING");
  });

  it("passes bucket names to migrations through session settings", () => {
    const migrateSource = readFileSync(
      join(dirname(fileURLToPath(import.meta.url)), "migrate.ts"),
      "utf8"
    );
    expect(migrateSource).toContain("app.s3_quarantine_bucket");
    expect(migrateSource).toContain("app.s3_public_bucket");
  });

  it("uses PostGIS geography points and spatial indexes", () => {
    expect(migration).toContain("geography(Point, 4326)");
    expect(migration).toContain("USING gist (geom)");
  });
});
