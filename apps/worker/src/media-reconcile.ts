import { config } from "./config";
import { pool } from "./db";
import { deleteObject, listObjects } from "./storage";
import { recordMediaObjectEvent } from "./media-ledger";
import {
  decideMediaObjectAction,
  parseMediaObjectKey,
  type ReconcileMediaRow
} from "./media-objects";

const MAX_OBJECTS_PER_PREFIX = 500;

async function loadMediaRow(cache: Map<string, ReconcileMediaRow | null>, mediaId: string): Promise<ReconcileMediaRow | null> {
  if (!cache.has(mediaId)) {
    const result = await pool.query<ReconcileMediaRow>(
      `SELECT id, privacy_status, deleted_at, quarantine_object_key, processed_object_key,
              thumbnail_object_key, public_object_key, public_thumbnail_object_key
       FROM media_assets WHERE id = $1`,
      [mediaId]
    );
    cache.set(mediaId, result.rows[0] ?? null);
  }
  return cache.get(mediaId) ?? null;
}

/**
 * Rebuilds references between media_assets rows and stored objects, and
 * removes orphan objects that no database row can claim. This is the backstop
 * for objects left behind by interrupted processing, including objects that
 * predate the media_object_events ledger.
 */
export async function reconcileMediaObjects(now: Date = new Date()): Promise<void> {
  const graceMs = config.MEDIA_ORPHAN_GRACE_MINUTES * 60_000;
  const targets = [
    { bucket: config.S3_QUARANTINE_BUCKET, prefix: "processed/" },
    { bucket: config.S3_QUARANTINE_BUCKET, prefix: "quarantine/" },
    { bucket: config.S3_PUBLIC_BUCKET, prefix: "media/" }
  ];
  const mediaCache = new Map<string, ReconcileMediaRow | null>();

  for (const { bucket, prefix } of targets) {
    const objects = await listObjects(bucket, prefix, MAX_OBJECTS_PER_PREFIX);
    for (const object of objects) {
      try {
        const parsed = parseMediaObjectKey(object.key);
        const media = parsed ? await loadMediaRow(mediaCache, parsed.mediaId) : null;
        const withinGrace = now.getTime() - object.lastModified.getTime() < graceMs;
        const decision = decideMediaObjectAction({ key: object.key, parsed, media, withinGrace });

        if (decision.action === "skip") continue;

        if (decision.action === "adopt" && parsed) {
          const column = decision.column;
          const adopted = await pool.query(
            `UPDATE media_assets SET ${column} = $2, updated_at = now()
             WHERE id = $1 AND ${column} IS NULL AND deleted_at IS NULL`,
            [parsed.mediaId, object.key]
          );
          if (adopted.rowCount) {
            mediaCache.delete(parsed.mediaId);
            await recordMediaObjectEvent({
              mediaId: parsed.mediaId,
              bucket,
              objectKey: object.key,
              event: "adopt",
              actor: "reconcile",
              byteSize: object.size,
              metadata: { column, reason: "rebuilt_reference" }
            });
            console.log(`reconciled media object ${object.key}: adopted into ${column}`);
          }
          continue;
        }

        if (decision.action === "purge") {
          await deleteObject(bucket, object.key);
          await recordMediaObjectEvent({
            mediaId: parsed?.mediaId ?? null,
            bucket,
            objectKey: object.key,
            event: "purge",
            actor: "reconcile",
            byteSize: object.size,
            metadata: { reason: decision.reason, lastModified: object.lastModified.toISOString() }
          });
          console.log(`purged orphan media object ${object.key}: ${decision.reason}`);
        }
      } catch (error) {
        console.error({ bucket, key: object.key, error }, "failed to reconcile media object");
      }
    }
  }
}
