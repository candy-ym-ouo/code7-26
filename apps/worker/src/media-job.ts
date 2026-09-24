import type { PrivacyRegion } from "@map/shared/contracts";
import { config } from "./config";
import { pool } from "./db";
import { deleteObject, objectExists, readQuarantineObject, writeQuarantineObject, copyToPublic } from "./storage";
import { scanForMalware } from "./clamav";
import { processPrivacyImage } from "./privacy";
import { recordMediaObjectEvent } from "./media-ledger";

export async function processMediaJob(mediaId: string): Promise<void> {
  const result = await pool.query<{
    id: string;
    privacy_status: string;
    quarantine_object_key: string;
    privacy_report: { manualRegions?: PrivacyRegion[] } | null;
  }>(
    `SELECT id, privacy_status, quarantine_object_key, privacy_report
     FROM media_assets WHERE id = $1 AND deleted_at IS NULL`,
    [mediaId]
  );
  const media = result.rows[0];
  if (!media) throw new Error("Media record not found");
  if (!["processing", "failed"].includes(media.privacy_status)) {
    console.log(`skip media ${mediaId}: status=${media.privacy_status}`);
    return;
  }

  const autoPublish = Boolean(config.PRIVACY_DETECTOR_URL);
  const publicKey = `media/${mediaId}.webp`;
  const publicThumbnailKey = `media/${mediaId}.thumb.webp`;
  const publishedKeys: string[] = [];

  try {
    await pool.query("UPDATE media_assets SET privacy_status = 'scanning', updated_at = now() WHERE id = $1", [mediaId]);
    const source = await readQuarantineObject(media.quarantine_object_key);
    await scanForMalware(source);

    await pool.query("UPDATE media_assets SET privacy_status = 'processing', updated_at = now() WHERE id = $1", [mediaId]);
    const manualRegions = media.privacy_report?.manualRegions ?? [];
    const processed = await processPrivacyImage(source, manualRegions);

    const processedKey = `processed/${mediaId}.webp`;
    const thumbnailKey = `processed/${mediaId}.thumb.webp`;
    const processedExists = await objectExists(config.S3_QUARANTINE_BUCKET, processedKey);
    const thumbnailExists = await objectExists(config.S3_QUARANTINE_BUCKET, thumbnailKey);
    await writeQuarantineObject(processedKey, processed.image, "image/webp");
    await recordMediaObjectEvent({
      mediaId,
      bucket: config.S3_QUARANTINE_BUCKET,
      objectKey: processedKey,
      event: processedExists ? "rewrite" : "write",
      actor: "worker",
      byteSize: processed.image.length
    });
    await writeQuarantineObject(thumbnailKey, processed.thumbnail, "image/webp");
    await recordMediaObjectEvent({
      mediaId,
      bucket: config.S3_QUARANTINE_BUCKET,
      objectKey: thumbnailKey,
      event: thumbnailExists ? "rewrite" : "write",
      actor: "worker",
      byteSize: processed.thumbnail.length
    });

    // Persist the derived object keys before any further step so an
    // interruption can never leave unreferenced objects in the bucket.
    await pool.query(
      `UPDATE media_assets
       SET processed_object_key = $2, thumbnail_object_key = $3, updated_at = now()
       WHERE id = $1`,
      [mediaId, processedKey, thumbnailKey]
    );

    if (autoPublish) {
      await copyToPublic(processedKey, publicKey);
      publishedKeys.push(publicKey);
      await recordMediaObjectEvent({
        mediaId,
        bucket: config.S3_PUBLIC_BUCKET,
        objectKey: publicKey,
        event: "write",
        actor: "worker",
        byteSize: processed.image.length,
        metadata: { source: "auto_publish" }
      });
      await copyToPublic(thumbnailKey, publicThumbnailKey);
      publishedKeys.push(publicThumbnailKey);
      await recordMediaObjectEvent({
        mediaId,
        bucket: config.S3_PUBLIC_BUCKET,
        objectKey: publicThumbnailKey,
        event: "write",
        actor: "worker",
        byteSize: processed.thumbnail.length,
        metadata: { source: "auto_publish" }
      });
    }

    const report = {
      manualRegions: processed.manualRegions,
      detectorRegions: processed.detectorRegions,
      detectorConfigured: autoPublish,
      originalMetadataRemoved: true,
      serverReencoded: true,
      width: processed.width,
      height: processed.height,
      sha256: processed.sha256,
      perceptualHash: processed.perceptualHash,
      completedAt: new Date().toISOString()
    };

    await pool.query(
      `UPDATE media_assets
       SET privacy_status = $2,
           processed_object_key = $3,
           thumbnail_object_key = $4,
           public_object_key = $5,
           public_thumbnail_object_key = $12,
           width = $6,
           height = $7,
           sha256 = $8,
           perceptual_hash = $9,
           privacy_report = $10::jsonb,
           failure_code = NULL,
           processed_at = now(),
           delete_after = now() + ($11::text || ' hours')::interval,
           updated_at = now()
       WHERE id = $1`,
      [
        mediaId,
        autoPublish ? "ready" : "manual_review",
        processedKey,
        thumbnailKey,
        autoPublish ? publicKey : null,
        processed.width,
        processed.height,
        processed.sha256,
        processed.perceptualHash,
        JSON.stringify(report),
        String(config.ORIGINAL_RETENTION_HOURS),
        autoPublish ? publicThumbnailKey : null
      ]
    );

    console.log(`media ${mediaId} processed as ${autoPublish ? "ready" : "manual_review"}`);
  } catch (error) {
    const message = error instanceof Error ? error.message.slice(0, 500) : "Unknown media processing error";
    await pool.query(
      `UPDATE media_assets
       SET privacy_status = 'failed', failure_code = $2,
           delete_after = now() + interval '7 days', updated_at = now()
       WHERE id = $1`,
      [mediaId, message]
    );
    for (const key of publishedKeys) {
      try {
        await deleteObject(config.S3_PUBLIC_BUCKET, key);
        await recordMediaObjectEvent({
          mediaId,
          bucket: config.S3_PUBLIC_BUCKET,
          objectKey: key,
          event: "delete",
          actor: "worker",
          metadata: { reason: "processing_failed" }
        });
      } catch (cleanupError) {
        console.error({ mediaId, key, cleanupError }, "failed to roll back published media object");
      }
    }
    throw error;
  }
}

export async function cleanupOriginalMedia(): Promise<void> {
  const abandoned = await pool.query<{ id: string; quarantine_object_key: string }>(
    `SELECT id, quarantine_object_key FROM media_assets
     WHERE privacy_status = 'quarantined'
       AND created_at < now() - interval '24 hours'
       AND deleted_at IS NULL
     LIMIT 50`
  );
  for (const row of abandoned.rows) {
    try {
      if (await objectExists(config.S3_QUARANTINE_BUCKET, row.quarantine_object_key)) {
        await deleteObject(config.S3_QUARANTINE_BUCKET, row.quarantine_object_key);
        await recordMediaObjectEvent({
          mediaId: row.id,
          bucket: config.S3_QUARANTINE_BUCKET,
          objectKey: row.quarantine_object_key,
          event: "delete",
          actor: "maintenance",
          metadata: { reason: "abandoned_upload" }
        });
      }
      await pool.query(
        `UPDATE media_assets
         SET privacy_status = 'deleted', deleted_at = now(), updated_at = now()
         WHERE id = $1`,
        [row.id]
      );
    } catch (error) {
      console.error({ mediaId: row.id, error }, "failed to clean abandoned upload");
    }
  }

  const result = await pool.query<{ id: string; quarantine_object_key: string }>(
    `SELECT id, quarantine_object_key FROM media_assets
     WHERE delete_after IS NOT NULL AND delete_after <= now()
       AND quarantine_object_key IS NOT NULL
       AND privacy_status IN ('ready', 'manual_review', 'rejected', 'failed', 'deleted')
     LIMIT 50`
  );
  for (const row of result.rows) {
    try {
      if (await objectExists(config.S3_QUARANTINE_BUCKET, row.quarantine_object_key)) {
        await deleteObject(config.S3_QUARANTINE_BUCKET, row.quarantine_object_key);
        await recordMediaObjectEvent({
          mediaId: row.id,
          bucket: config.S3_QUARANTINE_BUCKET,
          objectKey: row.quarantine_object_key,
          event: "delete",
          actor: "maintenance",
          metadata: { reason: "retention_expired" }
        });
      }
      await pool.query("UPDATE media_assets SET delete_after = NULL, updated_at = now() WHERE id = $1", [row.id]);
    } catch (error) {
      console.error({ mediaId: row.id, error }, "failed to clean original media");
    }
  }
}

export async function markStaleFeatures(): Promise<void> {
  await pool.query(
    `UPDATE map_features
     SET needs_review_at = COALESCE(needs_review_at, now()), updated_at = now()
     WHERE status = 'published' AND freshness_expires_at <= now() AND needs_review_at IS NULL`
  );
}

export async function recoverStuckMedia(): Promise<string[]> {
  const result = await pool.query<{ id: string }>(
    `UPDATE media_assets
     SET privacy_status = 'processing', failure_code = 'Recovered after worker timeout', updated_at = now()
     WHERE privacy_status IN ('scanning', 'processing')
       AND updated_at < now() - interval '20 minutes'
       AND deleted_at IS NULL
     RETURNING id`
  );
  return result.rows.map((row) => row.id);
}

export async function cleanupDeletedMediaObjects(): Promise<void> {
  const result = await pool.query<{
    id: string;
    quarantine_object_key: string;
    processed_object_key: string | null;
    thumbnail_object_key: string | null;
    public_object_key: string | null;
    public_thumbnail_object_key: string | null;
  }>(
    `SELECT id, quarantine_object_key, processed_object_key, thumbnail_object_key,
            public_object_key, public_thumbnail_object_key
     FROM media_assets
     WHERE privacy_status = 'deleted'
       AND (quarantine_object_key NOT LIKE 'deleted/%'
         OR processed_object_key IS NOT NULL
         OR thumbnail_object_key IS NOT NULL
         OR public_object_key IS NOT NULL
         OR public_thumbnail_object_key IS NOT NULL)
     LIMIT 50`
  );

  for (const item of result.rows) {
    try {
      const targets: Array<{ bucket: string; key: string }> = [];
      if (!item.quarantine_object_key.startsWith("deleted/")) {
        targets.push({ bucket: config.S3_QUARANTINE_BUCKET, key: item.quarantine_object_key });
      }
      if (item.processed_object_key) targets.push({ bucket: config.S3_QUARANTINE_BUCKET, key: item.processed_object_key });
      if (item.thumbnail_object_key) targets.push({ bucket: config.S3_QUARANTINE_BUCKET, key: item.thumbnail_object_key });
      if (item.public_object_key) targets.push({ bucket: config.S3_PUBLIC_BUCKET, key: item.public_object_key });
      if (item.public_thumbnail_object_key) targets.push({ bucket: config.S3_PUBLIC_BUCKET, key: item.public_thumbnail_object_key });
      await Promise.all(targets.map((target) => deleteObject(target.bucket, target.key)));
      for (const target of targets) {
        await recordMediaObjectEvent({
          mediaId: item.id,
          bucket: target.bucket,
          objectKey: target.key,
          event: "delete",
          actor: "maintenance",
          metadata: { reason: "media_deleted" }
        });
      }

      await pool.query(
        `UPDATE media_assets
         SET quarantine_object_key = $2,
             processed_object_key = NULL,
             thumbnail_object_key = NULL,
             public_object_key = NULL,
             public_thumbnail_object_key = NULL,
             delete_after = NULL,
             updated_at = now()
         WHERE id = $1`,
        [item.id, `deleted/${item.id}.object`]
      );
    } catch (error) {
      console.error({ mediaId: item.id, error }, "failed to clean deleted media objects");
    }
  }
}

export async function markUnreferencedMediaDeleted(): Promise<void> {
  await pool.query(
    `UPDATE media_assets ma
     SET privacy_status = 'deleted', deleted_at = now(), updated_at = now()
     WHERE ma.deleted_at IS NULL
       AND ma.created_at < now() - interval '7 days'
       AND NOT EXISTS (
         SELECT 1 FROM revision_media rm WHERE rm.media_id = ma.id
       )`
  );
}
