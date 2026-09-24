import { hostname } from "node:os";
import type { PrivacyRegion } from "@map/shared/contracts";
import {
  processedImageKey,
  processedThumbnailKey,
  publicImageKey,
  publicThumbnailKey
} from "@map/shared/media-keys";
import { config } from "./config";
import { pool } from "./db";
import {
  commitProcessedMedia,
  deleteLedgerObject,
  failProcessedMedia,
  listMediaLedgerObjects,
  markObjectPendingDeletion,
  recordObjectWritten,
  startProcessingAttempt,
  recoverLostAttempts as runLostAttemptRecovery,
  type AttemptTrigger
} from "./ledger";
import {
  copyToPublic,
  deleteObject,
  objectExists,
  readQuarantineObject,
  writeQuarantineObject
} from "./storage";
import { scanForMalware } from "./clamav";
import { processPrivacyImage } from "./privacy";
import { deletePendingLedgerObjects, reconcileObjectStore } from "./reconcile";

const workerId = config.WORKER_ID || `${hostname()}-${process.pid}`;

export type MediaJobData = {
  mediaId: string;
  trigger: AttemptTrigger;
  queuedBy?: string | null;
};

function storageExec() {
  return { remove: deleteObject, exists: objectExists };
}

/** 删除一个媒体在台账上的全部在役对象（用户删除、账号清除、删除级联共用）。 */
export async function deleteAllMediaObjects(mediaId: string, origin: "pipeline" | "operator" = "pipeline"): Promise<{
  deleted: number;
  failed: Array<{ bucket: string; objectKey: string; error: string }>;
}> {
  const entries = await listMediaLedgerObjects(pool, mediaId);
  let deleted = 0;
  const failed: Array<{ bucket: string; objectKey: string; error: string }> = [];
  await Promise.all(
    entries.map(async (entry) => {
      try {
        await deleteLedgerObject(pool, entry.bucket, entry.object_key, {
          executor: storageExec(),
          mediaId,
          role: entry.object_role,
          origin
        });
        deleted += 1;
      } catch (error) {
        failed.push({
          bucket: entry.bucket,
          objectKey: entry.object_key,
          error: error instanceof Error ? error.message.slice(0, 300) : "unknown error"
        });
      }
    })
  );
  return { deleted, failed };
}

export async function processMediaJob(
  mediaId: string,
  trigger: AttemptTrigger = "initial",
  queuedBy: string | null = null,
  jobId: string | null = null
): Promise<void> {
  // 先在事务里认领任务并创建处理尝试，防止同一媒体被并发/重复任务双重处理。
  const client = await pool.connect();
  let attempt: { attemptId: string; quarantineObjectKey: string } | null = null;
  try {
    await client.query("BEGIN");
    attempt = await startProcessingAttempt(client, mediaId, { trigger, queuedBy, workerId, jobId });
    await client.query("COMMIT");
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  } finally {
    client.release();
  }

  if (!attempt) {
    console.log(`skip media ${mediaId}: not claimable (already running or not retryable)`);
    return;
  }
  const { attemptId, quarantineObjectKey } = attempt;
  const autoPublish = Boolean(config.PRIVACY_DETECTOR_URL);
  const publicObjectKey = publicImageKey(mediaId);
  const publicThumbObjectKey = publicThumbnailKey(mediaId);

  try {
    await pool.query("UPDATE media_assets SET privacy_status = 'scanning', updated_at = now() WHERE id = $1 AND deleted_at IS NULL", [mediaId]);
    const source = await readQuarantineObject(quarantineObjectKey);
    await scanForMalware(source);

    await pool.query("UPDATE media_assets SET privacy_status = 'processing', updated_at = now() WHERE id = $1 AND deleted_at IS NULL", [mediaId]);
    const reportRow = await pool.query<{ privacy_report: { manualRegions?: PrivacyRegion[] } | null }>(
      "SELECT privacy_report FROM media_assets WHERE id = $1",
      [mediaId]
    );
    const manualRegions = reportRow.rows[0]?.privacy_report?.manualRegions ?? [];
    const processed = await processPrivacyImage(source, manualRegions);

    // 产物键带尝试 ID：重试不覆盖旧产物，半成品在任何时刻都能通过台账归属到媒体与尝试。
    const processedKey = processedImageKey(mediaId, attemptId);
    const thumbnailKey = processedThumbnailKey(mediaId, attemptId);
    await writeQuarantineObject(processedKey, processed.image, "image/webp");
    await recordObjectWritten(pool, {
      mediaId,
      bucket: config.S3_QUARANTINE_BUCKET,
      objectKey: processedKey,
      role: "processed_image",
      attemptId,
      metadata: { sha256: processed.sha256, width: processed.width, height: processed.height }
    });
    await writeQuarantineObject(thumbnailKey, processed.thumbnail, "image/webp");
    await recordObjectWritten(pool, {
      mediaId,
      bucket: config.S3_QUARANTINE_BUCKET,
      objectKey: thumbnailKey,
      role: "processed_thumbnail",
      attemptId
    });

    if (autoPublish) {
      await copyToPublic(processedKey, publicObjectKey);
      await recordObjectWritten(pool, {
        mediaId,
        bucket: config.S3_PUBLIC_BUCKET,
        objectKey: publicObjectKey,
        role: "public_image",
        attemptId
      });
      await copyToPublic(thumbnailKey, publicThumbObjectKey);
      await recordObjectWritten(pool, {
        mediaId,
        bucket: config.S3_PUBLIC_BUCKET,
        objectKey: publicThumbObjectKey,
        role: "public_thumbnail",
        attemptId
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
      attemptId,
      completedAt: new Date().toISOString()
    };

    // 提交与台账替换在一个事务里；事务后再物理删除被替换的旧尝试产物。
    const txClient = await pool.connect();
    let superseded: Array<{ bucket: string; object_key: string }> = [];
    try {
      await txClient.query("BEGIN");
      superseded = await commitProcessedMedia(txClient, {
        mediaId,
        attemptId,
        status: autoPublish ? "ready" : "manual_review",
        processedKey,
        thumbnailKey,
        publicKey: autoPublish ? publicObjectKey : null,
        publicThumbnailKey: autoPublish ? publicThumbObjectKey : null,
        width: processed.width,
        height: processed.height,
        sha256: processed.sha256,
        perceptualHash: processed.perceptualHash,
        report,
        retentionHours: config.ORIGINAL_RETENTION_HOURS
      });
      await txClient.query("COMMIT");
    } catch (error) {
      await txClient.query("ROLLBACK");
      throw error;
    } finally {
      txClient.release();
    }

    await cleanupSupersededObjects(superseded);
    console.log(`media ${mediaId} processed (attempt ${attemptId}) as ${autoPublish ? "ready" : "manual_review"}`);
  } catch (error) {
    const message = error instanceof Error ? error.message.slice(0, 500) : "Unknown media processing error";
    await failProcessedMedia(pool, { mediaId, attemptId, failureCode: message });

    // 只清理本尝试实际写入并登记过台账的对象：读崩溃时序、从未写成功的键不会凭空造台账。
    // 公开桶对象（仅 autoPublish 流程会产生）按隐私优先立即删除；隔离桶半成品设 1 小时宽限，
    // 让崩溃发生在"对象已写、台账未及登记"窗口的对象有机会被桶扫描补登记后一起收口。
    const written = await pool.query<{ bucket: string; object_key: string; object_role: import("@map/shared/media-keys").LedgerObjectRole }>(
      `SELECT bucket, object_key, object_role FROM media_object_ledger
       WHERE attempt_id = $1 AND state = 'committed'`,
      [attemptId]
    );
    await Promise.allSettled(
      written.rows.map(async (row) => {
        const isPublic = row.bucket === config.S3_PUBLIC_BUCKET;
        try {
          if (isPublic) {
            await deleteLedgerObject(pool, row.bucket, row.object_key, {
              executor: storageExec(),
              mediaId,
              origin: "pipeline",
              note: "removed after failed processing attempt"
            });
          } else {
            // 隔离桶半成品立即尝试物理删除；删除失败时由 1 小时宽限到期任务重试，
            // 也覆盖"对象已写、台账未及登记"崩溃窗口的残留。
            await markObjectPendingDeletion(pool, row.bucket, row.object_key, new Date(Date.now() + 60 * 60 * 1000), {
              state: "delete_pending",
              mediaId,
              role: row.object_role,
              attemptId,
              note: "processing attempt failed"
            }).catch(() => undefined);
          }
        } catch (cleanupError) {
          console.error({ mediaId, attemptId, row, cleanupError }, "failed to remove attempt object; pending task will retry");
        }
      })
    );
    throw error;
  }
}

async function cleanupSupersededObjects(rows: Array<{ bucket: string; object_key: string }>): Promise<void> {
  await Promise.all(
    rows.map(async (row) => {
      try {
        await deleteLedgerObject(pool, row.bucket, row.object_key, {
          executor: storageExec(),
          origin: "pipeline",
          note: "superseded by newer processing attempt"
        });
      } catch (error) {
        console.error({ row, error }, "failed to remove superseded object; pending task will retry");
      }
    })
  );
}

export async function cleanupOriginalMedia(): Promise<void> {
  // 只完成上传、从未进入处理的隔离原图（创建上传后放弃）：记录已被标记删除时清桶。
  const abandoned = await pool.query<{ id: string; quarantine_object_key: string }>(
    `SELECT id, quarantine_object_key FROM media_assets
     WHERE privacy_status = 'quarantined'
       AND created_at < now() - interval '24 hours'
       AND deleted_at IS NULL
     LIMIT 50`
  );
  for (const row of abandoned.rows) {
    try {
      await deleteLedgerObject(pool, config.S3_QUARANTINE_BUCKET, row.quarantine_object_key, {
        executor: storageExec(),
        mediaId: row.id,
        role: "quarantine_original",
        origin: "pipeline",
        note: "abandoned upload never submitted for processing"
      });
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

  // 原图保留期到期：按台账删除（即使对象键已被重建或此前缺失也能正确落墓碑）。
  const result = await pool.query<{ id: string; quarantine_object_key: string }>(
    `SELECT id, quarantine_object_key FROM media_assets
     WHERE delete_after IS NOT NULL AND delete_after <= now()
       AND quarantine_object_key IS NOT NULL
       AND quarantine_object_key NOT LIKE 'deleted/%'
       AND privacy_status IN ('ready', 'manual_review', 'rejected', 'failed', 'deleted')
     LIMIT 50`
  );
  for (const row of result.rows) {
    try {
      await deleteLedgerObject(pool, config.S3_QUARANTINE_BUCKET, row.quarantine_object_key, {
        executor: storageExec(),
        mediaId: row.id,
        role: "quarantine_original",
        origin: "pipeline",
        note: "original retention window elapsed"
      });
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

/**
 * 中断恢复：基于尝试表而不是 updated_at 猜测。把超时仍在 running 的尝试置 lost，
 * 释放媒体行占用并返回需要重新入队的媒体。
 */
export async function recoverStuckMedia(): Promise<string[]> {
  const lost = await runLostAttemptRecovery(20 * 60 * 1000);
  return lost.map((row) => row.mediaId);
}

/**
 * 已删除媒体的残留收口：所有对象删除都已走台账，这里只需处理台账中
 * superseded/delete_pending 到期的对象（包括用户删除时 S3 删除失败的重试）。
 * 媒体行上的对象键在对象物理删除后置空。
 */
export async function cleanupDeletedMediaObjects(): Promise<void> {
  await deletePendingLedgerObjects({
    remove: deleteObject,
    exists: objectExists
  });

  // 已删除媒体行仍在引用对象键（旧代码路径/手动改库）时，将键收敛到墓碑占位。
  const deletedRows = await pool.query<{ id: string }>(
    `SELECT id FROM media_assets
     WHERE privacy_status = 'deleted'
       AND (quarantine_object_key IS NOT NULL AND quarantine_object_key NOT LIKE 'deleted/%'
         OR processed_object_key IS NOT NULL
         OR thumbnail_object_key IS NOT NULL
         OR public_object_key IS NOT NULL
         OR public_thumbnail_object_key IS NOT NULL)
     LIMIT 50`
  );
  for (const row of deletedRows.rows) {
    // 台账删除（或确认对象已不存在）之后才清空列，避免"键没了对象还在"的新孤儿。
    const { failed } = await deleteAllMediaObjects(row.id, "pipeline");
    if (failed.length === 0) {
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
        [row.id, `deleted/${row.id}.object`]
      );
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

/**
 * 对象存储对账：列举隔离/公开桶对象，与台账互相校验，重建可判定的引用，
 * 给无法归属的存量孤儿登记归属线索并安排宽限期删除。维护 tick 每轮调用，
 * 通过持久化的游标分批推进。
 */
export async function reconcileMediaObjects(): Promise<void> {
  await reconcileObjectStore({
    quarantineBucket: config.S3_QUARANTINE_BUCKET,
    publicBucket: config.S3_PUBLIC_BUCKET,
    batchSize: config.RECONCILE_BATCH_SIZE,
    orphanQuarantineGraceHours: config.ORPHAN_QUARANTINE_GRACE_HOURS,
    orphanPublicGraceMinutes: config.ORPHAN_PUBLIC_GRACE_MINUTES,
    remove: deleteObject,
    exists: objectExists
  });
}
