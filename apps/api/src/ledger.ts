import type { PoolClient } from "pg";
import type { LedgerObjectRole } from "@map/shared/media-keys";
import { pool, query } from "./db";
import { deleteObject } from "./storage";

type Executor = typeof import("./db").pool | PoolClient;

/**
 * 对象写入（含跨桶发布）成功后立即登记台账。所有 API 侧产生的对象都必须
 * 经过这里登记，避免在"对象已存在、数据库还没引用"的窗口崩溃时产生无法归属的孤儿。
 */
export async function recordMediaObject(
  client: Executor,
  input: {
    mediaId: string | null;
    bucket: string;
    objectKey: string;
    role: LedgerObjectRole;
    attemptId?: string | null;
    note?: string | null;
  }
): Promise<void> {
  await client.query(
    `INSERT INTO media_object_ledger(
       media_id, bucket, object_key, object_role, state, attempt_id,
       written_at, last_seen_at, origin, note, metadata, updated_at
     )
     VALUES ($1, $2, $3, $4, 'committed', $5, now(), now(), 'pipeline', $6, '{}'::jsonb, now())
     ON CONFLICT (bucket, object_key) DO UPDATE
       SET media_id = EXCLUDED.media_id,
           object_role = EXCLUDED.object_role,
           state = 'committed',
           attempt_id = COALESCE(EXCLUDED.attempt_id, media_object_ledger.attempt_id),
           last_seen_at = now(),
           note = EXCLUDED.note,
           delete_after = NULL,
           deleted_at = NULL,
           updated_at = now()`,
    [input.mediaId, input.bucket, input.objectKey, input.role, input.attemptId ?? null, input.note ?? null]
  );
}

/**
 * 删除一个媒体台账上的全部在役对象并落墓碑。物理删除尽力执行，
 * 删除失败的对象保留在台账中，worker 的对账/到期清理会继续重试。
 */
export async function deleteLedgerObjectsForMedia(
  mediaIds: string | string[],
  options: { origin?: "pipeline" | "operator" } = {}
): Promise<{ deleted: number; failed: Array<{ bucket: string; objectKey: string; error: string }> }> {
  const ids = Array.isArray(mediaIds) ? mediaIds : [mediaIds];
  if (!ids.length) return { deleted: 0, failed: [] };

  const entries = await query<{
    bucket: string;
    object_key: string;
    object_role: LedgerObjectRole;
    media_id: string | null;
  }>(
    `SELECT DISTINCT bucket, object_key, object_role, media_id
     FROM media_object_ledger
     WHERE media_id = ANY($1::uuid[]) AND state <> 'deleted'`,
    [ids]
  );

  let deleted = 0;
  const failed: Array<{ bucket: string; objectKey: string; error: string }> = [];
  await Promise.all(
    entries.rows.map(async (entry) => {
      try {
        // S3 DeleteObject 对不存在的键也返回成功，因此删除与落墓碑可一步完成；
        // 抛错时保留在役状态，等 worker 对账重试。
        await deleteObject(entry.bucket, entry.object_key);
        await markLedgerObjectDeleted(pool, entry.bucket, entry.object_key, {
          mediaId: entry.media_id,
          role: entry.object_role,
          origin: options.origin ?? "pipeline"
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

export async function markLedgerObjectDeleted(
  client: Executor,
  bucket: string,
  objectKey: string,
  options: { mediaId?: string | null; role?: LedgerObjectRole; origin?: "pipeline" | "reconcile" | "backfill" | "operator"; note?: string | null } = {}
): Promise<void> {
  await client.query(
    `INSERT INTO media_object_ledger(
       media_id, bucket, object_key, object_role, state,
       deleted_at, delete_after, origin, note, updated_at
     )
     VALUES ($1, $2, $3, COALESCE($4, 'unknown'::ledger_object_role), 'deleted', now(), NULL, $5, $6, now())
     ON CONFLICT (bucket, object_key) DO UPDATE
       SET state = 'deleted',
           deleted_at = now(),
           delete_after = NULL,
           note = COALESCE(EXCLUDED.note, media_object_ledger.note),
           updated_at = now()`,
    [options.mediaId ?? null, bucket, objectKey, options.role ?? null, options.origin ?? "pipeline", options.note ?? null]
  );
}

/** 审核发布失败等场景：对象已复制到公开桶但事务失败，短宽限期后必须物理删除。 */
export async function markLedgerObjectPendingDeletion(
  client: Executor,
  bucket: string,
  objectKey: string,
  deleteAfter: Date,
  options: { mediaId?: string | null; role?: LedgerObjectRole | undefined; note?: string | null } = {}
): Promise<void> {
  await client.query(
    `INSERT INTO media_object_ledger(
       media_id, bucket, object_key, object_role, state,
       delete_after, origin, note, updated_at
     )
     VALUES ($1, $2, $3, COALESCE($4, 'unknown'::ledger_object_role), 'delete_pending', $5, 'pipeline', $6, now())
     ON CONFLICT (bucket, object_key) DO UPDATE
       SET state = CASE WHEN media_object_ledger.state = 'deleted' THEN media_object_ledger.state
                        ELSE 'delete_pending' END,
           delete_after = CASE
             WHEN media_object_ledger.state = 'deleted' THEN NULL
             WHEN media_object_ledger.state IN ('superseded', 'delete_pending')
                  AND media_object_ledger.delete_after IS NOT NULL
               THEN media_object_ledger.delete_after
             ELSE EXCLUDED.delete_after END,
           note = EXCLUDED.note,
           updated_at = now()`,
    [options.mediaId ?? null, bucket, objectKey, options.role ?? null, deleteAfter, options.note ?? null]
  );
}
