import type { Pool, PoolClient, QueryResultRow } from "pg";
import type { LedgerObjectRole } from "@map/shared/media-keys";
import { pool } from "./db";

export type LedgerOrigin = "pipeline" | "reconcile" | "backfill" | "operator";

export type LedgerEntryInput = {
  mediaId: string | null;
  bucket: string;
  objectKey: string;
  role: LedgerObjectRole;
  attemptId?: string | null;
  origin?: LedgerOrigin;
  note?: string | null;
  metadata?: Record<string, unknown>;
};

export type Executor = Pool | PoolClient;

function runQuery<T extends QueryResultRow = QueryResultRow>(
  client: Executor,
  text: string,
  values: unknown[] = []
) {
  return client.query<T>(text, values);
}

/**
 * 对象写入成功后立即登记台账。重复上报（重试、恢复）时刷新归属信息，
 * 并把可能存在的删除宽限期标记复位——对象既然又出现了，就还是在役的。
 */
export async function recordObjectWritten(client: Executor, input: LedgerEntryInput): Promise<void> {
  await runQuery(client,
    `INSERT INTO media_object_ledger(
       media_id, bucket, object_key, object_role, state, attempt_id,
       written_at, last_seen_at, origin, note, metadata, updated_at
     )
     VALUES ($1, $2, $3, $4, 'committed', $5, now(), now(), $6, $7, $8::jsonb, now())
     ON CONFLICT (bucket, object_key) DO UPDATE
       SET media_id = EXCLUDED.media_id,
           object_role = EXCLUDED.object_role,
           state = 'committed',
           attempt_id = COALESCE(EXCLUDED.attempt_id, media_object_ledger.attempt_id),
           last_seen_at = now(),
           origin = EXCLUDED.origin,
           note = EXCLUDED.note,
           metadata = media_object_ledger.metadata || EXCLUDED.metadata,
           delete_after = NULL,
           deleted_at = NULL,
           updated_at = now()`,
    [
      input.mediaId,
      input.bucket,
      input.objectKey,
      input.role,
      input.attemptId ?? null,
      input.origin ?? "pipeline",
      input.note ?? null,
      JSON.stringify(input.metadata ?? {})
    ]
  );
}

export type MarkResult = "deleted" | "missing" | "already";

/**
 * 物理删除一个对象并在台账中落墓碑。删除本身失败时抛出，调用方负责重试；
 * 对象在存储中已不存在（404/HEAD 失败）同样补记墓碑，保证台账与桶收敛。
 */
export async function deleteLedgerObject(
  client: Executor,
  bucket: string,
  objectKey: string,
  options: { executor: { remove: (bucket: string, key: string) => Promise<unknown>; exists: (bucket: string, key: string) => Promise<boolean> }; mediaId?: string | null; role?: LedgerObjectRole; note?: string; origin?: LedgerOrigin }
): Promise<MarkResult> {
  let result: MarkResult;
  if (await options.executor.exists(bucket, objectKey)) {
    await options.executor.remove(bucket, objectKey);
    result = "deleted";
  } else {
    result = "missing";
  }
  await markObjectDeleted(client, bucket, objectKey, {
    mediaId: options.mediaId ?? null,
    role: options.role,
    origin: options.origin ?? "pipeline",
    note: options.note ?? (result === "missing" ? "object absent at deletion" : null)
  });
  return result;
}

/** 仅更新台账状态为已删除（逻辑墓碑），不触碰对象存储。 */
export async function markObjectDeleted(
  client: Executor,
  bucket: string,
  objectKey: string,
  options: { mediaId?: string | null; role?: LedgerObjectRole | undefined; origin?: LedgerOrigin | undefined; note?: string | null } = {}
): Promise<void> {
  await runQuery(client,
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
           origin = CASE WHEN media_object_ledger.origin = 'pipeline' THEN media_object_ledger.origin ELSE EXCLUDED.origin END,
           updated_at = now()`,
    [options.mediaId ?? null, bucket, objectKey, options.role ?? null, options.origin ?? "pipeline", options.note ?? null]
  );
}

/** 将台账对象移入物理删除宽限期（孤儿、过期原图、被替换的旧尝试产物）。 */
export async function markObjectPendingDeletion(
  client: Executor,
  bucket: string,
  objectKey: string,
  deleteAfter: Date,
  options: { state?: "superseded" | "delete_pending"; mediaId?: string | null; role?: LedgerObjectRole; origin?: LedgerOrigin; note?: string | null; attemptId?: string | null } = {}
): Promise<void> {
  await runQuery(client,
    `INSERT INTO media_object_ledger(
       media_id, bucket, object_key, object_role, state, attempt_id,
       delete_after, origin, note, updated_at
     )
     VALUES ($1, $2, $3, COALESCE($4, 'unknown'::ledger_object_role),
             COALESCE($5::ledger_object_state, 'delete_pending'), $6, $7, $8, $9, now())
     ON CONFLICT (bucket, object_key) DO UPDATE
       SET media_id = COALESCE(EXCLUDED.media_id, media_object_ledger.media_id),
           object_role = COALESCE(EXCLUDED.object_role, media_object_ledger.object_role),
           attempt_id = COALESCE(EXCLUDED.attempt_id, media_object_ledger.attempt_id),
           state = CASE WHEN media_object_ledger.state = 'deleted' THEN media_object_ledger.state
                        ELSE COALESCE(EXCLUDED.state, 'delete_pending') END,
           -- 已在宽限窗口中的对象保留原始到期时间，避免每轮扫描把宽限期无限推后。
           delete_after = CASE
             WHEN media_object_ledger.state = 'deleted' THEN NULL
             WHEN media_object_ledger.state IN ('superseded', 'delete_pending')
                  AND media_object_ledger.delete_after IS NOT NULL
               THEN media_object_ledger.delete_after
             ELSE EXCLUDED.delete_after END,
           note = EXCLUDED.note,
           updated_at = now()`,
    [
      options.mediaId ?? null,
      bucket,
      objectKey,
      options.role ?? null,
      options.state ?? "delete_pending",
      options.attemptId ?? null,
      deleteAfter,
      options.origin ?? "pipeline",
      options.note ?? null
    ]
  );
}

/** 桶扫描观察到对象仍在存储中。 */
export async function touchObjectSeen(
  client: Executor,
  bucket: string,
  objectKey: string,
  options: { mediaId?: string | null; role?: LedgerObjectRole | undefined; origin?: LedgerOrigin | undefined } = {}
): Promise<void> {
  await runQuery(client,
    `INSERT INTO media_object_ledger(
       media_id, bucket, object_key, object_role, state, last_seen_at, origin, updated_at
     )
     VALUES ($1, $2, $3, COALESCE($4, 'unknown'::ledger_object_role), 'committed', now(), $5, now())
     ON CONFLICT (bucket, object_key) DO UPDATE
       SET last_seen_at = now(),
           media_id = COALESCE(media_object_ledger.media_id, EXCLUDED.media_id),
           object_role = CASE WHEN media_object_ledger.object_role = 'unknown'
                              THEN COALESCE(EXCLUDED.object_role, media_object_ledger.object_role)
                              ELSE media_object_ledger.object_role END,
           updated_at = now()`,
    [options.mediaId ?? null, bucket, objectKey, options.role ?? null, options.origin ?? "reconcile"]
  );
}

/** 某媒体台账下处于指定状态的对象（物理删除/替换清理用）。 */
export async function listMediaLedgerObjects(
  client: Executor,
  mediaId: string
): Promise<
  Array<{
    bucket: string;
    object_key: string;
    object_role: LedgerObjectRole;
    state: string;
    attempt_id: string | null;
  }>
> {
  const result = await runQuery<{
    bucket: string;
    object_key: string;
    object_role: LedgerObjectRole;
    state: string;
    attempt_id: string | null;
  }>(
    client,
    `SELECT bucket, object_key, object_role, state, attempt_id
     FROM media_object_ledger
     WHERE media_id = $1 AND state <> 'deleted'`,
    [mediaId]
  );
  return result.rows;
}

export type AttemptTrigger = "initial" | "retry" | "recovery";

/**
 * 以行锁认领一个媒体处理任务，并创建尝试记录（attempt_no 在表内连续编号）。
 * 调用方必须已开启事务。返回 null 表示已被其他 worker 抢先
 * （例如维护恢复与 BullMQ 重投并发），调用方应直接跳过。
 *
 * 尝试行统一由 worker 创建：API 入队时只翻转媒体状态，避免"队列里有任务、
 * 但任务从未被认领"时产生悬空的 running 尝试记录。
 */
export async function startProcessingAttempt(
  client: PoolClient,
  mediaId: string,
  options: { trigger: AttemptTrigger; queuedBy?: string | null; workerId: string; jobId?: string | null }
): Promise<{ attemptId: string; attemptNo: number; quarantineObjectKey: string } | null> {
  const mediaResult = await client.query<{
    quarantine_object_key: string;
    current_attempt_id: string | null;
    privacy_status: string;
  }>(
    `SELECT quarantine_object_key, current_attempt_id, privacy_status
     FROM media_assets
     WHERE id = $1 AND deleted_at IS NULL
     FOR UPDATE`,
    [mediaId]
  );
  const row = mediaResult.rows[0];
  if (!row) return null;
  const claimable =
    row.current_attempt_id === null && ["processing", "failed", "rejected"].includes(row.privacy_status);
  if (!claimable) return null;

  const numberResult = await client.query<{ attempt_no: number }>(
    `SELECT COALESCE(MAX(attempt_no), 0) + 1 AS attempt_no
     FROM media_processing_attempts WHERE media_id = $1`,
    [mediaId]
  );
  const attemptNo = numberResult.rows[0]!.attempt_no;

  const attempt = await client.query<{ id: string }>(
    `INSERT INTO media_processing_attempts(media_id, attempt_no, trigger_type, queued_by, worker_id, job_id, status)
     VALUES ($1, $2, $3::media_attempt_trigger, $4, $5, $6, 'running')
     RETURNING id`,
    [mediaId, attemptNo, options.trigger, options.queuedBy ?? null, options.workerId, options.jobId ?? null]
  );
  const attemptId = attempt.rows[0]!.id;
  await client.query(
    `UPDATE media_assets
       SET privacy_status = 'processing',
           current_attempt_id = $2,
           processing_attempt_count = $3,
           updated_at = now()
     WHERE id = $1`,
    [mediaId, attemptId, attemptNo]
  );
  return { attemptId, attemptNo, quarantineObjectKey: row.quarantine_object_key };
}

export async function finishProcessingAttempt(
  client: Executor,
  attemptId: string,
  status: "succeeded" | "failed",
  errorCode?: string | null
): Promise<void> {
  await runQuery(client,
    `UPDATE media_processing_attempts
       SET status = $2, error_code = $3, finished_at = now(), updated_at = now()
     WHERE id = $1`,
    [attemptId, status, errorCode ?? null]
  );
}

/**
 * 提交处理结果：媒体行更新、尝试收尾、上一尝试残留产物转入宽限期，同一事务完成。
 * 返回物理删除已被替换的旧处理产物所需的清单（事务提交后执行）。
 */
export async function commitProcessedMedia(client: PoolClient, params: {
  mediaId: string;
  attemptId: string;
  status: "ready" | "manual_review";
  processedKey: string;
  thumbnailKey: string;
  publicKey: string | null;
  publicThumbnailKey: string | null;
  width: number;
  height: number;
  sha256: string;
  perceptualHash: string;
  report: Record<string, unknown>;
  retentionHours: number;
}): Promise<Array<{ bucket: string; object_key: string }>> {
  await client.query(
    `UPDATE media_assets
       SET privacy_status = $2,
           processed_object_key = $3,
           thumbnail_object_key = $4,
           public_object_key = $5,
           public_thumbnail_object_key = $6,
           width = $7,
           height = $8,
           sha256 = $9,
           perceptual_hash = $10,
           privacy_report = $11::jsonb,
           failure_code = NULL,
           current_attempt_id = NULL,
           processed_at = now(),
           delete_after = now() + ($12::text || ' hours')::interval,
           updated_at = now()
     WHERE id = $1 AND deleted_at IS NULL`,
    [
      params.mediaId,
      params.status,
      params.processedKey,
      params.thumbnailKey,
      params.publicKey,
      params.publicThumbnailKey,
      params.width,
      params.height,
      params.sha256,
      params.perceptualHash,
      JSON.stringify(params.report),
      String(params.retentionHours)
    ]
  );
  // 媒体在处理期间被删除：放弃派生结果并抛错，触发失败路径清理本次尝试刚写入的对象。
  const aliveResult = await client.query<{ alive: boolean }>(
    "SELECT deleted_at IS NULL AS alive FROM media_assets WHERE id = $1",
    [params.mediaId]
  );
  if (!aliveResult.rows[0]?.alive) {
    throw new Error("media was deleted while processing; discarding derived objects");
  }

  const superseded = await client.query<{ bucket: string; object_key: string }>(
    `UPDATE media_object_ledger
       SET state = 'superseded',
           delete_after = now() + interval '1 hour',
           note = COALESCE(note, 'replaced by newer processing attempt'),
           updated_at = now()
     WHERE media_id = $1
       AND state = 'committed'
       AND object_role IN ('processed_image', 'processed_thumbnail', 'public_image', 'public_thumbnail')
       AND object_key <> ALL($2::text[])
     RETURNING bucket, object_key`,
    [params.mediaId, [params.processedKey, params.thumbnailKey, ...(params.publicKey ? [params.publicKey] : []), ...(params.publicThumbnailKey ? [params.publicThumbnailKey] : [])]]
  );

  await client.query(
    `UPDATE media_processing_attempts
       SET status = 'succeeded', finished_at = now(), updated_at = now()
     WHERE id = $1`,
    [params.attemptId]
  );
  return superseded.rows;
}

/** 失败收尾：释放尝试占用、记录错误；可选清理本次尝试已写入的对象。 */
export async function failProcessedMedia(client: Executor, params: {
  mediaId: string;
  attemptId: string;
  failureCode: string;
}): Promise<void> {
  await client.query(
    `UPDATE media_assets
       SET privacy_status = 'failed',
           failure_code = $2,
           current_attempt_id = NULL,
           delete_after = now() + interval '7 days',
           updated_at = now()
     WHERE id = $1 AND deleted_at IS NULL`,
    [params.mediaId, params.failureCode]
  );
  await finishProcessingAttempt(client, params.attemptId, "failed", params.failureCode);
}

/**
 * 恢复中断的处理：
 *  1. 超过 staleAfterMs 仍处于 running 的尝试标记 lost，释放媒体行占用；
 *  2. 媒体处于 processing/scanning 但从未被认领（current_attempt_id 为空，
 *     例如队列丢任务）也一并返回重新入队。
 */
export async function recoverLostAttempts(staleAfterMs: number): Promise<Array<{ mediaId: string; attemptId: string | null }>> {
  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    const lost = await client.query<{ media_id: string; attempt_id: string }>(
      `WITH stale AS (
         UPDATE media_processing_attempts
            SET status = 'lost',
                error_code = COALESCE(error_code, 'Recovered after worker interruption'),
                finished_at = now(),
                updated_at = now()
          WHERE status = 'running'
            AND started_at < now() - ($1::bigint * interval '1 millisecond')
          RETURNING id AS attempt_id, media_id
       )
       UPDATE media_assets ma
          SET privacy_status = 'processing',
              failure_code = COALESCE(failure_code, 'Recovered after worker interruption'),
              current_attempt_id = NULL,
              updated_at = now()
         FROM stale s
        WHERE ma.id = s.media_id AND ma.deleted_at IS NULL
      RETURNING s.media_id, s.attempt_id`,
      [String(staleAfterMs)]
    );

    const unclaimed = await client.query<{ media_id: string }>(
      `UPDATE media_assets
         SET failure_code = COALESCE(failure_code, 'Recovered: processing never claimed'),
             updated_at = now()
       WHERE privacy_status IN ('processing', 'scanning')
         AND current_attempt_id IS NULL
         AND deleted_at IS NULL
         AND updated_at < now() - ($1::bigint * interval '1 millisecond')
       RETURNING id AS media_id`,
      [String(staleAfterMs)]
    );
    await client.query("COMMIT");

    const byMedia = new Map<string, { mediaId: string; attemptId: string | null }>();
    for (const row of lost.rows) {
      byMedia.set(row.media_id, { mediaId: row.media_id, attemptId: row.attempt_id });
    }
    for (const row of unclaimed.rows) {
      byMedia.set(row.media_id, { mediaId: row.media_id, attemptId: null });
    }
    return [...byMedia.values()];
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  } finally {
    client.release();
  }
}
