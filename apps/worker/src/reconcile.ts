import { parseMediaObjectKey, type LedgerObjectRole } from "@map/shared/media-keys";
import { pool } from "./db";
import {
  deleteLedgerObject,
  markObjectDeleted,
  markObjectPendingDeletion,
  recordObjectWritten,
  touchObjectSeen
} from "./ledger";
import { listObjects, type ListedObject } from "./storage";

type StorageExec = {
  remove: (bucket: string, key: string) => Promise<unknown>;
  exists: (bucket: string, key: string) => Promise<boolean>;
};

export type ReconcileOptions = StorageExec & {
  quarantineBucket: string;
  publicBucket: string;
  batchSize: number;
  orphanQuarantineGraceHours: number;
  orphanPublicGraceMinutes: number;
};

export type MediaReferenceRow = {
  media_id: string;
  privacy_status: string;
  deleted_at: Date | null;
  quarantine_object_key: string;
  processed_object_key: string | null;
  thumbnail_object_key: string | null;
  public_object_key: string | null;
  public_thumbnail_object_key: string | null;
};

type BucketKind = "quarantine" | "public";

/**
 * 单个对象的对账判定。纯函数以便单测覆盖各种存量孤儿形态。
 *
 * 重建引用只在"媒体处于该角色对应的稳定终态、且数据库恰好缺失该键"时发生：
 * 这意味着媒体已提交但引用在崩溃/历史数据中丢失，补回引用是安全的。
 * 半成品（processing/failed/scanning 等）一律按孤儿清理，绝不复活。
 */
export type ReconcileDecision =
  | { action: "delete_after"; role: LedgerObjectRole; mediaId: string | null; graceMinutes: number; reason: string }
  | { action: "rebuild_reference"; role: LedgerObjectRole; mediaId: string; column: string }
  | { action: "already_tracked" };

const REFERENCE_COLUMN: Partial<Record<LedgerObjectRole, string>> = {
  quarantine_original: "quarantine_object_key",
  processed_image: "processed_object_key",
  processed_thumbnail: "thumbnail_object_key",
  public_image: "public_object_key",
  public_thumbnail: "public_thumbnail_object_key"
};

export function decideObjectFate(input: {
  bucket: BucketKind;
  objectKey: string;
  media: MediaReferenceRow | null;
  isTracked: boolean;
  publicGraceMinutes: number;
  quarantineGraceHours: number;
}): ReconcileDecision {
  const attribution = parseMediaObjectKey(input.objectKey);
  if (input.isTracked) return { action: "already_tracked" };

  if (!input.media || input.media.deleted_at) {
    // 无法归属到存活媒体：公开桶按隐私优先短宽限删除；私有桶给人工追溯留宽限期。
    return {
      action: "delete_after",
      role: attribution.role,
      mediaId: attribution.mediaId,
      graceMinutes: input.bucket === "public"
        ? input.publicGraceMinutes
        : input.quarantineGraceHours * 60,
      reason: input.media ? "media record deleted" : "no media record owns this object key"
    };
  }

  // 媒体存活但当前未引用该键。只对稳定终态做引用重建。
  const column = REFERENCE_COLUMN[attribution.role];
  const stableStatuses: Record<LedgerObjectRole, string[]> = {
    quarantine_original: ["quarantined", "processing", "scanning", "manual_review", "ready", "rejected", "failed"],
    processed_image: ["manual_review", "ready"],
    processed_thumbnail: ["manual_review", "ready"],
    public_image: ["ready"],
    public_thumbnail: ["ready"],
    unknown: []
  };
  if (
    attribution.mediaId === input.media.media_id &&
    column &&
    stableStatuses[attribution.role]?.includes(input.media.privacy_status)
  ) {
    const currentValue = input.media[column as keyof MediaReferenceRow] as string | null;
    if (currentValue === null) {
      return { action: "rebuild_reference", role: attribution.role, mediaId: input.media.media_id, column };
    }
    if (currentValue !== input.objectKey) {
      // 媒体引用了另一个键：本键是旧尝试残留/孤儿，而不是丢失的引用。
      return {
        action: "delete_after",
        role: attribution.role,
        mediaId: input.media.media_id,
        graceMinutes: input.bucket === "public" ? input.publicGraceMinutes : 60,
        reason: "media references a different object key"
      };
    }
  }

  // 键中的媒体 ID 与存活媒体不匹配，或媒体处于非稳定态（半成品）。
  return {
    action: "delete_after",
    role: attribution.role,
    mediaId: attribution.mediaId,
    graceMinutes: input.bucket === "public" ? input.publicGraceMinutes : 60,
    reason: !attribution.mediaId
      ? "unparseable legacy object key"
      : attribution.mediaId !== input.media.media_id
        ? "object key media id mismatch"
        : "media not in a stable state for this object role"
  };
}

async function fetchCursor(kind: BucketKind): Promise<string | null> {
  const result = await pool.query<{ cursor: string | null }>(
    `SELECT cursor FROM media_reconcile_cursors WHERE bucket_kind = $1`,
    [kind]
  );
  return result.rows[0]?.cursor ?? null;
}

async function storeCursor(kind: BucketKind, cursor: string | null): Promise<void> {
  await pool.query(
    `INSERT INTO media_reconcile_cursors(bucket_kind, cursor, updated_at)
     VALUES ($1, $2, now())
     ON CONFLICT (bucket_kind) DO UPDATE SET cursor = EXCLUDED.cursor, updated_at = now()`,
    [kind, cursor]
  );
}

async function fetchMediaRows(mediaIds: string[]): Promise<Map<string, MediaReferenceRow>> {
  if (!mediaIds.length) return new Map();
  const result = await pool.query<MediaReferenceRow>(
    `SELECT id AS media_id, privacy_status, deleted_at, quarantine_object_key,
            processed_object_key, thumbnail_object_key,
            public_object_key, public_thumbnail_object_key
     FROM media_assets WHERE id = ANY($1::uuid[])`,
    [mediaIds]
  );
  return new Map(result.rows.map((row) => [row.media_id, row]));
}

/**
 * 台账中所有非墓碑对象的当前状态。宽限中的（superseded/delete_pending）对象本轮
 * 只刷新 last_seen、不再重新判定，避免每轮扫描把 delete_after 宽限期无限推后。
 */
async function ledgerStates(bucket: string, keys: string[]): Promise<Map<string, string>> {
  if (!keys.length) return new Map();
  const result = await pool.query<{ object_key: string; state: string }>(
    `SELECT object_key, state FROM media_object_ledger
     WHERE bucket = $1 AND object_key = ANY($2::text[]) AND state <> 'deleted'`,
    [bucket, keys]
  );
  return new Map(result.rows.map((row) => [row.object_key, row.state]));
}

export async function reconcileObjectStore(options: ReconcileOptions): Promise<{ scanned: number; decisions: number }> {
  let scanned = 0;
  let decisions = 0;
  for (const kind of ["quarantine", "public"] as BucketKind[]) {
    const bucket = kind === "quarantine" ? options.quarantineBucket : options.publicBucket;
    let continuation = await fetchCursor(kind);
    const { objects, nextContinuationToken } = await listObjects(bucket, continuation, { maxKeys: options.batchSize });

    if (objects.length === 0) {
      await storeCursor(kind, null);
      await markAbsentLedgerObjects(bucket, options);
      continue;
    }
    scanned += objects.length;

    const keys = objects.map((item) => item.key);
    const parsedIds = keys
      .map((key) => parseMediaObjectKey(key).mediaId)
      .filter((id): id is string => Boolean(id));
    const [mediaMap, states] = await Promise.all([
      fetchMediaRows([...new Set(parsedIds)]),
      ledgerStates(bucket, keys)
    ]);

    for (const object of objects) {
      const attribution = parseMediaObjectKey(object.key);
      const media = attribution.mediaId ? mediaMap.get(attribution.mediaId) ?? null : null;
      // 台账中已有非墓碑记录（committed/superseded/delete_pending）的对象不再重复判定，
      // 宽限到期由 deletePendingLedgerObjects 统一收口。
      const ledgerState = states.get(object.key);
      const decision: ReconcileDecision = ledgerState
        ? { action: "already_tracked" }
        : decideObjectFate({
            bucket: kind,
            objectKey: object.key,
            media,
            isTracked: false,
            publicGraceMinutes: options.orphanPublicGraceMinutes,
            quarantineGraceHours: options.orphanQuarantineGraceHours
          });
      await applyDecision(kind, bucket, object, decision, options);
      const seenRole =
        decision.action === "already_tracked"
          ? attribution.role === "unknown"
            ? undefined
            : attribution.role
          : decision.role;
      await touchObjectSeen(pool, bucket, object.key, { mediaId: attribution.mediaId, role: seenRole });
      if (decision.action !== "already_tracked") decisions += 1;
    }

    if (nextContinuationToken) {
      // 本桶还有后续页：记录游标，下一轮继续（每个 tick 每桶只扫一批，限制负载）。
      await storeCursor(kind, nextContinuationToken);
      continue;
    }

    // 本桶最后一页扫完：清空游标下轮从头开始，并检测台账中已从桶里消失的对象。
    await storeCursor(kind, null);
    await markAbsentLedgerObjects(bucket, options);
  }
  return { scanned, decisions };
}

async function applyDecision(
  kind: BucketKind,
  bucket: string,
  object: ListedObject,
  decision: ReconcileDecision,
  options: ReconcileOptions
): Promise<void> {
  if (decision.action === "already_tracked") return;

  if (decision.action === "delete_after") {
    // 宽限期从本次发现（对象首次进入台账）起算，给人工追溯/引用重建留出固定时间窗。
    await markObjectPendingDeletion(
      pool,
      bucket,
      object.key,
      new Date(Date.now() + decision.graceMinutes * 60 * 1000),
      {
        state: "delete_pending",
        mediaId: decision.mediaId,
        role: decision.role,
        origin: "reconcile",
        note: decision.reason
      }
    );
    return;
  }

  if (decision.action === "rebuild_reference") {
    // 引用重建：媒体处于稳定终态、列却为空。先登记台账，再用 guarded UPDATE 补列，
    // 与并发的删除/重新处理互斥（只有列仍为 NULL 时才补）。
    await recordObjectWritten(pool, {
      mediaId: decision.mediaId,
      bucket,
      objectKey: object.key,
      role: decision.role,
      origin: "reconcile",
      note: "reference rebuilt from object store"
    });
    const result = await pool.query(
      `UPDATE media_assets
         SET ${decision.column} = $2, updated_at = now()
       WHERE id = $1 AND deleted_at IS NULL AND ${decision.column} IS NULL`,
      [decision.mediaId, object.key]
    );
    if (result.rowCount === 0) {
      // 竞态失败：列刚被别的流程填上/媒体被删，本键回到孤儿判定流程。
      await markObjectPendingDeletion(
        pool,
        bucket,
        object.key,
        new Date(Date.now() + (kind === "public" ? options.orphanPublicGraceMinutes : 60) * 60 * 1000),
        {
          state: "delete_pending",
          mediaId: decision.mediaId,
          role: decision.role,
          origin: "reconcile",
          note: "reference rebuild lost a concurrent race"
        }
      );
    }
    console.warn(
      { mediaId: decision.mediaId, bucket, objectKey: object.key, column: decision.column },
      "rebuilt media object reference from object store"
    );
  }
}

/** 台账中存在、但桶里已经观察不到的非墓碑对象：补齐墓碑，防止"引用了不存在对象"。 */
async function markAbsentLedgerObjects(bucket: string, options: StorageExec): Promise<void> {
  const result = await pool.query<{ object_key: string; media_id: string | null; object_role: LedgerObjectRole }>(
    `SELECT object_key, media_id, object_role
     FROM media_object_ledger
     WHERE bucket = $1 AND state <> 'deleted'
       AND (last_seen_at IS NULL OR last_seen_at < now() - interval '10 minutes')
     LIMIT 100`,
    [bucket]
  );
  for (const row of result.rows) {
    try {
      const exists = await options.exists(bucket, row.object_key);
      if (!exists) {
        await markObjectDeleted(pool, bucket, row.object_key, {
          mediaId: row.media_id,
          role: row.object_role,
          origin: "reconcile",
          note: "object absent during full bucket scan"
        });
      }
    } catch (error) {
      console.error({ bucket, objectKey: row.object_key, error }, "failed to probe ledger object");
    }
  }
}

/** 物理删除所有到期的 superseded / delete_pending 台账对象（维护 tick 收口）。 */
export async function deletePendingLedgerObjects(exec: StorageExec): Promise<{ deleted: number; failed: number }> {
  const due = await pool.query<{
    id: string;
    bucket: string;
    object_key: string;
    media_id: string | null;
    object_role: LedgerObjectRole;
  }>(
    `SELECT id, bucket, object_key, media_id, object_role
     FROM media_object_ledger
     WHERE state IN ('superseded', 'delete_pending')
       AND delete_after IS NOT NULL AND delete_after <= now()
     ORDER BY delete_after
     LIMIT 50`
  );
  let deleted = 0;
  let failed = 0;
  for (const row of due.rows) {
    try {
      await deleteLedgerObject(pool, row.bucket, row.object_key, {
        executor: exec,
        mediaId: row.media_id,
        role: row.object_role,
        origin: "reconcile",
        note: "grace period elapsed"
      });
      deleted += 1;
    } catch (error) {
      failed += 1;
      console.error({ ledgerId: row.id, bucket: row.bucket, objectKey: row.object_key, error }, "pending object deletion failed");
    }
  }
  return { deleted, failed };
}
