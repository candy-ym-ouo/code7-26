import { randomUUID } from "node:crypto";
import type { FastifyInstance } from "fastify";
import { z } from "zod";
import { mediaUploadCompleteSchema, mediaUploadInitSchema } from "@map/shared/contracts";
import {
  publicImageKey,
  publicThumbnailKey,
  quarantineOriginalKey
} from "@map/shared/media-keys";
import { config } from "../config";
import { pool, query, transaction } from "../db";
import { AppError, conflict, forbidden, notFound } from "../errors";
import { requireAuth, requireModerator, requireVerifiedContributor } from "../auth";
import {
  createPreviewUrl,
  createUploadUrl,
  deleteObject,
  getQuarantineMetadata,
  publishMediaObject,
  publicMediaUrl
} from "../storage";
import { enqueueMediaProcessing } from "../queue";
import { recordAudit } from "../audit";
import {
  deleteLedgerObjectsForMedia,
  markLedgerObjectDeleted,
  markLedgerObjectPendingDeletion,
  recordMediaObject
} from "../ledger";

function extensionForMime(mime: string) {
  if (mime === "image/jpeg") return "jpg";
  if (mime === "image/png") return "png";
  return "webp";
}

function mediaResponse(row: {
  id: string;
  privacy_status: string;
  public_object_key: string | null;
  public_thumbnail_object_key: string | null;
  privacy_report: unknown;
  failure_code: string | null;
  created_at: Date;
  processed_at: Date | null;
}) {
  return {
    id: row.id,
    status: row.privacy_status,
    url: row.privacy_status === "ready" ? publicMediaUrl(row.public_object_key) : null,
    thumbnailUrl: row.privacy_status === "ready" ? publicMediaUrl(row.public_thumbnail_object_key) : null,
    privacyReport: row.privacy_report,
    failureCode: row.failure_code,
    createdAt: row.created_at,
    processedAt: row.processed_at
  };
}

export async function mediaRoutes(app: FastifyInstance) {
  app.post("/media/uploads", { preHandler: requireVerifiedContributor }, async (request, reply) => {
    const input = mediaUploadInitSchema.parse(request.body);
    if (input.byteSize > config.MEDIA_MAX_BYTES) {
      throw new AppError(400, "VALIDATION_FAILED", `File exceeds ${config.MEDIA_MAX_BYTES} bytes`);
    }
    const id = randomUUID();
    const extension = extensionForMime(input.mimeType);
    const key = quarantineOriginalKey(request.user!.id, id, extension);
    await query(
      `INSERT INTO media_assets(id, owner_id, original_filename, mime_type, byte_size, quarantine_object_key)
       VALUES ($1, $2, $3, $4, $5, $6)`,
      [id, request.user!.id, input.filename, input.mimeType, input.byteSize, key]
    );
    // 原图由浏览器直传；先登记预期对象，complete 时会用 HEAD 校验它确实存在。
    await recordMediaObject(pool, {
      mediaId: id,
      bucket: config.S3_QUARANTINE_BUCKET,
      objectKey: key,
      role: "quarantine_original",
      note: "browser direct upload (pending object confirmation)"
    });
    const uploadUrl = await createUploadUrl(key, input.mimeType);
    return reply.code(201).send({ id, uploadUrl, expiresInSeconds: 600 });
  });

  app.post("/media/uploads/:id/complete", { preHandler: requireVerifiedContributor }, async (request) => {
    const params = z.object({ id: z.string().uuid() }).parse(request.params);
    const input = mediaUploadCompleteSchema.parse(request.body);
    const result = await query<{
      id: string;
      owner_id: string;
      byte_size: string;
      mime_type: string;
      quarantine_object_key: string;
      privacy_status: string;
    }>(
      `SELECT id, owner_id, byte_size, mime_type, quarantine_object_key, privacy_status
       FROM media_assets WHERE id = $1 AND deleted_at IS NULL`,
      [params.id]
    );
    const media = result.rows[0];
    if (!media) throw notFound("Media not found");
    if (media.owner_id !== request.user!.id) throw forbidden();
    if (media.privacy_status !== "quarantined") throw conflict("Media upload was already completed");

    let metadata;
    try {
      metadata = await getQuarantineMetadata(media.quarantine_object_key);
    } catch {
      throw new AppError(409, "CONFLICT", "Uploaded object was not found in quarantine storage");
    }
    const actualBytes = Number(metadata.ContentLength ?? 0);
    const actualContentType = metadata.ContentType?.split(";")[0]?.trim();
    if (!actualBytes || actualBytes > config.MEDIA_MAX_BYTES || actualBytes !== Number(media.byte_size)) {
      throw new AppError(400, "VALIDATION_FAILED", "Uploaded object size does not match the declared size");
    }
    if (actualContentType && actualContentType !== media.mime_type) {
      throw new AppError(400, "VALIDATION_FAILED", "Uploaded object content type does not match the declared type");
    }

    await transaction(async (client) => {
      await client.query(
        `UPDATE media_assets
         SET privacy_status = 'processing',
             privacy_report = $2::jsonb,
             failure_code = NULL,
             updated_at = now()
         WHERE id = $1`,
        [params.id, JSON.stringify({
          manualRegions: input.privacyRegions,
          containsPeopleOrPlates: input.containsPeopleOrPlates,
          rightsConfirmedAt: new Date().toISOString(),
          detector: "pending"
        })]
      );
      // 处理尝试行由 worker 认领任务时创建，避免任务从未被消费时留下悬空 running 记录。
      await recordAudit(client, {
        actorId: request.user!.id,
        action: "media.processing_requested",
        resourceType: "media",
        resourceId: params.id,
        metadata: { regionCount: input.privacyRegions.length }
      });
    });

    try {
      await enqueueMediaProcessing(
        { mediaId: params.id, trigger: "initial", queuedBy: request.user!.id },
        `media-${params.id}`
      );
    } catch (error) {
      await query(
        "UPDATE media_assets SET privacy_status = 'failed', failure_code = 'QUEUE_UNAVAILABLE', updated_at = now() WHERE id = $1",
        [params.id]
      );
      throw new AppError(503, "QUEUE_UNAVAILABLE", "Media processing queue is unavailable. Retry later.");
    }
    return { status: "processing" };
  });

  app.get("/media/:id", { preHandler: requireAuth }, async (request) => {
    const params = z.object({ id: z.string().uuid() }).parse(request.params);
    const result = await query<{
      id: string; owner_id: string; privacy_status: string; public_object_key: string | null;
      public_thumbnail_object_key: string | null; privacy_report: unknown; failure_code: string | null;
      created_at: Date; processed_at: Date | null;
    }>(
      `SELECT id, owner_id, privacy_status, public_object_key, public_thumbnail_object_key,
              privacy_report, failure_code, created_at, processed_at
       FROM media_assets WHERE id = $1 AND deleted_at IS NULL`,
      [params.id]
    );
    const row = result.rows[0];
    if (!row) throw notFound("Media not found");
    if (row.owner_id !== request.user!.id && !["moderator", "admin"].includes(request.user!.role)) throw forbidden();
    return mediaResponse(row);
  });

  app.post("/media/:id/retry", { preHandler: requireVerifiedContributor }, async (request) => {
    const params = z.object({ id: z.string().uuid() }).parse(request.params);
    const result = await query<{ owner_id: string; privacy_status: string }>(
      "SELECT owner_id, privacy_status FROM media_assets WHERE id = $1 AND deleted_at IS NULL",
      [params.id]
    );
    const row = result.rows[0];
    if (!row) throw notFound("Media not found");
    if (row.owner_id !== request.user!.id && !["moderator", "admin"].includes(request.user!.role)) throw forbidden();
    if (!["failed", "rejected"].includes(row.privacy_status)) throw conflict("Only failed media can be retried");

    // 重试只翻转状态并重新入队；尝试历史由 worker 认领时按 trigger='retry' 落库，
    // 新尝试产物使用独立对象键，不会覆盖旧产物，台账可完整追溯每次重试。
    const updated = await query(
      `UPDATE media_assets SET privacy_status = 'processing', failure_code = NULL, updated_at = now()
       WHERE id = $1 AND privacy_status IN ('failed', 'rejected') AND deleted_at IS NULL
       RETURNING id`,
      [params.id]
    );
    if (!updated.rows[0]) throw conflict("Only failed media can be retried");
    try {
      await enqueueMediaProcessing(
        { mediaId: params.id, trigger: "retry", queuedBy: request.user!.id },
        `media-${params.id}-${Date.now()}`
      );
    } catch (error) {
      await query(
        "UPDATE media_assets SET privacy_status = 'failed', failure_code = 'QUEUE_UNAVAILABLE', updated_at = now() WHERE id = $1",
        [params.id]
      );
      throw new AppError(503, "QUEUE_UNAVAILABLE", "Media processing queue is unavailable. Retry later.");
    }
    return { status: "processing" };
  });

  app.get("/media/:id/preview", { preHandler: requireModerator }, async (request) => {
    const params = z.object({ id: z.string().uuid() }).parse(request.params);
    const result = await query<{ processed_object_key: string | null; privacy_status: string }>(
      "SELECT processed_object_key, privacy_status FROM media_assets WHERE id = $1 AND deleted_at IS NULL",
      [params.id]
    );
    const media = result.rows[0];
    if (!media) throw notFound("Media not found");
    if (!media.processed_object_key) throw conflict("Processed preview is not available");
    await query(
      `INSERT INTO audit_logs(actor_id, action, resource_type, resource_id, metadata)
       VALUES ($1, 'media.preview_viewed', 'media', $2, '{}'::jsonb)`,
      [request.user!.id, params.id]
    );
    return {
      status: media.privacy_status,
      url: await createPreviewUrl(media.processed_object_key),
      expiresInSeconds: 600
    };
  });

  app.post("/media/:id/privacy-approve", { preHandler: requireModerator }, async (request) => {
    const params = z.object({ id: z.string().uuid() }).parse(request.params);
    const result = await query<{
      id: string;
      privacy_status: string;
      processed_object_key: string | null;
      thumbnail_object_key: string | null;
      public_object_key: string | null;
      public_thumbnail_object_key: string | null;
    }>(
      `SELECT id, privacy_status, processed_object_key, thumbnail_object_key,
              public_object_key, public_thumbnail_object_key
       FROM media_assets WHERE id = $1 AND deleted_at IS NULL`,
      [params.id]
    );
    const media = result.rows[0];
    if (!media) throw notFound("Media not found");
    if (media.privacy_status !== "manual_review" || !media.processed_object_key) {
      throw conflict("Media is not waiting for manual privacy approval");
    }

    const publicKey = publicImageKey(params.id);
    const thumbnailKey = publicThumbnailKey(params.id);
    try {
      await publishMediaObject(media.processed_object_key, publicKey);
      await recordMediaObject(pool, {
        mediaId: params.id,
        bucket: config.S3_PUBLIC_BUCKET,
        objectKey: publicKey,
        role: "public_image",
        note: "published after privacy review"
      });
      if (media.thumbnail_object_key) {
        await publishMediaObject(media.thumbnail_object_key, thumbnailKey);
        await recordMediaObject(pool, {
          mediaId: params.id,
          bucket: config.S3_PUBLIC_BUCKET,
          objectKey: thumbnailKey,
          role: "public_thumbnail",
          note: "published after privacy review"
        });
      }

      await transaction(async (client) => {
        const statusResult = await client.query<{ privacy_status: string }>(
          "SELECT privacy_status FROM media_assets WHERE id = $1 FOR UPDATE",
          [params.id]
        );
        if (statusResult.rows[0]?.privacy_status !== "manual_review") {
          throw conflict("Media is not waiting for manual privacy approval");
        }
        await client.query(
          `UPDATE media_assets
           SET privacy_status = 'ready', public_object_key = $2,
               public_thumbnail_object_key = $3, processed_at = now(), updated_at = now()
           WHERE id = $1`,
          [params.id, publicKey, media.thumbnail_object_key ? thumbnailKey : null]
        );
        await recordAudit(client, {
          actorId: request.user!.id,
          action: "media.privacy_approved",
          resourceType: "media",
          resourceId: params.id
        });
      });
    } catch (error) {
      // 发布失败：公开桶中可能已有副本。按隐私优先立即尝试物理删除；删除失败则把
      // 对象置为短宽限删除（worker 对账会在 15 分钟内强制清理），绝不只落墓碑而放任对象滞留。
      await Promise.allSettled(
        [
          { key: publicKey, role: "public_image" as const },
          ...(media.thumbnail_object_key ? [{ key: thumbnailKey, role: "public_thumbnail" as const }] : [])
        ].map(async ({ key, role }) => {
          try {
            await deleteObject(config.S3_PUBLIC_BUCKET, key);
            await markLedgerObjectDeleted(pool, config.S3_PUBLIC_BUCKET, key, {
              mediaId: params.id,
              role,
              note: "removed after aborted privacy approval"
            });
          } catch (deleteError) {
            console.error({ mediaId: params.id, key, deleteError }, "failed to remove public object after aborted approval; short grace deletion scheduled");
            await markLedgerObjectPendingDeletion(
              pool,
              config.S3_PUBLIC_BUCKET,
              key,
              new Date(Date.now() + 15 * 60 * 1000),
              { mediaId: params.id, role, note: "privacy approval aborted and delete failed" }
            );
          }
        })
      );
      throw error;
    }

    return { status: "ready", url: publicMediaUrl(publicKey), thumbnailUrl: media.thumbnail_object_key ? publicMediaUrl(thumbnailKey) : null };
  });

  app.delete("/media/:id", { preHandler: requireAuth }, async (request) => {
    const params = z.object({ id: z.string().uuid() }).parse(request.params);
    const result = await query<{
      id: string;
      owner_id: string;
    }>(
      `SELECT id, owner_id
       FROM media_assets WHERE id = $1 AND deleted_at IS NULL`,
      [params.id]
    );
    const media = result.rows[0];
    if (!media) throw notFound("Media not found");
    if (media.owner_id !== request.user!.id && !["moderator", "admin"].includes(request.user!.role)) throw forbidden();

    const publishedReference = await query<{ exists: boolean }>(
      `SELECT EXISTS (
         SELECT 1
         FROM revision_media rm
         JOIN map_features mf ON mf.current_revision_id = rm.revision_id
         WHERE rm.media_id = $1
           AND mf.status = 'published'
           AND mf.deleted_at IS NULL
       ) AS exists`,
      [params.id]
    );
    if (publishedReference.rows[0]?.exists) {
      throw conflict("Media attached to published content cannot be deleted separately");
    }

    await transaction(async (client) => {
      await client.query("UPDATE media_assets SET privacy_status = 'deleted', deleted_at = now(), updated_at = now() WHERE id = $1", [params.id]);
      await recordAudit(client, {
        actorId: request.user!.id,
        action: "media.deleted",
        resourceType: "media",
        resourceId: params.id
      });
    });

    // 台账驱动删除：除当前引用的对象外，中断尝试写入的半成品/历史产物也会被清掉。
    const { failed } = await deleteLedgerObjectsForMedia(params.id, { origin: "pipeline" });
    if (failed.length) {
      console.error({ mediaId: params.id, failed }, "some media objects failed to delete; reconcile will retry");
    }
    return { status: "deleted" };
  });
}

