import { pool } from "./db";

export const MEDIA_OBJECT_EVENTS = ["write", "rewrite", "delete", "adopt", "purge"] as const;
export type MediaObjectEventType = (typeof MEDIA_OBJECT_EVENTS)[number];

export async function recordMediaObjectEvent(input: {
  mediaId: string | null;
  bucket: string;
  objectKey: string;
  event: MediaObjectEventType;
  actor: "api" | "worker" | "maintenance" | "reconcile";
  byteSize?: number | null;
  metadata?: Record<string, unknown>;
}): Promise<void> {
  await pool.query(
    `INSERT INTO media_object_events(media_id, bucket, object_key, event, actor, byte_size, metadata)
     VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb)`,
    [
      input.mediaId,
      input.bucket,
      input.objectKey,
      input.event,
      input.actor,
      input.byteSize ?? null,
      JSON.stringify(input.metadata ?? {})
    ]
  );
}
