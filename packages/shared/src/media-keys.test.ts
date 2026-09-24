import { describe, expect, it } from "vitest";
import {
  isUuid,
  parseMediaObjectKey,
  processedImageKey,
  processedThumbnailKey,
  publicImageKey,
  publicThumbnailKey,
  quarantineOriginalKey
} from "./media-keys";

const mediaId = "11111111-2222-3333-4444-555555555555";
const ownerId = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee";
const attemptId = "99999999-8888-7777-6666-555555555555";

describe("media object key conventions", () => {
  it("builds deterministic, non-colliding keys per attempt", () => {
    expect(quarantineOriginalKey(ownerId, mediaId, "jpg")).toBe(`quarantine/${ownerId}/${mediaId}.jpg`);
    expect(processedImageKey(mediaId, attemptId)).toBe(`processed/${mediaId}/${attemptId}.webp`);
    expect(processedThumbnailKey(mediaId, attemptId)).toBe(`processed/${mediaId}/${attemptId}.thumb.webp`);
    expect(publicImageKey(mediaId)).toBe(`media/${mediaId}.webp`);
    expect(publicThumbnailKey(mediaId)).toBe(`media/${mediaId}.thumb.webp`);
  });

  it("round-trips attribution for current-layout keys", () => {
    const keys = [
      { key: quarantineOriginalKey(ownerId, mediaId, "png"), role: "quarantine_original", attempt: null },
      { key: processedImageKey(mediaId, attemptId), role: "processed_image", attempt: attemptId },
      { key: processedThumbnailKey(mediaId, attemptId), role: "processed_thumbnail", attempt: attemptId },
      { key: publicImageKey(mediaId), role: "public_image", attempt: null },
      { key: publicThumbnailKey(mediaId), role: "public_thumbnail", attempt: null }
    ] as const;
    for (const item of keys) {
      const parsed = parseMediaObjectKey(item.key);
      expect(parsed.mediaId).toBe(mediaId);
      expect(parsed.role).toBe(item.role);
      expect(parsed.attemptId).toBe(item.attempt);
      expect(parsed.legacy).toBe(false);
    }
  });

  it("recognizes pre-migration legacy processed keys and flags them", () => {
    const legacy = parseMediaObjectKey(`processed/${mediaId}.webp`);
    expect(legacy).toMatchObject({ role: "processed_image", mediaId, attemptId: null, legacy: true });
    const legacyThumb = parseMediaObjectKey(`processed/${mediaId}.thumb.webp`);
    expect(legacyThumb).toMatchObject({ role: "processed_thumbnail", mediaId, legacy: true });
  });

  it("returns unknown attribution for unrelated objects", () => {
    expect(parseMediaObjectKey("avatars/abc.png")).toEqual({ role: "unknown", mediaId: null, attemptId: null, legacy: false });
    expect(parseMediaObjectKey(`processed/not-a-uuid.webp`)).toMatchObject({ role: "unknown" });
    expect(parseMediaObjectKey(`quarantine/${ownerId}/${mediaId}`)).toMatchObject({ role: "unknown" });
  });

  it("accepts uppercase UUIDs but normalizes to lowercase", () => {
    const parsed = parseMediaObjectKey(`media/${mediaId.toUpperCase()}.webp`);
    expect(parsed.mediaId).toBe(mediaId);
    expect(isUuid(mediaId)).toBe(true);
    expect(isUuid("nope")).toBe(false);
  });
});
