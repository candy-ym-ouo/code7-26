import { describe, expect, it } from "vitest";
import {
  decideMediaObjectAction,
  MEDIA_OBJECT_COLUMNS,
  parseMediaObjectKey,
  type ReconcileMediaRow
} from "./media-objects";

const MEDIA_ID = "0b6f2a6e-4c3d-4e5f-8a9b-1c2d3e4f5a6b";
const OWNER_ID = "7c8e297e-c0b1-4f1e-9a2b-3c4d5e6f7a8b";

function mediaRow(overrides: Partial<ReconcileMediaRow> = {}): ReconcileMediaRow {
  return {
    id: MEDIA_ID,
    privacy_status: "processing",
    deleted_at: null,
    quarantine_object_key: `quarantine/${OWNER_ID}/${MEDIA_ID}.jpg`,
    processed_object_key: null,
    thumbnail_object_key: null,
    public_object_key: null,
    public_thumbnail_object_key: null,
    ...overrides
  };
}

describe("parseMediaObjectKey", () => {
  it("parses quarantine original keys with owner and media id", () => {
    expect(parseMediaObjectKey(`quarantine/${OWNER_ID}/${MEDIA_ID}.jpg`)).toEqual({
      kind: "quarantine",
      mediaId: MEDIA_ID,
      ownerId: OWNER_ID
    });
    expect(parseMediaObjectKey(`quarantine/${OWNER_ID}/${MEDIA_ID}.png`)?.kind).toBe("quarantine");
    expect(parseMediaObjectKey(`quarantine/${OWNER_ID}/${MEDIA_ID}.webp`)?.kind).toBe("quarantine");
  });

  it("parses processed and public derivative keys", () => {
    expect(parseMediaObjectKey(`processed/${MEDIA_ID}.webp`)).toEqual({ kind: "processed", mediaId: MEDIA_ID });
    expect(parseMediaObjectKey(`processed/${MEDIA_ID}.thumb.webp`)).toEqual({ kind: "processed_thumbnail", mediaId: MEDIA_ID });
    expect(parseMediaObjectKey(`media/${MEDIA_ID}.webp`)).toEqual({ kind: "public", mediaId: MEDIA_ID });
    expect(parseMediaObjectKey(`media/${MEDIA_ID}.thumb.webp`)).toEqual({ kind: "public_thumbnail", mediaId: MEDIA_ID });
  });

  it("normalizes uppercase UUIDs", () => {
    const key = `processed/${MEDIA_ID.toUpperCase()}.webp`;
    expect(parseMediaObjectKey(key)?.mediaId).toBe(MEDIA_ID);
  });

  it("rejects keys that cannot be attributed", () => {
    for (const key of [
      `deleted/${MEDIA_ID}.object`,
      `processed/${MEDIA_ID}.png`,
      `processed/not-a-uuid.webp`,
      `processed/${MEDIA_ID}.webp.bak`,
      `quarantine/${MEDIA_ID}.jpg`,
      `media/${MEDIA_ID}`,
      "random/file.webp",
      ""
    ]) {
      expect(parseMediaObjectKey(key)).toBeNull();
    }
  });
});

describe("decideMediaObjectAction", () => {
  const processedKey = `processed/${MEDIA_ID}.webp`;
  const publicKey = `media/${MEDIA_ID}.webp`;
  const publicThumbnailKey = `media/${MEDIA_ID}.thumb.webp`;

  it("skips objects still referenced by their media row", () => {
    const media = mediaRow({ processed_object_key: processedKey });
    const decision = decideMediaObjectAction({
      key: processedKey,
      parsed: parseMediaObjectKey(processedKey),
      media,
      withinGrace: false
    });
    expect(decision).toEqual({ action: "skip", reason: "referenced" });
  });

  it("skips referenced objects even when the media row is deleted", () => {
    const media = mediaRow({ processed_object_key: processedKey, deleted_at: new Date() });
    const decision = decideMediaObjectAction({
      key: processedKey,
      parsed: parseMediaObjectKey(processedKey),
      media,
      withinGrace: false
    });
    expect(decision).toEqual({ action: "skip", reason: "referenced" });
  });

  it("defers unreferenced objects inside the grace window", () => {
    const decision = decideMediaObjectAction({
      key: processedKey,
      parsed: parseMediaObjectKey(processedKey),
      media: mediaRow(),
      withinGrace: true
    });
    expect(decision).toEqual({ action: "skip", reason: "grace" });
  });

  it("purges keys that cannot be attributed to any media", () => {
    const decision = decideMediaObjectAction({
      key: "random/garbage.bin",
      parsed: null,
      media: null,
      withinGrace: false
    });
    expect(decision).toEqual({ action: "purge", reason: "unattributable_key" });
  });

  it("purges objects whose media row no longer exists", () => {
    const decision = decideMediaObjectAction({
      key: processedKey,
      parsed: parseMediaObjectKey(processedKey),
      media: null,
      withinGrace: false
    });
    expect(decision).toEqual({ action: "purge", reason: "media_row_missing" });
  });

  it("purges unreferenced objects of deleted media", () => {
    const decision = decideMediaObjectAction({
      key: processedKey,
      parsed: parseMediaObjectKey(processedKey),
      media: mediaRow({ deleted_at: new Date() }),
      withinGrace: false
    });
    expect(decision).toEqual({ action: "purge", reason: "media_deleted" });
  });

  it("purges objects superseded by a different reference", () => {
    const media = mediaRow({ processed_object_key: `processed/${MEDIA_ID}.other.webp` });
    const decision = decideMediaObjectAction({
      key: processedKey,
      parsed: parseMediaObjectKey(processedKey),
      media,
      withinGrace: false
    });
    expect(decision).toEqual({ action: "purge", reason: "superseded" });
  });

  it("adopts interrupted processed derivatives back into the media row", () => {
    const decision = decideMediaObjectAction({
      key: processedKey,
      parsed: parseMediaObjectKey(processedKey),
      media: mediaRow(),
      withinGrace: false
    });
    expect(decision).toEqual({ action: "adopt", column: MEDIA_OBJECT_COLUMNS.processed });
  });

  it("rebuilds missing public references for ready media", () => {
    const media = mediaRow({ privacy_status: "ready", public_object_key: publicKey });
    const decision = decideMediaObjectAction({
      key: publicThumbnailKey,
      parsed: parseMediaObjectKey(publicThumbnailKey),
      media,
      withinGrace: false
    });
    expect(decision).toEqual({ action: "adopt", column: MEDIA_OBJECT_COLUMNS.public_thumbnail });
  });

  it("purges public objects whose media is not privacy-confirmed", () => {
    for (const status of ["quarantined", "scanning", "processing", "manual_review", "failed", "rejected"]) {
      const decision = decideMediaObjectAction({
        key: publicKey,
        parsed: parseMediaObjectKey(publicKey),
        media: mediaRow({ privacy_status: status }),
        withinGrace: false
      });
      expect(decision).toEqual({ action: "purge", reason: "public_without_ready" });
    }
  });
});
