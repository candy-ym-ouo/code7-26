import { beforeAll, describe, expect, it } from "vitest";

let decideObjectFate: typeof import("./reconcile").decideObjectFate;
type ReconcileDecision = import("./reconcile").ReconcileDecision;
type MediaReferenceRow = import("./reconcile").MediaReferenceRow;

beforeAll(async () => {
  process.env.DATABASE_URL = "postgres://map:map@localhost:5432/map";
  process.env.S3_ENDPOINT = "http://localhost:9000";
  process.env.S3_PUBLIC_ENDPOINT = "http://localhost:9000";
  process.env.S3_ACCESS_KEY = "test";
  process.env.S3_SECRET_KEY = "test";
  process.env.S3_QUARANTINE_BUCKET = "quarantine";
  process.env.S3_PUBLIC_BUCKET = "public";
  process.env.PRIVACY_DETECTOR_URL = "";
  const module = await import("./reconcile");
  decideObjectFate = module.decideObjectFate;
});

const mediaId = "11111111-2222-3333-4444-555555555555";
const attemptId = "99999999-8888-7777-6666-555555555555";

function mediaRow(overrides: Partial<MediaReferenceRow> = {}): MediaReferenceRow {
  return {
    media_id: mediaId,
    privacy_status: "ready",
    deleted_at: null,
    quarantine_object_key: `quarantine/owner/${mediaId}.jpg`,
    processed_object_key: `processed/${mediaId}/${attemptId}.webp`,
    thumbnail_object_key: `processed/${mediaId}/${attemptId}.thumb.webp`,
    public_object_key: `media/${mediaId}.webp`,
    public_thumbnail_object_key: `media/${mediaId}.thumb.webp`,
    ...overrides
  };
}

const options = {
  isTracked: false,
  publicGraceMinutes: 15,
  quarantineGraceHours: 72
} as const;

describe("decideObjectFate", () => {
  it("leaves tracked objects alone", () => {
    const decision = decideObjectFate({
      bucket: "quarantine",
      objectKey: `processed/${mediaId}/${attemptId}.webp`,
      media: mediaRow(),
      ...options,
      isTracked: true
    });
    expect(decision.action).toBe("already_tracked");
  });

  it("rebuilds a missing reference for media in a stable terminal state", () => {
    const decision = decideObjectFate({
      bucket: "public",
      objectKey: `media/${mediaId}.webp`,
      media: mediaRow({ privacy_status: "ready", public_object_key: null }),
      ...options
    });
    expect(decision).toMatchObject({
      action: "rebuild_reference",
      column: "public_object_key",
      mediaId
    } satisfies Partial<ReconcileDecision>);
  });

  it("does not rebuild references for processing half-products; schedules cleanup", () => {
    const decision = decideObjectFate({
      bucket: "quarantine",
      objectKey: `processed/${mediaId}/${attemptId}.webp`,
      media: mediaRow({ privacy_status: "processing", processed_object_key: null }),
      ...options
    });
    expect(decision.action).toBe("delete_after");
    if (decision.action === "delete_after") expect(decision.reason).toContain("stable state");
  });

  it("treats objects with no owning media row as quarantined orphans with a long grace period", () => {
    const decision = decideObjectFate({
      bucket: "quarantine",
      objectKey: `processed/${mediaId}/${attemptId}.webp`,
      media: null,
      ...options
    });
    expect(decision.action).toBe("delete_after");
    if (decision.action === "delete_after") {
      expect(decision.mediaId).toBe(mediaId);
      expect(decision.graceMinutes).toBe(72 * 60);
    }
  });

  it("gives public-bucket orphans a short privacy-first grace period", () => {
    const decision = decideObjectFate({
      bucket: "public",
      objectKey: `media/${mediaId}.webp`,
      media: null,
      ...options
    });
    expect(decision.action).toBe("delete_after");
    if (decision.action === "delete_after") expect(decision.graceMinutes).toBe(15);
  });

  it("flags legacy processed leftovers for deletion when the media references a newer key", () => {
    const decision = decideObjectFate({
      bucket: "quarantine",
      objectKey: `processed/${mediaId}.webp`,
      media: mediaRow({ privacy_status: "manual_review" }),
      ...options
    });
    expect(decision.action).toBe("delete_after");
  });

  it("schedules unknown keys for deletion but preserves them for the configured grace window", () => {
    const decision = decideObjectFate({
      bucket: "quarantine",
      objectKey: "tmp/random-upload.bin",
      media: null,
      ...options
    });
    expect(decision).toMatchObject({ action: "delete_after", role: "unknown", mediaId: null });
  });
});
