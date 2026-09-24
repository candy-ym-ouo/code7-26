const UUID_PATTERN = "[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}";

export const MEDIA_OBJECT_KINDS = [
  "quarantine",
  "processed",
  "processed_thumbnail",
  "public",
  "public_thumbnail"
] as const;
export type MediaObjectKind = (typeof MEDIA_OBJECT_KINDS)[number];

export type ParsedMediaObjectKey = {
  kind: MediaObjectKind;
  mediaId: string;
  ownerId?: string;
};

const KEY_PATTERNS: Array<{ kind: MediaObjectKind; pattern: RegExp }> = [
  { kind: "quarantine", pattern: new RegExp(`^quarantine/(${UUID_PATTERN})/(${UUID_PATTERN})\\.(jpg|png|webp)$`) },
  { kind: "processed_thumbnail", pattern: new RegExp(`^processed/(${UUID_PATTERN})\\.thumb\\.webp$`) },
  { kind: "processed", pattern: new RegExp(`^processed/(${UUID_PATTERN})\\.webp$`) },
  { kind: "public_thumbnail", pattern: new RegExp(`^media/(${UUID_PATTERN})\\.thumb\\.webp$`) },
  { kind: "public", pattern: new RegExp(`^media/(${UUID_PATTERN})\\.webp$`) }
];

export function parseMediaObjectKey(key: string): ParsedMediaObjectKey | null {
  for (const { kind, pattern } of KEY_PATTERNS) {
    const match = pattern.exec(key);
    if (!match) continue;
    if (kind === "quarantine") {
      return { kind, mediaId: match[2]!.toLowerCase(), ownerId: match[1]!.toLowerCase() };
    }
    return { kind, mediaId: match[1]!.toLowerCase() };
  }
  return null;
}

export const MEDIA_OBJECT_COLUMNS = {
  quarantine: "quarantine_object_key",
  processed: "processed_object_key",
  processed_thumbnail: "thumbnail_object_key",
  public: "public_object_key",
  public_thumbnail: "public_thumbnail_object_key"
} as const;
export type MediaObjectColumn = (typeof MEDIA_OBJECT_COLUMNS)[MediaObjectKind];

export type ReconcileMediaRow = {
  id: string;
  privacy_status: string;
  deleted_at: Date | string | null;
  quarantine_object_key: string | null;
  processed_object_key: string | null;
  thumbnail_object_key: string | null;
  public_object_key: string | null;
  public_thumbnail_object_key: string | null;
};

export type ReconcileDecision =
  | { action: "skip"; reason: "referenced" | "grace" }
  | { action: "adopt"; column: MediaObjectColumn }
  | { action: "purge"; reason: "unattributable_key" | "media_row_missing" | "media_deleted" | "superseded" | "public_without_ready" };

export function isPublicKind(kind: MediaObjectKind): boolean {
  return kind === "public" || kind === "public_thumbnail";
}

export function decideMediaObjectAction(input: {
  key: string;
  parsed: ParsedMediaObjectKey | null;
  media: ReconcileMediaRow | null;
  withinGrace: boolean;
}): ReconcileDecision {
  const { key, parsed, media, withinGrace } = input;

  // Objects still referenced by their media row belong to the regular
  // lifecycle (retention and deletion cleanups). Never touch them here.
  if (parsed && media && media[MEDIA_OBJECT_COLUMNS[parsed.kind]] === key) {
    return { action: "skip", reason: "referenced" };
  }

  // Objects younger than the grace window may belong to an in-flight job.
  if (withinGrace) return { action: "skip", reason: "grace" };

  if (!parsed) return { action: "purge", reason: "unattributable_key" };
  if (!media) return { action: "purge", reason: "media_row_missing" };
  if (media.deleted_at) return { action: "purge", reason: "media_deleted" };

  const column = MEDIA_OBJECT_COLUMNS[parsed.kind];
  if (media[column]) return { action: "purge", reason: "superseded" };

  // The public bucket must only hold privacy-confirmed derivatives.
  if (isPublicKind(parsed.kind) && media.privacy_status !== "ready") {
    return { action: "purge", reason: "public_without_ready" };
  }

  return { action: "adopt", column };
}
