// 媒体对象存储键约定与归属解析（纯逻辑，供 API、worker 与对账任务共用）。
//
// 键布局：
//   原图（前端直传）：quarantine/<ownerId>/<mediaId>.<ext>
//   处理产物（按尝试隔离，重试不覆盖旧产物）：
//     processed/<mediaId>/<attemptId>.webp
//     processed/<mediaId>/<attemptId>.thumb.webp
//   已审核公开图（稳定键，由审核发布/自动发布拷贝）：
//     media/<mediaId>.webp
//     media/<mediaId>.thumb.webp
//
// 历史键（迁移前版本）仍需兼容：
//   processed/<mediaId>.webp / processed/<mediaId>.thumb.webp

export type LedgerObjectRole =
  | "quarantine_original"
  | "processed_image"
  | "processed_thumbnail"
  | "public_image"
  | "public_thumbnail"
  | "unknown";

const UUID_PATTERN = "[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}";

export function isUuid(value: string): boolean {
  return new RegExp(`^${UUID_PATTERN}$`, "i").test(value);
}

export function quarantineOriginalKey(ownerId: string, mediaId: string, extension: string): string {
  return `quarantine/${ownerId}/${mediaId}.${extension}`;
}

export function processedImageKey(mediaId: string, attemptId: string): string {
  return `processed/${mediaId}/${attemptId}.webp`;
}

export function processedThumbnailKey(mediaId: string, attemptId: string): string {
  return `processed/${mediaId}/${attemptId}.thumb.webp`;
}

export function publicImageKey(mediaId: string): string {
  return `media/${mediaId}.webp`;
}

export function publicThumbnailKey(mediaId: string): string {
  return `media/${mediaId}.thumb.webp`;
}

export type ObjectAttribution = {
  role: LedgerObjectRole;
  mediaId: string | null;
  attemptId: string | null;
  /** 键属于旧版布局（迁移前写入的存量对象）。 */
  legacy: boolean;
};

/**
 * 从对象键解析归属信息。任何无法识别的键返回 role='unknown'，
 * 由对账任务按孤儿宽限期流程处理，绝不静默忽略。
 */
export function parseMediaObjectKey(objectKey: string): ObjectAttribution {
  const legacyProcessed = new RegExp(`^processed/(${UUID_PATTERN})(\\.thumb)?\\.webp$`, "i").exec(objectKey);
  if (legacyProcessed) {
    return {
      role: legacyProcessed[2] ? "processed_thumbnail" : "processed_image",
      mediaId: legacyProcessed[1]!.toLowerCase(),
      attemptId: null,
      legacy: true
    };
  }

  const attemptProcessed = new RegExp(
    `^processed/(${UUID_PATTERN})/(${UUID_PATTERN})(\\.thumb)?\\.webp$`,
    "i"
  ).exec(objectKey);
  if (attemptProcessed) {
    return {
      role: attemptProcessed[3] ? "processed_thumbnail" : "processed_image",
      mediaId: attemptProcessed[1]!.toLowerCase(),
      attemptId: attemptProcessed[2]!.toLowerCase(),
      legacy: false
    };
  }

  const publicObject = new RegExp(`^media/(${UUID_PATTERN})(\\.thumb)?\\.webp$`, "i").exec(objectKey);
  if (publicObject) {
    return {
      role: publicObject[2] ? "public_thumbnail" : "public_image",
      mediaId: publicObject[1]!.toLowerCase(),
      attemptId: null,
      legacy: false
    };
  }

  const original = new RegExp(
    `^quarantine/(${UUID_PATTERN})/(${UUID_PATTERN})\\.[A-Za-z0-9]{1,8}$`,
    "i"
  ).exec(objectKey);
  if (original) {
    return {
      role: "quarantine_original",
      mediaId: original[2]!.toLowerCase(),
      attemptId: null,
      legacy: false
    };
  }

  return { role: "unknown", mediaId: null, attemptId: null, legacy: false };
}
