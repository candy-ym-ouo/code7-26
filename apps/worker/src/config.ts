import dotenv from "dotenv";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";

dotenv.config({ path: process.env.ENV_FILE || join(dirname(fileURLToPath(import.meta.url)), "../../../.env") });
import { z } from "zod";

const envSchema = z.object({
  NODE_ENV: z.enum(["development", "test", "production"]).default("development"),
  DATABASE_URL: z.string().min(1),
  REDIS_URL: z.string().min(1).default("redis://localhost:6379/0"),
  S3_ENDPOINT: z.string().url(),
  S3_PUBLIC_ENDPOINT: z.string().url(),
  S3_REGION: z.string().default("us-east-1"),
  S3_ACCESS_KEY: z.string().min(1),
  S3_SECRET_KEY: z.string().min(1),
  S3_QUARANTINE_BUCKET: z.string().min(1),
  S3_PUBLIC_BUCKET: z.string().min(1),
  MEDIA_MAX_PIXELS: z.coerce.number().int().positive().default(20_000_000),
  PRIVACY_DETECTOR_URL: z.string().url().optional().or(z.literal("")),
  PRIVACY_BLUR_SIGMA: z.coerce.number().positive().default(32),
  PRIVACY_BLUR_PADDING: z.coerce.number().min(0).max(0.5).default(0.08),
  ORIGINAL_RETENTION_HOURS: z.coerce.number().positive().default(24),
  // 无法归属的私有桶孤儿对象宽限期：给人工排查/引用重建留时间，到期后物理删除。
  ORPHAN_QUARANTINE_GRACE_HOURS: z.coerce.number().positive().default(72),
  // 公开桶中的无台账对象按隐私优先原则处理：短宽限期后立即删除（未审核内容不得滞留公开桶）。
  ORPHAN_PUBLIC_GRACE_MINUTES: z.coerce.number().positive().default(15),
  // 单次维护 tick 最多对账的对象数，限制扫描与删除压力。
  RECONCILE_BATCH_SIZE: z.coerce.number().int().positive().default(200),
  WORKER_ID: z.string().optional(),
  CLAMAV_ENABLED: z.string().default("true").transform((value) => value === "true"),
  CLAMAV_HOST: z.string().default("localhost"),
  CLAMAV_PORT: z.coerce.number().int().positive().default(3310),
  SMTP_HOST: z.string().default("localhost"),
  SMTP_PORT: z.coerce.number().int().positive().default(1025),
  SMTP_SECURE: z.string().default("false").transform((value) => value === "true"),
  SMTP_USER: z.string().optional(),
  SMTP_PASSWORD: z.string().optional(),
  MAIL_FROM: z.string().default("公共空间细节地图 <noreply@example.test>")
});

export const config = envSchema.parse(process.env);
