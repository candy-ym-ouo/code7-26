import {
  CopyObjectCommand,
  DeleteObjectCommand,
  GetObjectCommand,
  HeadObjectCommand,
  ListObjectsV2Command,
  PutObjectCommand,
  S3Client
} from "@aws-sdk/client-s3";
import { config } from "./config";

const credentials = {
  accessKeyId: config.S3_ACCESS_KEY,
  secretAccessKey: config.S3_SECRET_KEY
};

const s3 = new S3Client({
  endpoint: config.S3_ENDPOINT,
  region: config.S3_REGION,
  forcePathStyle: true,
  credentials
});

export async function readQuarantineObject(key: string): Promise<Buffer> {
  const response = await s3.send(new GetObjectCommand({
    Bucket: config.S3_QUARANTINE_BUCKET,
    Key: key
  }));
  if (!response.Body) throw new Error("Object body is empty");
  return Buffer.from(await response.Body.transformToByteArray());
}

export async function writeQuarantineObject(key: string, body: Buffer, contentType: string): Promise<void> {
  await s3.send(new PutObjectCommand({
    Bucket: config.S3_QUARANTINE_BUCKET,
    Key: key,
    Body: body,
    ContentType: contentType,
    CacheControl: "private, max-age=0"
  }));
}

export async function copyToPublic(processedKey: string, publicKey: string): Promise<void> {
  await s3.send(new CopyObjectCommand({
    Bucket: config.S3_PUBLIC_BUCKET,
    Key: publicKey,
    CopySource: `${config.S3_QUARANTINE_BUCKET}/${processedKey}`,
    MetadataDirective: "REPLACE",
    ContentType: "image/webp",
    CacheControl: "public, max-age=31536000, immutable"
  }));
}

export async function objectExists(bucket: string, key: string): Promise<boolean> {
  try {
    await s3.send(new HeadObjectCommand({ Bucket: bucket, Key: key }));
    return true;
  } catch {
    return false;
  }
}

export async function deleteObject(bucket: string, key: string): Promise<void> {
  await s3.send(new DeleteObjectCommand({ Bucket: bucket, Key: key }));
}

export type ListedObject = {
  key: string;
  size: number;
  lastModified: Date | null;
};

/**
 * 分页列举桶内对象。stopAfter 给出本次对账允许扫描的对象数上限；
 * 返回 nextContinuationToken 供下一个维护周期续扫，避免大桶单次扫爆。
 */
export async function listObjects(
  bucket: string,
  continuationToken: string | null,
  options: { maxKeys?: number; prefix?: string } = {}
): Promise<{ objects: ListedObject[]; nextContinuationToken: string | null }> {
  const response = await s3.send(new ListObjectsV2Command({
    Bucket: bucket,
    MaxKeys: options.maxKeys ?? 200,
    Prefix: options.prefix,
    ContinuationToken: continuationToken ?? undefined
  }));
  return {
    objects: (response.Contents ?? []).flatMap((item) =>
      item.Key
        ? [{ key: item.Key, size: item.Size ?? 0, lastModified: item.LastModified ?? null }]
        : []
    ),
    nextContinuationToken: response.IsTruncated && response.NextContinuationToken
      ? response.NextContinuationToken
      : null
  };
}
