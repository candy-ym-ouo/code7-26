import { pool } from "./db";
import { randomToken } from "@map/shared/server";
import { deleteAllMediaObjects } from "./media-job";

type DeletableAccount = { id: string };

export async function purgeDeletedAccounts(): Promise<void> {
  const accounts = await pool.query<DeletableAccount>(
    `SELECT id FROM users
     WHERE status = 'deletion_pending'
       AND deleted_at < now() - interval '30 days'
     LIMIT 10`
  );

  for (const account of accounts.rows) {
    const mediaIds = await pool.query<{ id: string }>(
      "SELECT id FROM media_assets WHERE owner_id = $1",
      [account.id]
    );

    // 台账驱动删除：处理尝试的半成品与历史残留也在删除范围内，
    // 任一对象删除失败则跳过本账号，下个维护周期重试，避免留下孤儿。
    let hadFailure = false;
    for (const media of mediaIds.rows) {
      const { failed } = await deleteAllMediaObjects(media.id, "pipeline");
      if (failed.length > 0) {
        hadFailure = true;
        console.error({ userId: account.id, mediaId: media.id, failed }, "account purge object deletion failed; will retry");
      }
    }
    if (hadFailure) continue;

    const client = await pool.connect();
    try {
      await client.query("BEGIN");
      await client.query(
        `UPDATE media_assets SET privacy_status = 'deleted', deleted_at = now(), updated_at = now()
         WHERE owner_id = $1`,
        [account.id]
      );
      await client.query(
        `UPDATE comments SET status = 'deleted', deleted_at = now(), updated_at = now()
         WHERE author_id = $1 AND deleted_at IS NULL`,
        [account.id]
      );
      await client.query(
        `UPDATE map_features SET status = 'deleted', deleted_at = now(), updated_at = now()
         WHERE owner_id = $1 AND deleted_at IS NULL`,
        [account.id]
      );
      await client.query(
        `UPDATE users
         SET email = $2,
             email_normalized = $3,
             display_name = '已删除用户',
             password_hash = $4,
             status = 'deleted',
             updated_at = now()
         WHERE id = $1`,
        [account.id, `deleted+${account.id}@invalid.local`, `deleted+${account.id}@invalid.local`, `!unusable:${randomToken(24)}`]
      );
      await client.query("COMMIT");
    } catch (error) {
      await client.query("ROLLBACK");
      throw error;
    } finally {
      client.release();
    }
  }
}
