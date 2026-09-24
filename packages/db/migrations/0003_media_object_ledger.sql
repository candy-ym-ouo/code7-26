-- 媒体对象台账：记录隔离桶/公开桶中每一个对象的写入、替换与删除历史，
-- 使处理中断后的残留对象仍可被归属和追溯；配合桶扫描可重建引用、清理存量孤儿。
--
-- 桶名由迁移进程通过会话参数 app.s3_quarantine_bucket / app.s3_public_bucket
-- 传入（见 packages/db/src/migrate.ts）；未提供时使用默认桶名 map-quarantine /
-- map-public。即使回填桶名与实际不符，worker 首次对象对账时也会按实际桶扫描
-- 修正，因此历史数据不会丢失归属线索。

CREATE TYPE ledger_object_role AS ENUM (
  'quarantine_original',
  'processed_image',
  'processed_thumbnail',
  'public_image',
  'public_thumbnail',
  'unknown'
);

CREATE TYPE ledger_object_state AS ENUM (
  'committed',       -- 对象已写入：被 media_assets 当前引用，或等待归属判定
  'superseded',      -- 被新的处理尝试产物替换，等待物理删除
  'delete_pending',  -- 孤儿/过期对象，宽限期后物理删除
  'deleted'          -- 已确认从对象存储删除（逻辑墓碑，保留用于追溯）
);

CREATE TYPE media_attempt_trigger AS ENUM (
  'initial',   -- 用户首次提交处理
  'retry',     -- 用户或审核员重试
  'recovery'   -- worker 维护任务从超时中断中恢复
);

CREATE TABLE media_object_ledger (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  media_id uuid REFERENCES media_assets(id) ON DELETE SET NULL,
  bucket text NOT NULL,
  object_key text NOT NULL,
  object_role ledger_object_role NOT NULL,
  state ledger_object_state NOT NULL DEFAULT 'committed',
  attempt_id uuid,
  written_at timestamptz NOT NULL DEFAULT now(),
  last_seen_at timestamptz,
  delete_after timestamptz,
  deleted_at timestamptz,
  origin text NOT NULL DEFAULT 'pipeline',
  note text,
  metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (bucket, object_key)
);

-- 归属媒体的在役对象；孤儿扫描走 state/delete_after 索引。
CREATE INDEX media_object_ledger_media_idx
  ON media_object_ledger(media_id, state) WHERE state <> 'deleted';
CREATE INDEX media_object_ledger_pending_idx
  ON media_object_ledger(state, delete_after)
  WHERE state IN ('superseded', 'delete_pending');
CREATE INDEX media_object_ledger_attempt_idx
  ON media_object_ledger(attempt_id) WHERE attempt_id IS NOT NULL;
CREATE INDEX media_object_ledger_bucket_idx
  ON media_object_ledger(bucket, object_key) WHERE state <> 'deleted';

-- 处理尝试历史：每次首次处理、重试和中断恢复各占一行，串联台账中的对象。
CREATE TABLE media_processing_attempts (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  media_id uuid NOT NULL REFERENCES media_assets(id) ON DELETE CASCADE,
  attempt_no integer NOT NULL,
  trigger_type media_attempt_trigger NOT NULL,
  queued_by uuid REFERENCES users(id) ON DELETE SET NULL,
  worker_id text,
  job_id text,
  status text NOT NULL DEFAULT 'running'
    CHECK (status IN ('running', 'failed', 'lost', 'succeeded')),
  error_code text,
  started_at timestamptz NOT NULL DEFAULT now(),
  finished_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (media_id, attempt_no)
);
CREATE INDEX media_processing_attempts_media_idx
  ON media_processing_attempts(media_id, attempt_no DESC);
CREATE INDEX media_processing_attempts_running_idx
  ON media_processing_attempts(status, started_at) WHERE status = 'running';

ALTER TABLE media_assets
  ADD COLUMN current_attempt_id uuid
    REFERENCES media_processing_attempts(id) ON DELETE SET NULL,
  ADD COLUMN processing_attempt_count integer NOT NULL DEFAULT 0;

ALTER TABLE media_object_ledger
  ADD CONSTRAINT media_object_ledger_attempt_fk
  FOREIGN KEY (attempt_id) REFERENCES media_processing_attempts(id) ON DELETE SET NULL;

-- 对象存储对账的续扫游标（每个桶一行，null 表示下一轮从头开始全量扫描）。
CREATE TABLE media_reconcile_cursors (
  bucket_kind text PRIMARY KEY CHECK (bucket_kind IN ('quarantine', 'public')),
  cursor text,
  updated_at timestamptz NOT NULL DEFAULT now()
);
INSERT INTO media_reconcile_cursors(bucket_kind, cursor) VALUES ('quarantine', NULL), ('public', NULL);

-- 用现有列回填台账，使迁移前写入的在役对象立刻纳入归属与对账体系。
INSERT INTO media_object_ledger(media_id, bucket, object_key, object_role, state, written_at, origin, metadata)
SELECT t.media_id, t.bucket, t.object_key, t.object_role, 'committed', now(), 'backfill', '{}'::jsonb
FROM (
  SELECT ma.id AS media_id,
         COALESCE(current_setting('app.s3_quarantine_bucket', true), 'map-quarantine') AS bucket,
         ma.quarantine_object_key AS object_key,
         'quarantine_original'::ledger_object_role AS object_role
  FROM media_assets ma
  WHERE ma.quarantine_object_key IS NOT NULL
    AND ma.quarantine_object_key NOT LIKE 'deleted/%'

  UNION ALL
  SELECT ma.id,
         COALESCE(current_setting('app.s3_quarantine_bucket', true), 'map-quarantine'),
         ma.processed_object_key, 'processed_image'
  FROM media_assets ma
  WHERE ma.processed_object_key IS NOT NULL

  UNION ALL
  SELECT ma.id,
         COALESCE(current_setting('app.s3_quarantine_bucket', true), 'map-quarantine'),
         ma.thumbnail_object_key, 'processed_thumbnail'
  FROM media_assets ma
  WHERE ma.thumbnail_object_key IS NOT NULL

  UNION ALL
  SELECT ma.id,
         COALESCE(current_setting('app.s3_public_bucket', true), 'map-public'),
         ma.public_object_key, 'public_image'
  FROM media_assets ma
  WHERE ma.public_object_key IS NOT NULL

  UNION ALL
  SELECT ma.id,
         COALESCE(current_setting('app.s3_public_bucket', true), 'map-public'),
         ma.public_thumbnail_object_key, 'public_thumbnail'
  FROM media_assets ma
  WHERE ma.public_thumbnail_object_key IS NOT NULL
) t
ON CONFLICT (bucket, object_key) DO NOTHING;
