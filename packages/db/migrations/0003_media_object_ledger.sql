CREATE TABLE media_object_events (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  media_id uuid REFERENCES media_assets(id) ON DELETE SET NULL,
  bucket text NOT NULL,
  object_key text NOT NULL,
  event text NOT NULL CHECK (event IN ('write', 'rewrite', 'delete', 'adopt', 'purge')),
  actor text NOT NULL,
  byte_size bigint,
  metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX media_object_events_media_idx ON media_object_events(media_id, created_at DESC);
CREATE INDEX media_object_events_object_idx ON media_object_events(bucket, object_key, created_at DESC);
