-- Normalize legacy accounts before public testing can enforce four-hour retention.
UPDATE users
SET retention_exempt = FALSE
WHERE retention_exempt
  AND NOT is_platform_admin
  AND id <> '00000000-0000-0000-0000-000000000001'::uuid;

UPDATE users
SET last_login_at = COALESCE(last_login_at, timezone('UTC', now())),
    data_expires_at = COALESCE(
        data_expires_at,
        COALESCE(last_login_at, timezone('UTC', now())) + interval '4 hours'
    )
WHERE NOT retention_exempt
  AND NOT is_platform_admin
  AND id <> '00000000-0000-0000-0000-000000000001'::uuid;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'ck_users_retention_exempt_protected'
          AND conrelid = 'users'::regclass
    ) THEN
        ALTER TABLE users
        ADD CONSTRAINT ck_users_retention_exempt_protected
        CHECK (
            NOT retention_exempt
            OR is_platform_admin
            OR id = '00000000-0000-0000-0000-000000000001'::uuid
        );
    END IF;

END $$;
