-- Clamp accounts that were created under an older retention window without
-- mutating the already-applied 102 migration.
UPDATE users
SET last_login_at = COALESCE(last_login_at, timezone('UTC', now())),
    data_expires_at = LEAST(
        COALESCE(data_expires_at, 'infinity'::timestamptz),
        COALESCE(last_login_at, timezone('UTC', now())) + interval '4 hours'
    )
WHERE NOT retention_exempt
  AND NOT is_platform_admin
  AND id <> '00000000-0000-0000-0000-000000000001'::uuid;
