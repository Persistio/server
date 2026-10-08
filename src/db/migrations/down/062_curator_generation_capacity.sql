-- Intentionally irreversible: an automatic rollback cannot distinguish the
-- migrated default from an operator's subsequent explicit 8192-token setting.
SELECT 1;
