ALTER TABLE memories ADD COLUMN IF NOT EXISTS subject_encrypted TEXT;
ALTER TABLE memories ADD COLUMN IF NOT EXISTS subject_hmac TEXT;
CREATE INDEX IF NOT EXISTS idx_memories_subject_hmac ON memories(vault_id, subject_hmac);
