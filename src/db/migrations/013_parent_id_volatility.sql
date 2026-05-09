DO $$ BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'memory_volatility') THEN
    CREATE TYPE memory_volatility AS ENUM ('very_low', 'low', 'medium', 'high');
  END IF;
END $$;
ALTER TABLE memories ADD COLUMN IF NOT EXISTS parent_id UUID REFERENCES memories(id) ON DELETE SET NULL;
ALTER TABLE memories ADD COLUMN IF NOT EXISTS volatility memory_volatility NOT NULL DEFAULT 'low';
CREATE INDEX IF NOT EXISTS idx_memories_parent ON memories(parent_id);
