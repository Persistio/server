DROP TRIGGER IF EXISTS memory_authority_events_schedule_contradiction_activation ON memory_authority_events;
DROP TRIGGER IF EXISTS memories_schedule_contradiction_activation ON memories;
DROP FUNCTION IF EXISTS schedule_authority_event_contradiction_activation();
DROP FUNCTION IF EXISTS schedule_memory_contradiction_activation();
DROP FUNCTION IF EXISTS refresh_memory_contradiction_schedule(uuid, uuid);
DROP TABLE IF EXISTS memory_contradiction_schedule;
DROP FUNCTION IF EXISTS maintain_contradiction_pending_vault();
DROP TABLE IF EXISTS memory_contradiction_pending_vaults;
DROP FUNCTION IF EXISTS contradiction_authority_eligible(memories, text);
