ALTER TABLE message_queue ADD COLUMN dispatch_attempt INTEGER NOT NULL DEFAULT 0;
