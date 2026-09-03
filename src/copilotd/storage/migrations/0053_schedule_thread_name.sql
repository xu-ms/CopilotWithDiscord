ALTER TABLE schedules ADD COLUMN thread_name TEXT;

DROP TABLE IF EXISTS taskdeck_panel_state;
DROP TABLE IF EXISTS task_card_projections;

CREATE TRIGGER schedule_thread_name_insert
BEFORE INSERT ON schedules
WHEN NEW.thread_name IS NOT NULL
  AND (
      length(NEW.thread_name) < 1
      OR length(NEW.thread_name) > 100
      OR NEW.thread_name != trim(NEW.thread_name)
      OR instr(NEW.thread_name, char(10)) > 0
      OR instr(NEW.thread_name, char(13)) > 0
  )
BEGIN
    SELECT RAISE(ABORT, 'invalid:schedules.thread_name');
END;

CREATE TRIGGER schedule_thread_name_update
BEFORE UPDATE OF thread_name ON schedules
WHEN NEW.thread_name IS NOT NULL
  AND (
      length(NEW.thread_name) < 1
      OR length(NEW.thread_name) > 100
      OR NEW.thread_name != trim(NEW.thread_name)
      OR instr(NEW.thread_name, char(10)) > 0
      OR instr(NEW.thread_name, char(13)) > 0
  )
BEGIN
    SELECT RAISE(ABORT, 'invalid:schedules.thread_name');
END;
