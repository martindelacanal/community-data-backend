-- Existing terminal deliveries keep their status. Their original SMTP reason is unknown.
SET @participant_retry_ddl = IF(
  EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema = DATABASE()
    AND table_name = 'health_event_participant_delivery' AND column_name = 'attempts'),
  'SELECT 1',
  'ALTER TABLE health_event_participant_delivery
     ADD COLUMN attempts SMALLINT UNSIGNED NOT NULL DEFAULT 0,
     ADD COLUMN retry_at DATETIME DEFAULT NULL,
     ADD COLUMN recipient_rejected TINYINT UNSIGNED NOT NULL DEFAULT 0,
     ADD KEY idx_participant_delivery_due (status, retry_at, job_id, id)'
);
PREPARE participant_retry_stmt FROM @participant_retry_ddl;
EXECUTE participant_retry_stmt;
DEALLOCATE PREPARE participant_retry_stmt;

-- All participant campaigns share one SMTP sender.
CREATE TABLE IF NOT EXISTS health_event_participant_mail_sender (
  id TINYINT UNSIGNED NOT NULL PRIMARY KEY,
  paused_until DATETIME DEFAULT NULL
) ENGINE=InnoDB;
INSERT IGNORE INTO health_event_participant_mail_sender (id) VALUES (1);
