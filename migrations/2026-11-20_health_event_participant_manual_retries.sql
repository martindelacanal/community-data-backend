-- Existing campaigns have no recoverable recipient snapshot. Leave them ineligible.
SET @clinic_manual_retry_ddl = IF(
  EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema=DATABASE()
    AND table_name='health_event_participant_email' AND column_name='retry_generation'),
  'SELECT 1',
  'ALTER TABLE health_event_participant_email
     ADD COLUMN retry_generation INT UNSIGNED NOT NULL DEFAULT 0,
     ADD COLUMN retry_key_check CHAR(64) CHARACTER SET ascii COLLATE ascii_bin DEFAULT NULL'
);
PREPARE clinic_manual_retry_stmt FROM @clinic_manual_retry_ddl;
EXECUTE clinic_manual_retry_stmt;
DEALLOCATE PREPARE clinic_manual_retry_stmt;

-- A keyed per-campaign digest cannot be used as an email address. Keep only
-- registration IDs and reservation versions while delivery or retry is possible.
SET @clinic_manual_retry_ddl = IF(
  EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema=DATABASE()
    AND table_name='health_event_participant_delivery' AND column_name='recipient_key'),
  'SELECT 1',
  'ALTER TABLE health_event_participant_delivery
     ADD COLUMN recipient_key CHAR(64) CHARACTER SET ascii COLLATE ascii_bin DEFAULT NULL,
     ADD COLUMN retry_recipient_key CHAR(64) CHARACTER SET ascii COLLATE ascii_bin DEFAULT NULL,
     ADD COLUMN registration_snapshot JSON DEFAULT NULL'
);
PREPARE clinic_manual_retry_stmt FROM @clinic_manual_retry_ddl;
EXECUTE clinic_manual_retry_stmt;
DEALLOCATE PREPARE clinic_manual_retry_stmt;
