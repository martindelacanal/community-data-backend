SET @clinic_reservation_ddl = IF(
  EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema = DATABASE()
    AND table_name = 'health_event_registration' AND column_name = 'reservation_version'),
  'SELECT 1',
  'ALTER TABLE health_event_registration ADD COLUMN reservation_version INT UNSIGNED NOT NULL DEFAULT 1'
);
PREPARE clinic_reservation_stmt FROM @clinic_reservation_ddl;
EXECUTE clinic_reservation_stmt;
DEALLOCATE PREPARE clinic_reservation_stmt;

CREATE TABLE IF NOT EXISTS health_event_confirmation (
  registration_id INT NOT NULL,
  reservation_version INT UNSIGNED NOT NULL,
  language ENUM('en','es') NOT NULL,
  include_default_login TINYINT NOT NULL DEFAULT 0,
  state ENUM('queued','sent','failed','skipped') NOT NULL DEFAULT 'queued',
  attempts INT UNSIGNED NOT NULL DEFAULT 0,
  available_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  claim_id CHAR(36) NULL,
  PRIMARY KEY (registration_id, reservation_version),
  INDEX idx_hec_due (state, available_at),
  CONSTRAINT fk_hec_registration FOREIGN KEY (registration_id)
    REFERENCES health_event_registration(id) ON DELETE CASCADE
) ENGINE=InnoDB;
