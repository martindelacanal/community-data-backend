-- Persistent queue for participant email. A request key is scoped to its clinic.
CREATE TABLE IF NOT EXISTS health_event_participant_email (
  id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  health_event_id INT NOT NULL,
  request_id CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  created_by_user_id INT DEFAULT NULL,
  slot_id INT DEFAULT NULL,
  subject VARCHAR(160) NOT NULL,
  message TEXT NOT NULL,
  registrations INT UNSIGNED NOT NULL,
  recipients INT UNSIGNED NOT NULL,
  without_email INT UNSIGNED NOT NULL,
  duplicates INT UNSIGNED NOT NULL,
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY uq_participant_email_request (health_event_id, request_id),
  KEY idx_participant_email_latest (health_event_id, id),
  CONSTRAINT fk_participant_email_event FOREIGN KEY (health_event_id) REFERENCES health_event(id) ON DELETE CASCADE,
  CONSTRAINT fk_participant_email_creator FOREIGN KEY (created_by_user_id) REFERENCES user(id) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_spanish_ci;

-- Addresses are cleared when each delivery reaches a terminal state. The
-- original slot ID intentionally survives deletion to keep the audience scoped.
CREATE TABLE IF NOT EXISTS health_event_participant_delivery (
  id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  job_id INT NOT NULL,
  email VARCHAR(254) CHARACTER SET ascii COLLATE ascii_bin DEFAULT NULL,
  status ENUM('pending','sending','sent','failed','uncertain','skipped') NOT NULL DEFAULT 'pending',
  UNIQUE KEY uq_participant_email_delivery (job_id, email),
  KEY idx_participant_email_work (status, job_id, id),
  CONSTRAINT fk_participant_email_job FOREIGN KEY (job_id) REFERENCES health_event_participant_email(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_spanish_ci;
