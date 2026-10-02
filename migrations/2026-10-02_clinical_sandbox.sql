-- Synthetic clinical pilot only. Deliberately independent of public health events and beneficiary tables.
CREATE TABLE IF NOT EXISTS clinical_sandbox_event (
 id int NOT NULL AUTO_INCREMENT PRIMARY KEY,
 name varchar(120) NOT NULL,
 start_date date NOT NULL,
 end_date date NOT NULL,
 mode enum('synthetic') NOT NULL DEFAULT 'synthetic',
 created_by int NOT NULL,
 created_at datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3)
) ENGINE=InnoDB;
CREATE TABLE IF NOT EXISTS clinical_sandbox_patient (
 id int NOT NULL AUTO_INCREMENT PRIMARY KEY,
 event_id int NOT NULL,
 synthetic_code varchar(64) NOT NULL UNIQUE,
 display_name varchar(100) NOT NULL,
 date_of_birth date NOT NULL,
 sex varchar(12) NOT NULL,
 is_synthetic tinyint NOT NULL DEFAULT 1,
 openemr_patient_uuid varchar(64) NULL,
 CONSTRAINT fk_clinical_patient_event FOREIGN KEY(event_id) REFERENCES clinical_sandbox_event(id) ON DELETE CASCADE
) ENGINE=InnoDB;
CREATE TABLE IF NOT EXISTS clinical_sandbox_grant (
 event_id int NOT NULL,
 user_id int NOT NULL,
 specialties json NOT NULL,
 can_write tinyint NOT NULL DEFAULT 0,
 can_finalize tinyint NOT NULL DEFAULT 0,
 granted_by int NOT NULL,
 granted_at datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
 revoked_at datetime(3) NULL,
 PRIMARY KEY(event_id,user_id),
 CONSTRAINT fk_clinical_grant_event FOREIGN KEY(event_id) REFERENCES clinical_sandbox_event(id) ON DELETE CASCADE
) ENGINE=InnoDB;
CREATE TABLE IF NOT EXISTS clinical_sandbox_record (
 id char(36) NOT NULL PRIMARY KEY,
 event_id int NOT NULL,
 patient_id int NOT NULL,
 specialty enum('general','dental','optometry','clearance') NOT NULL,
 status enum('draft','final') NOT NULL DEFAULT 'draft',
 revision int NOT NULL DEFAULT 1,
 data json NOT NULL,
 recorded_by int NOT NULL,
 finalized_by int NULL,
 created_at datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
 updated_at datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
 finalized_at datetime(3) NULL,
 supersedes_record_id char(36) NULL,
 amendment_reason varchar(1000) NULL,
 idempotency_key varchar(100) NOT NULL,
 request_hash char(64) NOT NULL,
 finalize_key varchar(100) NULL,
 finalization_context json NULL,
 openemr_record_uuid varchar(64) NULL,
 openemr_encounter_uuid varchar(64) NULL,
 sync_status enum('pending','synced','error') NOT NULL DEFAULT 'pending',
 UNIQUE KEY uq_clinical_idempotency(event_id,recorded_by,idempotency_key),
 KEY idx_clinical_patient(patient_id,created_at),
 CONSTRAINT fk_clinical_record_event FOREIGN KEY(event_id) REFERENCES clinical_sandbox_event(id) ON DELETE CASCADE,
 CONSTRAINT fk_clinical_record_patient FOREIGN KEY(patient_id) REFERENCES clinical_sandbox_patient(id) ON DELETE CASCADE
) ENGINE=InnoDB;
CREATE TABLE IF NOT EXISTS clinical_sandbox_attachment (
 id char(36) NOT NULL PRIMARY KEY,
 record_id char(36) NOT NULL,
 openemr_attachment_uuid varchar(64) NOT NULL,
 filename varchar(150) NOT NULL,
 mime_type varchar(80) NOT NULL,
 size_bytes int NOT NULL,
 sha256 char(64) NOT NULL,
 uploaded_by int NOT NULL,
 created_at datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
 UNIQUE KEY uq_clinical_attachment_content(record_id,sha256),
 CONSTRAINT fk_clinical_attachment_record FOREIGN KEY(record_id) REFERENCES clinical_sandbox_record(id) ON DELETE CASCADE
) ENGINE=InnoDB;
CREATE TABLE IF NOT EXISTS clinical_sandbox_revision (
 id bigint NOT NULL AUTO_INCREMENT PRIMARY KEY,
 record_id char(36) NOT NULL,
 revision int NOT NULL,
 status varchar(20) NOT NULL,
 data json NOT NULL,
 actor_user_id int NOT NULL,
 action varchar(50) NOT NULL,
 created_at datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
 UNIQUE KEY uq_clinical_revision(record_id,revision),
 CONSTRAINT fk_clinical_revision_record FOREIGN KEY(record_id) REFERENCES clinical_sandbox_record(id) ON DELETE CASCADE
) ENGINE=InnoDB;
CREATE TABLE IF NOT EXISTS clinical_sandbox_feedback (
 id bigint NOT NULL AUTO_INCREMENT PRIMARY KEY,
 event_id int NOT NULL,
 user_id int NOT NULL,
 category varchar(30) NOT NULL,
 message varchar(3000) NOT NULL,
 rating int NULL,
 created_at datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
 CONSTRAINT fk_clinical_feedback_event FOREIGN KEY(event_id) REFERENCES clinical_sandbox_event(id) ON DELETE CASCADE
) ENGINE=InnoDB;
CREATE TABLE IF NOT EXISTS clinical_sandbox_audit (
 id bigint NOT NULL AUTO_INCREMENT PRIMARY KEY,
 actor_user_id int NOT NULL,
 event_id int NULL,
 record_id char(36) NULL,
 action varchar(60) NOT NULL,
 revision int NULL,
 created_at datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
 KEY idx_clinical_audit_event(event_id,created_at)
) ENGINE=InnoDB;
