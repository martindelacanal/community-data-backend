CREATE TABLE IF NOT EXISTS volunteer_privacy_consent (
  id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  user_id INT NULL,
  volunteer_id INT NULL,
  registration_id INT NULL,
  document_version VARCHAR(30) NOT NULL,
  document_path VARCHAR(255) NOT NULL,
  full_name VARCHAR(100) NOT NULL,
  participant_role VARCHAR(20) NOT NULL,
  email VARCHAR(255) NOT NULL,
  device_model VARCHAR(200) NOT NULL,
  accepted_at DATETIME(3) NOT NULL,
  INDEX privacy_user (user_id),
  INDEX privacy_volunteer (volunteer_id),
  INDEX privacy_registration (registration_id)
);
