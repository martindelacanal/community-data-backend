-- One current administrative review per registration. A review applies only to
-- the recorded reservation version and exact normalized contact address.
CREATE TABLE IF NOT EXISTS health_event_registration_contact_review (
  registration_id INT NOT NULL PRIMARY KEY,
  reservation_version INT UNSIGNED NOT NULL,
  contact_email VARCHAR(254) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  reviewed_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT fk_he_contact_review_registration FOREIGN KEY (registration_id)
    REFERENCES health_event_registration(id) ON DELETE CASCADE
) ENGINE=InnoDB;
