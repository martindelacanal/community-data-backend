-- Health Events only. Returned/replaced bindings remain as historical records.
-- Nullable active keys permit history while UNIQUE keys enforce global reuse.
CREATE TABLE IF NOT EXISTS health_event_wristband (
  id int NOT NULL AUTO_INCREMENT,
  health_event_id int NOT NULL,
  registration_id int NOT NULL,
  uid char(14) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  active_uid char(14) CHARACTER SET ascii COLLATE ascii_bin DEFAULT NULL,
  active_registration_id int DEFAULT NULL,
  source enum('usb','bluetooth','nfc_native','nfc_web') NOT NULL,
  assigned_by_user_id int DEFAULT NULL,
  assigned_at datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  released_by_user_id int DEFAULT NULL,
  released_at datetime(3) DEFAULT NULL,
  release_reason enum('returned','replaced') DEFAULT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY uq_health_wristband_active_uid (active_uid),
  UNIQUE KEY uq_health_wristband_active_registration (active_registration_id),
  KEY idx_health_wristband_event_registration (health_event_id, registration_id),
  KEY idx_health_wristband_uid_history (uid, assigned_at),
  CONSTRAINT fk_health_wristband_event FOREIGN KEY (health_event_id) REFERENCES health_event(id) ON DELETE CASCADE,
  CONSTRAINT fk_health_wristband_registration FOREIGN KEY (registration_id) REFERENCES health_event_registration(id) ON DELETE CASCADE,
  CONSTRAINT fk_health_wristband_assigned_by FOREIGN KEY (assigned_by_user_id) REFERENCES user(id) ON DELETE SET NULL,
  CONSTRAINT fk_health_wristband_released_by FOREIGN KEY (released_by_user_id) REFERENCES user(id) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_spanish_ci;

-- Optional hardware metadata for beneficiary scans; legacy identity paths and
-- the registration's source (web/import/walkin) retain their existing meaning.
CREATE TABLE IF NOT EXISTS health_event_wristband_scan (
  scan_id bigint NOT NULL,
  wristband_id int DEFAULT NULL,
  source enum('usb','bluetooth','nfc_native','nfc_web') NOT NULL,
  PRIMARY KEY (scan_id),
  KEY idx_health_wristband_scan_binding (wristband_id),
  CONSTRAINT fk_health_wristband_scan_scan FOREIGN KEY (scan_id) REFERENCES health_event_scan(id) ON DELETE CASCADE,
  CONSTRAINT fk_health_wristband_scan_binding FOREIGN KEY (wristband_id) REFERENCES health_event_wristband(id) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_spanish_ci;

-- Volunteer presence is never a beneficiary stand visit or service metric.
CREATE TABLE IF NOT EXISTS health_event_staff_attendance (
  id bigint NOT NULL AUTO_INCREMENT,
  health_event_id int NOT NULL,
  registration_id int NOT NULL,
  wristband_id int DEFAULT NULL,
  stand_id int DEFAULT NULL,
  volunteer_user_id int DEFAULT NULL,
  scan_type enum('checkin','checkout') NOT NULL,
  paired_scan_id bigint DEFAULT NULL,
  source enum('usb','bluetooth','nfc_native','nfc_web') NOT NULL,
  scanned_at datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  PRIMARY KEY (id),
  KEY idx_health_staff_registration_time (registration_id, scanned_at),
  KEY idx_health_staff_event_time (health_event_id, scanned_at),
  CONSTRAINT fk_health_staff_event FOREIGN KEY (health_event_id) REFERENCES health_event(id) ON DELETE CASCADE,
  CONSTRAINT fk_health_staff_registration FOREIGN KEY (registration_id) REFERENCES health_event_registration(id) ON DELETE CASCADE,
  CONSTRAINT fk_health_staff_wristband FOREIGN KEY (wristband_id) REFERENCES health_event_wristband(id) ON DELETE SET NULL,
  CONSTRAINT fk_health_staff_stand FOREIGN KEY (stand_id) REFERENCES health_event_stand(id) ON DELETE SET NULL,
  CONSTRAINT fk_health_staff_operator FOREIGN KEY (volunteer_user_id) REFERENCES user(id) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_spanish_ci;
