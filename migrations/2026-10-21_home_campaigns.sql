-- Run with scripts/migrateHomeCampaigns.js (idempotent column/index checks).
-- Existing card content is preserved. No historical clicks are invented.
CREATE TABLE IF NOT EXISTS home_promotion_settings (
  id TINYINT NOT NULL,
  settings_json LONGTEXT NOT NULL,
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_spanish_ci;

CREATE TABLE IF NOT EXISTS home_campaign_clicks (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  event_id CHAR(36) NOT NULL,
  entity_type VARCHAR(24) NOT NULL,
  entity_id INT NOT NULL DEFAULT 0,
  card_id INT NULL,
  session_id VARCHAR(64) NULL,
  platform VARCHAR(8) NOT NULL,
  device_type VARCHAR(12) NOT NULL,
  access_channel VARCHAR(24) NOT NULL,
  destination VARCHAR(500) NULL,
  clicked_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  UNIQUE KEY uq_home_campaign_event (event_id),
  KEY idx_home_campaign_click_date (clicked_at, session_id),
  KEY idx_home_campaign_click_entity (entity_type, entity_id, clicked_at),
  KEY idx_home_campaign_click_platform (platform, clicked_at),
  KEY idx_home_campaign_click_device (device_type, clicked_at),
  CONSTRAINT fk_home_campaign_click_card FOREIGN KEY (card_id) REFERENCES home_cards(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_spanish_ci;

CREATE TABLE IF NOT EXISTS home_campaign_daily (
  click_date DATE NOT NULL,
  entity_type VARCHAR(24) NOT NULL,
  entity_id INT NOT NULL DEFAULT 0,
  card_id INT NULL,
  platform VARCHAR(8) NOT NULL,
  device_type VARCHAR(12) NOT NULL,
  access_channel VARCHAR(24) NOT NULL,
  clicks BIGINT UNSIGNED NOT NULL DEFAULT 0,
  last_clicked_at DATETIME NOT NULL,
  PRIMARY KEY (click_date, entity_type, entity_id, platform, device_type, access_channel),
  KEY idx_home_campaign_daily_entity (entity_type, entity_id, click_date),
  CONSTRAINT fk_home_campaign_daily_card FOREIGN KEY (card_id) REFERENCES home_cards(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_spanish_ci;

CREATE TABLE IF NOT EXISTS home_campaign_history (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  entity_type VARCHAR(24) NOT NULL,
  entity_id INT NOT NULL DEFAULT 0,
  card_id INT NULL,
  title_en VARCHAR(255) NOT NULL,
  title_es VARCHAR(255) NOT NULL,
  action VARCHAR(24) NOT NULL,
  changes_json LONGTEXT NOT NULL,
  actor_id INT NULL,
  platform VARCHAR(8) NOT NULL DEFAULT 'all',
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  KEY idx_home_campaign_history_date (created_at, id),
  KEY idx_home_campaign_history_entity (entity_type, entity_id, created_at),
  CONSTRAINT fk_home_campaign_history_card FOREIGN KEY (card_id) REFERENCES home_cards(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_spanish_ci;
