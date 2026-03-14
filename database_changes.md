# Trusted Resource Cities Database Changes

## 1. Create `resource_cities` table
This table will store the city names for Trusted Resources. Since city names are typically identical in English and Spanish, we only use a single `name` column.

```sql
CREATE TABLE `resource_cities` (
  `id` int NOT NULL AUTO_INCREMENT,
  `name` varchar(255) COLLATE utf8mb4_spanish_ci NOT NULL,
  `created_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `name` (`name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_spanish_ci;
```

## 2. Add `city_id` to `trusted_resources`
Add a `city_id` foreign key column to the `trusted_resources` table to link a resource to exactly one city.

```sql
ALTER TABLE `trusted_resources`
ADD COLUMN `city_id` int DEFAULT NULL AFTER `address`;

ALTER TABLE `trusted_resources`
ADD CONSTRAINT `fk_trusted_resources_city_id`
FOREIGN KEY (`city_id`) REFERENCES `resource_cities` (`id`) ON DELETE SET NULL;
```
