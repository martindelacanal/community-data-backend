const axios = require('axios');

const GEO_PROVIDER_NAME = 'ipwho.is';
const GEO_PROVIDER_URL = process.env.INTERACTION_GEOLOOKUP_URL || 'https://ipwho.is';
const GEO_PROVIDER_TIMEOUT_MS = Number.parseInt(process.env.INTERACTION_GEOLOOKUP_TIMEOUT_MS || '2500', 10);

const inFlightLookups = new Map();

function normalizeIpAddress(ipAddress) {
  const rawIp = String(ipAddress || '').trim();

  if (!rawIp) {
    return null;
  }

  if (rawIp === '::1') {
    return '127.0.0.1';
  }

  if (rawIp.startsWith('::ffff:')) {
    return rawIp.substring(7);
  }

  const ipv4WithPortMatch = rawIp.match(/^(\d{1,3}(?:\.\d{1,3}){3}):\d+$/);
  if (ipv4WithPortMatch) {
    return ipv4WithPortMatch[1];
  }

  return rawIp;
}

function isPrivateIpv4(ipAddress) {
  const octets = ipAddress.split('.').map((part) => Number.parseInt(part, 10));

  if (octets.length !== 4 || octets.some((octet) => Number.isNaN(octet) || octet < 0 || octet > 255)) {
    return false;
  }

  if (octets[0] === 10 || octets[0] === 127) {
    return true;
  }

  if (octets[0] === 169 && octets[1] === 254) {
    return true;
  }

  if (octets[0] === 192 && octets[1] === 168) {
    return true;
  }

  return octets[0] === 172 && octets[1] >= 16 && octets[1] <= 31;
}

function isPrivateIpv6(ipAddress) {
  const normalizedIp = ipAddress.toLowerCase();

  return normalizedIp === '::1'
    || normalizedIp.startsWith('fe80:')
    || normalizedIp.startsWith('fc')
    || normalizedIp.startsWith('fd');
}

function isPrivateOrLocalIp(ipAddress) {
  if (!ipAddress) {
    return true;
  }

  if (ipAddress === 'localhost') {
    return true;
  }

  return ipAddress.includes(':')
    ? isPrivateIpv6(ipAddress)
    : isPrivateIpv4(ipAddress);
}

function normalizeText(value, maxLength) {
  if (value === null || value === undefined || value === '') {
    return null;
  }

  const normalizedValue = String(value).trim();
  if (!normalizedValue) {
    return null;
  }

  return normalizedValue.substring(0, maxLength);
}

function normalizeCoordinate(value, min, max) {
  if (value === null || value === undefined || value === '') {
    return null;
  }

  const numericValue = Number(value);
  if (!Number.isFinite(numericValue) || numericValue < min || numericValue > max) {
    return null;
  }

  return Number(numericValue.toFixed(7));
}

function mapCacheRow(row) {
  if (!row) {
    return {
      ipAddress: null,
      status: 'missing',
      source: null,
      countryCode: null,
      countryName: null,
      regionName: null,
      cityName: null,
      latitude: null,
      longitude: null
    };
  }

  return {
    ipAddress: row.ip_address,
    status: normalizeText(row.geo_status, 32) || 'unknown',
    source: normalizeText(row.geo_source, 64),
    countryCode: normalizeText(row.country_code, 8),
    countryName: normalizeText(row.country_name, 100),
    regionName: normalizeText(row.region_name, 100),
    cityName: normalizeText(row.city_name, 100),
    latitude: normalizeCoordinate(row.latitude, -90, 90),
    longitude: normalizeCoordinate(row.longitude, -180, 180)
  };
}

async function getCachedIpGeo(connection, ipAddress) {
  const [rows] = await connection.query(
    `
      SELECT
        ip_address,
        geo_status,
        geo_source,
        country_code,
        country_name,
        region_name,
        city_name,
        latitude,
        longitude
      FROM interaction_ip_geolocation_cache
      WHERE ip_address = ?
      LIMIT 1
    `,
    [ipAddress]
  );

  return rows.length ? mapCacheRow(rows[0]) : null;
}

async function persistIpGeo(connection, ipAddress, geoData) {
  await connection.query(
    `
      INSERT INTO interaction_ip_geolocation_cache (
        ip_address,
        geo_status,
        geo_source,
        country_code,
        country_name,
        region_name,
        city_name,
        latitude,
        longitude,
        last_resolved_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
      ON DUPLICATE KEY UPDATE
        geo_status = VALUES(geo_status),
        geo_source = VALUES(geo_source),
        country_code = VALUES(country_code),
        country_name = VALUES(country_name),
        region_name = VALUES(region_name),
        city_name = VALUES(city_name),
        latitude = VALUES(latitude),
        longitude = VALUES(longitude),
        last_resolved_at = CURRENT_TIMESTAMP
    `,
    [
      ipAddress,
      normalizeText(geoData.status, 32) || 'unknown',
      normalizeText(geoData.source, 64),
      normalizeText(geoData.countryCode, 8),
      normalizeText(geoData.countryName, 100),
      normalizeText(geoData.regionName, 100),
      normalizeText(geoData.cityName, 100),
      normalizeCoordinate(geoData.latitude, -90, 90),
      normalizeCoordinate(geoData.longitude, -180, 180)
    ]
  );
}

async function fetchProviderGeo(ipAddress) {
  try {
    const response = await axios.get(`${GEO_PROVIDER_URL}/${encodeURIComponent(ipAddress)}`, {
      timeout: GEO_PROVIDER_TIMEOUT_MS,
      headers: {
        Accept: 'application/json'
      }
    });

    const payload = response?.data || {};
    const providerMessage = normalizeText(payload.message, 100);

    if (payload.success === false) {
      const normalizedStatus = providerMessage && providerMessage.toLowerCase().includes('reserved')
        ? 'private'
        : 'not_found';

      return {
        ipAddress,
        status: normalizedStatus,
        source: GEO_PROVIDER_NAME,
        countryCode: null,
        countryName: null,
        regionName: null,
        cityName: null,
        latitude: null,
        longitude: null
      };
    }

    return {
      ipAddress,
      status: 'resolved',
      source: GEO_PROVIDER_NAME,
      countryCode: normalizeText(payload.country_code, 8),
      countryName: normalizeText(payload.country, 100),
      regionName: normalizeText(payload.region, 100),
      cityName: normalizeText(payload.city, 100),
      latitude: normalizeCoordinate(payload.latitude, -90, 90),
      longitude: normalizeCoordinate(payload.longitude, -180, 180)
    };
  } catch (error) {
    return {
      ipAddress,
      status: 'failed',
      source: GEO_PROVIDER_NAME,
      countryCode: null,
      countryName: null,
      regionName: null,
      cityName: null,
      latitude: null,
      longitude: null
    };
  }
}

async function resolveInteractionGeoFromIp(connection, rawIpAddress) {
  const ipAddress = normalizeIpAddress(rawIpAddress);

  if (!ipAddress) {
    return {
      ipAddress: null,
      status: 'missing',
      source: null,
      countryCode: null,
      countryName: null,
      regionName: null,
      cityName: null,
      latitude: null,
      longitude: null
    };
  }

  const cachedGeo = await getCachedIpGeo(connection, ipAddress);
  if (cachedGeo) {
    return cachedGeo;
  }

  if (isPrivateOrLocalIp(ipAddress)) {
    const localGeo = {
      ipAddress,
      status: 'private',
      source: 'local',
      countryCode: null,
      countryName: null,
      regionName: null,
      cityName: null,
      latitude: null,
      longitude: null
    };

    await persistIpGeo(connection, ipAddress, localGeo);
    return localGeo;
  }

  if (inFlightLookups.has(ipAddress)) {
    return inFlightLookups.get(ipAddress);
  }

  const lookupPromise = (async () => {
    const providerGeo = await fetchProviderGeo(ipAddress);
    await persistIpGeo(connection, ipAddress, providerGeo);
    return providerGeo;
  })();

  inFlightLookups.set(ipAddress, lookupPromise);

  try {
    return await lookupPromise;
  } finally {
    inFlightLookups.delete(ipAddress);
  }
}

module.exports = {
  resolveInteractionGeoFromIp
};
