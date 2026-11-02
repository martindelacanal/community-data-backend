'use strict';

function normalizeIdentifier(value) {
  return typeof value === 'string' ? value.trim().toLowerCase() : '';
}

function normalizePhone(value) {
  if (typeof value !== 'string' || !/^[+()\d\s.-]+$/.test(value)) return '';
  const digits = value.replace(/\D/g, '');
  if (digits.length < 7 || digits.length > 15) return '';
  // US numbers are stored both with and without the country prefix.
  return digits.length === 11 && digits.startsWith('1') ? digits.slice(1) : digits;
}

function validRecoveryDate(value) {
  if (typeof value !== 'string' || !/^\d{4}-\d{2}-\d{2}$/.test(value)) return false;
  const parsed = new Date(`${value}T12:00:00Z`);
  return Number.isFinite(parsed.getTime()) && parsed.toISOString().slice(0, 10) === value
    && value <= new Date().toISOString().slice(0, 10);
}

function createPasswordRecoveryHandler({ pool, bcrypt, logger }) {
  return async (req, res) => {
    const identifier = normalizeIdentifier(req.body?.email);
    const dateOfBirth = req.body?.dateOfBirth;
    if (!identifier || identifier.length > 254 || !validRecoveryDate(dateOfBirth)) {
      return res.status(400).json('A valid identifier and date of birth are required');
    }

    try {
      // Date and account eligibility remain mandatory. Compare the small set of
      // same-birthday candidates consistently for both legacy apps and the web.
      const [rows] = await pool.query(
        `SELECT u.id, u.username, u.phone,
           (TRIM(u.email) = ? OR TRIM(u.username) = ? OR u.phone = ?) AS identifier_match
         FROM user u INNER JOIN role r ON r.id = u.role_id
         WHERE u.date_of_birth = ? AND r.name = 'beneficiary'
           AND u.enabled = 'Y' AND u.deleted = 'N'`, [identifier, identifier, identifier, dateOfBirth]);
      const phone = normalizePhone(identifier);
      // Let MySQL retain the existing identifier collation (including accented
      // names); JavaScript case-folding alone would change account matching.
      const matches = rows.filter(user => Number(user.identifier_match) === 1
        || (phone && normalizePhone(user.phone) === phone));

      if (!matches.length) return res.status(401).json('Unauthorized');
      // Shared contacts must never reset an arbitrary person's account.
      if (matches.length !== 1) return res.status(409).json('Please use your username or contact support');

      // Preserve the response used by already installed mobile releases.
      const password = 'bienestar';
      const passwordHash = await bcrypt.hash(password, 8);
      const [result] = await pool.query(
        `UPDATE user u INNER JOIN role r ON r.id = u.role_id
         SET u.password = ?, u.reset_password = 'Y'
         WHERE u.id = ? AND u.date_of_birth = ? AND r.name = 'beneficiary'
           AND u.enabled = 'Y' AND u.deleted = 'N'`,
        [passwordHash, matches[0].id, dateOfBirth]);
      if (result.affectedRows !== 1) return res.status(401).json('Unauthorized');
      return res.json({ password, username: matches[0].username });
    } catch (error) {
      // Do not log submitted identity information or password hashes.
      logger.error(`Password recovery failed: ${error.code || error.name || 'Error'}`);
      return res.status(500).json('Internal server error');
    }
  };
}

module.exports = { createPasswordRecoveryHandler };
