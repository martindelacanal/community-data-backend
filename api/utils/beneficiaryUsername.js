'use strict';

/** An easy account name assigned after INSERT, so concurrent registrations differ. */
async function assignBeneficiaryUsername(connection, userId, firstName) {
  const name = String(firstName || '').normalize('NFD').replace(/[\u0300-\u036f]/g, '')
    .toLowerCase().replace(/[^a-z]/g, '').slice(0, 24) || 'bienestar';
  const base = `${name}${userId}`;
  for (let suffix = 0; suffix < 100; suffix++) {
    const username = suffix ? `${base}.${suffix}` : base;
    const [existing] = await connection.query('SELECT id FROM user WHERE username = ? AND id <> ? LIMIT 1', [username, userId]);
    if (existing.length) continue;
    try {
      await connection.query('UPDATE user SET username = ? WHERE id = ?', [username, userId]);
      return username;
    } catch (error) {
      if (error.code !== 'ER_DUP_ENTRY') throw error;
    }
  }
  throw new Error('Could not assign beneficiary username');
}

module.exports = { assignBeneficiaryUsername };
