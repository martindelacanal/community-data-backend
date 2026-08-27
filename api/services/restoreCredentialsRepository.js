const crypto = require('node:crypto');

const ACCOUNT_SQL = `
  SELECT u.id, u.firstname, u.username, u.email, u.password, u.client_id,
         u.language, u.enabled, u.deleted, u.reset_password, u.creation_date,
         r.name AS role
  FROM user AS u INNER JOIN role AS r ON r.id = u.role_id
  WHERE u.id = ? LIMIT 1`;

const SCHEMA_SQL = [
  `CREATE TABLE IF NOT EXISTS android_restore_credentials (
    credential_hash CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL PRIMARY KEY,
    credential_id VARCHAR(2048) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    user_id BIGINT UNSIGNED NOT NULL,
    user_handle CHAR(43) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    public_key BLOB NOT NULL,
    sign_count BIGINT UNSIGNED NOT NULL DEFAULT 0,
    auth_binding CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    revocation_hash CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    registration_request_id CHAR(43) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    created_at DATETIME(3) NOT NULL,
    last_used_at DATETIME(3) NULL,
    INDEX idx_restore_user (user_id),
    UNIQUE KEY idx_restore_registration (registration_request_id)
  ) ENGINE=InnoDB`,
  `CREATE TABLE IF NOT EXISTS android_restore_challenges (
    request_id CHAR(43) CHARACTER SET ascii COLLATE ascii_bin NOT NULL PRIMARY KEY,
    challenge CHAR(43) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    purpose VARCHAR(16) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    user_id BIGINT UNSIGNED NULL,
    user_handle CHAR(43) CHARACTER SET ascii COLLATE ascii_bin NULL,
    auth_binding CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NULL,
    expires_at DATETIME(3) NOT NULL,
    consumed_at DATETIME(3) NULL,
    cancelled TINYINT(1) NOT NULL DEFAULT 0,
    INDEX idx_restore_challenge_expiry (expires_at),
    INDEX idx_restore_challenge_user (user_id)
  ) ENGINE=InnoDB`,
];

function credentialHash(credentialId) {
  return crypto.createHash('sha256').update(credentialId).digest('hex');
}

function createRestoreCredentialsRepository(pool) {
  let schemaPromise;
  function ensureSchema() {
    if (!schemaPromise) {
      schemaPromise = (async () => {
        for (const sql of SCHEMA_SQL) await pool.query(sql);
      })().catch((error) => {
        schemaPromise = undefined;
        throw error;
      });
    }
    return schemaPromise;
  }

  async function transaction(operation) {
    await ensureSchema();
    const connection = await pool.getConnection();
    try {
      await connection.beginTransaction();
      const result = await operation(connection);
      await connection.commit();
      return result;
    } catch (error) {
      await connection.rollback();
      throw error;
    } finally {
      connection.release();
    }
  }

  async function account(connection, userId, lock = false) {
    const [rows] = await connection.query(ACCOUNT_SQL + (lock ? ' FOR UPDATE' : ''), [userId]);
    return rows[0] || null;
  }

  return {
    ensureSchema,
    async findAccount(userId) {
      return account(pool, userId);
    },
    async issueChallenge(record) {
      await ensureSchema();
      // No cron/timer or user-data migration is needed; each new challenge does
      // bounded cleanup. Public request rate limits cap outstanding challenges.
      await pool.query(
        'DELETE FROM android_restore_challenges WHERE expires_at <= UTC_TIMESTAMP(3) LIMIT 1000'
      );
      await pool.query(
        `INSERT INTO android_restore_challenges
           (request_id, challenge, purpose, user_id, user_handle, auth_binding, expires_at)
         VALUES (?, ?, ?, ?, ?, ?, TIMESTAMPADD(MICROSECOND, ?, UTC_TIMESTAMP(3)))`,
        [record.requestId, record.challenge, record.purpose, record.userId ?? null,
          record.userHandle ?? null, record.authBinding ?? null, record.ttlMs * 1000]
      );
    },
    async consumeChallenge(requestId, purpose, userId = null) {
      return transaction(async (connection) => {
        const [rows] = await connection.query(
          `SELECT *, expires_at > UTC_TIMESTAMP(3) AS is_fresh
           FROM android_restore_challenges WHERE request_id = ? FOR UPDATE`, [requestId]
        );
        const row = rows[0];
        if (!row) return null;
        // Consume and commit BEFORE signature verification. Registration keeps
        // its row until TTL so a concurrent cancellation has a lock/tombstone.
        const alreadyUsed = !!row.consumed_at || !!row.cancelled;
        if (row.purpose === 'registration') {
          await connection.query(
            'UPDATE android_restore_challenges SET consumed_at = UTC_TIMESTAMP(3) WHERE request_id = ?', [requestId]
          );
        } else {
          await connection.query('DELETE FROM android_restore_challenges WHERE request_id = ?', [requestId]);
        }
        if (alreadyUsed || !row.is_fresh || row.purpose !== purpose
            || (row.user_id === null ? userId !== null : Number(row.user_id) !== userId)) return null;
        return {
          challenge: row.challenge, userId: row.user_id, userHandle: row.user_handle,
          authBinding: row.auth_binding,
        };
      });
    },
    async registerCredential(record, validateAccount) {
      return transaction(async (connection) => {
        const user = await account(connection, record.userId, true);
        if (!validateAccount(user)) return false;
        const [operations] = await connection.query(
          `SELECT *, expires_at > UTC_TIMESTAMP(3) AS is_fresh
           FROM android_restore_challenges WHERE request_id = ? FOR UPDATE`,
          [record.registrationRequestId]
        );
        const operation = operations[0];
        if (!operation || operation.purpose !== 'registration' || !operation.is_fresh
            || !operation.consumed_at || operation.cancelled
            || Number(operation.user_id) !== record.userId || operation.auth_binding !== record.authBinding) return false;
        const [existing] = await connection.query(
          'SELECT credential_hash FROM android_restore_credentials WHERE user_id = ? ORDER BY created_at ASC FOR UPDATE',
          [record.userId]
        );
        // Bounded account storage, with room for multiple devices. Registering a
        // new device only evicts the oldest key after ten enrolled devices.
        const evict = existing.slice(0, Math.max(0, existing.length - 9));
        for (const row of evict) {
          await connection.query('DELETE FROM android_restore_credentials WHERE credential_hash = ?', [row.credential_hash]);
        }
        await connection.query(
          `INSERT INTO android_restore_credentials
            (credential_hash, credential_id, user_id, user_handle, public_key, sign_count,
             auth_binding, revocation_hash, registration_request_id, created_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, UTC_TIMESTAMP(3))`,
          [credentialHash(record.credentialId), record.credentialId, record.userId,
            record.userHandle, Buffer.from(record.publicKey), record.counter,
            record.authBinding, record.revocationHash, record.registrationRequestId]
        );
        return true;
      });
    },
    async authenticateCredential(credentialId, verify) {
      return transaction(async (connection) => {
        const hash = credentialHash(credentialId);
        const [candidates] = await connection.query(
          'SELECT user_id FROM android_restore_credentials WHERE credential_hash = ?', [hash]
        );
        if (!candidates[0]) return null;
        // Consistent user-then-credential lock order also serializes password/
        // disable changes and parallel counter updates.
        const user = await account(connection, candidates[0].user_id, true);
        const [rows] = await connection.query(
          'SELECT * FROM android_restore_credentials WHERE credential_hash = ? FOR UPDATE', [hash]
        );
        const row = rows[0];
        if (!row || row.credential_id !== credentialId) return null;
        const result = await verify({
          user,
          credential: {
            credentialId: row.credential_id, userId: Number(row.user_id),
            userHandle: row.user_handle, publicKey: new Uint8Array(row.public_key),
            counter: Number(row.sign_count), authBinding: row.auth_binding,
          },
        });
        await connection.query(
          'UPDATE android_restore_credentials SET sign_count = ?, last_used_at = UTC_TIMESTAMP(3) WHERE credential_hash = ?',
          [result.counter, hash]
        );
        return result.session;
      });
    },
    async revokeCredential(credentialId, revocationHash) {
      await ensureSchema();
      await pool.query(
        'DELETE FROM android_restore_credentials WHERE credential_hash = ? AND credential_id = ? AND revocation_hash = ?',
        [credentialHash(credentialId), credentialId, revocationHash]
      );
    },
    async cancelRegistration(requestId) {
      await transaction(async (connection) => {
        await connection.query(
          'SELECT request_id FROM android_restore_challenges WHERE request_id = ? FOR UPDATE', [requestId]
        );
        await connection.query(
          `UPDATE android_restore_challenges SET cancelled = 1
           WHERE request_id = ? AND purpose = 'registration'`, [requestId]
        );
        // The link remains with the credential after challenge expiry/cleanup.
        // Cancellation therefore still works after a response was lost and
        // the device was offline for longer than the challenge's five minutes.
        await connection.query(
          'DELETE FROM android_restore_credentials WHERE registration_request_id = ?', [requestId]
        );
      });
    },
    async revokeUser(userId, connection) {
      // Call ensureSchema BEFORE starting an outer transaction: CREATE TABLE
      // must never implicitly commit an account change.
      await connection.query('DELETE FROM android_restore_challenges WHERE user_id = ?', [userId]);
      await connection.query('DELETE FROM android_restore_credentials WHERE user_id = ?', [userId]);
    },
  };
}

async function setUserEnabledWithRestoreRevocation(pool, userId, enabled) {
  const repository = createRestoreCredentialsRepository(pool);
  await repository.ensureSchema();
  const connection = await pool.getConnection();
  try {
    await connection.beginTransaction();
    const [result] = await connection.query('UPDATE user SET enabled = ? WHERE id = ?', [enabled, userId]);
    if (result.affectedRows > 0) await repository.revokeUser(userId, connection);
    await connection.commit();
    return result;
  } catch (error) {
    await connection.rollback();
    throw error;
  } finally {
    connection.release();
  }
}

module.exports = { createRestoreCredentialsRepository, credentialHash, setUserEnabledWithRestoreRevocation };
