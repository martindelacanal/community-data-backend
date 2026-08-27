const { test } = require('node:test');
const assert = require('node:assert/strict');
const {
  createRestoreCredentialsRepository, credentialHash, setUserEnabledWithRestoreRevocation,
} = require('./restoreCredentialsRepository');

function mockPool(query) {
  const calls = [];
  const connection = {
    beginTransaction: async () => { calls.push(['begin']); },
    commit: async () => { calls.push(['commit']); },
    rollback: async () => { calls.push(['rollback']); },
    release: () => { calls.push(['release']); },
    query: async (sql, parameters) => {
      calls.push([sql, parameters]);
      return query(sql, parameters);
    },
  };
  return {
    calls, connection,
    pool: {
      query: async (sql, parameters) => {
        calls.push([sql, parameters]);
        return query(sql, parameters);
      },
      getConnection: async () => connection,
    },
  };
}

test('schema creation is additive, shared between concurrent calls, and retryable after failure', async () => {
  let shouldFail = true;
  const f = mockPool(async () => {
    if (shouldFail) {
      shouldFail = false;
      throw new Error('temporary test failure');
    }
    return [[], []];
  });
  const repository = createRestoreCredentialsRepository(f.pool);
  await assert.rejects(() => repository.ensureSchema(), /temporary/);
  await Promise.all([repository.ensureSchema(), repository.ensureSchema()]);
  await repository.ensureSchema();
  assert.equal(f.calls.length, 3);
  assert.ok(f.calls.every(([sql]) => sql.startsWith('CREATE TABLE IF NOT EXISTS android_restore_')));
});

test('challenge consumption locks then deletes and commits exactly once', async () => {
  let row = {
    request_id: 'request', challenge: 'challenge', purpose: 'authentication',
    user_id: null, user_handle: null, auth_binding: null, is_fresh: 1,
  };
  const f = mockPool(async (sql) => {
    if (sql.startsWith('SELECT *, expires_at')) return [row ? [row] : [], []];
    if (sql.startsWith('DELETE FROM android_restore_challenges')) row = null;
    return [[], []];
  });
  const repository = createRestoreCredentialsRepository(f.pool);
  assert.equal((await repository.consumeChallenge('request', 'authentication')).challenge, 'challenge');
  assert.equal(await repository.consumeChallenge('request', 'authentication'), null);
  const firstSelect = f.calls.findIndex(([sql]) => sql.startsWith('SELECT *, expires_at'));
  assert.match(f.calls[firstSelect][0], /FOR UPDATE/);
  assert.match(f.calls[firstSelect + 1][0], /^DELETE FROM android_restore_challenges/);
  assert.equal(f.calls[firstSelect + 2][0], 'commit');
});

test('wrong-purpose, wrong-user and expired challenges are consumed without being returned', async (t) => {
  for (const changes of [{ purpose: 'registration' }, { user_id: 8 }, { is_fresh: 0 }]) {
    await t.test(JSON.stringify(changes), async () => {
      const row = { challenge: 'challenge', purpose: 'authentication', user_id: null, is_fresh: 1, ...changes };
      const f = mockPool(async (sql) => sql.startsWith('SELECT *, expires_at') ? [[row], []] : [[], []]);
      assert.equal(await createRestoreCredentialsRepository(f.pool).consumeChallenge('request', 'authentication'), null);
      const consumeStatement = changes.purpose === 'registration'
        ? 'UPDATE android_restore_challenges SET consumed_at'
        : 'DELETE FROM android_restore_challenges';
      assert.ok(f.calls.some(([sql]) => sql.startsWith(consumeStatement)));
      assert.ok(f.calls.some(([sql]) => sql === 'commit'));
    });
  }
});

test('authentication locks user then credential and advances counter before committing session', async () => {
  const user = { id: 7, role: 'beneficiary' };
  const row = {
    credential_id: 'credential', user_id: 7, user_handle: 'handle',
    public_key: Buffer.from([1, 2]), sign_count: 0, auth_binding: 'binding',
  };
  const f = mockPool(async (sql) => {
    if (sql.startsWith('SELECT user_id FROM android_restore_credentials')) return [[{ user_id: 7 }], []];
    if (sql.includes('FROM user AS u')) return [[user], []];
    if (sql.startsWith('SELECT * FROM android_restore_credentials')) return [[row], []];
    return [[], []];
  });
  const session = await createRestoreCredentialsRepository(f.pool).authenticateCredential('credential', async (value) => {
    assert.deepEqual(value.user, user);
    assert.deepEqual(value.credential.publicKey, new Uint8Array([1, 2]));
    return { counter: 3, session: { token: 'isolated-test-token' } };
  });
  assert.equal(session.token, 'isolated-test-token');
  const userLock = f.calls.findIndex(([sql]) => sql.includes('FROM user AS u'));
  const credentialLock = f.calls.findIndex(([sql]) => sql.startsWith('SELECT * FROM android_restore_credentials'));
  const update = f.calls.findIndex(([sql]) => sql.startsWith('UPDATE android_restore_credentials'));
  assert.ok(userLock < credentialLock && credentialLock < update);
  assert.match(f.calls[userLock][0], /FOR UPDATE/);
  assert.match(f.calls[credentialLock][0], /FOR UPDATE/);
  assert.deepEqual(f.calls[update][1], [3, credentialHash('credential')]);
  assert.equal(f.calls[update + 1][0], 'commit');
});

test('failed signature transaction rolls back without advancing a counter', async () => {
  const f = mockPool(async (sql) => {
    if (sql.startsWith('SELECT user_id FROM android_restore_credentials')) return [[{ user_id: 7 }], []];
    if (sql.includes('FROM user AS u')) return [[{ id: 7 }], []];
    if (sql.startsWith('SELECT * FROM android_restore_credentials')) {
      return [[{ credential_id: 'credential', user_id: 7, public_key: Buffer.from([1]), sign_count: 0 }], []];
    }
    return [[], []];
  });
  await assert.rejects(() => createRestoreCredentialsRepository(f.pool).authenticateCredential('credential', async () => {
    throw new Error('invalid test signature');
  }), /invalid test signature/);
  assert.equal(f.calls.some(([sql]) => sql.startsWith('UPDATE android_restore_credentials')), false);
  assert.deepEqual(f.calls.slice(-2), [['rollback'], ['release']]);
});

test('disable account revokes keys and pending enrollment in the same transaction, with DDL outside', async () => {
  const f = mockPool(async (sql) => sql.startsWith('UPDATE user') ? [{ affectedRows: 1 }, []] : [[], []]);
  assert.equal((await setUserEnabledWithRestoreRevocation(f.pool, 7, 'N')).affectedRows, 1);
  assert.ok(f.calls[0][0].startsWith('CREATE TABLE IF NOT EXISTS'));
  assert.ok(f.calls[1][0].startsWith('CREATE TABLE IF NOT EXISTS'));
  assert.equal(f.calls[2][0], 'begin');
  assert.match(f.calls[3][0], /^UPDATE user/);
  assert.deepEqual(f.calls[3][1], ['N', 7]);
  assert.match(f.calls[4][0], /^DELETE FROM android_restore_challenges WHERE user_id/);
  assert.match(f.calls[5][0], /^DELETE FROM android_restore_credentials WHERE user_id/);
  assert.deepEqual(f.calls.slice(-2), [['commit'], ['release']]);
});

test('disable account rolls back if revocation fails', async () => {
  const f = mockPool(async (sql) => {
    if (sql.startsWith('UPDATE user')) return [{ affectedRows: 1 }, []];
    if (sql.startsWith('DELETE FROM android_restore_credentials')) throw new Error('isolated DB failure');
    return [[], []];
  });
  await assert.rejects(() => setUserEnabledWithRestoreRevocation(f.pool, 7, 'N'), /isolated DB failure/);
  assert.equal(f.calls.some(([sql]) => sql === 'commit'), false);
  assert.deepEqual(f.calls.slice(-2), [['rollback'], ['release']]);
});

test('final registration locks and rechecks its consumed operation before inserting', async (t) => {
  for (const cancelled of [0, 1]) {
    await t.test('cancelled=' + cancelled, async () => {
      const f = mockPool(async (sql) => {
        if (sql.includes('FROM user AS u')) return [[{ id: 7 }], []];
        if (sql.startsWith('SELECT *, expires_at')) {
          return [[{
            purpose: 'registration', is_fresh: 1, consumed_at: new Date(),
            cancelled, user_id: 7, auth_binding: 'binding',
          }], []];
        }
        return [[], []];
      });
      const inserted = await createRestoreCredentialsRepository(f.pool).registerCredential({
        userId: 7, credentialId: 'key', registrationRequestId: 'request', authBinding: 'binding',
        userHandle: 'handle', publicKey: Buffer.from([1]), counter: 0, revocationHash: 'hash',
      }, () => true);
      assert.equal(inserted, !cancelled);
      const operationLock = f.calls.findIndex(([sql]) => sql.startsWith('SELECT *, expires_at'));
      const insert = f.calls.findIndex(([sql]) => sql.startsWith('INSERT INTO android_restore_credentials'));
      assert.match(f.calls[operationLock][0], /FOR UPDATE/);
      if (cancelled) assert.equal(insert, -1);
      else assert.ok(insert > operationLock);
    });
  }
});

test('cancellation locks the request before tombstone/delete and works after request cleanup', async () => {
  const f = mockPool(async () => [[], []]);
  await createRestoreCredentialsRepository(f.pool).cancelRegistration('request');
  const lock = f.calls.findIndex(([sql]) => sql.startsWith('SELECT request_id'));
  assert.match(f.calls[lock][0], /FOR UPDATE/);
  assert.match(f.calls[lock + 1][0], /^UPDATE android_restore_challenges SET cancelled/);
  assert.match(f.calls[lock + 2][0], /^DELETE FROM android_restore_credentials WHERE registration_request_id/);
  assert.deepEqual(f.calls[lock + 2][1], ['request']);
  assert.equal(f.calls[lock + 3][0], 'commit');
});
