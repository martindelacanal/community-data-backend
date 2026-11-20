'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const { Resolver } = require('node:dns/promises');
const { validateClinicContactEmail } = require('./clinicContactEmail');

test('validates the optional single contact and caches DNS by domain only', async t => {
  const lookups = [];
  t.mock.method(Resolver.prototype, 'resolveMx', async domain => {
    lookups.push(domain);
    return [{ exchange: 'mail.example.test', priority: 10 }];
  });
  for (const value of [null, undefined, '', '  ']) {
    assert.deepEqual(await validateClinicContactEmail(value), { email: null });
  }
  for (const value of [42, ['alias@sample.test'], 'a..b@sample.test', 'a@sample.test,b@sample.test', 'a@127.0.0.1']) {
    assert.deepEqual(await validateClinicContactEmail(value), { error: 'INVALID_EMAIL' });
  }
  const addresses = [' One+tag@CAFÉ.test ', 'two@café.test'];
  assert.deepEqual(await Promise.all(addresses.map(validateClinicContactEmail)), [
    { email: 'One+tag@xn--caf-dma.test' }, { email: 'two@xn--caf-dma.test' }
  ]);
  await validateClinicContactEmail('three@café.test');
  assert.deepEqual(lookups, ['xn--caf-dma.test']);
});

test('rejects permanent domain failures, permits implicit MX and tolerates DNS outages', async t => {
  const fail = code => { throw Object.assign(new Error(code), { code }); };
  t.mock.method(Resolver.prototype, 'resolveMx', async domain => {
    if (domain === 'nonexistent.test') return fail('ENOTFOUND');
    if (domain === 'temporarily-down.test') return fail('ESERVFAIL');
    if (domain === 'null-mx.test') return [{ priority: 0, exchange: '.' }];
    if (domain === 'null-label.test') return [{ priority: 0, exchange: '' }];
    return fail('ENODATA');
  });
  t.mock.method(Resolver.prototype, 'resolve4', async domain => {
    if (domain === 'implicit-ipv4.test') return ['192.0.2.1'];
    if (domain === 'partial-timeout.test') return fail('ETIMEOUT');
    return fail('ENODATA');
  });
  t.mock.method(Resolver.prototype, 'resolve6', async domain =>
    domain === 'implicit-ipv6.test' ? ['2001:db8::1'] : fail('ENODATA'));
  for (const domain of ['nonexistent.test', 'null-mx.test', 'null-label.test', 'no-addresses.test']) {
    assert.deepEqual(await validateClinicContactEmail('a@' + domain), { error: 'EMAIL_DOMAIN_UNDELIVERABLE' });
  }
  for (const domain of ['temporarily-down.test', 'implicit-ipv4.test', 'implicit-ipv6.test', 'partial-timeout.test']) {
    const email = 'a@' + domain;
    assert.deepEqual(await validateClinicContactEmail(email), { email });
  }
});

test('rechecks an indeterminate domain, and expires a definitive result', async t => {
  t.mock.timers.enable({ apis: ['Date'] });
  let calls = 0;
  t.mock.method(Resolver.prototype, 'resolveMx', async () => {
    calls++;
    if (calls === 1) throw Object.assign(new Error(), { code: 'ETIMEOUT' });
    return calls === 2 ? [{ priority: 0, exchange: '.' }] : [{ priority: 5, exchange: 'mx.test' }];
  });
  const email = 'qa@changing-domain.test';
  assert.deepEqual(await validateClinicContactEmail(email), { email });
  assert.deepEqual(await validateClinicContactEmail(email), { error: 'EMAIL_DOMAIN_UNDELIVERABLE' });
  assert.deepEqual(await validateClinicContactEmail(email), { error: 'EMAIL_DOMAIN_UNDELIVERABLE' });
  assert.equal(calls, 2);
  t.mock.timers.tick(5 * 60 * 1000 + 1);
  assert.deepEqual(await validateClinicContactEmail(email), { email });
});

test('cancels slow DNS before allowing the registration to continue', async t => {
  t.mock.timers.enable({ apis: ['setTimeout'] });
  let rejectLookup;
  t.mock.method(Resolver.prototype, 'resolveMx', () => new Promise((resolve, reject) => { rejectLookup = reject; }));
  const cancel = t.mock.method(Resolver.prototype, 'cancel', () => rejectLookup(Object.assign(new Error(), { code: 'ECANCELLED' })));
  const email = 'participant@slow-domain.test';
  const result = validateClinicContactEmail(email);
  t.mock.timers.tick(1500);
  assert.deepEqual(await result, { email });
  assert.equal(cancel.mock.callCount(), 1);
});
