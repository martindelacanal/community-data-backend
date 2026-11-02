'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const { buildStaffPasswordRecoveryEmail } = require('./staffPasswordRecovery');

test('reset emails use the browser-only API origin and keep the token out of the query string', () => {
  for (const language of ['en', 'es']) {
    const token = 'synthetic.token.signature';
    const mail = buildStaffPasswordRecoveryEmail({ email: 'staff@example.test', token, language });
    assert.equal(mail.to, 'staff@example.test');
    assert.equal(mail.from, 'bienestarcommunity@gmail.com');
    assert.equal(mail.attachments, undefined);
    const url = new URL(mail.text.match(/https:\/\/\S+/)[0]);
    assert.equal(url.origin, 'https://api.bienestarcommunity.org');
    assert.equal(url.pathname, '/api/auth/password-reset/open');
    assert.equal(url.search, '');
    const fragment = new URLSearchParams(url.hash.slice(1));
    assert.equal(fragment.get('token'), token);
    assert.equal(fragment.get('lang'), language);
    assert.ok(mail.text.includes('20'));
    assert.ok(mail.html.includes('&amp;lang='));
    assert.match(mail.text, language === 'es' ? /solo puede usarse una vez/ : /only be used once/);
    assert.match(mail.text, language === 'es' ? /Tu contraseña no se modificará/ : /password will remain unchanged/);
  }
});

test('unexpected language falls back to English and link input cannot inject markup or query parameters', () => {
  const token = '"><script>alert(1)</script>&lang=es?extra=true';
  const mail = buildStaffPasswordRecoveryEmail({ email: 'staff@example.test', token, language: '<script>' });
  assert.match(mail.subject, /^Reset your/);
  assert.equal(mail.html.includes('<script>'), false);
  const url = new URL(mail.text.match(/https:\/\/\S+/)[0]);
  assert.equal(url.search, '');
  const fragment = new URLSearchParams(url.hash.slice(1));
  assert.equal(fragment.get('token'), token);
  assert.equal(fragment.get('lang'), 'en');
});
