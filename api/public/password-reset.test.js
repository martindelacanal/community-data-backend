const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');
const vm = require('node:vm');

const html = fs.readFileSync(path.join(__dirname, 'password-reset.html'), 'utf8');
const script = html.match(/<script>([\s\S]*?)<\/script>/)[1];

function page({ hash = '#token=single-use-secret&lang=en', reply, deferReady = false } = {}) {
  const nodes = new Map();
  for (const match of html.matchAll(/<[^>]+\bid="([^"]+)"[^>]*>/g)) {
    const node = {
      value: '', textContent: '', hidden: match[0].includes(' hidden'), disabled: false, attributes: {}, listeners: {},
      dataset: { text: match[0].match(/data-text="([^"]+)"/)?.[1] },
      setAttribute(name, value) { this.attributes[name] = value; },
      addEventListener(name, handler) { this.listeners[name] = handler; },
      focus() { this.focused = true; }
    };
    nodes.set(match[1], node);
  }
  const listeners = {};
  const windowListeners = {};
  const requests = [];
  const replacements = [];
  const timers = new Map();
  let nextTimer = 0;
  let reloads = 0;
  const document = {
    documentElement: { lang: '' }, title: '',
    getElementById: id => {
      assert.ok(nodes.has(id), `The document must contain #${id}`);
      return nodes.get(id);
    },
    querySelectorAll: () => [...nodes.values()].filter(node => node.dataset.text),
    addEventListener: (name, handler) => { listeners[name] = handler; }
  };
  const context = {
    document, URLSearchParams, TextEncoder, AbortController,
    window: {
      location: { hash, pathname: '/api/auth/password-reset/open', search: '', reload: () => { reloads++; } },
      history: { state: null, replaceState: (...args) => replacements.push(args) },
      addEventListener: (name, handler) => { windowListeners[name] = handler; }
    },
    setTimeout: callback => { const id = ++nextTimer; timers.set(id, callback); return id; },
    clearTimeout: id => timers.delete(id),
    fetch: async (url, options) => {
      requests.push({ url, options, body: JSON.parse(options.body) });
      return reply ? reply(url, options) : { ok: true, status: 200, json: async () => ({ status: 'password_changed', username: 'actual-user' }) };
    }
  };
  vm.runInNewContext(script, context);
  if (!deferReady) listeners.DOMContentLoaded();
  return {
    nodes, requests, replacements, document, windowListeners, timers, location: context.window.location,
    get reloads() { return reloads; },
    ready: () => listeners.DOMContentLoaded(),
    submit(password = 'new-password', confirmation = password) {
      nodes.get('password').value = password;
      nodes.get('confirmation').value = confirmation;
      return nodes.get('reset-form').listeners.submit({ preventDefault() {} });
    },
    request(identifier = 'staff') {
      nodes.get('identifier').value = identifier;
      return nodes.get('request-form').listeners.submit({ preventDefault() {} });
    },
    click: id => nodes.get(id).listeners.click()
  };
}

test('removes the secret fragment before DOM ready and never submits on load', () => {
  const view = page({ deferReady: true });
  assert.deepEqual(view.replacements, [[null, '', '/api/auth/password-reset/open']]);
  assert.equal(view.requests.length, 0);
  view.ready();
  assert.equal(view.nodes.get('reset-form').hidden, false);
  assert.equal(view.requests.length, 0);
  assert.equal(/localStorage|sessionStorage|console\./.test(script), false);
  assert.equal(/<script[^>]+src=|<link[^>]+href=|<img[^>]+src=/.test(html), false);
});

test('missing or empty tokens show the email request form without starting a request', async () => {
  for (const hash of ['', '#token=']) {
    const view = page({ hash });
    assert.equal(view.nodes.get('reset-form').hidden, true);
    assert.equal(view.nodes.get('request-form').hidden, false);
    assert.equal(view.nodes.get('error').hidden, true);
    await view.submit();
    assert.equal(view.requests.length, 0);
  }
});

test('a new email link opened in the same tab reloads to consume its fragment without submitting it', () => {
  const view = page({ hash: '' });
  view.location.hash = '#token=new-email-secret&lang=es';
  view.windowListeners.hashchange();
  assert.equal(view.reloads, 1);
  assert.equal(view.requests.length, 0);
  view.location.hash = '#unrelated';
  view.windowListeners.hashchange();
  assert.equal(view.reloads, 1);
});

test('email request works on the API host and keeps the same generic outcome for unknown accounts', async () => {
  const view = page({ hash: '#lang=es', reply: async () => ({ ok: true, status: 202, json: async () => ({ status: 'accepted' }) }) });
  await view.request(' Staff@example.org ');
  assert.equal(view.requests[0].url, './request');
  assert.deepEqual(view.requests[0].body, { identifier: 'Staff@example.org', language: 'es' });
  assert.equal(view.nodes.get('sent').hidden, false);
  assert.equal(view.nodes.get('request-form').hidden, true);
  assert.equal(view.nodes.get('identifier').value, '');
  assert.equal(view.nodes.get('title').textContent, 'Revisa tu correo');
  assert.ok(html.includes('href="./open"'));
  await view.request();
  assert.equal(view.requests.length, 1);
});

test('invalid identifiers stay local, while email service errors are visible and retryable', async () => {
  const view = page({ hash: '', reply: async () => ({ ok: false, status: 503, json: async () => ({ error: 'unavailable' }) }) });
  await view.request('   ');
  assert.equal(view.requests.length, 0);
  assert.match(view.nodes.get('error').textContent, /valid email or username/);
  await view.request('staff');
  assert.match(view.nodes.get('error').textContent, /could not send/);
  assert.equal(view.nodes.get('request-submit').disabled, false);
  await view.request('staff');
  assert.equal(view.requests.length, 2);
});

test('email requests cannot be duplicated while the first request is pending', async () => {
  let resolve;
  const view = page({ hash: '', reply: () => new Promise(done => { resolve = done; }) });
  const pending = view.request();
  assert.equal(view.nodes.get('request-submit').disabled, true);
  await view.request();
  assert.equal(view.requests.length, 1);
  resolve({ ok: true, status: 202, json: async () => ({ status: 'accepted' }) });
  await pending;
});

test('honors the email language and permits switching without losing token', async () => {
  const view = page({ hash: '#token=secret&lang=es' });
  assert.equal(view.document.documentElement.lang, 'es');
  assert.equal(view.nodes.get('title').textContent, 'Elige una nueva contraseña');
  view.click('english');
  assert.equal(view.document.documentElement.lang, 'en');
  await view.submit();
  assert.equal(view.requests[0].body.token, 'secret');
});

test('checks Unicode character count, UTF-8 byte limit and matching passwords before sending', async () => {
  for (const [password, confirmation] of [['short', 'short'], ['😀'.repeat(4), '😀'.repeat(4)], ['😀'.repeat(19), '😀'.repeat(19)], ['password1', 'password2']]) {
    const view = page();
    await view.submit(password, confirmation);
    assert.equal(view.requests.length, 0);
    assert.equal(view.nodes.get('error').hidden, false);
  }
});

test('successful reset sends only token/password, preserves password whitespace and clears credentials', async () => {
  const view = page();
  await view.submit(' leading and trailing ');
  assert.equal(view.requests.length, 1);
  assert.equal(view.requests[0].url, './complete');
  assert.deepEqual(view.requests[0].body, { token: 'single-use-secret', password: ' leading and trailing ' });
  assert.equal(view.requests[0].options.credentials, 'omit');
  assert.equal(view.requests[0].options.cache, 'no-store');
  assert.equal(view.nodes.get('password').value, '');
  assert.equal(view.nodes.get('confirmation').value, '');
  assert.equal(view.nodes.get('reset-form').hidden, true);
  assert.equal(view.nodes.get('success').hidden, false);
  assert.equal(view.nodes.get('username').textContent, 'Username: actual-user');
  await view.submit();
  assert.equal(view.requests.length, 1);
});

test('accepts an eight-code-point emoji password within bcrypt byte limit', async () => {
  const view = page();
  await view.submit('😀'.repeat(8));
  assert.equal(view.requests.length, 1);
});

test('does not allow duplicate submission while a request is pending', async () => {
  let resolve;
  const view = page({ reply: () => new Promise(done => { resolve = done; }) });
  const pending = view.submit();
  assert.equal(view.nodes.get('submit').disabled, true);
  await view.submit();
  assert.equal(view.requests.length, 1);
  resolve({ ok: true, status: 200, json: async () => ({ status: 'password_changed' }) });
  await pending;
  assert.equal(view.nodes.get('submit').disabled, false);
});

test('invalid or expired links clear token and password and offer a new request', async () => {
  const view = page({ reply: async () => ({ ok: false, status: 400, json: async () => ({ error: 'invalid_or_expired_token' }) }) });
  await view.submit();
  assert.match(view.nodes.get('error').textContent, /Request a new link/);
  assert.equal(view.nodes.get('password').value, '');
  assert.equal(view.nodes.get('reset-form').hidden, true);
  await view.submit();
  assert.equal(view.requests.length, 1);
});

for (const status of [400, 429, 503]) {
  test(`HTTP ${status} failure is visible and retryable`, async () => {
    const view = page({ reply: async () => ({ ok: false, status, json: async () => ({ error: status === 400 ? 'invalid_password' : 'unavailable' }) }) });
    await view.submit();
    assert.equal(view.nodes.get('error').hidden, false);
    assert.equal(view.nodes.get('submit').disabled, false);
    await view.submit();
    assert.equal(view.requests.length, 2);
  });
}

test('network and timeout failures release the submit button and show an error', async () => {
  const view = page({ reply: (_url, options) => new Promise((_resolve, reject) => {
    options.signal.addEventListener('abort', () => reject(new Error('aborted')));
  }) });
  const pending = view.submit();
  [...view.timers.values()][0]();
  await pending;
  assert.equal(view.nodes.get('submit').disabled, false);
  assert.match(view.nodes.get('error').textContent, /Check your connection/);
});

test('leaving the page clears the token and passwords even if restored from browser history', async () => {
  const view = page();
  view.nodes.get('password').value = 'sensitive-password';
  view.windowListeners.pagehide();
  view.windowListeners.pageshow();
  assert.equal(view.nodes.get('password').value, '');
  assert.equal(view.nodes.get('reset-form').hidden, true);
  await view.submit();
  assert.equal(view.requests.length, 0);
});
