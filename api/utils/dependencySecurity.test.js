'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const { parse } = require('csv-parse/sync');
const { v5: uuid } = require('uuid');
const XLSX = require('xlsx');
const nodemailer = require('nodemailer');
const sharp = require('sharp');

test('CSV duplicate prototype-named headers remain own data properties without replacing record prototypes', () => {
  // Regression for GHSA-8cw4-87c7-c6xx, including the duplicate-column path.
  const [record] = parse('__proto__,__proto__,constructor,prototype,name\nfirst,second,third,fourth,Synthetic\n', {
    columns: true, group_columns_by_name: true,
  });
  assert.equal(Object.getPrototypeOf(record), Object.prototype);
  assert.equal(Object.hasOwn(record, '__proto__'), true);
  assert.deepEqual(record.__proto__, ['first', 'second']);
  assert.equal(Object.hasOwn(record, 'constructor'), true);
  assert.equal(record.constructor, 'third');
  assert.equal(record.prototype, 'fourth');
  assert.equal(record.name, 'Synthetic');
  assert.equal(record[0], undefined, 'CSV data must not become inherited array entries');
});

test('UUID v5 keeps stable identifiers and rejects undersized or out-of-range output buffers', () => {
  assert.equal(uuid('www.example.com', uuid.DNS), '2ed6657d-e927-568b-95e1-2665a8aea6a2');
  const output = new Uint8Array(18).fill(0xa5);
  assert.equal(uuid('www.example.com', uuid.DNS, output, 1), output);
  assert.equal(Buffer.from(output.subarray(1, 17)).toString('hex'), '2ed6657de927568b95e12665a8aea6a2');
  assert.equal(output[0], 0xa5);
  assert.equal(output[17], 0xa5);
  for (const [buffer, offset] of [[new Uint8Array(15), 0], [new Uint8Array(16), 1], [new Uint8Array(16), -1]]) {
    assert.throws(() => uuid('www.example.com', uuid.DNS, buffer, offset), RangeError);
  }
});

test('updated SheetJS preserves the import-script row API using an entirely synthetic in-memory workbook', () => {
  const rows = [['Name', 'Count', 'Email'], ['Synthetic Álvarez', 3, 'synthetic@example.invalid'], ['Blank email', 0, null]];
  const workbook = XLSX.utils.book_new();
  XLSX.utils.book_append_sheet(workbook, XLSX.utils.aoa_to_sheet(rows), 'Synthetic registrants');
  const bytes = XLSX.write(workbook, { type: 'buffer', bookType: 'xlsx' });
  assert.ok(Buffer.isBuffer(bytes));
  const parsed = XLSX.read(bytes, { type: 'buffer' });
  assert.deepEqual(parsed.SheetNames, ['Synthetic registrants']);
  assert.deepEqual(XLSX.utils.sheet_to_json(parsed.Sheets[parsed.SheetNames[0]], { header: 1, raw: false, defval: null }),
    [['Name', 'Count', 'Email'], ['Synthetic Álvarez', '3', 'synthetic@example.invalid'], ['Blank email', '0', null]]);
});

test('Nodemailer composes report attachments offline and honors disabled attachment file access', async () => {
  // Stream transport only composes MIME bytes; it has no SMTP/network transport.
  const transporter = nodemailer.createTransport({ streamTransport: true, buffer: true, disableFileAccess: true, disableUrlAccess: true });
  const message = {
    from: 'synthetic-sender@example.invalid', to: 'synthetic-recipient@example.invalid',
    subject: 'Synthetic report', text: 'Synthetic report body',
    attachments: [{ filename: 'synthetic.txt', content: Buffer.from('synthetic attachment') }],
  };
  const result = await transporter.sendMail(message);
  assert.ok(Buffer.isBuffer(result.message));
  const mime = result.message.toString('utf8');
  assert.match(mime, /To: synthetic-recipient@example\.invalid/);
  assert.match(mime, /Subject: Synthetic report/);
  assert.match(mime, /filename=synthetic\.txt/);
  assert.match(mime, /c3ludGhldGljIGF0dGFjaG1lbnQ=/);
  await assert.rejects(transporter.sendMail({ ...message,
    attachments: [{ filename: 'blocked.txt', path: 'synthetic-file-access-must-stay-disabled.txt' }],
  }), /File access rejected/i);
  transporter.close();
});

test('updated Sharp decodes and transforms synthetic image buffers while rejecting invalid image bytes', async () => {
  const original = await sharp({ create: { width: 4, height: 2, channels: 3, background: '#28714f' } }).png().toBuffer();
  const transformed = await sharp(original).resize({ width: 2 }).webp().toBuffer();
  const metadata = await sharp(transformed).metadata();
  assert.equal(metadata.format, 'webp');
  assert.equal(metadata.width, 2);
  assert.equal(metadata.height, 1);
  await assert.rejects(sharp(Buffer.from('synthetic text, not an image')).metadata(), /unsupported image format/i);
});
