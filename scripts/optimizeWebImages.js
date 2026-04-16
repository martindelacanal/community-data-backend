/**
 * optimizeWebImages.js
 *
 * Generates optimized responsive variants (sm, md, lg) for every enabled
 * image in the web_images table and uploads them to S3.
 *
 * Usage:  node scripts/optimizeWebImages.js
 *
 * Prerequisites — run the ALTER TABLE from OPTIMIZE_WEB_IMAGES.md first.
 */

const path = require('path');
require('dotenv').config({ path: path.resolve(__dirname, '../.env') });

const sharp = require('sharp');
const mysql = require('mysql2/promise');
const {
  S3Client,
  GetObjectCommand,
  PutObjectCommand,
} = require('@aws-sdk/client-s3');

// ── Configuration ──────────────────────────────────────────────────────────────

const VARIANTS = [
  { suffix: 'sm', maxWidth: 480 },   // phones 1×
  { suffix: 'md', maxWidth: 960 },   // phones 2×, tablets 1×
  { suffix: 'lg', maxWidth: 1440 },  // desktop 1×, tablets 2×
];

const JPEG_QUALITY = 100;

// ── AWS S3 ─────────────────────────────────────────────────────────────────────

const s3 = new S3Client({
  credentials: {
    accessKeyId: process.env.ACCESS_KEY,
    secretAccessKey: process.env.SECRET_ACCESS_KEY,
  },
  region: process.env.BUCKET_REGION,
});

const BUCKET = process.env.BUCKET_NAME;

async function downloadFromS3(key) {
  const command = new GetObjectCommand({ Bucket: BUCKET, Key: key });
  const response = await s3.send(command);
  const chunks = [];
  for await (const chunk of response.Body) {
    chunks.push(chunk);
  }
  return Buffer.concat(chunks);
}

async function uploadToS3(key, buffer) {
  const command = new PutObjectCommand({
    Bucket: BUCKET,
    Key: key,
    Body: buffer,
    ContentType: 'image/jpeg',
  });
  await s3.send(command);
}

// ── Main ───────────────────────────────────────────────────────────────────────

async function main() {
  console.log('Connecting to database...');

  const connection = await mysql.createConnection({
    host: process.env.DB_HOST,
    user: process.env.DB_USER,
    password: process.env.DB_PASSWORD,
    database: process.env.DB_DATABASE,
    port: process.env.DB_PORT || 3306,
  });

  try {
    const [rows] = await connection.query(
      "SELECT id, file_hash, original_filename, mime_type FROM web_images WHERE enabled = 'Y' AND file_hash IS NOT NULL"
    );

    console.log(`Found ${rows.length} image(s) to process.\n`);

    for (const image of rows) {
      console.log(`--- ${image.original_filename} (id=${image.id}) ---`);
      console.log(`  S3 key: ${image.file_hash}`);

      let originalBuffer;
      try {
        originalBuffer = await downloadFromS3(image.file_hash);
      } catch (err) {
        console.error(`  ERROR downloading from S3: ${err.message}`);
        continue;
      }

      const meta = await sharp(originalBuffer).metadata();
      const originalKB = (originalBuffer.length / 1024).toFixed(0);
      console.log(`  Original: ${meta.width}x${meta.height}, ${originalKB} KB`);

      for (const variant of VARIANTS) {
        const variantKey = `${image.file_hash}_${variant.suffix}`;
        const column = `file_hash_${variant.suffix}`;

        let pipeline = sharp(originalBuffer);

        // Only resize if the original is wider than the target
        if (meta.width > variant.maxWidth) {
          pipeline = pipeline.resize({ width: variant.maxWidth, withoutEnlargement: true });
        }

        const optimized = await pipeline
          .jpeg({ quality: JPEG_QUALITY, progressive: true })
          .toBuffer();

        const optimizedMeta = await sharp(optimized).metadata();
        const optimizedKB = (optimized.length / 1024).toFixed(0);

        await uploadToS3(variantKey, optimized);
        await connection.query(
          `UPDATE web_images SET \`${column}\` = ? WHERE id = ?`,
          [variantKey, image.id]
        );

        console.log(
          `  ${variant.suffix}: ${optimizedMeta.width}x${optimizedMeta.height}, ${optimizedKB} KB -> ${variantKey}`
        );
      }

      console.log('');
    }

    console.log('Done. All images optimized successfully.');
  } finally {
    await connection.end();
  }
}

main().catch((err) => {
  console.error('Fatal error:', err);
  process.exit(1);
});
