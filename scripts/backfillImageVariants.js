const path = require('path');
require('dotenv').config({
  path: path.resolve(__dirname, '../.env')
});

const mysql = require('mysql2/promise');
const {
  regenerateImageVariants
} = require('../api/services/imageVariants');

function parseArgs(argv) {
  const args = new Set(argv.slice(2));
  const limitArg = argv.slice(2).find((arg) => arg.startsWith('--limit='));
  const limit = limitArg ? Number.parseInt(limitArg.split('=')[1], 10) : null;

  return {
    includeArticles: !args.has('--trusted-only'),
    includeTrustedResources: !args.has('--articles-only'),
    dryRun: args.has('--dry-run'),
    limit: Number.isFinite(limit) && limit > 0 ? limit : null
  };
}

async function createPool() {
  return mysql.createPool({
    connectionLimit: 10,
    host: process.env.DB_HOST,
    user: process.env.DB_USER,
    password: process.env.DB_PASSWORD,
    database: process.env.DB_DATABASE,
    port: process.env.DB_PORT,
    decimalNumbers: true
  });
}

async function backfillArticlePreviewVariants(pool, options) {
  const limitClause = options.limit ? 'LIMIT ?' : '';
  const params = options.limit ? [options.limit] : [];
  const [rows] = await pool.query(
    `SELECT id, article_id, image_type, s3_key, s3_key_small, s3_key_medium, mime_type
     FROM article_images
     WHERE image_type IN ('preview_en', 'preview_es')
       AND s3_key IS NOT NULL
       AND (s3_key_small IS NULL OR s3_key_medium IS NULL)
     ORDER BY id ASC
     ${limitClause}`,
    params
  );

  const summary = {
    scanned: rows.length,
    updated: 0,
    skipped: 0,
    failed: 0
  };

  for (const row of rows) {
    try {
      if (options.dryRun) {
        summary.skipped += 1;
        console.log(`[dry-run] article_images.id=${row.id} article_id=${row.article_id} type=${row.image_type}`);
        continue;
      }

      const variants = await regenerateImageVariants({
        originalKey: row.s3_key,
        presetName: 'article',
        contentType: row.mime_type || null
      });

      await pool.query(
        `UPDATE article_images
         SET s3_key_small = ?, s3_key_medium = ?, width = COALESCE(width, ?), height = COALESCE(height, ?)
         WHERE id = ?`,
        [
          variants.smallKey,
          variants.mediumKey,
          variants.metadata?.width || null,
          variants.metadata?.height || null,
          row.id
        ]
      );

      summary.updated += 1;
      console.log(`[ok] article_images.id=${row.id} article_id=${row.article_id} type=${row.image_type}`);
    } catch (error) {
      summary.failed += 1;
      console.error(`[error] article_images.id=${row.id}: ${error.message}`);
    }
  }

  return summary;
}

async function backfillTrustedResourceVariants(pool, options) {
  const limitClause = options.limit ? 'LIMIT ?' : '';
  const params = options.limit ? [options.limit] : [];
  const [rows] = await pool.query(
    `SELECT id, image_url, image_url_small, image_url_medium
     FROM trusted_resources
     WHERE image_url IS NOT NULL
       AND (image_url_small IS NULL OR image_url_medium IS NULL)
     ORDER BY id ASC
     ${limitClause}`,
    params
  );

  const summary = {
    scanned: rows.length,
    updated: 0,
    skipped: 0,
    failed: 0
  };

  for (const row of rows) {
    try {
      if (options.dryRun) {
        summary.skipped += 1;
        console.log(`[dry-run] trusted_resources.id=${row.id}`);
        continue;
      }

      const variants = await regenerateImageVariants({
        originalKey: row.image_url,
        presetName: 'trustedResource'
      });

      await pool.query(
        `UPDATE trusted_resources
         SET image_url_small = ?, image_url_medium = ?
         WHERE id = ?`,
        [
          variants.smallKey,
          variants.mediumKey,
          row.id
        ]
      );

      summary.updated += 1;
      console.log(`[ok] trusted_resources.id=${row.id}`);
    } catch (error) {
      summary.failed += 1;
      console.error(`[error] trusted_resources.id=${row.id}: ${error.message}`);
    }
  }

  return summary;
}

async function main() {
  const options = parseArgs(process.argv);
  const pool = await createPool();

  try {
    console.log('Starting image variants backfill...');
    console.log(JSON.stringify(options, null, 2));

    if (options.includeArticles) {
      const articleSummary = await backfillArticlePreviewVariants(pool, options);
      console.log('Article preview summary:', articleSummary);
    }

    if (options.includeTrustedResources) {
      const trustedSummary = await backfillTrustedResourceVariants(pool, options);
      console.log('Trusted resources summary:', trustedSummary);
    }

    console.log('Backfill finished.');
  } finally {
    await pool.end();
  }
}

main().catch((error) => {
  console.error('Backfill failed:', error);
  process.exitCode = 1;
});
