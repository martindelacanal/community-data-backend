const sharp = require('sharp');
const {
  S3Client,
  PutObjectCommand,
  GetObjectCommand,
  DeleteObjectsCommand
} = require('@aws-sdk/client-s3');

const bucketName = process.env.BUCKET_NAME;
const bucketRegion = process.env.BUCKET_REGION;
const accessKey = process.env.ACCESS_KEY;
const secretAccessKey = process.env.SECRET_ACCESS_KEY;

const s3 = new S3Client({
  credentials: {
    accessKeyId: accessKey,
    secretAccessKey: secretAccessKey,
  },
  region: bucketRegion,
});

const IMAGE_VARIANT_PRESETS = {
  article: {
    small: { width: 512, quality: 80 },
    medium: { width: 960, quality: 82 }
  },
  trustedResource: {
    small: { width: 240, quality: 80 },
    medium: { width: 512, quality: 82 }
  }
};

function buildVariantKey(originalKey, variantName) {
  return `${originalKey}__${variantName}`;
}

function getVariantPreset(presetName) {
  const preset = IMAGE_VARIANT_PRESETS[presetName];

  if (!preset) {
    throw new Error(`Unknown image variant preset: ${presetName}`);
  }

  return preset;
}

function isAnimatedMetadata(metadata) {
  return Number(metadata?.pages || 1) > 1;
}

function shouldSkipVariantGeneration(metadata, mimeType) {
  return mimeType === 'image/svg+xml' || metadata?.format === 'svg' || isAnimatedMetadata(metadata);
}

async function streamToBuffer(stream) {
  if (!stream) {
    return Buffer.alloc(0);
  }

  if (Buffer.isBuffer(stream)) {
    return stream;
  }

  if (typeof stream.transformToByteArray === 'function') {
    const byteArray = await stream.transformToByteArray();
    return Buffer.from(byteArray);
  }

  const chunks = [];

  return new Promise((resolve, reject) => {
    stream.on('data', (chunk) => {
      chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
    });
    stream.on('end', () => resolve(Buffer.concat(chunks)));
    stream.on('error', reject);
  });
}

async function getImageMetadata(buffer, mimeType) {
  try {
    return await sharp(buffer, {
      animated: mimeType === 'image/gif' || mimeType === 'image/webp'
    }).metadata();
  } catch (error) {
    return {};
  }
}

async function buildImageVariants(buffer, mimeType, presetName) {
  const metadata = await getImageMetadata(buffer, mimeType);

  if (shouldSkipVariantGeneration(metadata, mimeType)) {
    return {
      metadata,
      variants: {}
    };
  }

  const presets = getVariantPreset(presetName);
  const variants = {};

  for (const [variantName, preset] of Object.entries(presets)) {
    const variantBuffer = await sharp(buffer)
      .rotate()
      .resize({
        width: preset.width,
        withoutEnlargement: true,
        fit: 'inside'
      })
      .webp({
        quality: preset.quality
      })
      .toBuffer();

    variants[variantName] = {
      buffer: variantBuffer,
      contentType: 'image/webp'
    };
  }

  return {
    metadata,
    variants
  };
}

async function uploadBufferToS3(key, buffer, contentType) {
  await s3.send(new PutObjectCommand({
    Bucket: bucketName,
    Key: key,
    Body: buffer,
    ContentType: contentType
  }));
}

async function uploadImageWithVariants({
  originalKey,
  buffer,
  contentType,
  presetName
}) {
  let metadata;
  let variants;

  try {
    const builtVariants = await buildImageVariants(buffer, contentType, presetName);
    metadata = builtVariants.metadata;
    variants = builtVariants.variants;
  } catch (error) {
    metadata = await getImageMetadata(buffer, contentType);
    variants = {};
  }

  const uploadTasks = [
    uploadBufferToS3(originalKey, buffer, contentType)
  ];

  const variantKeys = {
    originalKey,
    smallKey: null,
    mediumKey: null,
    metadata
  };

  for (const [variantName, variant] of Object.entries(variants)) {
    const variantKey = buildVariantKey(originalKey, variantName);
    variantKeys[`${variantName}Key`] = variantKey;
    uploadTasks.push(uploadBufferToS3(variantKey, variant.buffer, variant.contentType));
  }

  await Promise.all(uploadTasks);

  return variantKeys;
}

async function downloadS3Object(key) {
  const response = await s3.send(new GetObjectCommand({
    Bucket: bucketName,
    Key: key
  }));

  return {
    buffer: await streamToBuffer(response.Body),
    contentType: response.ContentType || null
  };
}

async function regenerateImageVariants({
  originalKey,
  presetName,
  contentType = null
}) {
  const downloaded = await downloadS3Object(originalKey);
  const effectiveContentType = contentType || downloaded.contentType || 'application/octet-stream';
  const { metadata, variants } = await buildImageVariants(downloaded.buffer, effectiveContentType, presetName);

  const result = {
    originalKey,
    smallKey: null,
    mediumKey: null,
    metadata,
    contentType: effectiveContentType
  };

  const uploadTasks = [];

  for (const [variantName, variant] of Object.entries(variants)) {
    const variantKey = buildVariantKey(originalKey, variantName);
    result[`${variantName}Key`] = variantKey;
    uploadTasks.push(uploadBufferToS3(variantKey, variant.buffer, variant.contentType));
  }

  await Promise.all(uploadTasks);

  return result;
}

async function deleteS3Objects(keys) {
  const uniqueKeys = Array.from(new Set((keys || []).filter(Boolean)));

  if (uniqueKeys.length === 0) {
    return;
  }

  await s3.send(new DeleteObjectsCommand({
    Bucket: bucketName,
    Delete: {
      Objects: uniqueKeys.map((key) => ({ Key: key })),
      Quiet: true
    }
  }));
}

module.exports = {
  IMAGE_VARIANT_PRESETS,
  buildVariantKey,
  buildImageVariants,
  deleteS3Objects,
  downloadS3Object,
  getImageMetadata,
  regenerateImageVariants,
  uploadImageWithVariants
};
