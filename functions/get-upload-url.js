import { S3Client, PutObjectCommand } from '@aws-sdk/client-s3';
import { getSignedUrl } from '@aws-sdk/s3-request-presigner';
import { ulid } from 'ulid';

const s3Client = new S3Client();
const bucketName = process.env.BUCKET_NAME;

export const handler = async (event) => {
  const id = ulid();

  let key = `${event.identity.username}/${id}`; // Use timestamp to ensure unique file names

  const extension = event.arguments.extension;

  if (extension) {
    if (extension.startsWith('.')) {
      key += extension;
    } else {
      key += `.${extension}`;
    }
  }

  const contentType = event.arguments.contentType || 'image/jpg';

  if (!contentType.startsWith('image/')) {
    throw new Error('content type should be image');
  }

  const s3Params = {
    Bucket: bucketName,
    Key: key,
    ContentType: contentType,
    ACL: 'public-read', // TODO: cloud front
  };

  // Generate a pre-signed URL for uploading the object
  const command = new PutObjectCommand(s3Params);
  const presignedUrl = await getSignedUrl(s3Client, command, {
    expiresIn: 3600,
  });

  return presignedUrl;
};
