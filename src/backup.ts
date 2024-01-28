import { createReadStream, createWriteStream, unlink, statSync } from 'fs';
import { pipeline, Transform } from 'stream';
import * as zlib from 'zlib';
import * as crypto from 'crypto';
import { promisify } from 'util';
import path from 'path';
import os from 'os';

import { env } from './env';

const pipelineAsync = promisify(pipeline);

const uploadToS3 = async ({ name, path }: { name: string, path: string }) => {
  console.log("Uploading backup to S3...");

  const bucket = env.AWS_S3_BUCKET;

  const clientOptions: S3ClientConfig = {
    region: env.AWS_S3_REGION
  }

  if (env.AWS_S3_ENDPOINT) {
    console.log(`Using custom endpoint: ${env.AWS_S3_ENDPOINT}`)
    clientOptions['endpoint'] = env.AWS_S3_ENDPOINT;
  }

  const client = new S3Client(clientOptions);

  await new Upload({
    client,
    params: {
      Bucket: bucket,
      Key: name,
      Body: createReadStream(path),
    },
  }).done();

  console.log("Backup uploaded to S3...");
}


const dumpToFile = async (filePath: string) => {
  console.log('Dumping DB to file...');

  const dumpStream = exec(`pg_dump -d ${env.BACKUP_DATABASE_URL} -Fc`);

  const compressStream = zlib.createGzip();
  const encryptStream = crypto.createCipher('aes-256-cbc', env.BACKUP_ENCRYPTION_PASSWORD);

  const writeStream = createWriteStream(filePath);

  // Pipe the dump output through compression and encryption streams to the file
  await pipelineAsync(
    dumpStream.stdout!,
    compressStream,
    encryptStream,
    writeStream
  );

  // Handle errors
  dumpStream.on('error', (error) => {
    console.error('Error dumping database:', error);
    throw error;
  });

  writeStream.on('error', (error) => {
    console.error('Error writing to file:', error);
    throw error;
  });

  // Wait for the streams to finish
  await Promise.all([
    new Promise<void>((resolve) => dumpStream.on('close', resolve)),
    new Promise<void>((resolve) => writeStream.on('close', resolve))
  ]);

  console.log('Backup archive file is valid');
  console.log('Backup filesize:', filesize(statSync(filePath).size);

  console.log('DB dumped to file...');
};

const deleteFile = async (path: string) => {
  console.log("Deleting file...");
  await new Promise((resolve, reject) => {
    unlink(path, (err) => {
      reject({ error: err });
      return;
    });
    resolve(undefined);
  });
}

export const backup = async () => {
  console.log("Initiating DB backup...");

  const date = new Date().toISOString();
  const timestamp = date.replace(/[:.]+/g, '-');
  const filename = `${env.BACKUP_PROJECT_NAME}-${timestamp}.dump`;
  const filepath = path.join(os.tmpdir(), filename);

  await dumpToFile(filepath);
  await uploadToS3({ name: filename, path: filepath });
  await deleteFile(filepath);

  console.log("DB backup complete...");
}
