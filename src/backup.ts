import { exec, execSync } from "child_process";
import { S3Client, S3ClientConfig } from "@aws-sdk/client-s3";
import { Upload } from "@aws-sdk/lib-storage";
import { createReadStream, unlink, statSync } from "fs";
import { filesize } from "filesize";
import path from "path";
import os from "os";

import { env } from "./env";

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
  console.log("Dumping DB to file...");

  await new Promise((resolve, reject) => {
    exec(`pg_dump -d ${env.BACKUP_DATABASE_URL} -Ft > ${filePath}`, (error, stdout, stderr) => {
      // ... error handling and logging ...
    });
  });

  console.log("DB dumped to file...");
}

const zipAndPasswordProtect = async (filePath: string) => {
  console.log("Zipping and password protecting file...");

  await new Promise((resolve, reject) => {
    exec(`zip -P ${env.BACKUP_PASSWORD} -r ${filePath}.zip ${filePath}`, (error, stdout, stderr) => {
      if (error) {
        reject({ error: error, stderr: stderr.trimEnd() });
        return;
      }

      console.log("File zipped and password protected...");
      resolve(undefined);
    });
  });
}

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
  const filename = `${env.BACKUP_PROJECT_NAME}-${timestamp}`; // Remove .dump extension
  const filepath = path.join(os.tmpdir(), filename);

  await dumpToFile(filepath + ".dump"); // Add .dump extension here
  await zipAndPasswordProtect(filepath);
  await uploadToS3({ name: `${filename}.zip`, path: `${filepath}.zip` });
  await deleteFile(filepath + ".dump");
  await deleteFile(`${filepath}.zip`);

  console.log("DB backup complete...");
}
