import type { StorageDriver, StorageObject } from "./index.js";
import { lazyImport } from "../internal/lazy-import.js";

export interface S3StorageDriverOptions {
  bucket: string;
  region?: string;
  endpoint?: string;
  forcePathStyle?: boolean;
  accessKeyId?: string;
  secretAccessKey?: string;
}

interface S3LikeClient {
  send(command: unknown): Promise<unknown>;
}

interface S3CommandsModule {
  S3Client?: new (options: Record<string, unknown>) => S3LikeClient;
  PutObjectCommand?: new (input: Record<string, unknown>) => unknown;
  GetObjectCommand?: new (input: Record<string, unknown>) => unknown;
  DeleteObjectCommand?: new (input: Record<string, unknown>) => unknown;
}

export class S3StorageDriver implements StorageDriver {
  constructor(
    private readonly client: S3LikeClient,
    private readonly bucket: string,
    private readonly commands: Required<
      Pick<S3CommandsModule, "PutObjectCommand" | "GetObjectCommand" | "DeleteObjectCommand">
    >
  ) {}

  async put(object: StorageObject): Promise<void> {
    const command = new this.commands.PutObjectCommand({
      Bucket: this.bucket,
      Key: object.key,
      Body: object.body,
      ContentType: object.contentType,
    });
    await this.client.send(command);
  }

  async get(key: string): Promise<StorageObject | null> {
    const command = new this.commands.GetObjectCommand({
      Bucket: this.bucket,
      Key: key,
    });

    try {
      const result = (await this.client.send(command)) as {
        Body?: unknown;
        ContentType?: string;
      };
      if (!result.Body) {
        return null;
      }
      const body = await toUint8Array(result.Body);
      return {
        key,
        body,
        contentType: result.ContentType,
      };
    } catch (error) {
      const statusCode = (error as { $metadata?: { httpStatusCode?: number } }).$metadata?.httpStatusCode;
      if (statusCode === 404) {
        return null;
      }
      throw error;
    }
  }

  async del(key: string): Promise<void> {
    const command = new this.commands.DeleteObjectCommand({
      Bucket: this.bucket,
      Key: key,
    });
    await this.client.send(command);
  }
}

export async function createS3StorageDriver(
  options: S3StorageDriverOptions
): Promise<S3StorageDriver> {
  const module = await lazyImport<S3CommandsModule>(
    "@aws-sdk/client-s3",
    "Install with `pnpm add @aws-sdk/client-s3` (or npm/yarn equivalent)"
  );

  if (
    !module.S3Client ||
    !module.PutObjectCommand ||
    !module.GetObjectCommand ||
    !module.DeleteObjectCommand
  ) {
    throw new Error("Failed to initialize S3 driver.");
  }

  const region = options.region || process.env.AWS_REGION || "auto";
  const accessKeyId = options.accessKeyId || process.env.AWS_ACCESS_KEY_ID;
  const secretAccessKey = options.secretAccessKey || process.env.AWS_SECRET_ACCESS_KEY;

  const client = new module.S3Client({
    region,
    endpoint: options.endpoint,
    forcePathStyle: options.forcePathStyle,
    ...(accessKeyId && secretAccessKey
      ? {
          credentials: {
            accessKeyId,
            secretAccessKey,
          },
        }
      : {}),
  });

  return new S3StorageDriver(client, options.bucket, {
    PutObjectCommand: module.PutObjectCommand,
    GetObjectCommand: module.GetObjectCommand,
    DeleteObjectCommand: module.DeleteObjectCommand,
  });
}

async function toUint8Array(body: unknown): Promise<Uint8Array> {
  if (body instanceof Uint8Array) {
    return body;
  }

  if (
    typeof body === "object" &&
    body !== null &&
    "transformToByteArray" in body &&
    typeof (body as { transformToByteArray?: unknown }).transformToByteArray === "function"
  ) {
    const bytes = await (body as { transformToByteArray: () => Promise<Uint8Array> }).transformToByteArray();
    return bytes;
  }

  if (
    typeof body === "object" &&
    body !== null &&
    Symbol.asyncIterator in body
  ) {
    const chunks: Uint8Array[] = [];
    for await (const chunk of body as AsyncIterable<Uint8Array | Buffer | string>) {
      if (chunk instanceof Uint8Array) {
        chunks.push(chunk);
      } else if (typeof chunk === "string") {
        chunks.push(new TextEncoder().encode(chunk));
      } else {
        chunks.push(new Uint8Array(chunk));
      }
    }
    return concatChunks(chunks);
  }

  throw new Error("Unsupported S3 body type.");
}

function concatChunks(chunks: Uint8Array[]): Uint8Array {
  const total = chunks.reduce((sum, chunk) => sum + chunk.length, 0);
  const out = new Uint8Array(total);
  let offset = 0;
  for (const chunk of chunks) {
    out.set(chunk, offset);
    offset += chunk.length;
  }
  return out;
}

