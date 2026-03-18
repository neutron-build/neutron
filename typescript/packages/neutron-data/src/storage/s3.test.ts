import assert from "node:assert/strict";
import test from "node:test";
import { S3StorageDriver } from "./s3.js";

// Mock S3 client
class MockS3Client {
  private storage: Map<string, { body: Uint8Array; contentType?: string }> = new Map();

  async send(command: any): Promise<any> {
    if (command.constructor.name === "PutObjectCommand") {
      this.storage.set(command.input.Key, {
        body: new Uint8Array(command.input.Body as any),
        contentType: command.input.ContentType,
      });
      return { ETag: "mock-etag" };
    }

    if (command.constructor.name === "GetObjectCommand") {
      const data = this.storage.get(command.input.Key);
      if (!data) {
        const error: any = new Error("NoSuchKey");
        error.$metadata = { httpStatusCode: 404 };
        throw error;
      }
      return {
        Body: data.body,
        ContentType: data.contentType,
      };
    }

    if (command.constructor.name === "DeleteObjectCommand") {
      this.storage.delete(command.input.Key);
      return { DeleteMarker: true };
    }

    throw new Error("Unknown command");
  }

  getStoredData() {
    return this.storage;
  }
}

// Mock command classes
class MockPutObjectCommand {
  constructor(public input: any) {}
}

class MockGetObjectCommand {
  constructor(public input: any) {}
}

class MockDeleteObjectCommand {
  constructor(public input: any) {}
}

test("S3StorageDriver.put stores object", async () => {
  const client = new MockS3Client();
  const driver = new S3StorageDriver(
    client as any,
    "test-bucket",
    {
      PutObjectCommand: MockPutObjectCommand as any,
      GetObjectCommand: MockGetObjectCommand as any,
      DeleteObjectCommand: MockDeleteObjectCommand as any,
    }
  );

  await driver.put({
    key: "file.txt",
    body: new Uint8Array([1, 2, 3]),
    contentType: "text/plain",
  });

  const stored = client.getStoredData();
  assert.ok(stored.has("file.txt"));
});

test("S3StorageDriver.get retrieves stored object", async () => {
  const client = new MockS3Client();
  const driver = new S3StorageDriver(
    client as any,
    "test-bucket",
    {
      PutObjectCommand: MockPutObjectCommand as any,
      GetObjectCommand: MockGetObjectCommand as any,
      DeleteObjectCommand: MockDeleteObjectCommand as any,
    }
  );

  const originalData = new Uint8Array([1, 2, 3]);
  await driver.put({
    key: "file.txt",
    body: originalData,
    contentType: "text/plain",
  });

  const retrieved = await driver.get("file.txt");
  assert.ok(retrieved);
  assert.equal(retrieved.key, "file.txt");
  assert.deepEqual(retrieved.body, originalData);
  assert.equal(retrieved.contentType, "text/plain");
});

test("S3StorageDriver.get returns null for 404", async () => {
  const client = new MockS3Client();
  const driver = new S3StorageDriver(
    client as any,
    "test-bucket",
    {
      PutObjectCommand: MockPutObjectCommand as any,
      GetObjectCommand: MockGetObjectCommand as any,
      DeleteObjectCommand: MockDeleteObjectCommand as any,
    }
  );

  const result = await driver.get("nonexistent.txt");
  assert.equal(result, null);
});

test("S3StorageDriver.del removes object", async () => {
  const client = new MockS3Client();
  const driver = new S3StorageDriver(
    client as any,
    "test-bucket",
    {
      PutObjectCommand: MockPutObjectCommand as any,
      GetObjectCommand: MockGetObjectCommand as any,
      DeleteObjectCommand: MockDeleteObjectCommand as any,
    }
  );

  await driver.put({
    key: "file.txt",
    body: new Uint8Array([1, 2, 3]),
  });

  await driver.del("file.txt");

  const result = await driver.get("file.txt");
  assert.equal(result, null);
});

test("S3StorageDriver handles multiple files", async () => {
  const client = new MockS3Client();
  const driver = new S3StorageDriver(
    client as any,
    "test-bucket",
    {
      PutObjectCommand: MockPutObjectCommand as any,
      GetObjectCommand: MockGetObjectCommand as any,
      DeleteObjectCommand: MockDeleteObjectCommand as any,
    }
  );

  await driver.put({
    key: "file1.txt",
    body: new Uint8Array([1, 2, 3]),
  });
  await driver.put({
    key: "file2.txt",
    body: new Uint8Array([4, 5, 6]),
  });

  const file1 = await driver.get("file1.txt");
  const file2 = await driver.get("file2.txt");

  assert.deepEqual(file1?.body, new Uint8Array([1, 2, 3]));
  assert.deepEqual(file2?.body, new Uint8Array([4, 5, 6]));
});

test("S3StorageDriver passes correct bucket name", async () => {
  let capturedBucket = "";
  const mockClient: any = {
    send: async (command: any) => {
      if (command.input.Bucket) {
        capturedBucket = command.input.Bucket;
      }
      return { ETag: "mock-etag" };
    },
  };

  const driver = new S3StorageDriver(mockClient, "my-bucket", {
    PutObjectCommand: MockPutObjectCommand as any,
    GetObjectCommand: MockGetObjectCommand as any,
    DeleteObjectCommand: MockDeleteObjectCommand as any,
  });

  await driver.put({
    key: "test.txt",
    body: new Uint8Array([1]),
  });

  assert.equal(capturedBucket, "my-bucket");
});
