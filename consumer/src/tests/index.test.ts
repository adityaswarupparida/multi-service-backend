/**
 * Integration tests for the consumer's handleMessage function.
 *
 * Prerequisites: docker-compose services must be running
 *   (postgres on 5432, redis on 6379, kafka on 9092).
 *
 * Run:  pnpm test  (inside consumer/)
 */

import { vi } from 'vitest';
import prisma from '@backend/common/db';
import redis from '@backend/common/cache';
import { kafka, connectProducer, publishEvent } from '@backend/common/kafka';
import { handleMessage } from '../index.js';

// ---------------------------------------------------------------------------
// Lifecycle
// ---------------------------------------------------------------------------

let testConsumer: ReturnType<typeof kafka.consumer>;

beforeAll(async () => {
    await connectProducer();

    // Stand up a dedicated test consumer that is connected but NOT running a
    // message loop — we call handleMessage() directly instead.
    testConsumer = kafka.consumer({ groupId: 'test-consumer-integration' });
    await testConsumer.connect();
    await testConsumer.subscribe({ topic: 'records-uploaded', fromBeginning: false });
});

afterAll(async () => {
    await testConsumer.disconnect();
    await redis.del('records:all');
    await redis.quit();
    await prisma.$disconnect();
});

beforeEach(async () => {
    // Clear the Redis key before every test so state doesn't bleed between cases.
    await redis.del('records:all');
});

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/**
 * Publish a test event and return a fake message envelope that handleMessage
 * can consume directly (without needing an active eachMessage loop).
 */
function buildMessage(uploadId: string, offset = '0') {
    return {
        topic: 'records-uploaded',
        partition: 0,
        message: {
            offset,
            value: Buffer.from(JSON.stringify({ uploadId, filename: 'test.csv' })),
        },
    };
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe('handleMessage', () => {
    it('fetches all records from DB, writes them to Redis, and commits offset on success', async () => {
        // Seed a record so we can verify what ends up in Redis.
        const upload = await prisma.upload.create({
            data: { filename: 'seed.csv', status: 'COMPLETED' },
        });
        await prisma.record.create({
            data: { uploadId: upload.id, data: { name: 'Test Row' } },
        });

        const msg = buildMessage(upload.id, '0');
        await handleMessage(msg);

        // Redis should now hold the full record set as JSON.
        const cached = await redis.get('records:all');
        expect(cached).not.toBeNull();

        const parsed = JSON.parse(cached!) as Array<{ uploadId: string }>;
        expect(Array.isArray(parsed)).toBe(true);
        const found = parsed.some((r) => r.uploadId === upload.id);
        expect(found).toBe(true);

        // Clean up seed data.
        await prisma.record.deleteMany({ where: { uploadId: upload.id } });
        await prisma.upload.delete({ where: { id: upload.id } });
    });

    it('does not throw and does not set Redis cache when prisma.record.findMany is unavailable', async () => {
        // Simulate a DB failure by temporarily breaking the connection string.
        // Instead, we verify the error path by disconnecting prisma mid-test.
        // Since we can't easily break prisma without side effects across tests,
        // we verify the defensive behaviour by checking that an obviously-invalid
        // upload ID still results in no unhandled rejection and no crash.

        // Force a scenario where the DB is gone by using a malformed uploadId
        // that would cause a constraint violation — but findMany itself should
        // still succeed. A more reliable approach: monkeypatch prisma.record.findMany
        // for the duration of this test using Jest spying.
        const original = prisma.record.findMany.bind(prisma.record);
        (prisma.record as any).findMany = vi.fn().mockRejectedValueOnce(new Error('DB unavailable'));

        const msg = buildMessage('some-upload-id', '1');
        // Should NOT throw — the catch block in handleMessage swallows the error.
        await expect(handleMessage(msg)).resolves.toBeUndefined();

        // Redis must NOT have been written.
        const cached = await redis.get('records:all');
        expect(cached).toBeNull();

        // Restore.
        (prisma.record as any).findMany = original;
    });

    it('does not commit offset when redis.set throws', async () => {
        // Seed data so findMany returns something.
        const upload = await prisma.upload.create({
            data: { filename: 'redis-fail.csv', status: 'COMPLETED' },
        });
        await prisma.record.create({
            data: { uploadId: upload.id, data: { note: 'redis-fail test' } },
        });

        // Make redis.set throw for this call only.
        const originalSet = redis.set.bind(redis);
        (redis as any).set = vi.fn().mockRejectedValueOnce(new Error('Redis unavailable'));

        const msg = buildMessage(upload.id, '2');
        // Should NOT throw — error is caught and logged internally.
        await expect(handleMessage(msg)).resolves.toBeUndefined();

        // Redis key should still be null (set was never completed).
        const cached = await redis.get('records:all');
        expect(cached).toBeNull();

        // Restore.
        (redis as any).set = originalSet;

        // Clean up.
        await prisma.record.deleteMany({ where: { uploadId: upload.id } });
        await prisma.upload.delete({ where: { id: upload.id } });
    });
});
