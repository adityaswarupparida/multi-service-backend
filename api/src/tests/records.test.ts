/**
 * Integration tests for /records routes.
 *
 * Prerequisites: docker-compose services must be running
 *   (postgres on 5432, redis on 6379, kafka on 9092).
 *
 * Run:  pnpm test  (inside api/)
 */

import { vi } from 'vitest';
import request from 'supertest';
import express from 'express';
import prisma from '@backend/common/db';
import redis from '@backend/common/cache';
import * as kafkaModule from '@backend/common/kafka';
import recordsRouter from '../routes/records.js';

// ---------------------------------------------------------------------------
// App bootstrap
// ---------------------------------------------------------------------------

const app = express();
app.use('/records', recordsRouter);

// ---------------------------------------------------------------------------
// Lifecycle
// ---------------------------------------------------------------------------

beforeAll(async () => {
    await kafkaModule.connectProducer();
});

afterAll(async () => {
    // Clean up all rows written during the test run.
    await prisma.record.deleteMany();
    await prisma.upload.deleteMany();
    await redis.del('records:all');
    await redis.quit();
    await prisma.$disconnect();
});

// ---------------------------------------------------------------------------
// POST /records/upload
// ---------------------------------------------------------------------------

describe('POST /records/upload', () => {
    it('returns 400 when Content-Type is not text/csv', async () => {
        const res = await request(app)
            .post('/records/upload')
            .set('Content-Type', 'application/json')
            .send('{}');

        expect(res.status).toBe(400);
        expect(res.body).toMatchObject({ error: 'Content-Type must be text/csv' });
    });

    it('processes a valid CSV and returns 200 with status COMPLETED', async () => {

        const csv = 'name,age\nAlice,30\nBob,25\n';

        const res = await request(app)
            .post('/records/upload')
            .set('Content-Type', 'text/csv')
            .set('x-filename', 'test-data.csv')
            .send(csv);

        expect(res.status).toBe(200);
        expect(res.body).toMatchObject({ message: 'Uploaded successfully!!' });

        // Verify upload row exists in DB and is COMPLETED.
        const uploads = await prisma.upload.findMany({
            where: { filename: 'test-data.csv', status: 'COMPLETED' },
        });
        expect(uploads.length).toBeGreaterThanOrEqual(1);

        // Verify the two records were inserted.
        const latestUpload = uploads[uploads.length - 1]!;
        const records = await prisma.record.findMany({
            where: { uploadId: latestUpload.id },
        });
        expect(records).toHaveLength(2);
    });

    it('uses x-filename header value when provided', async () => {

        const csv = 'city\nLondon\n';

        await request(app)
            .post('/records/upload')
            .set('Content-Type', 'text/csv')
            .set('x-filename', 'cities.csv')
            .send(csv);

        const upload = await prisma.upload.findFirst({
            where: { filename: 'cities.csv' },
            orderBy: { createdAt: 'desc' },
        });
        expect(upload).not.toBeNull();
        expect(upload!.filename).toBe('cities.csv');
    });

    it('defaults filename to unknown.csv when x-filename header is absent', async () => {

        const csv = 'item\nwidget\n';

        await request(app)
            .post('/records/upload')
            .set('Content-Type', 'text/csv')
            .send(csv);

        const upload = await prisma.upload.findFirst({
            where: { filename: 'unknown.csv' },
            orderBy: { createdAt: 'desc' },
        });
        expect(upload).not.toBeNull();
        expect(upload!.filename).toBe('unknown.csv');
    });

    it('handles a large CSV (>1000 rows) correctly — batch insert', async () => {
        const rows = ['index'];
        for (let i = 1; i <= 1050; i++) {
            rows.push(String(i));
        }
        const csv = rows.join('\n') + '\n';

        const res = await request(app)
            .post('/records/upload')
            .set('Content-Type', 'text/csv')
            .set('x-filename', 'large.csv')
            .send(csv);

        expect(res.status).toBe(200);

        const upload = await prisma.upload.findFirst({
            where: { filename: 'large.csv', status: 'COMPLETED' },
            orderBy: { createdAt: 'desc' },
        });
        expect(upload).not.toBeNull();

        const records = await prisma.record.count({ where: { uploadId: upload!.id } });
        expect(records).toBe(1050);
    });

    it('skips duplicate upload when checksum matches an existing COMPLETED upload', async () => {
        const csv = 'color\nred\nblue\n';

        // first upload
        const res1 = await request(app)
            .post('/records/upload')
            .set('Content-Type', 'text/csv')
            .set('x-filename', 'colors.csv')
            .send(csv);

        expect(res1.status).toBe(200);
        expect(res1.body.message).toBe('Uploaded successfully!!');

        // same file again — should be detected as duplicate
        const res2 = await request(app)
            .post('/records/upload')
            .set('Content-Type', 'text/csv')
            .set('x-filename', 'colors.csv')
            .send(csv);

        expect(res2.status).toBe(200);
        expect(res2.body.message).toBe('File already uploaded');
        expect(res2.body).toHaveProperty('uploadId');
    });

    it('replaces old records when re-uploading same filename with different data', async () => {
        const csv1 = 'fruit\napple\nbanana\n';
        const csv2 = 'fruit\ncherry\ndate\nelderberry\n';

        // first upload
        await request(app)
            .post('/records/upload')
            .set('Content-Type', 'text/csv')
            .set('x-filename', 'fruits.csv')
            .send(csv1);

        const upload1 = await prisma.upload.findFirst({
            where: { filename: 'fruits.csv', status: 'COMPLETED' },
        });
        expect(upload1).not.toBeNull();

        const records1 = await prisma.record.count({ where: { uploadId: upload1!.id } });
        expect(records1).toBe(2);

        // re-upload with different data
        const res2 = await request(app)
            .post('/records/upload')
            .set('Content-Type', 'text/csv')
            .set('x-filename', 'fruits.csv')
            .send(csv2);

        expect(res2.status).toBe(200);
        expect(res2.body.message).toBe('Uploaded successfully!!');

        // old upload should be REPLACED
        const oldUpload = await prisma.upload.findUnique({ where: { id: upload1!.id } });
        expect(oldUpload!.status).toBe('REPLACED');

        // old records should be deleted
        const oldRecords = await prisma.record.count({ where: { uploadId: upload1!.id } });
        expect(oldRecords).toBe(0);

        // new upload should have 3 records
        const newUpload = await prisma.upload.findFirst({
            where: { filename: 'fruits.csv', status: 'COMPLETED' },
        });
        const newRecords = await prisma.record.count({ where: { uploadId: newUpload!.id } });
        expect(newRecords).toBe(3);
    });

    it('returns 400 when CSV has header but no data rows', async () => {
        const csv = 'name,age\n';

        const res = await request(app)
            .post('/records/upload')
            .set('Content-Type', 'text/csv')
            .set('x-filename', 'empty.csv')
            .send(csv);

        expect(res.status).toBe(400);
        expect(res.body).toMatchObject({ error: 'CSV file has no data rows' });
    });

    it('returns 500 and marks upload as FAILED when transaction fails', async () => {
        const csv = 'tool\nhammer\n';

        // mock $transaction to throw after upload is created
        const originalTransaction = prisma.$transaction.bind(prisma);
        (prisma as any).$transaction = vi.fn().mockRejectedValue(new Error('Transaction failed'));

        const res = await request(app)
            .post('/records/upload')
            .set('Content-Type', 'text/csv')
            .set('x-filename', 'fail-test.csv')
            .send(csv);

        expect(res.status).toBe(500);
        expect(res.body).toMatchObject({ message: 'Internal server error' });

        // upload should be marked as FAILED
        const upload = await prisma.upload.findFirst({
            where: { filename: 'fail-test.csv', status: 'FAILED' },
        });
        expect(upload).not.toBeNull();

        // restore
        (prisma as any).$transaction = originalTransaction;

        // cleanup
        if (upload) {
            await prisma.upload.delete({ where: { id: upload.id } });
        }
    });

    it('stores checksum on the upload record', async () => {
        const csv = 'animal\ncat\n';

        await request(app)
            .post('/records/upload')
            .set('Content-Type', 'text/csv')
            .set('x-filename', 'animals.csv')
            .send(csv);

        const upload = await prisma.upload.findFirst({
            where: { filename: 'animals.csv' },
            orderBy: { createdAt: 'desc' },
        });
        expect(upload).not.toBeNull();
        expect(upload!.checksum).toBeTruthy();
        expect(upload!.checksum!.length).toBe(64); // SHA256 hex length
    });
});

// ---------------------------------------------------------------------------
// GET /records
// ---------------------------------------------------------------------------

describe('GET /records', () => {
    beforeEach(async () => {
        // Start each GET test with a clean cache so tests don't interfere.
        await redis.del('records:all');
    });

    it('returns records from the database when cache is cold', async () => {


        const res = await request(app).get('/records/');

        expect(res.status).toBe(200);
        expect(res.body).toHaveProperty('data');
        expect(Array.isArray(res.body.data)).toBe(true);
    });

    it('returns cached data on a warm cache hit', async () => {
        const cachedPayload = [{ id: 9999, uploadId: 'fake', data: { cached: true } }];
        await redis.set('records:all', JSON.stringify(cachedPayload));

        const res = await request(app).get('/records/');

        expect(res.status).toBe(200);
        // The route returns whatever is in the cache verbatim.
        expect(res.body.data).toEqual(cachedPayload);
    });

    it('DB results match what was inserted via upload', async () => {


        // Upload a known CSV first.
        const csv = 'product\nanvil\n';
        await request(app)
            .post('/records/upload')
            .set('Content-Type', 'text/csv')
            .set('x-filename', 'products.csv')
            .send(csv);

        // Fetch all records — cache is cold so it hits DB.
        const res = await request(app).get('/records/');
        expect(res.status).toBe(200);

        const found = (res.body.data as Array<{ data: Record<string, string> }>).some(
            (r) => r.data?.product === 'anvil'
        );
        expect(found).toBe(true);
    });
});
