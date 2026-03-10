/**
 * Integration tests for /records routes.
 *
 * Prerequisites: docker-compose services must be running
 *   (postgres on 5432, redis on 6379, kafka on 9092).
 *
 * Run:  pnpm test  (inside api/)
 */

import request from 'supertest';
import express from 'express';
import prisma from '@backend/common/db';
import redis from '@backend/common/cache';
import { connectProducer } from '@backend/common/kafka';
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
    // Ensure the Kafka producer is connected before any upload test fires.
    await connectProducer();
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

    it('handles a large CSV (>1000 rows) correctly — batch flushing', async () => {


        // Build a CSV with 1050 data rows to exercise both a full batch flush and the
        // trailing partial batch in the Transform's flush() hook.
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
