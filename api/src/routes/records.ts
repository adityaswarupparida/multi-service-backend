import { Router } from "express";
import crypto from "crypto";
import { parse } from "csv-parse";
import { pipeline } from 'stream/promises';
import { Transform } from 'stream';
import prisma from "@backend/common/db";
import redis from "@backend/common/cache";
import { publishEvent } from "@backend/common/kafka";

const CACHE_KEY = process.env['REDIS_CACHE_KEY'] ?? 'records:all';
const KAFKA_TOPIC = process.env['KAFKA_TOPIC'] ?? 'records-uploaded';

const router: Router = Router();

router.post("/upload", async (req, res) => {
    if (req.headers['content-type'] !== 'text/csv') {
        return res.status(400).json({ error: 'Content-Type must be text/csv' });
    }

    let uploadId: string | null = null;

    try {
        const filename = req.headers['x-filename'] as string || 'unknown.csv';

        // stream-parse CSV and compute checksum simultaneously
        const hash = crypto.createHash('sha256');
        const rows: Object[] = [];
        const parser = parse({ columns: true, skip_empty_lines: true, trim: true });

        const checksumStream = new Transform({
            transform(chunk, _, cb) {
                hash.update(chunk);
                cb(null, chunk);
            }
        });

        parser.on('data', (row) => rows.push(row));

        await pipeline(req, checksumStream, parser);

        const checksum = hash.digest('hex');

        // skip if exact same file was already uploaded
        const existing = await prisma.upload.findFirst({
            where: { checksum, status: 'COMPLETED' },
        });
        if (existing) {
            return res.status(200).json({ message: 'File already uploaded', uploadId: existing.id });
        }

        // create upload record
        const upload = await prisma.upload.create({
            data: { filename, checksum, status: 'PROCESSING' },
        });
        uploadId = upload.id;

        // transaction: delete old records for same filename, insert new ones
        await prisma.$transaction(async (tx) => {
            const previous = await tx.upload.findFirst({
                where: { filename, status: 'COMPLETED' },
            });

            if (previous) {
                await tx.record.deleteMany({ where: { uploadId: previous.id } });
                await tx.upload.update({
                    where: { id: previous.id },
                    data: { status: 'REPLACED' },
                });
            }

            // batch insert
            for (let i = 0; i < rows.length; i += 1000) {
                const batch = rows.slice(i, i + 1000);
                await tx.record.createMany({
                    data: batch.map(b => ({ data: b as any, uploadId: upload.id })),
                });
            }

            await tx.upload.update({
                where: { id: upload.id },
                data: { status: 'COMPLETED' },
            });
        });

        await publishEvent(KAFKA_TOPIC, {
            uploadId: upload.id,
            filename: upload.filename,
        });
        res.status(200).json({ message: 'Uploaded successfully!!' });

    } catch (err) {
        console.error(err);
        if (uploadId) {
            await prisma.upload.update({
                where: { id: uploadId },
                data: { status: 'FAILED' },
            }).catch(console.error);
        }
        res.status(500).json({ message: 'Internal server error' });
    }
});

router.get("/", async (req, res) => {
    try {
        const cached = await redis.get(CACHE_KEY);
        if (cached) {
            return res.status(200).json({ data: JSON.parse(cached) });
        }
    } catch (err) {
        console.error('Cache unavailable:', err);
    }

    // cache fallback
    try {
        const records = await prisma.record.findMany();
        return res.status(200).json({ data: records });
    } catch (err) {
        return res.status(503).json({ message: "Service unavailable" });
    }
});

export default router;