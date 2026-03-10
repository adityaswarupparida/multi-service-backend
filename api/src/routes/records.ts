import { Router } from "express";
import { parse } from "csv-parse";
import { pipeline } from 'stream/promises';
import { Transform } from 'stream';
import prisma from "../db/index.js";
import type { Record } from "../db/src/generated/prisma/client.js";

const router: Router = Router();

router.post("/upload", async (req, res) => {
    if (req.headers['content-type'] !== 'text/csv') {
        return res.status(400).json({ error: 'Content-Type must be text/csv' });
    }

    let upload;
    try {
        upload = await prisma.upload.create({
            data: {
                filename: req.headers['x-filename'] as string || 'unknown.csv',
                status: "PROCESSING",
            }
        });
    } catch (err) {
        return res.status(503).json({ message: "File upload failed, try again later" });
    }

    try {
        const parser = parse({ columns: true, skip_empty_lines: true, trim: true });
        let batch: Object[] = [];

        const processor = new Transform({
            objectMode: true,
            async transform(row, _, cb) {
                try {
                    batch.push(row);
                    if (batch.length >= 1000) {
                        await upsertBatch(batch, upload.id);
                        batch = [];
                    }
                    cb();
                } catch (err) {
                    cb(err as Error);
                }
            },
            async flush(cb) {
                try {    
                    if (batch.length) 
                        await upsertBatch(batch, upload.id); 
                    cb();                  
                } catch (err) {
                    cb(err as Error);
                }
            }
        });

        await pipeline(req, parser, processor);

        await prisma.upload.update({
            where: { id: upload.id },
            data: { status: "COMPLETED" }
        });

        // publish to kafka
        res.status(200).json({ message: "Uploaded" });

    } catch (err) {
        console.error(err);
        await prisma.upload.update({
            where: { id: upload.id },
            data: { status: "FAILED" }
        });

        res.status(500).json({ message: "Internal server error" });
    }
});

async function upsertBatch(batch: Object[], uploadId: string) {
    const records: Record = batch.map(b => { data: b as JSON, uploadId });
    await prisma.record.createMany({
        data: records
    })
}

export default router;