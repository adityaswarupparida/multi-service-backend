import 'dotenv/config';
import prisma from '@backend/common/db';
import { kafka } from '@backend/common/kafka';
import redis from '@backend/common/cache';

const CACHE_KEY = process.env['REDIS_CACHE_KEY'] ?? 'records:all';
const KAFKA_TOPIC = process.env['KAFKA_TOPIC'] ?? 'records-uploaded';
const GROUP_ID = process.env['KAFKA_GROUP_ID'] ?? 'kafka-consumer';

const consumer = kafka.consumer({ groupId: GROUP_ID });

export async function handleMessage({ topic, partition, message }: { topic: string; partition: number; message: any }) {
    const payload = JSON.parse(message.value?.toString() ?? '{}');
    console.log(`[consumer] Processing upload: ${payload.filename} (offset: ${message.offset})`);

    try {
        const records = await prisma.record.findMany();
        await redis.set(CACHE_KEY, JSON.stringify(records));
        await consumer.commitOffsets([{
            topic,
            partition,
            offset: (Number(message.offset) + 1).toString()
        }]);
        console.log(`[consumer] Cache updated with ${records.length} records`);

    } catch (err) {
        console.error(`[consumer] Failed to process offset ${message.offset}:`, err);
    }
}

const run = async () => {
    await consumer.connect();
    console.log('[consumer] Connected to Kafka');

    await consumer.subscribe({ topic: KAFKA_TOPIC, fromBeginning: false });
    console.log(`[consumer] Subscribed to topic: ${KAFKA_TOPIC}`);

    await consumer.run({
        autoCommit: false,
        eachMessage: handleMessage,
    });
}

run().catch(console.error);
