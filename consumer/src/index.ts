import 'dotenv/config';
import prisma from '@backend/common/db';
import { kafka, producer, connectProducer } from '@backend/common/kafka';
import redis from '@backend/common/cache';

const CACHE_KEY = process.env['REDIS_CACHE_KEY'] ?? 'records:all';
const KAFKA_TOPIC = process.env['KAFKA_TOPIC'] ?? 'records-uploaded';
const DLQ_TOPIC = process.env['KAFKA_DLQ_TOPIC'] ?? 'records-uploaded-dlq';
const GROUP_ID = process.env['KAFKA_GROUP_ID'] ?? 'kafka-consumer';

const MAX_RETRIES = 3;
const RETRY_DELAYS = [1000, 3000, 5000];

const consumer = kafka.consumer({ groupId: GROUP_ID });
const processedOffsets = new Set<string>();       //  tracks processed messages within the session

async function updateCache() {
    const records = await prisma.record.findMany();
    await redis.set(CACHE_KEY, JSON.stringify(records));
    return records.length;
}

export async function handleMessage({ topic, partition, message }: { topic: string; partition: number; message: any }) {
    const payload = JSON.parse(message.value?.toString() ?? '{}');
    const offsetKey = `${topic}-${partition}-${message.offset}`;

    console.log(`[consumer] Processing upload: ${payload.filename} (offset: ${message.offset})`);

    // skip duplicate messages within the same session
    if (processedOffsets.has(offsetKey)) {
        console.log(`[consumer] Skipping duplicate message (offset: ${message.offset})`);
        return;
    }

    for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
        try {
            const count = await updateCache();

            await consumer.commitOffsets([{
                topic,
                partition,
                offset: (Number(message.offset) + 1).toString()
            }]);

            processedOffsets.add(offsetKey);
            console.log(`[consumer] Cache updated with ${count} records (offset: ${message.offset})`);
            return;

        } catch (err) {
            console.error(`[consumer] Attempt ${attempt}/${MAX_RETRIES} failed for offset ${message.offset}:`, err);

            if (attempt < MAX_RETRIES) {
                const delay = RETRY_DELAYS[attempt - 1] ?? 5000;
                console.log(`[consumer] Retrying in ${delay}ms...`);
                await new Promise(resolve => setTimeout(resolve, delay));
            } else {
                console.error(`[consumer] All ${MAX_RETRIES} retries exhausted for offset ${message.offset}. Sending to DLQ.`);
                await producer.send({
                    topic: DLQ_TOPIC,
                    messages: [{
                        value: JSON.stringify({
                            originalTopic: topic,
                            partition,
                            offset: message.offset,
                            payload,
                            error: String(err),
                            failedAt: new Date().toISOString(),
                        })
                    }]
                });
                await consumer.commitOffsets([{
                    topic,
                    partition,
                    offset: (Number(message.offset) + 1).toString()
                }]);
            }
        }
    }
}

const run = async () => {
    await connectProducer();
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
