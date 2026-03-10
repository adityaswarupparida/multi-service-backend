import { Kafka } from 'kafkajs';

export const kafka = new Kafka({
    clientId: 'my-app',
    brokers: ['kafka1:9092', 'kafka2:9092']
})

// Producing
export const producer = kafka.producer();

export async function connectProducer() {
    await producer.connect();
}

export async function publishEvent(topic: string, payload: object) {
    await producer.send({
        topic,
        messages: [{ value: JSON.stringify(payload) }]
    });
}