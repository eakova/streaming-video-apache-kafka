const { Kafka } = require("kafkajs");

const consume = async () => {
  const kafka = new Kafka({
    clientId: "my-app2",
    brokers: ["localhost:9092"],
  });
  const consumer = kafka.consumer({ groupId: "whatever" });

  await consumer.connect();
  await consumer.subscribe({ topic: "test-streaming", fromBeginning: true });

  return consumer;
};

module.exports = consume;
