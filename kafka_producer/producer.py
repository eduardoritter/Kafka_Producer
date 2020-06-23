import confluent_kafka
import arrow


class CustomKafkaProducer:

    def __init__(self):

        self.topic_name = 'test'

        conf = {'bootstrap.servers': '192.168.25.76:9092',
                'message.max.bytes': 50331648,
                'queue.buffering.max.ms': 0,
                'queue.buffering.max.messages': 15000 }

        self.producer = confluent_kafka.Producer(**conf)

    def on_delivery(self, err, msg):
        if err:
            print("Message failed delivery, error: %s", err)
        else:
            print("Message delivered to %s on partition %s",
                  msg.topic(), msg.partition())

    def produce(self, kafka_msg, kafka_key):
        try:
            self.producer.produce(topic=self.topic_name,
                                  value=kafka_msg,
                                  key=kafka_key,
                                  callback=lambda err, msg: self.on_delivery(err, msg))
            self.producer.flush()

        except Exception as e:
            print("Error during producing to kafka topic. Stacktrace is %s", e)


def start_producer():
    producer = CustomKafkaProducer()
    line = 'test msg 2'
    utc = str(arrow.now().timestamp)
    producer.produce(kafka_msg=line, kafka_key=utc)


if __name__ == "__main__":
    start_producer()
