
from confluent_kafka import Consumer
import ccloud_lib
import logging
from log import set_log_format


set_log_format()
log = logging.getLogger("consume-latest-lp-reserves")

class MyConsumer(object):
    consumer: Consumer
    topic: str

    def __init__(self, configFile: str, topic: str):
        try:
            self.topic = topic
            conf = ccloud_lib.read_ccloud_config(configFile)
            producer_conf = ccloud_lib.pop_schema_registry_params_from_config(conf)
            self.consumer = Consumer(producer_conf)
            self.consumer.subscribe([topic])
        except Exception as e:
            log.error(f"init confluent error: {e}")
            exit(1)

    def consume(self):
        self.consumer.subscribe([self.topic])
        msg = self.consumer.poll(timeout=4.0)

        # if msg.error():
        #     if msg.error().code() == KafkaError._PARTITION_EOF:
        #         # End of partition event
        #         sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
        #                          (msg.topic(), msg.partition(), msg.offset()))
        #     elif msg.error():
        #         raise KafkaException(msg.error())

        return msg

    @staticmethod
    def delivery_report(err, msg):
        """ Called once for each message produced to indicate delivery result.
            Triggered by poll() or flush(). """
        if err is not None:
            log.info('Message delivery failed: {}'.format(err))
        else:
            log.info('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

