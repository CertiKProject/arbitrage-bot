#!/usr/bin/env python

import time
from confluent_kafka import Producer
import ccloud_lib
import logging
from log import set_log_format

set_log_format()
log = logging.getLogger("latest-lp-reserves")


class MyProducer(object):
    producer: Producer
    topic: str

    def __init__(self, configFile: str, topic: str):
        try:
            self.topic = topic
            conf = ccloud_lib.read_ccloud_config(configFile)
            producer_conf = ccloud_lib.pop_schema_registry_params_from_config(conf)
            self.producer = Producer(producer_conf)
            ccloud_lib.create_topic(conf, self.topic)
        except Exception as e:
            log.error(f"init confluent error: {e}")
            exit(1)

    def push(self, key: str, value: str):
        self.producer.produce(self.topic, key=key, value=value, callback=self.delivery_report)
        self.producer.flush()

    @staticmethod
    def delivery_report(err, msg):
        """ Called once for each message produced to indicate delivery result.
            Triggered by poll() or flush(). """
        if err is not None:
            log.info('Message delivery failed: {}'.format(err))
        else:
            log.info('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))




def testProduce():

    configFile = ".ccloud"
    topic = "test-topic"

    myProducer = MyProducer(configFile=configFile, topic=topic)

    key = "test"
    value = "testa"
    while True:
        myProducer.push(key=key, value=value)
        time.sleep(1)



if __name__ == "__main__":
    testProduce()

