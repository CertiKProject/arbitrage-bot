#!/usr/bin/env python3

import os
from dotenv import load_dotenv
import time
import json
import math
import dataclasses
from dataclasses import dataclass

from confluent_kafka import Consumer
import ccloud_lib
import logging
from log import set_log_format

from arbi2 import ArbiLP, arbiLPsToCycles

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


def lprsToArbiLP(lprs) -> ArbiLP:
    r1 = lprs['reserve1']
    r2 = lprs['reserve2']
    return ArbiLP(token1=lprs['token1Symbol'],
                  token2=lprs['token2Symbol'],
                  reserve1 = r1,
                  reserve2 = r2,
                  ratio = r1 / r2,
                  logratio = math.log(r1 / r2),
                  neglogratio = -math.log(r1 / r2),
                  lpAddress = lprs['lpAddress'],
                  token1Address=lprs['token1Address'],
                  token2Address=lprs['token2Address'])


def lprsToArbiLPs(lprsList) -> [ArbiLP]:
    return list(map(lambda lprs: lprsToArbiLP(lprs), lprsList))



def testConsume():

    configFile = ".ccloud"
    # topic = "test-topic"
    topic = "latest-lp-reserves-2"

    myConsumer = MyConsumer(configFile=configFile, topic=topic)

    while True:
        r = myConsumer.consume()
        # print(None if r is None else r.value())

        print('-' * 80)
        print('time = ', time.time())

        if r is not None:
            rval = json.loads(r.value().decode('utf-8'))
            latestTimestamp = max(list(map(lambda lp: lp['timestamp'], rval)))
            print('latest timestamp to now = ', time.time() - latestTimestamp)

            arbiLPs = lprsToArbiLPs(rval)
            arbios = arbiLPsToCycles(arbiLPs)

            with open('lprs.json', 'w') as f:
                f.write(r.value().decode('utf-8'))
            # break



if __name__ == "__main__":
    testConsume()

    # with open('lprs.json', 'r') as f:
    #     r = json.loads(f.read())
    #     arbiLPs = lprsToArbiLPs(r)
    #
    #     arbios = arbiLPsToCycles(arbiLPs)
    #     # print(list(map(lambda arbio: arbio[-1].simAmount, arbios)))

