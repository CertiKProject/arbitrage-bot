#!/usr/bin/env python3

import os
from dotenv import load_dotenv
import time
import json
import math
import dataclasses
from cryptography.fernet import Fernet

from arbi2 import ArbiLP, arbiLPsToCycles
from my_consumer import *
from my_producer import *
from arbi_types import *


class EnhancedJSONEncoder(json.JSONEncoder):
    def default(self, o):
        if dataclasses.is_dataclass(o):
            return dataclasses.asdict(o)
        return super().default(o)


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


def alpToOutputLP(lp: ArbiLP) -> OutputLP:
    return OutputLP(token1=lp.token1,
                    token2=lp.token2,
                    lpAddress=lp.lpAddress,
                    token1Address=lp.token1Address,
                    token2Address=lp.token2Address)

def aosToOutputAos (aos: [ArbiOpportunity]) -> [OutputAO]:
    return list(map(lambda ao: OutputAO(tokenAddresses=ao.tokenAddresses,
                                        tokens=ao.tokens,
                                        lps=list(map(alpToOutputLP, ao.lps)),
                                        profit=ao.totalGain - 1),
                    aos))




def testConsume():

    configFile = ".ccloud"
    configFileProduce = ".ccloud_produce"
    topic = "latest-lp-reserves-2"
    output_topic = "test-ao"

    myConsumer = MyConsumer(configFile=configFile, topic=topic)
    myProducer = MyProducer(configFile=configFileProduce, topic=output_topic)
    cs = Fernet(os.environ.get('encryption_key'))

    # return
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
            arbios: [ArbiOpportunity] = arbiLPsToCycles(arbiLPs)
            oaos = aosToOutputAos(arbios)

            # print(oaos)

            text = cs.encrypt(str.encode(json.dumps(arbios, cls=EnhancedJSONEncoder)))
            myProducer.push('test', text)

            # with open('lprs.json', 'w') as f:
            #     f.write(r.value().decode('utf-8'))
            # break


if __name__ == "__main__":
    load_dotenv()
    testConsume()

    # with open('lprs.json', 'r') as f:
    #     r = json.loads(f.read())
    #     arbiLPs = lprsToArbiLPs(r)
    #
    #     arbios = arbiLPsToCycles(arbiLPs)
    #     # print(list(map(lambda arbio: arbio[-1].simAmount, arbios)))

