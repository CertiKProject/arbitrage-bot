#!/usr/bin/env python3

# pip3 install web3
import json
from dotenv import load_dotenv
import time
import dataclasses
from dataclasses import dataclass

from my_producer import MyProducer
from get_lps import *
from fetch_reserves import *


@dataclass
class LPReserveOutput:
    token1Address: str
    token2Address: str
    token1Symbol: str
    token2Symbol: str
    lpAddress: str
    reserve1: float     # discounted decimals
    reserve2: float
    timestamp: int


class EnhancedJSONEncoder(json.JSONEncoder):
    def default(self, o):
        if dataclasses.is_dataclass(o):
            return dataclasses.asdict(o)
        return super().default(o)


# check how many lps changed
def compareReservesDataDelta(oDict: Dict[str, LPReserve], nDict: Dict[str, LPReserve]):
    okeys = set(oDict.keys())
    nkeys = set(nDict.keys())

    commonKeys = okeys.intersection(nkeys)
    updatedKeys = list(filter(lambda key:
                              nDict[key].reserve1 != oDict[key].reserve1 or
                              nDict[key].reserve2 != oDict[key].reserve2 or
                              nDict[key].timestamp != oDict[key].timestamp,
                              commonKeys))

    print('total reserve data = ', len(nDict), \
          'set diffs = ', okeys.difference(nkeys), nkeys.difference(okeys), \
          'updated keys = ', len(updatedKeys))


def getLpLiquidity(lprs: LPReserve) -> float:
    return lprs.reserve1 / pow(10, lprs.lp.token1Decimals) * lprs.lp.token1Price + \
           lprs.reserve2 / pow(10, lprs.lp.token2Decimals) * lprs.lp.token2Price


# ignore any LP with lower liquidity than this
TVL_THRESHOLD = 10_000
def filterLprsByLiquidity(lprsDict: Dict[str, LPReserve]) -> [LPReserve]:
    # for lprs in list(lprsDict.values())[:10]:
    #     print(lprs.reserve1, lprs.lp.token1, lprs.lp.decimal1, lprs.lp.token1Price, \
    #           lprs.reserve2, lprs.lp.token2, lprs.lp.decimal2, lprs.lp.token2Price, \
    #           getLprsTvl(lprs))
    return list(filter(lambda lprs: getLpLiquidity(lprs) >= TVL_THRESHOLD, lprsDict.values()))


def lpReserveToLpReserveOutput(lprs: LPReserve) -> LPReserveOutput:
    return LPReserveOutput(
        token1Address=lprs.lp.token1Address,
        token2Address=lprs.lp.token2Address,
        token1Symbol=lprs.lp.token1,
        token2Symbol=lprs.lp.token2,
        lpAddress=lprs.lp.lpAddress,
        reserve1=lprs.reserve1 / pow(10, lprs.lp.token1Decimals),
        reserve2=lprs.reserve2 / pow(10, lprs.lp.token2Decimals),
        timestamp=lprs.timestamp)


# Repeatedly get the latest lp-reserved for a given set of lps, using multicall
def testGetReservesMulticall(lps: [LP], myProducer: MyProducer):
    w3 = getW3FromAddress(lps[0].lpAddress)

    # mapping from lp address to lp
    lpDict = dict(list(map(lambda lp: (deprefixAddress(lp.lpAddress.lower()), lp), lps)))
    # print(lpDict)

    funcs = getLPGetReservesFuncs(lps)

    # repeatedly get latest reserves and feed
    lprsDict = getLatestReserves(w3, funcs, lpDict)
    while True:

        # fetch lp reserves, this should be changed to listen to kafka topic
        newDict = getLatestReserves(w3, funcs, lpDict)
        compareReservesDataDelta(lprsDict, newDict)
        lprsDict = newDict

        # filter by liquidity
        lprsFiltered = filterLprsByLiquidity(lprsDict)
        print("lps = ", len(lprsFiltered))

        lprsOutput = list(map(lpReserveToLpReserveOutput, lprsFiltered))
        latestTimestamp = max(list(map(lambda lprs: lprs.timestamp, lprsOutput)))

        print('latest timestamp to now = ', time.time() - latestTimestamp)
        # print(lprsOutput)
        myProducer.push('lp-reserves', json.dumps(lprsOutput, cls=EnhancedJSONEncoder))


if __name__ == "__main__":
    startTime = time.time()
    load_dotenv()

    myProducer = MyProducer(configFile=".ccloud", topic="latest-lp-reserves-2")
    lps = getLPs()
    # print(lps)

    testGetReservesMulticall(lps, myProducer)

    duration = time.time() - startTime
    print(f"All done, spent {duration / 60:.2f} minutes.")


