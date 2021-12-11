#!/usr/bin/env python3

import time
from typing import Dict

from lp_abi import LPFactoryABI
from web3_multicall import Multicall
from tools import *
from get_lps import *


@dataclass
class LPReserve:
    lp: LP
    reserve1: float
    reserve2: float
    timestamp: int


def getLPContract(lpAddress):
    return getW3FromAddress(lpAddress).eth.contract(
        address=Web3.toChecksumAddress(deprefixAddress(lpAddress)),
        abi=LPFactoryABI
    )

def getLPGetReservesFuncs(lps: [LP]):
    # get reserves multicall function
    return list(map(lambda lp: getLPContract(lp.lpAddress).functions.getReserves(), lps))



# def testGetReserves(lp):
#     c = getLPContract(lp.lpAddress)
#     print(lp, c.functions.getReserves().call())


# multicall library returns a weird data structure, we can use this function to parse it before a better library is found
def parseMulticallResults(multicallResults, lpDict: Dict[str, LP]):
    return list(map(lambda mr: \
                        LPReserve(lp=lpDict[mr.contract_address.lower()], \
                                  reserve1=mr.results[0], \
                                  reserve2=mr.results[1], \
                                  timestamp=mr.results[2]),
                    multicallResults.results))


def makeMulticallCall(eth, funcs):
    timeout = 5
    while True:
        try:
            return Multicall(eth).aggregate(funcs)
        except Exception as e:
            print ("error: ", e, " wait for ", timeout, "seconds")
            time.sleep(timeout)
            timeout = timeout * 2


def getLatestReserves(w3, funcs, lpDict: Dict[str, LP]) -> Dict[str, LPReserve]:
    results = makeMulticallCall(w3.eth, funcs)
    lpReserves = parseMulticallResults(results, lpDict)
    return dict(list(map(lambda lrs: (lrs.lp.lpAddress, lrs), lpReserves)))



