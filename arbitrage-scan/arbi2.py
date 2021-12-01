#!/usr/bin/env python3

# install snowflake-connector-python, python-dotenv
# create a file .env
# SNOW_USER=...
# SNOW_PASSWORD=...
# SNOW_ACCOUNT=wra72362.us-east-1

import os
from dotenv import load_dotenv
import time
import json
import math
import dataclasses
from dataclasses import dataclass
from typing import Dict

# we do not consider arbitrage with more than this many steps
MAX_ARBI_LENGTH = 10

# we do not consider arbitrage with less than this much profit
ARBI_GAIN_THRESHOLD = 0.01


@dataclass
class ArbiLP:
    token1: str
    token2: str
    reserve1: float
    reserve2: float
    ratio: float
    logratio: float
    neglogratio: float
    lpAddress: str
    token1Address: str
    token2Address: str


@dataclass
class BFBacktracker:
    fromI: int
    lp: ArbiLP
    neglogratio: float


@dataclass
class ArbiOpportunity:
    tokenAddresses: [str]
    tokens: [str]
    lps: [ArbiLP]
    totalLogRatio: float
    totalGain: float


@dataclass
class ArbiStep:
    tokenAddresses: str
    tokenSymbol: str
    lpAddress: str
    simAmount: float




WBNB_ADDRESS = 'bsc:0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c'.lower()



def simulateArbi(arbio: ArbiOpportunity) -> [ArbiStep]:
    arbiss: [ArbiStep] = []
    simAmount = 1

    for i in range(len(arbio.tokenAddresses)):

        startTA = arbio.tokenAddresses[i]
        lp: ArbiLP = arbio.lps[i]

        arbis = ArbiStep(tokenAddresses=startTA,
                         tokenSymbol=arbio.tokens[i],
                         lpAddress=lp.lpAddress,
                         simAmount=simAmount)
        arbiss.append(arbis)

        if startTA == lp.token1Address:
            simAmount = simAmount * (lp.reserve2 / lp.reserve1)
            # print(arbio.tokens[(i + 1) % len(arbio.tokens)], lp.token2)
        else:
            simAmount = simAmount * (lp.reserve1 / lp.reserve2)
            # print(arbio.tokens[(i + 1) % len(arbio.tokens)], lp.token1)

    arbiss.append(ArbiStep(tokenAddresses=arbio.tokenAddresses[0],
                           tokenSymbol=arbio.tokens[0],
                           lpAddress='',
                           simAmount=simAmount))
    return arbiss


# massage and record arbi opportunity
def recordArbiOpportunity(indices_stack: [int],
                          lp_stack: [ArbiLP],
                          neglogratio_stack: [float],
                          repeat_index: int,
                          token_addresses: [str],
                          token_map: Dict[str, str],
                          arbios: Dict[str, ArbiOpportunity]):

    # first remove everything before the start of the cycle
    _i = indices_stack.index(repeat_index)
    indices = indices_stack[_i:]
    lps = lp_stack[_i:]
    neglogratios = neglogratio_stack[_i:]

    # start from the lowest index to dedup
    mini = min(indices)
    minid = indices.index(mini)
    indices = indices[minid:] + indices[:minid]
    lps = lps[minid:] + lps[:minid]
    neglogratios = neglogratios[minid:] + neglogratios[:minid]

    # pack
    arbi_token_addresses = list(map(lambda ti: token_addresses[ti], indices))
    arbi_token_symbols = list(map(lambda ta: token_map[ta], arbi_token_addresses))
    key = json.dumps(indices)

    totalLogRatio = -sum(neglogratios)
    if totalLogRatio > 0:
        # record arbio
        arbios[key] = ArbiOpportunity(
            tokenAddresses=arbi_token_addresses,
            tokens=arbi_token_symbols,
            lps=lps,
            totalLogRatio=-sum(neglogratios),
            totalGain=math.exp(-sum(neglogratios)))


# change to recursive search for one reason:
# there might be overlapping cycles:
# 1 -> 2 -> 3 -> 4 -> 1
# 5 -> 6 -> 3 -> 4 -> 5
# here the incoming for 3 needs to be both 2 and 6
# therefore we need to record all incoming
# fortunately, this is not too bad:
# 1. no repeating vertices
# 2. we can prune all edges before n steps, after n steps, all updates are negative cycle updates


# recursive search
# lpStack and neglogratioStack already have the next leg (for vertex) pushed
# stack still doesn't
def searchForArbi(records: Dict[int, Dict[int, BFBacktracker]],
                  indices_stack: [int],
                  lp_stack: [ArbiLP],
                  neglogratio_stack: [float],
                  vertex: int,
                  token_addresses: [str],
                  token_map: Dict[str, str],
                  arbios: Dict[str, ArbiOpportunity]):

    # print(indices_stack, vertex)

    # repeated vertex, found cycle
    if vertex in indices_stack:
        recordArbiOpportunity(indices_stack=indices_stack,
                              lp_stack=lp_stack,
                              neglogratio_stack=neglogratio_stack,
                              repeat_index=vertex,
                              token_addresses=token_addresses,
                              token_map=token_map,
                              arbios=arbios)
        return

    indices_stack.append(vertex)

    for bt in records[vertex].values():
        lp_stack.append(bt.lp)
        neglogratio_stack.append(bt.neglogratio)

        searchForArbi(records, indices_stack, lp_stack, neglogratio_stack, bt.fromI, \
                      token_addresses, token_map, arbios)

        lp_stack.pop()
        neglogratio_stack.pop()

    indices_stack.pop()


def bf(token_map: Dict[str, str], lps: [ArbiLP]):
    n = len(token_map)
    token_addresses = list(token_map.keys())

    # token map
    token_address_to_index_map: Dict[str, int] = {}
    for i in range(n):
        token_address_to_index_map[token_addresses[i]] = i

    # distance to start index
    dist: [float] = []
    for i in range(n):
        dist.append(1e100)

    # wbnb is the most connected, start from it
    wbnbi = token_address_to_index_map[WBNB_ADDRESS]
    dist[wbnbi] = 0


    # let use multiple from index for each token
    records: Dict[int, Dict[int, BFBacktracker]] = {}

    # r rounds
    # bellman ford to find negative cycles
    # first we iterate for n times
    # here the edge length is the negation of the log of the exchange ratio
    # all edges are directed
    #
    # records here is purely for backtracking purpose

    # can discard all the records before n step, then step for another
    # max arbi length steps, recording steps
    for r in range(n + MAX_ARBI_LENGTH):
        # print(dist[token_address_to_index_map[dPOOCOIN]], dist[token_address_to_index_map[dSUPDOG]])

        # print('round ', r)
        # iterate over edges
        for lp in lps:
            i1 = token_address_to_index_map[lp.token1Address]
            i2 = token_address_to_index_map[lp.token2Address]
            if dist[i1] + lp.neglogratio < dist[i2]:
                dist[i2] = dist[i1] + lp.neglogratio
                if i2 not in records:
                    records[i2] = {}
                # once r >= n, all updates are negative cycle updates
                if r >= n:
                    records[i2][i1] = BFBacktracker(fromI=i1, lp=lp, neglogratio=lp.neglogratio)

            # the reverse direction, ratio = 1 / ratio, or say, log = -log
            if dist[i2] - lp.neglogratio < dist[i1]:
                dist[i1] = dist[i2] - lp.neglogratio
                if i1 not in records:
                    records[i1] = {}
                # once r >= n, all updates are negative cycle updates
                if r > n:
                    records[i1][i2] = BFBacktracker(fromI=i2, lp=lp, neglogratio=-lp.neglogratio)

    # check unreachable vertices
    urvis = list(filter(lambda vertex_i: dist[vertex_i] == 1e100, range(n)))
    s = ', '.join(list(map(lambda vertex_i: token_addresses[vertex_i], urvis)))
    print('unreachable vertices: ', s)


    # find neg cycles
    arbios: Dict[str, ArbiOpportunity] = {}

    for lp in lps:
        i1 = token_address_to_index_map[lp.token1Address]
        i2 = token_address_to_index_map[lp.token2Address]

        if dist[i1] + lp.neglogratio < dist[i2]:
            dist[i2] = dist[i1] + lp.neglogratio
            searchForArbi(records=records, indices_stack=[], lp_stack=[],
                          neglogratio_stack=[], vertex=i1, token_addresses=token_addresses,
                          token_map=token_map, arbios=arbios)

        if dist[i2] - lp.neglogratio < dist[i1]:
            dist[i1] = dist[i2] - lp.neglogratio
            searchForArbi(records=records, indices_stack=[], lp_stack=[],
                          neglogratio_stack=[], vertex=i2, token_addresses=token_addresses,
                          token_map=token_map, arbios=arbios)

    # for key in arbios:
    #     print(key, arbios[key].totalGain)

    arbios = list(arbios.values())
    arbios = list(filter(lambda arbio: arbio.totalGain > 1 + ARBI_GAIN_THRESHOLD, arbios))
    arbios.sort(key=lambda arbio: -arbio.totalGain)

    print(f"================== Arbitrage Opportunities (ts = {time.time():.2f}) ====================")
    print("Arbitrage Tokens        Profit      LPs ")
    for arbio in arbios:
        # print (arbio.tokens, arbio.totalGain)
        l = list(map(lambda lp:lp.lpAddress[:12]+'...', arbio.lps))
        print (f"{','.join(arbio.tokens):20}   {(arbio.totalGain - 1) * 100:6.2f}%      {','.join(l)}")

    return arbios


def arbiLPsToCycles(arbiLPs: [ArbiLP]):
    tokenMap: Dict[str, str] = {}
    for lp in arbiLPs:
        tokenMap[lp.token1Address] = lp.token1
        tokenMap[lp.token2Address] = lp.token2

    return list(map(lambda arbio: simulateArbi(arbio), bf(tokenMap, arbiLPs)))



if __name__ == "__main__":
    startTime = time.time()
    load_dotenv()

    duration = time.time() - startTime
    print(f"All done, spent {duration / 60:.2f} minutes.")


