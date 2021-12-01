#!/usr/bin/env python3

# pip3 install web3
import json
import os
from dotenv import load_dotenv
import time
import dataclasses
from dataclasses import dataclass
from typing import Dict

from lpFactoryABI import LPFactoryABI
from web3_multicall import Multicall
from tools import *
from nodeRegistry import *
import snowflake.connector
from MyProducer import MyProducer



@dataclass
class LP:
    token1: str
    token2: str
    lpAddress: str
    token1Price: float
    token2Price: float
    decimal1: int
    decimal2: int
    token1Address: str
    token2Address: str

@dataclass
class LPReserve:
    lp: LP
    reserve1: float
    reserve2: float
    timestamp: int


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



def runSnowflakeSQL(sql: str):
    conn = snowflake.connector.connect(
        user=os.environ.get('SNOW_USER'),
        password=os.environ.get('SNOW_PASSWORD'),
        account=os.environ.get('SNOW_ACCOUNT')
    )
    cur = conn.cursor()
    cur.execute("USE WAREHOUSE DATADOG_WH;")
    cur.execute("USE DATABASE WATERDROP_DB;")

    cur.execute(sql)
    r = cur.fetchall()
    cur.close()
    return r


def getLPs():
    # sql = """
    # with data as
    #
    # ((
    #   SELECT ADDRESS, Label
    #   FROM waterdrop_db.public.LIQUIDITY_PAIRS as pair
    #   JOIN PRJ_ADDR_MAPPING as project
    #   ON pair."TOKEN0" = project.contract_address
    #   JOIN POPULAR_SWAP_PARTNERS as partner
    #   ON pair."TOKEN1" = partner.contract_address
    # ) UNION (
    #   SELECT ADDRESS, Label
    #   FROM waterdrop_db.public.LIQUIDITY_PAIRS as pair
    #   JOIN PRJ_ADDR_MAPPING as project
    #   ON pair."TOKEN1" = project.contract_address
    #   JOIN POPULAR_SWAP_PARTNERS as partner
    #   ON pair."TOKEN0" = partner.contract_address
    # ))
    #
    # select address,
    #     split(split(label, ':')[1], '/')[0]::string as token1,
    #     split(split(label, ':')[1], '/')[1]::string as token2
    #     from data
    #     where startswith(address, 'bsc')
    # """

    # select some LPs with prices
    sql = """

    with data as 

    ((
      SELECT ADDRESS, Label, pair.token0 as token0_address, pair.token1 as token1_address, decimal0, decimal1
      FROM PRODUCTION_DB.LIQUIDITY.LIQUIDITY_PAIRS_DECIMALS as pair
      JOIN PRJ_ADDR_MAPPING as project
      ON pair."TOKEN0" = project.contract_address
      JOIN POPULAR_SWAP_PARTNERS as partner
      ON pair."TOKEN1" = partner.contract_address
    ) UNION (
      SELECT ADDRESS, Label, pair.token0 as token0_address, pair.token1 as token1_address, decimal0, decimal1
      FROM PRODUCTION_DB.LIQUIDITY.LIQUIDITY_PAIRS_DECIMALS as pair
      JOIN PRJ_ADDR_MAPPING as project
      ON pair."TOKEN1" = project.contract_address
      JOIN POPULAR_SWAP_PARTNERS as partner
      ON pair."TOKEN0" = partner.contract_address
    )),
    
    
    prices as 
    
    (select token_address, max(latest_price_in_usd) as latest_price_in_usd 
    from 
        (select token_address, first_value(avg_price) over (partition by token_address order by date desc) as latest_price_in_usd 
         from PRODUCTION_DB.LIQUIDITY.TOKEN_PRICE_BYDATE)
    group by token_address)


    select address, 
        split(split(label, ':')[1], '/')[0]::string as token0,
        split(split(label, ':')[1], '/')[1]::string as token1,
        decimal0, 
        decimal1,
        token0_address,
        token1_address,
        p0.latest_price_in_usd as token0_price,
        p1.latest_price_in_usd as token1_price
        
        from data    
        join prices p0
        join prices p1
        on data.token0_address = p0.token_address and data.token1_address = p1.token_address
        where startswith(address, 'bsc')
    """

    r = runSnowflakeSQL(sql)
    r = list(map(lambda e: LP(lpAddress=e[0], token1=e[1], token2=e[2], \
                              decimal1=int(e[3]), decimal2=int(e[4]), \
                              token1Address=e[5], token2Address=e[6], \
                              token1Price=float(e[7]), token2Price=float(e[8])), r))
    return r


def getLPContract(lpAddress):
    return getW3FromAddress(lpAddress).eth.contract(
        address=Web3.toChecksumAddress(deprefixAddress(lpAddress)),
        abi=LPFactoryABI
    )


def testGetReserves(lp):
    c = getLPContract(lp.lpAddress)
    print(lp, c.functions.getReserves().call())


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


def getLprsTvl(lprs: LPReserve) -> float:
    return lprs.reserve1 / pow(10, lprs.lp.decimal1) * lprs.lp.token1Price + \
           lprs.reserve2 / pow(10, lprs.lp.decimal2) * lprs.lp.token2Price


TVL_THRESHOLD = 10_000
def getReserveDataFilteredByTVL(lprsDict: Dict[str, LPReserve]) -> [LPReserve]:
    # for lprs in list(lprsDict.values())[:10]:
    #     print(lprs.reserve1, lprs.lp.token1, lprs.lp.decimal1, lprs.lp.token1Price, \
    #           lprs.reserve2, lprs.lp.token2, lprs.lp.decimal2, lprs.lp.token2Price, \
    #           getLprsTvl(lprs))

    return list(filter(lambda lprs: getLprsTvl(lprs) >= TVL_THRESHOLD, lprsDict.values()))


def lpReserveToLpReserveOutput(lprs: LPReserve) -> LPReserveOutput:
    return LPReserveOutput(
        token1Address=lprs.lp.token1Address,
        token2Address=lprs.lp.token2Address,
        token1Symbol=lprs.lp.token1,
        token2Symbol=lprs.lp.token2,
        lpAddress=lprs.lp.lpAddress,
        reserve1=lprs.reserve1 / pow(10, lprs.lp.decimal1),
        reserve2=lprs.reserve2 / pow(10, lprs.lp.decimal2),
        timestamp=lprs.timestamp)


# Repeatedly get the latest lp-reserved for a given set of lps, using multicall
def testGetReservesMulticall(lps: [LP], myProducer: MyProducer):
    w3 = getW3FromAddress(lps[0].lpAddress)
    lpDict = dict(list(map(lambda lp: (deprefixAddress(lp.lpAddress.lower()), lp), lps)))
    print(lpDict)

    funcs = list(map(lambda lp: getLPContract(lp.lpAddress).functions.getReserves(), lps))

    lprsDict = getLatestReserves(w3, funcs, lpDict)
    while True:
        newDict = getLatestReserves(w3, funcs, lpDict)
        compareReservesDataDelta(lprsDict, newDict)
        lprsDict = newDict
        lprsFiltered = getReserveDataFilteredByTVL(lprsDict)
        print(len(lprsFiltered))

        lprsOutput = list(map(lpReserveToLpReserveOutput, lprsFiltered))
        latestTimestamp = max(list(map(lambda lprs: lprs.timestamp, lprsOutput)))
        print('latest timestamp to now = ', time.time() - latestTimestamp)
        myProducer.push('lp-reserves', json.dumps(lprsOutput, cls=EnhancedJSONEncoder))


if __name__ == "__main__":
    startTime = time.time()
    load_dotenv()

    myProducer = MyProducer(configFile=".ccloud", topic="latest-lp-reserves-2")

    lps = getLPs()
    testGetReservesMulticall(lps, myProducer)

    duration = time.time() - startTime
    print(f"All done, spent {duration / 60:.2f} minutes.")


# filter by tvl
# calc arbi
# send back to stream
# expose as gql sub
# rolling display in frontend







