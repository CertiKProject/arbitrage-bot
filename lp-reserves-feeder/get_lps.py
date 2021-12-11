
import os
from dataclasses import dataclass
import snowflake.connector


@dataclass
class LP:
    lpAddress: str
    token1: str
    token2: str
    token1Address: str
    token2Address: str
    token1Decimals: int
    token2Decimals: int
    token1Price: float   # this might get outdated
    token2Price: float   # this might get outdated
    balance: float


def runSnowflakeSQL(sql: str):
    conn = snowflake.connector.connect(
        user=os.environ.get('SNOW_USER'),
        password=os.environ.get('SNOW_PASSWORD'),
        account=os.environ.get('SNOW_ACCOUNT')
    )
    cur = conn.cursor()
    cur.execute("USE WAREHOUSE WORKSHEET_WH;")
    cur.execute("USE DATABASE PRODUCTION_DB;")

    cur.execute(sql)
    r = cur.fetchall()
    cur.close()
    return r


def getSQL():

    # sql = """
    #
    # with data as
    #
    # ((
    #   SELECT ADDRESS, Label, pair.token0 as token0_address, pair.token1 as token1_address, decimal0, decimal1
    #   FROM PRODUCTION_DB.LIQUIDITY.LIQUIDITY_PAIRS_DECIMALS as pair
    #   JOIN PRJ_ADDR_MAPPING as project
    #   ON pair."TOKEN0" = project.contract_address
    #   JOIN POPULAR_SWAP_PARTNERS as partner
    #   ON pair."TOKEN1" = partner.contract_address
    # ) UNION (
    #   SELECT ADDRESS, Label, pair.token0 as token0_address, pair.token1 as token1_address, decimal0, decimal1
    #   FROM PRODUCTION_DB.LIQUIDITY.LIQUIDITY_PAIRS_DECIMALS as pair
    #   JOIN PRJ_ADDR_MAPPING as project
    #   ON pair."TOKEN1" = project.contract_address
    #   JOIN POPULAR_SWAP_PARTNERS as partner
    #   ON pair."TOKEN0" = partner.contract_address
    # )),
    #
    #
    # prices as
    #
    # (select token_address, max(latest_price_in_usd) as latest_price_in_usd
    # from
    #     (select token_address, first_value(avg_price) over (partition by token_address order by date desc) as latest_price_in_usd
    #      from PRODUCTION_DB.LIQUIDITY.TOKEN_PRICE_BYDATE)
    # group by token_address)
    #
    #
    # select address,
    #     split(split(label, ':')[1], '/')[0]::string as token0,
    #     split(split(label, ':')[1], '/')[1]::string as token1,
    #     decimal0,
    #     decimal1,
    #     token0_address,
    #     token1_address,
    #     p0.latest_price_in_usd as token0_price,
    #     p1.latest_price_in_usd as token1_price
    #
    #     from data
    #     join prices p0
    #     join prices p1
    #     on data.token0_address = p0.token_address and data.token1_address = p1.token_address
    #     where startswith(address, 'bsc')
    # """


    return \
    """
    with prices as 
    (select token_address, max(latest_price_in_usd) as latest_price_in_usd 
        from 
            (select token_address, first_value(price_in_usd) over (partition by token_address order by timestamp desc) as latest_price_in_usd 
             from PRODUCTION_DB.PUBLIC.COINGECKO_TOKEN_PRICES)
        group by token_address
    ),
        
    reserves as
    (select pool_address, token0, token1, max(latest_reserve0) as latest_reserve0, max(latest_reserve1) as latest_reserve1 
        from
        (select 
          pool_address, token0, token1, 
          first_value(reserve0) over (partition by pool_address order by timestamp desc) as latest_reserve0, 
          first_value(reserve1) over (partition by pool_address order by timestamp desc) as latest_reserve1
          from PRODUCTION_DB.LIQUIDITY.RESERVE_AMOUNT
          where startswith(pool_address, 'bsc'))
        group by pool_address, token0, token1
    ),
    
    balance0 as
    (select p.address, p.label, p.token0, t.decimals, prices.latest_price_in_usd, reserves.latest_reserve0 * prices.latest_price_in_usd as balance0
      from PRODUCTION_DB.LIQUIDITY.LIQUIDITY_PAIRS p
      join PRODUCTION_DB.PUBLIC.SKYNET_PRD_TOKENS t  
      join prices
      join reserves
      on p.token0 = t.address and p.token0 = prices.token_address and p.address = reserves.pool_address 
      where startswith(p.address, 'bsc')
      order by balance0 desc
    ),
    
    balance1 as
    (select p.address, p.label, p.token1, t.decimals, prices.latest_price_in_usd, reserves.latest_reserve1 * prices.latest_price_in_usd as balance1
      from PRODUCTION_DB.LIQUIDITY.LIQUIDITY_PAIRS p
      join PRODUCTION_DB.PUBLIC.SKYNET_PRD_TOKENS t  
      join prices
      join reserves
      on p.token0 = t.address and p.token1 = prices.token_address and p.address = reserves.pool_address 
      where startswith(p.address, 'bsc') 
      order by balance1 desc
    )
    
    select 
        b0.address, 
        split(split(b0.label, ':')[1], '/')[0]::string as token0,
        split(split(b0.label, ':')[1], '/')[1]::string as token1,    
        b0.token0 as token0_address,
        b1.token1 as token1_address,
        b0.decimals as decimals0,
        b1.decimals as decimals1,
        b0.latest_price_in_usd as token0_price,
        b1.latest_price_in_usd as token1_price,
        b0.balance0 + b1.balance1 as balance
        
        from balance0 b0
        join balance1 b1
        on b0.address = b1.address
        where balance > 10000
    """


def getLPs():

    r = runSnowflakeSQL(getSQL())
    r = list(map(lambda e: LP(lpAddress=e[0], token1=e[1], token2=e[2],
                              token1Address=e[3], token2Address=e[4],
                              token1Decimals=int(e[5]), token2Decimals=int(e[6]),
                              token1Price=float(e[7]), token2Price=float(e[8]),
                              balance=float(e[9])), r))
    return r

