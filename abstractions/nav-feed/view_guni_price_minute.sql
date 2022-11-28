-- The goal of this query is to calculate the price of a G-UNI Token
/*
1. Get the lowerTick, upperTick, liquidity, and Uniswap Pool Address of the position
2. Get the current sqrtRatioX96 and liquidity of the uniswap pool
3. Calculate the amount of Token A and Token B in the position
4. Calculate the USD value of tokenA and tokenB to find the USD Value of the position
5. Calculate the number of G-UNI Tokens in Existence
6. Divide by the amount of G-Uni tokens
*/

with

position as (
select
    "call_block_time" as block_time,
    "call_block_number" as block_number,
    0 as index,
    0 as liquidity,
    "_lowerTick" as l_tick,
    "_upperTick" as u_tick
from    gelato."GUniPool_call_initialize"
where   "contract_address" = '\x0D2A2Df39436b5c5f986552869124bA29b7Df1AC'
and     "call_success" = true

union

select
    "evt_block_time" as block_time,
    "evt_block_number" as block_number,
    "evt_index" as index,
    "liquidityAfter" - "liquidityBefore" as liquidity,
    "lowerTick_" as l_tick,
    "upperTick_" as u_tick
from    gelato."GUniPool_evt_Rebalance"
where   contract_address = '\x0D2A2Df39436b5c5f986552869124bA29b7Df1AC'
),

swaps as (
select 
    "evt_block_time" as block_time,
    "evt_block_number" as block_number,
    "evt_index" as index,
    "sqrtPriceX96"
from    uniswap_v3."Pair_evt_Initialize"
where   "contract_address" = (select "uniPool" from gelato."GUniFactory_evt_PoolCreated" where "pool" = '\x0D2A2Df39436b5c5f986552869124bA29b7Df1')

union

select
    "evt_block_time" as block_time,
    "evt_block_number" as block_number,
    "evt_index" as index,
    "sqrtPriceX96"
from    uniswap_v3."Pair_evt_Swap"
where   "contract_address" = (select "uniPool" from gelato."GUniFactory_evt_PoolCreated" where "pool" = '\x0D2A2Df39436b5c5f986552869124bA29b7Df1AC')
),

guni_tokens as (
select 
    "evt_block_time" as block_time,
    "evt_block_number" as block_number,
    "evt_index" as index,
    "liquidityMinted" as liquidity,
    "mintAmount" as guni_amount
from    gelato."GUniPool_evt_Minted"
where   "contract_address" = '\x0D2A2Df39436b5c5f986552869124bA29b7Df1AC'

union

select 
    "evt_block_time" as block_time,
    "evt_block_number" as block_number,
    "evt_index" as index,
    -"liquidityBurned" as liquidity,
    -"burnAmount" as guni_amount
from    gelato."GUniPool_evt_Burned"
where   "contract_address" = '\x0D2A2Df39436b5c5f986552869124bA29b7Df1AC'
),

position_final as (
select
    block_time, block_number, index,
    first_value(l_tick) over (partition by pos_part order by block_number, index) as l_tick,
    first_value(u_tick) over (partition by pos_part order by block_number, index) as u_tick,
    sum(liquidity) over (order by block_number, index) as liquidity
from    (
        select
            block_time, block_number, index,
            l_tick, u_tick,
            sum(case when l_tick is null then 0 else 1 end) over (order by block_number, index) as pos_part,
            liquidity
        from    (
                select * from position
                union
                select block_time, block_number, index, liquidity, null as l_tick, null as u_tick from guni_tokens
                ) t0
        ) t1
),

position_breakdown as (
select
    block_time, block_number, index,
    l_tick, u_tick, liquidity, "sqrtPriceX96",
    case
        when p <= pa then l * (pb - pa) / (pb * pa) -- eq 4
        when p >= pb then 0 
        else l * (pb - p) / (p * pb)
    end as token0_amount,
    case
        when p <= pa then 0
        when p >= pb then l * (pb - pa) --eq 8
        else l * (p - pa)
    end as token1_amount    
from    (
        select
            block_time, block_number, index,
            l_tick, u_tick, liquidity, "sqrtPriceX96",
            liquidity as l,
            "sqrtPriceX96" / 2^96 as p,
            sqrt(1.0001 ^ l_tick) as pa,
            sqrt(1.0001 ^ u_tick) as pb
        from    (
                select
                    block_time, block_number, index,
                    first_value(l_tick) over (partition by pos_part order by block_number, index) as l_tick,
                    first_value(u_tick) over (partition by pos_part order by block_number, index) as u_tick,
                    first_value(liquidity) over (partition by pos_part order by block_number, index) as liquidity,
                    first_value("sqrtPriceX96") over (partition by price_part order by block_number, index) as "sqrtPriceX96"
                from    (
                        select
                            block_time, block_number, index, "sqrtPriceX96", l_tick, u_tick, liquidity,
                            sum(case when l_tick is null then 0 else 1 end) over (order by block_number, index) as pos_part,
                            sum(case when "sqrtPriceX96" is null then 0 else 1 end) over (order by block_number, index) as price_part
                        from    (
                                select block_time, block_number, index, "sqrtPriceX96", null as l_tick, null as u_tick, null as liquidity from swaps
                                union
                                select block_time, block_number, index, null as "sqrtPriceX96", l_tick, u_tick, liquidity from position_final
                                ) t0
                        ) t1
                ) t2
        ) t3
),

token_amounts as (
select
    day,
    first_value(l_tick) over (partition by pos_part order by day) as l_tick,
    first_value(u_tick) over (partition by pos_part order by day) as u_tick,
    first_value(token0_amount) over (partition by pos_part order by day) as token0_amount,
    first_value(token1_amount) over (partition by pos_part order by day) as token1_amount
from    (
        select
            day,
            l_tick,
            u_tick,
            token0_amount, token1_amount,
            sum(case when l_tick is null then 0 else 1 end) over (order by block_number, index) as pos_part
        from    (
                select
                    d.day,
                    block_time, block_number, index,
                    l_tick, u_tick, liquidity, "sqrtPriceX96",
                    token0_amount, token1_amount,
                    row_number() over (partition by d.day order by block_number desc, index desc) as rnb
                from        (select generate_series(date_trunc('day', (select min(block_time) from position_breakdown)), date_trunc('day', now()), '1 day') as day) d
                left join   position_breakdown pb on date_trunc('day', pb.block_time) = d.day
                ) t0
        where   rnb = 1
        ) t1
)

select * from token_amounts
