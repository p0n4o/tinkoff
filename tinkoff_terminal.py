import asyncio
import pandas as pd
import os
from dotenv import load_dotenv

from tinkoff.invest import (
    AsyncClient,
    CandleInstrument,
    MarketDataRequest,
    SubscribeCandlesRequest,
    SubscriptionAction,
    SubscriptionInterval
)

load_dotenv()
TOKEN = os.getenv('INVEST_TOKEN')

def cast_money(v):
    """
    https://tinkoff.github.io/investAPI/faq_custom_types/
    :param v:
    :return:
    """
    return v.units + v.nano / 1e9 # nano - 9 нулей

df = None
data = None

def on_message(c):
    global df, data
    pr_data = data
    data = {'time': [c.time.strftime('%Y-%m-%d %H:%M')],
            'open': [cast_money(c.open)],
            'high': [cast_money(c.high)],
            'low': [cast_money(c.high)],
            'close': [cast_money(c.high)],
            'volume': [c.volume]}

    if pr_data is not None:

        if pr_data['time'][0] != data['time'][0]:
            new_df = pd.DataFrame(pr_data)
            new_df.set_index('time', inplace=True)

            if df is None:
                df = new_df
            else:
                df = pd.concat([df, new_df])
                df.to_csv('data.csv')
            return df

async def main():
    async def request_iterator():
        yield MarketDataRequest(
            subscribe_candles_request=SubscribeCandlesRequest(
                subscription_action=SubscriptionAction.SUBSCRIPTION_ACTION_SUBSCRIBE,
                instruments=[
                    CandleInstrument(
                        figi="BBG004730N88",
                        interval=SubscriptionInterval.SUBSCRIPTION_INTERVAL_ONE_MINUTE,
                    )
                ],
                waiting_close=True
            )
        )
        while True:
            await asyncio.sleep(1)

    async with AsyncClient(TOKEN) as client:
        async for marketdata in client.market_data_stream.market_data_stream(
            request_iterator()
        ):
            if marketdata.candle is None:
                print(marketdata)
            else:
                print(on_message(marketdata.candle))

if __name__ == "__main__":
    asyncio.run(main())
