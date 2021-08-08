import time

import ccxt
import numpy as np
import psycopg2

from utils.db_queries import *


class CryptoCrawler:
    def __init__(self, exchange_id='binance', max_retries=3, symbol='RSRUSDT',
                 timeframe='1m', since='2021-08-08T00:00:00Z', limit=100):
        self.exchange_id = exchange_id
        self.exchange = getattr(ccxt, self.exchange_id)({
            'enableRateLimit': True,  # required by the Manual
        })
        # convert since from string to milliseconds integer if needed
        if isinstance(since, str):
            self.since = self.exchange.parse8601(since)
        if self.since is None:
            raise "Cannot convert begin time to milliseconds integer"
        # preload all markets from the exchange
        self.exchange.load_markets()
        self.max_retries = max_retries
        self.symbol = symbol
        self.timeframe = timeframe
        self.limit = limit
        # create hypertable
        with psycopg2.connect(CONNECTION, **keepalive_kwargs) as conn:
            cursor = conn.cursor()
            try:
                # if table exists, delete it
                cursor.execute(delete_table_query.format(SYMBOL=self.symbol))
            except:
                pass
            conn.commit()
            cursor.execute(create_table_query.format(SYMBOL=self.symbol))
            cursor.execute(create_hypertable_query.format(SYMBOL=self.symbol))

    def retry_fetch_ohlcv(self, since):
        num_retries = 0
        try:
            num_retries += 1
            ohlcv = self.exchange.fetch_ohlcv(
                self.symbol, self.timeframe, since, self.limit)
            # print('Fetched', len(ohlcv), symbol, 'candles from', exchange.iso8601 (ohlcv[0][0]), 'to', exchange.iso8601 (ohlcv[-1][0]))
            return ohlcv
        except Exception:
            if num_retries > self.max_retries:
                # Exception('Failed to fetch', timeframe, symbol, 'OHLCV in', max_retries, 'attempts')
                raise

    def update_table(self, ohlcv: np.array):
        with psycopg2.connect(CONNECTION, **keepalive_kwargs) as conn:
            cursor = conn.cursor()
            conn.commit()
            tmp_queries = []
            for candle in ohlcv:
                tmp_queries.append(
                    f"('{self.symbol}', TO_TIMESTAMP({candle[0]/1000}),\
                        {candle[1]}, {candle[2]}, {candle[3]}, {candle[4]}, {candle[5]})")
            try:
                cursor.execute(f"""
                            INSERT INTO {self.symbol} (token, time, open, high, low, close, volume) 
                            VALUES {",".join(tmp_queries)};
                """)
            except (Exception, psycopg2.Error) as error:
                print(error.pgerror)
            conn.commit()

    def scrape_ohlcv(self):
        timeframe_duration_in_seconds = self.exchange.parse_timeframe(
            self.timeframe)
        timeframe_duration_in_ms = timeframe_duration_in_seconds * 1000
        timedelta = self.limit * timeframe_duration_in_ms
        now = self.exchange.milliseconds()
        fetch_since = self.since
        time_length = now - fetch_since
        num_candles = 0
        while fetch_since < now:
            ohlcv = self.retry_fetch_ohlcv(since=fetch_since)
            ohlcv = self.exchange.filter_by_since_limit(
                ohlcv, fetch_since, None, key=0)
            fetch_since = (
                ohlcv[-1][0] + 1) if len(ohlcv) else (fetch_since + timedelta)
            if len(ohlcv):
                self.update_table(ohlcv)
                num_candles += len(ohlcv)
                print(
                    f"{(fetch_since - self.since)/time_length*100:.2f}% - {num_candles} candles crawled from {self.exchange_id}")
        return


if __name__ == "__main__":
    crawler = CryptoCrawler(since='2021-01-01T00:00:00Z', limit=1000)
    crawler.scrape_ohlcv()
