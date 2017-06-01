import datetime

import pymongo
from dateutil.parser import parse

from easyctp.pipeline import SaveInflux
from easyctp.quotation import MarketDataApi
from easyctp.trader import EasyTrader


class MarketDataFacade:
    @classmethod
    def to_influx(cls, user, password, broker, front, instrument_ids, influxdb_uri, worker=10, trade_front=None):
        if instrument_ids == 'all':
            trader = EasyTrader()
            trader.login(user=user, password=password, broker=broker,
                         front=trade_front)
            instrument_ids = trader.query_all_instruments()
        print('合约总数: ', len(instrument_ids))
        md = MarketDataApi()
        market_data = md.prepare(user=user, password=password, broker=broker,
                                 front=front,
                                 instrument_ids=instrument_ids)

        pipe = SaveInflux(
            market_data, worker=worker, host=influxdb_uri)
        pipe.start()


class MarketDataExporter:
    @classmethod
    def export_to(cls, strategy, user, password, broker, front, instrument_ids, trade_front=None):
        if instrument_ids == 'all':
            trader = EasyTrader()
            trader.login(user=user, password=password, broker=broker,
                         front=trade_front)
            instrument_ids = trader.query_all_instruments()
        print('合约总数: ', len(instrument_ids))
        md = MarketDataApi()
        market_data = md.prepare(user=user, password=password, broker=broker,
                                 front=front,
                                 instrument_ids=instrument_ids)

        for data in market_data:
            print(data)
            strategy.save(data)


class MongoStrategy:
    def __init__(self, mongo_uri):
        self._client = pymongo.MongoClient(host=mongo_uri)
        self._db = self._client.get_default_database()

    def save(self, item):
        now = datetime.datetime.now()
        time_str = '{}T{}.{:03d}'.format(item.TradingDay.decode(), item.UpdateTime.decode(),
                                         item.UpdateMillisec)
        tick = dict(LastPrice=item.LastPrice, PreSettlementPrice=item.PreSettlementPrice,
                    PreClosePrice=item.PreClosePrice, PreOpenInterest=item.PreOpenInterest,
                    OpenPrice=item.OpenPrice, HighestPrice=item.HighestPrice,
                    LowestPrice=item.LowestPrice,
                    Volume=item.Volume,
                    Turnover=item.Turnover, OpenInterest=item.OpenInterest, ClosePrice=item.ClosePrice,
                    SettlementPrice=item.SettlementPrice, UpperLimitPrice=item.UpperLimitPrice,
                    LowerLimitPrice=item.LowerLimitPrice,
                    PreDelta=item.PreDelta,
                    BidPrice1=item.BidPrice1, BidVolume1=item.BidVolume1, AskPrice1=item.AskPrice1,
                    AskVolume1=item.AskVolume1, AveragePrice=item.AveragePrice,
                    InstrumentID=item.InstrumentID.decode(),
                    ActionDay=item.ActionDay.decode(),
                    TradingDay=item.TradingDay.decode(),
                    time_str=time_str,
                    time=parse(time_str),
                    created_at=now, created_date=now.strftime('%Y%m%d'))

        self._db.history.insert(tick)
