from easyctp.pipeline import SaveInflux, SaveMongo
from easyctp.quotation import MarketDataApi
from easyctp.trader import EasyTrader


class MarketDataFacade:
    @classmethod
    def to_db(cls, user, password, broker, front, instrument_ids, db_uri, worker=10, trade_front=None, model='influx'):
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
        if 'influx' in model:
            pipe = SaveInflux(
                market_data, worker=worker, host=db_uri)
        elif 'mongo' in model:
            pipe = SaveMongo(
                market_data, worker=worker, host=db_uri)
        else:
            pipe = SaveInflux(
                market_data, worker=worker, host=db_uri)
        pipe.start()
