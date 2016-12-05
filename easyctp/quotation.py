import tempfile
from multiprocessing.dummy import Pool
from queue import Queue, Empty

import influxdb
from ctp.futures import ApiStruct, MdApi

from easyctp.utils import dict_iter


class MarketData(object):
    def __init__(self, queue=Queue(), timeout=None):
        assert isinstance(queue, Queue)
        self.queue = queue
        self.timeout = timeout

        ApiStruct.DepthMarketData.__iter__ = dict_iter

    def __next__(self):
        try:
            item = self.queue.get(timeout=self.timeout)
        except Empty:
            raise StopIteration
        return item

    def put(self, *args, **kwargs):
        self.queue.put_nowait(*args, **kwargs)

    def __iter__(self):
        return self


class InfluxMarketData(MarketData):
    def __init__(self, db_name='ctp', *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.client = influxdb.InfluxDBClient()
        self.client.create_database(db_name)
        self.pool = Pool(1)

        self.db_name = db_name

    def __next__(self):
        item = super().__next__()
        self.pool.apply_async(self.insert, args=(item,))
        return item

    def insert(self, item):
        points = [{
            'measurement': 'ctp',
            'tags': {
                'instrument_id': item.InstrumentID.decode(),
            },
            'fields': dict(LastPrice=item.LastPrice, PreSettlementPrice=item.PreSettlementPrice,
                           PreClosePrice=item.PreClosePrice, PreOpenInterest=item.PreOpenInterest,
                           OpenPrice=item.OpenPrice, HighestPrice=item.HighestPrice, LowestPrice=item.LowestPrice,
                           Volume=item.Volume,
                           Turnover=item.Turnover, OpenInterest=item.OpenInterest, ClosePrice=item.ClosePrice,
                           SettlementPrice=item.SettlementPrice, UpperLimitPrice=item.UpperLimitPrice,
                           LowerLimitPrice=item.LowerLimitPrice,
                           PreDelta=item.PreDelta, CurrDelta=item.CurrDelta,
                           BidPrice1=item.BidPrice1, BidVolume1=item.BidVolume1, AskPrice1=item.AskPrice1,
                           AskVolume1=item.AskVolume1,
                           BidPrice2=item.BidPrice2, BidVolume2=item.BidVolume2, AskPrice2=item.AskPrice2,
                           AskVolume2=item.AskVolume2, BidPrice3=item.BidPrice3, BidVolume3=item.BidVolume3,
                           AskPrice3=item.AskPrice3, AskVolume3=item.AskVolume3, BidPrice4=item.BidPrice4,
                           BidVolume4=item.BidVolume4, AskPrice4=item.AskPrice4, AskVolume4=item.AskVolume4,
                           BidPrice5=item.BidPrice5, BidVolume5=item.BidVolume5, AskPrice5=item.AskPrice5,
                           AskVolume5=item.AskVolume5, AveragePrice=item.AveragePrice),
            'time': '{}T{}.{:03d}'.format(item.ActionDay.decode(), item.UpdateTime.decode(), item.UpdateMillisec)

        }]
        self.client.write_points(points, database=self.db_name)


class MarketDataApi(MdApi):
    def __init__(self):
        super(MdApi, self).__init__()
        self.user = None
        self.password = None
        self.broker = None
        self.instrument_ids = None

        self.request_id = 0
        self.market_data = None

    def prepare(self, user, password, broker, front, instrument_ids, market_data=MarketData()):
        """
        :param user: investor id
        :param password: password
        :param broker: broker id
        :param front: 行情服务器地址, 类似 tcp://127.0.0.1:8000
        :param instrument_ids: 订阅合约列表, 类似　['rb1705', 'rb1710]
        :param market_data: 返回的接受市场数据对象
        :return: market_data: 返回的接受市场数据对象
        """
        self.user = self.auto_encode_bytes(user)
        self.password = self.auto_encode_bytes(password)
        self.broker = self.auto_encode_bytes(broker)

        self.market_data = market_data

        self.instrument_ids = [self.auto_encode_bytes(instrument) for instrument in instrument_ids]

        self.Create(tempfile.mktemp().encode())
        self.RegisterFront(self.auto_encode_bytes(front))
        self.Init()

        return self.market_data

    @staticmethod
    def auto_encode_bytes(value):
        if isinstance(value, str):
            return value.encode()
        return value

    def OnRspUserLogin(self, pRspUserLogin, pRspInfo, nRequestID, bIsLast):
        assert isinstance(pRspInfo, ApiStruct.RspInfo)
        if pRspInfo.ErrorID == 0:
            print('登录成功，开始订阅合约 {}'.format(self.instrument_ids))
            self.SubscribeMarketData(self.instrument_ids)
        else:
            print('登录失败 ErrorID: {} ErrorMsg: {}'.format(pRspInfo.ErrorID, pRspInfo.ErrorMsg))

    def OnFrontConnected(self):
        print('客户端与交易后台建立起通信连接成功, 开始登录')
        user_login_args = ApiStruct.ReqUserLogin(UserID=self.user,
                                                 Password=self.password,
                                                 BrokerID=self.broker)
        ret = self.ReqUserLogin(user_login_args, self.request_id)
        self.request_id += 1

        if ret == 0:
            print('登录信息发送成功，等待返回')

    def OnRtnDepthMarketData(self, pDepthMarketData):
        self.market_data.put(pDepthMarketData)
