import itertools
import tempfile
from copy import copy
from queue import Queue, Empty

from ctp.futures import ApiStruct, MdApi

from easyctp.log import log
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

    def get(self, *args, **kwargs):
        return self.queue.get(*args, **kwargs)

    def __iter__(self):
        return self


class MarketDataApi(MdApi):
    def __init__(self):
        super(MdApi, self).__init__()
        self.user = None
        self.password = None
        self.broker = None
        self.instrument_ids = None

        self.request_id = itertools.count()
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
        if pRspInfo.ErrorID == 0:
            log.info('登录成功，开始订阅合约 {}'.format(self.instrument_ids))
            self.SubscribeMarketData(self.instrument_ids)
        else:
            log.error('登录失败 ErrorID: {} ErrorMsg: {}'.format(pRspInfo.ErrorID, pRspInfo.ErrorMsg))

    def OnFrontDisconnected(self, nReason):
        log.error('客户端无法注册服务器地址: ', nReason)

    def OnFrontConnected(self):
        log.info('客户端与交易后台建立连接成功, 开始登录')
        user_login_args = ApiStruct.ReqUserLogin(UserID=self.user,
                                                 Password=self.password,
                                                 BrokerID=self.broker)
        ret = self.ReqUserLogin(user_login_args, next(self.request_id))

        if ret == 0:
            log.info('登录信息发送成功，等待返回')

    def OnRtnDepthMarketData(self, pDepthMarketData):
        self.market_data.put(copy(pDepthMarketData))
