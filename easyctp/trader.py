import itertools
import tempfile
import time
from collections import defaultdict
from copy import copy
from queue import Queue, Empty

from ctp.futures import TraderApi, ApiStruct

from easyctp.log import log


class ResultMap:
    def __init__(self):
        self.map = defaultdict(Queue)

    def get(self, request_id, timeout=None):
        res_collect = []
        while True:
            try:
                *res, is_last = self.map[request_id].get(timeout=timeout)
            except Empty as e:
                raise TimeoutError from e
            res_collect.append(res)
            if is_last:
                del self.map[request_id]
                return res_collect

    def put(self, request_id, *args):
        item = tuple(copy(x) for x in args)
        self.map[request_id].put(item)


class EasyTrader(TraderApi):
    def __init__(self):
        super(TraderApi, self).__init__()
        self.user = None
        self.password = None
        self.broker = None
        self.instrument_ids = None

        self.request_id = itertools.count()
        self.results_map = ResultMap()
        self.login_success = False

    def login(self, user, password, broker, front):
        """
        :param user: investor id
        :param password: password
        :param broker: broker id
        :param front: 行情服务器地址, 类似 tcp://127.0.0.1:8000
        """
        self.user = self.auto_encode_bytes(user)
        self.password = self.auto_encode_bytes(password)
        self.broker = self.auto_encode_bytes(broker)

        self.Create(tempfile.mktemp().encode())
        self.RegisterFront(self.auto_encode_bytes(front))
        self.SubscribePublicTopic(ApiStruct.TERT_RESUME)
        self.SubscribePrivateTopic(ApiStruct.TERT_RESUME)
        self.Init()

        while True:
            if self.login_success:
                return
            else:
                time.sleep(0.01)

    @staticmethod
    def auto_encode_bytes(value):
        if isinstance(value, str):
            return value.encode()
        return value

    def OnRspUserLogin(self, pRspUserLogin, pRspInfo, nRequestID, bIsLast):
        assert isinstance(pRspInfo, ApiStruct.RspInfo)
        if pRspInfo.ErrorID == 0:
            log.info('登录成功')
            self.login_success = True
        else:
            log.error('登录失败 ErrorID: {}, ErrorMsg: {}'.format(pRspInfo.ErrorID, pRspInfo.ErrorMsg.decode('gbk')))

    def OnFrontDisconnected(self, nReason):
        log.error('客户端无法注册服务器地址, ErrorID: {}'.format(nReason))

    def OnFrontConnected(self):
        log.info('客户端与交易后台建立连接成功, 开始登录')
        user_login_args = ApiStruct.ReqUserLogin(UserID=self.user,
                                                 Password=self.password,
                                                 BrokerID=self.broker)
        ret = self.ReqUserLogin(user_login_args, next(self.request_id))

        if ret == 0:
            log.info('登录信息发送成功，等待返回')

    def OnRspQryInstrument(self, pInstrument, pRspInfo, nRequestID, bIsLast):
        self.results_map.put(nRequestID, pInstrument, pRspInfo, bIsLast)

    def OnRspError(self, pRspInfo, nRequestID, bIsLast):
        log.error('发生错误 ErrorID: {} ErrorMsg: {}'.format(pRspInfo.ErrorID, pRspInfo.ErrorMsg.decode('gbk')))

    def query_all_instruments(self, timeout=10):
        instrument = ApiStruct.QryInstrument()
        request_id = next(self.request_id)
        self.ReqQryInstrument(instrument, request_id)

        instrument_ids = set()
        response = self.results_map.get(request_id, timeout=timeout)
        for pInstrument, _ in response:
            instrument_ids.add(pInstrument.InstrumentID)
        return instrument_ids
