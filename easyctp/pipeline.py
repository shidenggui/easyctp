import itertools
import threading
import traceback
from multiprocessing.dummy import Pool
from queue import Queue
from urllib.parse import urlparse

import influxdb
from ctp.futures import ApiStruct

from easyctp.log import log


class BasePipeline:
    def __init__(self, queue, worker=1):
        self.queue = queue
        self.pool = Pool(worker)
        self.result_queue = Queue()
        threading.Thread(target=self._detect_error, daemon=True).start()

    def __next__(self):
        return self.get()

    def __iter__(self):
        return self

    def start(self):
        while True:
            self.__next__()

    def get(self):
        while True:
            item = self.queue.get()

            future = self.pool.apply_async(self._process_item, args=(item,))
            self.result_queue.put(future)

            convert_item = self._convert_item(item)
            if convert_item is not None:
                return convert_item

    def _process_item(self, item):
        """impl code here"""
        pass

    def _convert_item(self, item):
        return item

    def _detect_error(self):
        while True:
            future = self.result_queue.get()
            try:
                future.get()
            except:
                traceback.print_exc()


class ConvertDict(BasePipeline):
    def _convert_item(self, item):
        return dict(item)


class SaveMysql(BasePipeline):
    pass


class SaveMongo(BasePipeline):
    pass


class FilterInvalidItem(BasePipeline):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.request_id = itertools.count()

    def _convert_item(self, item: ApiStruct.DepthMarketData):
        item.request_id = next(self.request_id)
        if len(item.UpdateTime) != 8:
            log.warn('invalid update time, item: {} skipping'.format(simple(item)))
            return None
        if len(item.ActionDay) != 8:
            log.warn('invalid time, item: {} skipping'.format(simple(item)))
            return None
        if len(item.InstrumentID) <= 2:
            log.warn('invalid instrument id, item: {} skipping'.format(simple(item)))
            return None
        return item


class SaveInflux(BasePipeline):
    def __init__(self, queue, worker=10,
                 host='localhost',
                 port=8086,
                 username='root',
                 password='root',
                 database=None):
        super().__init__(queue, worker)
        if 'influxdb://' in host:
            args = urlparse(host)
            host = args.hostname
            port = args.port
            username = args.username
            password = args.password
            database = args.path[1:]

        self.database = database

        self.client = influxdb.InfluxDBClient(host=host, username=username, password=password, database=database,
                                              port=port)
        self.client.create_database(self.database)
        self.client.query('''
        CREATE CONTINUOUS QUERY "ctp_1m" ON "ctp"
        BEGIN
              SELECT min(*), max(*), mean(*) INTO ctp..ctp_1m  from ctp GROUP BY time(1m), instrument_id
        END''')
        self.client.query('''
        CREATE CONTINUOUS QUERY "ctp_5m" ON "ctp"
        BEGIN
              SELECT min(*), max(*), mean(*) INTO ctp..ctp_5m  from ctp_1m GROUP BY time(5m), instrument_id
        END''')

    def _process_item(self, item):
        try:
            points = [{
                'measurement': 'ctp',
                'tags': {
                    'instrument_id': item.InstrumentID.decode(),
                },
                'fields': dict(LastPrice=item.LastPrice, PreSettlementPrice=item.PreSettlementPrice,
                               PreClosePrice=item.PreClosePrice, PreOpenInterest=item.PreOpenInterest,
                               OpenPrice=item.OpenPrice, HighestPrice=item.HighestPrice,
                               LowestPrice=item.LowestPrice,
                               Volume=item.Volume,
                               Turnover=item.Turnover, OpenInterest=item.OpenInterest, ClosePrice=item.ClosePrice,
                               SettlementPrice=item.SettlementPrice, UpperLimitPrice=item.UpperLimitPrice,
                               LowerLimitPrice=item.LowerLimitPrice,
                               PreDelta=item.PreDelta, CurrDelta=item.CurrDelta,
                               BidPrice1=item.BidPrice1, BidVolume1=item.BidVolume1, AskPrice1=item.AskPrice1,
                               AskVolume1=item.AskVolume1, AveragePrice=item.AveragePrice),
                'time': '{}T{}.{:03d}+08:00'.format(item.ActionDay.decode(), item.UpdateTime.decode(),
                                                    item.UpdateMillisec)
            }]
        except UnicodeDecodeError:
            log.error('invalid decode item {}, skipping'.format(simple(item)))
            return
        try:
            self.client.write_points(points, database=self.database)
        except Exception as e:
            log.error(
                'influxdb client write points error: {}, item: {}, points: {} item: {}'.format(e, simple(item), points,
                                                                                               simple(item)))


class PrintItem(BasePipeline):
    def _process_item(self, item):
        log.info('item: {}'.format(item))


def simple(item: ApiStruct.DepthMarketData):
    return {
        'instrument_id': item.InstrumentID,
        'update_time': item.UpdateTime,
        'update_time_len': len(item.UpdateTime),
        'update_millisec': item.UpdateMillisec,
        'action_day': item.ActionDay,
        'id': item.request_id
    }
