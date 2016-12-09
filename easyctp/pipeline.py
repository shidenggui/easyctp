import threading
import traceback
from multiprocessing.dummy import Pool
from queue import Queue
from urllib.parse import urlparse

import influxdb
from ctp.futures import ApiStruct

from easyctp.log import log


class BasePipeline:
    def __init__(self, queue):
        self.queue = queue

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

            convert_item = self._process_item(item)
            if convert_item is not None:
                return convert_item

    def _process_item(self, item):
        self._process_item(item)
        return item


class AsyncPipeline(BasePipeline):
    def __init__(self, queue, worker=10):
        super().__init__(queue)
        self.pool = Pool(worker)
        self.result_queue = Queue()
        threading.Thread(target=self._detect_error, daemon=True).start()

    def _process_item(self, item):
        future = self.pool.apply_async(self._process_item, args=(item,))
        self.result_queue.put(future)
        return item

    def _detect_error(self):
        while True:
            future = self.result_queue.get()
            try:
                future.get()
            except:
                traceback.print_exc()


class ConvertDict(BasePipeline):
    def _process_item(self, item):
        return dict(item)


class SaveMysql(BasePipeline):
    pass


class SaveMongo(BasePipeline):
    pass


class FilterInvalidItem(BasePipeline):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def _process_item(self, item: ApiStruct.DepthMarketData):
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


class SaveInflux(AsyncPipeline):
    CQ_TEMPLATE = '''
        CREATE CONTINUOUS QUERY "{db}_ctp_{interval}" ON "{db}"
        BEGIN
              SELECT min(*), max(*), mean(*), first(*), last(*) INTO ctp_{interval}
              FROM ctp{previous_interval}
              GROUP BY time({interval}), instrument_id
        END'''

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

        self.client = influxdb.InfluxDBClient(host=host, username=username, password=password, port=port)

        self.client.create_database(database)
        self.client.switch_database(database)

        intervals = ['1m', '5m', '15m', '30m', '1h', '1d']
        for i, interval in enumerate(intervals):
            previous_interval = '_' + intervals[i - 1] if i != 0 else ''
            cq = self.CQ_TEMPLATE.format(previous_interval=previous_interval, interval=interval, db=database)
            self.client.query(cq)

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
            self.client.write_points(points)
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
    }
