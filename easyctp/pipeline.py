import threading
from queue import Queue, Empty
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
        return item


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


class SaveInflux(BasePipeline):
    CQ_TEMPLATE = '''
        CREATE CONTINUOUS QUERY "{db}_ctp_{interval}" ON "{db}"
        BEGIN
              SELECT min(*), max(*), mean(*), first(*), last(*) INTO ctp_{interval}
              FROM ctp{previous_interval}
              GROUP BY time({interval}), instrument_id
        END'''

    def __init__(self, queue, worker=10, batch_size=20,
                 host='localhost',
                 port=8086,
                 username='root',
                 password='root',
                 database=None):
        super().__init__(queue)
        if 'influxdb://' in host:
            args = urlparse(host)
            host = args.hostname
            port = args.port
            username = args.username
            password = args.password
            database = args.path[1:]

        # init client
        self.client = influxdb.InfluxDBClient(host=host, username=username, password=password, port=port)

        # create database if not exists
        self.client.create_database(database)
        self.client.switch_database(database)

        # create cq if not exists
        intervals = ['1m', '5m', '15m', '30m', '1h', '1d']
        for i, interval in enumerate(intervals):
            previous_interval = '_' + intervals[i - 1] if i != 0 else ''
            cq = self.CQ_TEMPLATE.format(previous_interval=previous_interval, interval=interval, db=database)
            self.client.query(cq)

        self.batch_queue = Queue()

        for _ in range(worker):
            threading.Thread(target=self.batch_insert_worker, args=(batch_size,)).start()

    def batch_insert_worker(self, batch_size):
        points = []
        while True:
            try:
                try:
                    item = self.batch_queue.get(timeout=0.5)
                except Empty:
                    if len(points) > 0:
                        self.client.write_points(points)
                    continue
                point = self.convert_to_point(item)
                points.append(point)

                if len(points) < batch_size:
                    continue
                self.flush_points(points)
            except Exception as e:
                log.error('batch insert unexpected error: {}'.format(e))

    @staticmethod
    def convert_to_point(item):
        return {
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
            'time': '{}T{}.{:03d}+08:00'.format(item.TradingDay.decode(), item.UpdateTime.decode(),
                                                item.UpdateMillisec)
        }

    def flush_points(self, points):
        try:
            self.client.write_points(points)
        except Exception as e:
            log.error(
                'influxdb client write points error: {}, points: {}'.format(e, points))
        del points[:]

    def _process_item(self, item):
        self.batch_queue.put(item)
        return item


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
