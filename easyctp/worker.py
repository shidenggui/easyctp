import threading
import traceback
from multiprocessing.dummy import Pool
from multiprocessing.pool import ApplyResult
from queue import Queue
from urllib.parse import urlparse

import influxdb
from ctp.futures import ApiStruct


class InfluxWorker:
    def __init__(self, queue, worker=1,
                 host='localhost',
                 port=8086,
                 username='root',
                 password='root',
                 database=None,
                 *args, **kwargs):
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

        self.queue = queue
        self.pool = Pool(worker)
        self.result_queue = Queue()
        detect_error_worker = threading.Thread(target=self.detect_error, daemon=True)
        detect_error_worker.start()

    def start(self):
        for item in self.queue:
            future = self.pool.apply_async(self.insert, args=(item,))
            self.result_queue.put(future)

    def detect_error(self):
        while True:
            future = self.result_queue.get()
            assert isinstance(future, ApplyResult)
            try:
                future.get()
            except:
                traceback.print_exc()

    def insert(self, item):
        assert isinstance(item, ApiStruct.DepthMarketData)
        print(item)
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
                           AskVolume1=item.AskVolume1, AveragePrice=item.AveragePrice),
            'time': '{}T{}.{:03d}'.format(item.ActionDay.decode(), item.UpdateTime.decode(), item.UpdateMillisec)
        }]
        try:
            self.client.write_points(points, database=self.database)
        except Exception as e:
            print('influxdb client write points error: ', e)
