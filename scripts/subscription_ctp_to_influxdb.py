from argparse import ArgumentParser

from easyctp.quotation import MarketDataApi
from easyctp.worker import InfluxWorker


def subscription_ctp(user, password, broker, front, instrument_ids, influxdb_uri):
    md = MarketDataApi()
    market_data = md.prepare(user=user, password=password, broker=broker,
                             front=front,
                             instrument_ids=instrument_ids)

    influx_worker = InfluxWorker(market_data, worker=1, host=influxdb_uri)
    influx_worker.start()


if __name__ == '__main__':
    opt = ArgumentParser()
    opt.add_argument('--user')
    opt.add_argument('--password')
    opt.add_argument('--broker')
    opt.add_argument('--front')
    opt.add_argument('--instruments')
    opt.add_argument('--influxdb', help='like influxdb://user:password@127.0.0.1:8086/database_name')
    args = opt.parse_args()
    instrument_ids = args.instruments.split(',')
    subscription_ctp(user=args.user, password=args.password, broker=args.broker,
                     front=args.front,
                     instrument_ids=instrument_ids, influxdb_uri=args.influxdb)
