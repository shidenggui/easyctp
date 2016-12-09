from argparse import ArgumentParser

from easyctp.facade import MarketDataFacade

if __name__ == '__main__':
    opt = ArgumentParser()
    opt.add_argument('--user', required=True)
    opt.add_argument('--password', required=True)
    opt.add_argument('--broker', required=True)
    opt.add_argument('--front', required=True)
    opt.add_argument('--trade_front', type=str, help='订阅所有合约时需要设置 trade_front')
    opt.add_argument('--instruments', type=str, default='all', help='订阅所有合约时需要设置 trade_front')
    opt.add_argument('--influxdb', help='like influxdb://user:password@127.0.0.1:8086/database_name')
    opt.add_argument('--worker', type=int, default=10)
    args = opt.parse_args()
    instrument_ids = args.instruments if args.instruments == 'all' else args.instruments.split(',')
    if instrument_ids == 'all' and args.trade_front is None:
        raise Exception('订阅所有合约时需要设置 trade_front')
    MarketDataFacade.to_influx(user=args.user, password=args.password, broker=args.broker,
                               front=args.front,
                               instrument_ids=instrument_ids, influxdb_uri=args.influxdb, worker=args.worker,
                               trade_front=args.trade_front)
