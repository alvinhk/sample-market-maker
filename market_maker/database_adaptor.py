import datetime
import dateutil.parser
from opentsdb import TSDBClient
import pytz

class DatabaseAdaptor:
    db = TSDBClient('127.0.0.1')

    @staticmethod
    def add(table, data):
        if table == 'orderBook10':
            DatabaseAdaptor.add_order_book(data)
        if table == 'trade':
            DatabaseAdaptor.add_trade(data)

    @staticmethod
    def add_order_book(data):
        for record in data:
            kwargs = {'symbol': record['symbol']}
            count_bid = 1
            for bid in record['bids']:
                price = bid[0]
                size = bid[1]
                kwargs['bid'+str(count_bid)+'price'] = price
                kwargs['bid'+str(count_bid)+'size'] = size
                count_bid += 1

            count_ask = 1
            for ask in record['asks']:
                price = ask[0]
                size = ask[1]
                kwargs['ask'+str(count_ask)+'price'] = price
                kwargs['ask'+str(count_ask)+'size'] = size
                count_ask += 1

            dt = dateutil.parser.parse(record['timestamp'])
            epoch = pytz.utc.localize(datetime.datetime.utcfromtimestamp(0))
            DatabaseAdaptor.db.send("marketdata.orderbook", (dt - epoch).total_seconds() * 1000.0, **kwargs)

    @staticmethod
    def add_trade(data):
        for record in data:
            kwargs = {'symbol': record['symbol']}
            kwargs['side'] = record['side']
            kwargs['size'] = record['size']
            kwargs['price'] = record['price']
            kwargs['tickDirection'] = record['tickDirection']
            kwargs['trdMatchID'] = record['trdMatchID']
            kwargs['grossValue'] = record['grossValue']
            kwargs['homeNotional'] = record['homeNotional']
            kwargs['foreignNotional'] = record['foreignNotional']

            dt = dateutil.parser.parse(record['timestamp'])
            epoch = pytz.utc.localize(datetime.datetime.utcfromtimestamp(0))
            DatabaseAdaptor.db.send("marketdata.trade", (dt - epoch).total_seconds() * 1000.0, **kwargs)

