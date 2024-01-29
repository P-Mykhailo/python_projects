from kafka import KafkaProducer
import json
import ccxt

exchange = ccxt.binance()
markets = exchange.load_markets()
symbol = 'ETH/BTC'
market = exchange.market(symbol)
now = exchange.milliseconds()
since = exchange.parse8601(exchange.ymd(now))
end = 0
producer = KafkaProducer(bootstrap_servers='localhost:9092',
value_serializer=lambda m: json.dumps(m).encode('ascii'))

trades = exchange.fetch_trades(symbol, since, 100)
for trade in trades:
    transaction = {
    'id': trade['id'],
    'symbol': trade['symbol'],
    'price': trade['price'],
    'quantity': trade['side']
    #'timestamp': trade['datetime']
        }
    producer.send('CCXT', transaction)
    print(transaction)
metrics = producer.metrics()
print(metrics)
#comment