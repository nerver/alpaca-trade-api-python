import logging

from alpaca_trade_api.stream2 import Stream

log = logging.getLogger('')


async def print_trade(t):
    print('trade', t.symbol, t.timestamp)

async def print_quote(q):
    print('quote', q.symbol, q.timestamp)

async def print_trade_update(tu):
    print('trade update', tu)

def main():
    # TODO: better example
    logging.basicConfig(level=logging.INFO)
    stream = Stream()
    stream.subscribe_trade_updates(print_trade_update)
    stream.subscribe_trades(print_trade, 'AAPL', 'MSFT')
    stream.subscribe_quotes(print_quote, 'NIO')

    @stream.on_bar('*')
    async def _(bar):
        print('bar', bar)

    stream.run()


if __name__ == "__main__":
    main()
