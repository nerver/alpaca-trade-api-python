[![PyPI version](https://badge.fury.io/py/alpaca-trade-api.svg)](https://badge.fury.io/py/alpaca-trade-api)
[![CircleCI](https://circleci.com/gh/alpacahq/alpaca-trade-api-python.svg?style=shield)](https://circleci.com/gh/alpacahq/alpaca-trade-api-python)
[![Updates](https://pyup.io/repos/github/alpacahq/alpaca-trade-api-python/shield.svg)](https://pyup.io/repos/github/alpacahq/alpaca-trade-api-python/)
[![Python 3](https://pyup.io/repos/github/alpacahq/alpaca-trade-api-python/python-3-shield.svg)](https://pyup.io/repos/github/alpacahq/alpaca-trade-api-python/)

# alpaca-trade-api-python

`alpaca-trade-api-python` is a python library for the [Alpaca Commission Free Trading API](https://alpaca.markets).
It allows rapid trading algo development easily, with support for
both REST and streaming data interfaces. For details of each API behavior,
please see the online [API document](https://docs.alpaca.markets).
```diff
- To use the Streaming abilities go to section called StreamConn
```


Note this module supports only python version 3.6 and above, due to
the async/await and websockets module dependency.

## Install
We support python 3.x. If you want to work with python < 3.7 please note that these package dropped support for python <3.7 for the following versions:
```
pandas >= 1.2.0
numpy >= 1.20.0
scipy >= 1.6.0
```
The solution - manually install these package before installing alpaca-trade-api. e.g:
```
pip install pandas==1.1.5 numpy==1.19.4 scipy==1.5.4
```

```bash
$ pip3 install alpaca-trade-api
```

## Example

In order to call Alpaca's trade API, you need to sign up for an account and obtain API key pairs. Replace <key_id> and <secret_key> with what you get from the web console.

### REST example
```python
import alpaca_trade_api as tradeapi

api = tradeapi.REST('<key_id>', '<secret_key>', base_url='https://paper-api.alpaca.markets') # or use ENV Vars shown below
account = api.get_account()
api.list_positions()
```

please note the exact format of the dates

## Example Scripts

Please see the `examples/` folder for some example scripts that make use of this API

## API Document

The HTTP API document is located at https://docs.alpaca.markets/

## API Version

API Version now defaults to 'v2', however, if you still have a 'v1' account, you may need to specify api_version='v1' to properly use the API until you migrate.

## Authentication

The Alpaca API requires API key ID and secret key, which you can obtain from the
web console after you sign in.  You can pass `key_id` and `secret_key` to the initializers of
`REST` or `StreamConn` as arguments, or set up environment variables as
outlined below.

## Alpaca Environment Variables

The Alpaca SDK will check the environment for a number of variables that can be used rather than hard-coding these into your scripts.

| Environment                      | default                                                                                | Description                                                                                                            |
| -------------------------------- | -------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------- |
| APCA_API_KEY_ID=<key_id>         |                                                                                        | Your API Key                                                                                                           |
| APCA_API_SECRET_KEY=<secret_key> |                                                                                        | Your API Secret Key                                                                                                    |
| APCA_API_BASE_URL=url            | https://api.alpaca.markets (for live)<br/>https://paper-api.alpaca.markets (for paper) | Specify the URL for API calls, *Default is live, you must specify this to switch to paper endpoint!*                   |
| APCA_API_DATA_URL=url            | https://data.alpaca.markets                                                            | Endpoint for data API                                                                                                  |
| APCA_RETRY_MAX=3                 | 3                                                                                      | The number of subsequent API calls to retry on timeouts                                                                |
| APCA_RETRY_WAIT=3                | 3                                                                                      | seconds to wait between each retry attempt                                                                             |
| APCA_RETRY_CODES=429,504         | 429,504                                                                                | comma-separated HTTP status code for which retry is attempted                                                          |
| POLYGON_WS_URL                   | wss://socket.polygon.io/stocks                                                         | Endpoint for streaming polygon data.  You likely don't need to change this unless you want to proxy it for example     |
| POLYGON_KEY_ID                   |                                                                                        | Your Polygon key, if it's not the same as your Alpaca API key. Most users will not need to set this to access Polygon. |
| DATA_PROXY_WS                    |                                                                                        | When using the alpaca-proxy-agent you need to set this environment variable as described ![here](https://github.com/shlomikushchi/alpaca-proxy-agent) |

## REST

The `REST` class is the entry point for the API request.  The instance of this
class provides all REST API calls such as account, orders, positions,
and bars.

Each returned object is wrapped by a subclass of the `Entity` class (or a list of it).
This helper class provides property access (the "dot notation") to the
json object, backed by the original object stored in the `_raw` field.
It also converts certain types to the appropriate python object.

```python
import alpaca_trade_api as tradeapi

api = tradeapi.REST()
account = api.get_account()
account.status
=> 'ACTIVE'
```

The `Entity` class also converts the timestamp string field to a pandas.Timestamp
object.  Its `_raw` property returns the original raw primitive data unmarshaled
from the response JSON text.

Please note that the API is throttled, currently 200 requests per minute, per account.  If your client exceeds this number, a 429 Too many requests status will be returned and this library will retry according to the retry environment variables as configured.

If the retries are exceeded, or other API error is returned, `alpaca_trade_api.rest.APIError` is raised.
You can access the following information through this object.
- the API error code: `.code` property
- the API error message: `str(error)`
- the original request object: `.request` property
- the original response objecgt: `.response` property
- the HTTP status code: `.status_code` property

### API REST Methods

| Rest Method                                      | End Point          |   Result                                                                                | 
| --------------------------------                 | -------------------| ------------------------------------------------------------------ |
| get_account()                                    | `GET /account` and | `Account` entity.|
| get_order_by_client_order_id(client_order_id)    | `GET /orders` with client_order_id | `Order` entity.|
| list_orders(status=None, limit=None, after=None, until=None, direction=None, nested=None) | `GET /orders` | list of `Order` entities. `after` and `until` need to be string format, which you can obtain by `pd.Timestamp().isoformat()` |
| submit_order(symbol, qty, side, type, time_in_force, limit_price=None, stop_price=None, client_order_id=None, order_class=None, take_profit=None, stop_loss=None, trail_price=None, trail_percent=None)| `POST /orders` |  `Order` entity. |
| get_order(order_id)                              | `GET /orders/{order_id}` | `Order` entity.|
| cancel_order(order_id)                           | `DELETE /orders/{order_id}` | |
| cancel_all_orders()                              | `DELETE /orders`| |
| list_positions()                                 | `GET /positions` | list of `Position` entities|
| get_position(symbol)                             | `GET /positions/{symbol}` | `Position` entity.|
| list_assets(status=None, asset_class=None)       | `GET /assets` | list of `Asset` entities|
| get_asset(symbol)                                | `GET /assets/{symbol}` | `Asset` entity|
| get_barset(symbols, timeframe, limit, start=None, end=None, after=None, until=None) | `GET /bars/{timeframe}` | Barset with `limit` Bar objects for each of the the requested symbols. `timeframe` can be one of `minute`, `1Min`, `5Min`, `15Min`, `day` or `1D`. `minute` is an alias of `1Min`. Similarly, `day` is an alias of `1D`. `start`, `end`, `after`, and `until` need to be string format, which you can obtain with `pd.Timestamp().isoformat()` `after` cannot be used with `start` and `until` cannot be used with `end`.|
| get_aggs(symbol, timespan, multiplier, _from, to)| `GET /aggs/ticker/{symbol}/range/{multiplier}/{timespan}/{from}/{to}` | `Aggs` entity. `multiplier` is the size of the timespan multiplier. `timespan` is the size of the time window, can be one of `minute`, `hour`, `day`, `week`, `month`, `quarter` or `year`. `_from` and `to` must be in `YYYY-MM-DD` format, e.g. `2020-01-15`.| 
| get_last_trade(symbol)                           | `GET /last/stocks/{symbol}` | `Trade` entity|
| get_last_quote(symbol)                           | `GET /last_quote/stocks/{symbol}` | `Quote` entity|
| get_clock()                                      | `GET /clock` | `Clock` entity|
| get_calendar(start=None, end=None)               | `GET /calendar` | `Calendar` entity|
| get_portfolio_history(date_start=None, date_end=None, period=None, timeframe=None, extended_hours=None) | `GET /account/portfolio/history` | PortfolioHistory entity. PortfolioHistory.df can be used to get the results as a dataframe|

### Rest Examples

##### Using `submit_order()`
Below is an example of submitting a bracket order.
```py
api.submit_order(
    symbol='SPY',
    side='buy',
    type='market',
    qty='100',
    time_in_force='day',
    order_class='bracket',
    take_profit=dict(
        limit_price='305.0',
    ),
    stop_loss=dict(
        stop_price='295.5',
        limit_price='295.5',
    )
)
```

##### Using `get_barset()`
```python 
import pandas as pd
NY = 'America/New_York'
start=pd.Timestamp('2020-08-01', tz=NY).isoformat()
end=pd.Timestamp('2020-08-30', tz=NY).isoformat()
print(api.get_barset(['AAPL', 'GOOG'], 'day', start=start, end=end).df)

# Minute data example
start=pd.Timestamp('2020-08-28 9:30', tz=NY).isoformat()
end=pd.Timestamp('2020-08-28 16:00', tz=NY).isoformat()
print(api.get_barset(['AAPL', 'GOOG'], 'minute', start=start, end=end).df)

```

---

## StreamConn

The `StreamConn` class provides WebSocket-based event-driven
interfaces.  Using the `on` decorator of the instance, you can
define custom event handlers that are called when the pattern
is matched on the channel name.  Once event handlers are set up,
call the `run` method which runs forever until a critical exception
is raised. This module itself does not provide any threading
capability, so if you need to consume the messages pushed from the
server, you need to run it in a background thread.

We provide 2 price data websockets. The polygon data, and the alpaca data.
We default to Alpaca data, and one must explicitly specify the polygon data stream in order to use that.
It is done by passing the `data_stream` keyword to the `__init__()` function of `StreamConn` (options: `'alpacadatav1', 'polygon'`)

This class provides a unique interface to the two interfaces, both
Alpaca's account/trade updates events and Polygon's price updates.
One connection is established when the `subscribe()` is called with
the corresponding channel names.  For example, if you subscribe to
`trade_updates`, a WebSocket connects to Alpaca stream API, and
if `AM.*` given to the `subscribe()` method, a WebSocket connection is
established to Polygon's interface. If your account is enabled for
Alpaca Data API streaming, adding `alpacadatav1/` prefix to `T.<symbol>`,
`Q.<symbol>` and `AM.<symbol>` will also connect to the data stream
interface.

The `run` method is a short-cut to start subscribing to channels and
running forever.  The call will be blocked forever until a critical
exception is raised, and each event handler is called asynchronously
upon the message arrivals.

The `run` method tries to reconnect to the server in the event of
connection failure.  In this case, you may want to reset your state
which is best in the `connect` event.  The method still raises
an exception in the case any other unknown error happens inside the
event loop.

The `msg` object passed to each handler is wrapped by the entity
helper class if the message is from the server.

Each event handler has to be a marked as `async`.  Otherwise,
a `ValueError` is raised when registering it as an event handler.

```python
conn = StreamConn()

@conn.on(r'^trade_updates$')
async def on_account_updates(conn, channel, account):
    print('account', account)

@conn.on(r'^status$')
async def on_status(conn, channel, data):
    print('polygon status update', data)

@conn.on(r'^AM$')
async def on_minute_bars(conn, channel, bar):
    print('bars', bar)

@conn.on(r'^A$')
async def on_second_bars(conn, channel, bar):
    print('bars', bar)

# blocks forever
conn.run(['trade_updates', 'AM.*'])

# if Data API streaming is enabled
# conn.run(['trade_updates', 'alpacadatav1/AM.SPY'])

```

You will likely call the `run` method in a thread since it will keep running
unless an exception is raised.

| StreamConn Method                     | Description                                                                            | 
| --------------------------------      | -------------------------------------------------------------------------------------- |
| subscribe(channels)                   |  Request "listen" to the server.  `channels` must be a list of string channel names.|
| unsubscribe(channels)                 |  Request to stop "listening" to the server.  `channels` must be a list of string channel names.|
| run(channels)                         |  Goes into an infinite loop and awaits for messages from the server.  You should set up event listeners using the `on` or `register` method before calling `run`. |
| on(channel_pat)                       |  As in the above example, this is a decorator method to add an event handler function. `channel_pat` is used as a regular expression pattern to filter stream names.|
| register(channel_pat, func)           |  Registers a function as an event handler that is triggered by the stream events that match with `channel_path` regular expression. Calling this method with the same `channel_pat` will overwrite the old handler.|
| deregister(channel_pat)               | Deregisters the event handler function that was previously registered via `on` or `register` method. |


#### Debugging
Websocket exceptions may occur during execution.
It will usually happen during the `consume()` method, which basically is the 
websocket steady-state.<br>
exceptions during the consume method may occur due to:
- server disconnections
- error while handling the response data

We handle the first issue by reconnecting the websocket every time there's a disconnection.
The second issue, is usually a user's code issue. To help you find it, we added a flag to the 
StreamConn object called `debug`. It is set to False by default, but you can turn it on to get a more
verbose logs when this exception happens.
Turn it on like so `StreamConn(debug=True)`  

## Logging
You should define a logger in your app in order to make sure you get all the messages from the different components.<br>
It will help you debug, and make sure you don't miss issues when they occur.<br>
The simplest way to define a logger, if you have no experience with the python logger - will be something like this:
```py
import logging
logging.basicConfig(format='%(asctime)s %(message)s', level=logging.INFO)
```

# Polygon API Service

Alpaca's API key ID can be used to access Polygon API, the documentation for
which is found [here](https://polygon.io/docs/).
This python SDK wraps their API service and seamlessly integrates it with the Alpaca
API. `alpaca_trade_api.REST.polygon` will be the `REST` object for Polygon.

The example below gives AAPL daily OHLCV data in a DataFrame format.

```py
import alpaca_trade_api as tradeapi

api = tradeapi.REST()
# all of these examples work
aapl = api.polygon.historic_agg_v2('AAPL', 1, 'day', _from='2019-01-01', to='2019-02-01').df
aapl = api.polygon.historic_agg_v2('AAPL', 1, 'day', _from=datetime.datetime(2019, 1, 1), to='2019-02-01').df
aapl = api.polygon.historic_agg_v2('AAPL', 1, 'day', _from=datetime.date(2019, 1, 1), to='2019-02-01').df
aapl = api.polygon.historic_agg_v2('AAPL', 1, 'day', _from=pd.Timestamp('2019-01-01'), to='2019-02-01').df
# timestamp should be in milliseconds datetime.datetime(2019, 1, 1).timestamp()*1000 == 1546293600000
aapl = api.polygon.historic_agg_v2('AAPL', 1, 'day', _from=1546293600000, to='2019-02-01').df
```
and here's a minute example usage:
```py
import pytz
NY = 'America/New_York'

start = pytz.timezone(NY).localize(datetime(2020,1,2,9,30)).timestamp()*1000  # timestamp in micro seconds
# another alternative will be: start = pd.Timestamp('2020-01-02 09:30', tz=NY).value/1e6
end = pytz.timezone(NY).localize(datetime(2020,1,2,16,0)).timestamp()*1000
df = api.polygon.historic_agg_v2('AAPL', 1, 'minute', _from=start, to=end).df

```

## polygon/REST
It is initialized through the alpaca `REST` object.

| REST Method                           | Description                                                                            | 
| --------------------------------      | -------------------------------------------------------------------------------------- |
| exchanges()                           |  Returns a list of `Exchange` entity.|
| symbol_type_map()                     |  Returns a `SymbolTypeMap` object.|
| historic_trades_v2(symbol, date, timestamp=None, timestamp_limit=None, reverse=None, limit=None) |  Returns a `TradesV2` which is a list of `Trade` entities. `date` is a date string such as '2018-2-2'.  The returned quotes are from this day only. `timestamp` is an integer in Unix Epoch nanoseconds as the lower bound filter, exclusive. `timestamp_limit` is an integer in Unix Epoch nanoseconds as the maximum timestamp allowed in the results. `limit` is an integer for the number of ticks to return.  Default and max is 50000.      |
| TradesV2.df                           | Returns a pandas DataFrame object with the ticks returned by `historic_trades_v2`. |
| historic_quotes_v2(symbol, date, timestamp=None, timestamp_limit=None, reverse=None, limit=None)|  Returns a `QuotesV2` which is a list of `Quote` entities. `date` is a date string such as '2018-2-2'.  The returned quotes are from this day only. `timestamp` is an integer in Unix Epoch nanoseconds as the lower bound filter, exclusive. `timestamp_limit` is an integer in Unix Epoch nanoseconds as the maximum timestamp allowed in the results. `limit` is an integer for the number of ticks to return.  Default and max is 50000.      |
| QuotesV2.df                           | Returns a pandas DataFrame object with the ticks returned by the `historic_quotes_v2`.|
| historic_agg_v2(self, symbol, multiplier, timespan, _from, to, unadjusted=False, limit=None)|  Returns an `AggsV2` which is a list of `Agg` entities. `AggsV2.df` gives you the DataFrame object.<br> - `multiplier` is an integer affecting the amount of data contained in each Agg object. <br> -`timespan` is a string affecting the length of time represented by each Agg object. It is one of the following values: `minute`, `hour`, `day`, `week`, `month`, `quarter`, `year`. <br> - `_from` is an Eastern Time timestamp string/object that filters the result for the lower bound, inclusive. we accept the date in these formats: datetime.datetime, datetime.date, pd.Timestamp, datetime.timestamp, isoformat string (YYYY-MM-DD).<br> -  `to` is an Eastern Time timestamp string that filters the result for the upper bound, inclusive. we support the same formats as the _from field.<br> -  `unadjusted` can be set to true if results should not be adjusted for splits.<br> -  `limit` is an integer to limit the number of results.  3000 is the default and max value. <br>The returned entities have fields relabeled with the longer name instead of shorter ones. For example, the `o` field is renamed to `open`.|
| Aggs.df                               |  Returns a pandas DataFrame object with the ticks returned by `historic_agg_v2`.|
| daily_open_close(symbol, date)        |  Returns a `DailyOpenClose` entity.|
| last_trade(symbol)                    |  Returns a `Trade` entity representing the last trade for the symbol.|
| last_quote(symbol)                    |  Returns a `Quote` entity representing the last quote for the symbol.|
| condition_map(ticktype='trades')      |  Returns a `ConditionMap` entity.|
| company(symbol)                       |  Returns a `Company` entity if `symbol` is string, or a dict[symbol -> `Company`] if `symbol` is a list of string.|
| dividends(symbol)                     |  Returns a `Dividends` entity if `symbol` is string, or a dict[symbol -> `Dividends`] if `symbol` is a list of string.|
| splits(symbol)                        |  Returns a `Splits` entity for the symbol.|
| earnings(symbol)                      |  Returns an `Earnings` entity if `symbol` is string, or a dict[symbol -> `Earnings`] if `symbol` is a list of string.|
| financials_v2(symbol, limit, report_type, sort)   | Returns an `Financials` entity if `symbol` is string, or a dict[symbol -> `Financials`] if `symbol` is a list of string. |
| news(symbol)                          |  Returns a `NewsList` entity for the symbol.|


## Running Multiple Strategies
There's a way to execute more than one algorithm at once.<br>
The websocket connection is limited to 1 connection per account. <br>
For that exact purpose this ![project](https://github.com/shlomikushchi/alpaca-proxy-agent)  was created<br>
The steps to execute this are:
* Run the Alpaca Proxy Agent as described in the project's README
* Define this env variable: `DATA_PROXY_WS` to be the address of the proxy agent. (e.g: `DATA_PROXY_WS=ws://127.0.0.1:8765`)
* If you are using the Alpaca data stream, make sure you you initiate the StreamConn object with the container's url, like so: data_url='http://127.0.0.1:8765'
* execute your algorithm. it will connect to the servers through the proxy agent allowing you to execute multiple strategies


## Raw Data vs Entity Data
By default the data returned from the api or streamed via StreamConn is wrapped with an Entity object for ease of use.<br>
Some users may prefer working with raw python objects (lists, dicts, ...). <br>You have 2 options to get the raw data:
* Each Entity object as a `_raw` property that extract the raw data from the object.
* If you only want to work with raw data, and avoid casting to Entity (which may take more time, casting back and forth) <br>you could pass `raw_data` argument
  to `Rest()` object or the `StreamConn()` object.

## Support and Contribution

For technical issues particular to this module, please report the
issue on this GitHub repository. Any API issues can be reported through
Alpaca's customer support.

New features, as well as bug fixes, by sending a pull request is always
welcomed.
