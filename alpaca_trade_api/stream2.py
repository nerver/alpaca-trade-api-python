import asyncio
import json
import os
import re
import traceback

import msgpack
import websockets

from .common import get_base_url, get_data_url, get_credentials, URL
from .entity import Account, Entity, trade_mapping, trade_mapping_v2, \
    agg_mapping, quote_mapping, quote_mapping_v2, bar_mapping_v2
from . import polygon
from .entity import Trade, Quote, Agg, Bar
import logging
from typing import List, Callable


log = logging.getLogger(__name__)


class _StreamConn(object):
    def __init__(self, key_id: str,
                 secret_key: str,
                 base_url: URL,
                 oauth: str = None):
        self._key_id = key_id
        self._secret_key = secret_key
        self._oauth = oauth
        self._base_url = re.sub(r'^http', 'ws', base_url)
        self._endpoint = self._base_url + '/stream'
        self._handlers = {}
        self._handler_symbols = {}
        self._streams = set([])
        self._ws = None
        self._retry = int(os.environ.get('APCA_RETRY_MAX', 3))
        self._retry_wait = int(os.environ.get('APCA_RETRY_WAIT', 3))
        self._retries = 0
        self._consume_task = None

    async def _connect(self):
        message = {
            'action': 'authenticate',
            'data': {
                'oauth_token': self._oauth
            } if self._oauth else {
                'key_id': self._key_id,
                'secret_key': self._secret_key,
            }
        }

        ws = await websockets.connect(self._endpoint)
        await ws.send(json.dumps(message))
        r = await ws.recv()
        if isinstance(r, bytes):
            r = r.decode('utf-8')
        msg = json.loads(r)

        if msg.get('data', {}).get('status'):
            status = msg.get('data').get('status')
            if status != 'authorized':
                raise ValueError(
                    (f"Invalid Alpaca API credentials, Failed to "
                     f"authenticate: {msg}")
                )
            else:
                self._retries = 0
        elif msg.get('data', {}).get('error'):
            raise Exception(f"Error while connecting to {self._endpoint}:"
                            f"{msg.get('data').get('error')}")
        else:
            self._retries = 0

        self._ws = ws
        await self._dispatch('authorized', msg)
        logging.info(f"connected to: {self._endpoint}")
        self._consume_task = asyncio.ensure_future(self._consume_msg())

    async def consume(self):
        if self._consume_task:
            await self._consume_task

    async def _consume_msg(self):
        ws = self._ws
        try:
            while True:
                r = await ws.recv()
                if isinstance(r, bytes):
                    r = r.decode('utf-8')
                msg = json.loads(r)
                stream = msg.get('stream')
                if stream is not None:
                    await self._dispatch(stream, msg)
        except websockets.WebSocketException as wse:
            log.warn(wse)
            await self.close()
            asyncio.ensure_future(self._ensure_ws())

    async def _ensure_ws(self):
        if self._ws is not None:
            return

        while self._retries <= self._retry:
            try:
                await self._connect()
                if self._streams:
                    await self.subscribe(self._streams)
                break
            except websockets.WebSocketException as wse:
                logging.warn(wse)
                self._ws = None
                self._retries += 1
                await asyncio.sleep(self._retry_wait * self._retry)
        else:
            raise ConnectionError("Max Retries Exceeded")

    async def subscribe(self, channels):
        if isinstance(channels, str):
            channels = [channels]
        if len(channels) > 0:
            await self._ensure_ws()
            self._streams |= set(channels)
            await self._ws.send(json.dumps({
                'action': 'listen',
                'data': {
                    'streams': channels,
                }
            }))

    async def unsubscribe(self, channels):
        if isinstance(channels, str):
            channels = [channels]
        if len(channels) > 0:
            await self._ws.send(json.dumps({
                'action': 'unlisten',
                'data': {
                    'streams': channels,
                }
            }))

    async def close(self):
        if self._consume_task:
            self._consume_task.cancel()
        if self._ws:
            await self._ws.close()
            self._ws = None

    def _cast(self, channel, msg):
        if channel == 'account_updates':
            return Account(msg)
        if channel.startswith('T.'):
            return Trade({trade_mapping[k]: v for k,
                          v in msg.items() if k in trade_mapping})
        if channel.startswith('Q.'):
            return Quote({quote_mapping[k]: v for k,
                          v in msg.items() if k in quote_mapping})
        if channel.startswith('A.') or channel.startswith('AM.'):
            # to be compatible with REST Agg
            msg['t'] = msg['s']
            return Agg({agg_mapping[k]: v for k,
                        v in msg.items() if k in agg_mapping})
        return Entity(msg)

    async def _dispatch(self, channel, msg):
        for pat, handler in self._handlers.items():
            if pat.match(channel):
                ent = self._cast(channel, msg['data'])
                await handler(self, channel, ent)

    def on(self, channel_pat, symbols=None):
        def decorator(func):
            self.register(channel_pat, func, symbols)
            return func

        return decorator

    def register(self, channel_pat, func: Callable, symbols=None):
        if not asyncio.iscoroutinefunction(func):
            raise ValueError('handler must be a coroutine function')
        if isinstance(channel_pat, str):
            channel_pat = re.compile(channel_pat)
        self._handlers[channel_pat] = func
        self._handler_symbols[func] = symbols

    def deregister(self, channel_pat):
        if isinstance(channel_pat, str):
            channel_pat = re.compile(channel_pat)
        self._handler_symbols.pop(self._handlers[channel_pat], None)
        del self._handlers[channel_pat]


class StreamConn(object):

    def __init__(
            self,
            key_id: str = None,
            secret_key: str = None,
            base_url: URL = None,
            data_url: URL = None,
            data_stream: str = None,
            debug: bool = False,
            oauth: str = None
    ):
        self._key_id, self._secret_key, self._oauth = \
            get_credentials(key_id, secret_key, oauth)
        self._base_url = base_url or get_base_url()
        self._data_url = data_url or get_data_url()
        if data_stream is not None:
            if data_stream in ('alpacadatav1', 'polygon'):
                _data_stream = data_stream
            else:
                raise ValueError('invalid data_stream name {}'.format(
                    data_stream))
        else:
            _data_stream = 'alpacadatav1'
        self._data_stream = _data_stream
        self._debug = debug

        self.trading_ws = _StreamConn(self._key_id,
                                      self._secret_key,
                                      self._base_url,
                                      self._oauth)

        if self._data_stream == 'polygon':
            # DATA_PROXY_WS is used for the alpaca-proxy-agent.
            # this is how we set the polygon ws to go through the proxy agent
            endpoint = os.environ.get("DATA_PROXY_WS", '')
            if endpoint:
                os.environ['POLYGON_WS_URL'] = endpoint
            self.data_ws = polygon.StreamConn(
                self._key_id + '-staging' if 'staging' in self._base_url else
                self._key_id)
            self._data_prefixes = (('Q.', 'T.', 'A.', 'AM.'))
        else:
            self.data_ws = _StreamConn(self._key_id,
                                       self._secret_key,
                                       self._data_url,
                                       self._oauth)
            self._data_prefixes = (
                ('Q.', 'T.', 'AM.', 'alpacadatav1/'))

        self._handlers = {}
        self._handler_symbols = {}

        try:
            self.loop = asyncio.get_event_loop()
        except websockets.WebSocketException as wse:
            logging.warn(wse)
            self.loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.loop)

    async def _ensure_ws(self, conn):
        if conn._handlers:
            return
        conn._handlers = self._handlers.copy()
        conn._handler_symbols = self._handler_symbols.copy()
        if isinstance(conn, _StreamConn):
            await conn._connect()
        else:
            await conn.connect()

    async def subscribe(self, channels: List[str]):
        '''Start subscribing to channels.
        If the necessary connection isn't open yet, it opens now.
        This may raise ValueError if a channel is not recognized.
        '''
        trading_channels, data_channels = [], []

        for c in channels:
            if c in ('trade_updates', 'account_updates'):
                trading_channels.append(c)
            elif c.startswith(self._data_prefixes):
                data_channels.append(c)
            else:
                raise ValueError(
                    ('unknown channel {} (you may need to specify ' +
                     'the right data_stream)').format(c))

        if trading_channels:
            await self._ensure_ws(self.trading_ws)
            await self.trading_ws.subscribe(trading_channels)
        if data_channels:
            await self._ensure_ws(self.data_ws)
            await self.data_ws.subscribe(data_channels)

    async def unsubscribe(self, channels: List[str]):
        '''Handle unsubscribing from channels.'''

        data_channels = [
            c for c in channels
            if c.startswith(self._data_prefixes)
        ]

        if data_channels:
            await self.data_ws.unsubscribe(data_channels)

    async def consume(self):
        await asyncio.gather(
            self.trading_ws.consume(),
            self.data_ws.consume(),
        )

    def run(self, initial_channels: List[str] = []):
        '''Run forever and block until exception is raised.
        initial_channels is the channels to start with.
        '''
        loop = self.loop
        should_renew = True  # should renew connection if it disconnects
        while should_renew:
            try:
                if loop.is_closed():
                    self.loop = asyncio.new_event_loop()
                    loop = self.loop
                loop.run_until_complete(self.subscribe(initial_channels))
                loop.run_until_complete(self.consume())
            except KeyboardInterrupt:
                logging.info("Exiting on Interrupt")
                should_renew = False
            except Exception as e:
                m = 'consume cancelled' \
                    if isinstance(e, asyncio.CancelledError) else e
                logging.error(f"error while consuming ws messages: {m}")
                if self._debug:
                    traceback.print_exc()
                loop.run_until_complete(self.close(should_renew))
                if loop.is_running():
                    loop.close()

    async def close(self, renew):
        """
        Close any of open connections
        :param renew: should re-open connection?
        """
        if self.trading_ws is not None:
            await self.trading_ws.close()
            self.trading_ws = None
        if self.data_ws is not None:
            await self.data_ws.close()
            self.data_ws = None
        if renew:
            self.trading_ws = _StreamConn(self._key_id,
                                          self._secret_key,
                                          self._base_url,
                                          self._oauth)
            if self._data_stream == 'polygon':
                self.data_ws = polygon.StreamConn(
                    self._key_id + '-staging' if 'staging' in
                    self._base_url else self._key_id)
            else:
                self.data_ws = _StreamConn(self._key_id,
                                           self._secret_key,
                                           self._data_url,
                                           self._oauth)

    def on(self, channel_pat, symbols=None):
        def decorator(func):
            self.register(channel_pat, func, symbols)
            return func

        return decorator

    def register(self, channel_pat, func: Callable, symbols=None):
        if not asyncio.iscoroutinefunction(func):
            raise ValueError('handler must be a coroutine function')
        if isinstance(channel_pat, str):
            channel_pat = re.compile(channel_pat)
        self._handlers[channel_pat] = func
        self._handler_symbols[func] = symbols

        if self.trading_ws:
            self.trading_ws.register(channel_pat, func, symbols)
        if self.data_ws:
            self.data_ws.register(channel_pat, func, symbols)

    def deregister(self, channel_pat):
        if isinstance(channel_pat, str):
            channel_pat = re.compile(channel_pat)
        self._handler_symbols.pop(self._handlers[channel_pat], None)
        del self._handlers[channel_pat]

        if self.trading_ws:
            self.trading_ws.deregister(channel_pat)
        if self.data_ws:
            self.data_ws.deregister(channel_pat)


###############################################################################
# Alpaca stream v2 implementation start here
###############################################################################

def _ensure_coroutine(handler):
    if not asyncio.iscoroutinefunction(handler):
        raise ValueError('handler must be a coroutine function')


class DataStream:
    def __init__(self, key_id: str, secret_key: str, base_url: URL):
        self._key_id = key_id
        self._secret_key = secret_key
        # TODO: get feed from the account
        self._feed = 'sip'
        base_url = re.sub(r'^http', 'ws', base_url)
        self._endpoint = base_url + '/v2/stream/' + self._feed
        self._trade_handlers = {}
        self._quote_handlers = {}
        self._bar_handlers = {}
        self._ws = None
        self._running = False

    async def _connect(self):
        self._ws = await websockets.connect(self._endpoint, extra_headers={
            'Content-Type': 'application/msgpack'})
        r = await self._ws.recv()
        msg = msgpack.unpackb(r)
        if msg[0]['T'] != 'success' or msg[0]['msg'] != 'connected':
            raise ValueError('connected message not received')

    async def _auth(self):
        await self._ws.send(msgpack.packb({
            'action': 'auth',
            'key': self._key_id,
            'secret': self._secret_key,
        }))
        r = await self._ws.recv()
        msg = msgpack.unpackb(r)
        if msg[0]['T'] != 'success' or msg[0]['msg'] != 'authenticated':
            raise ValueError('failed to authenticate')

    async def _dispatch(self, msg):
        t = msg.get('T')
        sym = msg.get('S')
        # convert msgpack timestamp to nanoseconds
        if 't' in msg:
            msg['t'] = msg['t'].seconds * int(1e9) + msg['t'].nanoseconds
        if t == 't':
            handler = self._trade_handlers.get(
                sym, self._trade_handlers.get('*', None))
            if handler:
                trade = Trade({trade_mapping_v2[k]: v
                               for k, v in msg.items()
                               if k in trade_mapping_v2})
                await handler(trade)
        elif t == 'q':
            handler = self._quote_handlers.get(
                sym, self._quote_handlers.get('*', None))
            if handler:
                quote = Quote({quote_mapping_v2[k]: v
                               for k, v in msg.items()
                               if k in quote_mapping_v2})
                await handler(quote)
        elif t == 'b':
            handler = self._bar_handlers.get(
                sym, self._bar_handlers.get('*', None))
            if handler:
                bar = Bar({bar_mapping_v2[k]: v
                           for k, v in msg.items()
                           if k in bar_mapping_v2})
                await handler(bar)
        elif t == 'subscription':
            log.info(f'subscribed to trades: {msg.get("trades", [])}, ' +
                     f'quotes: {msg.get("quotes", [])} and bars: {msg.get("bars", [])}')
        elif t == 'error':
            log.error(f'error: {msg.get("msg")} ({msg.get("code")})')

    async def _subscribe_all(self):
        if self._trade_handlers or self._quote_handlers or self._bar_handlers:
            await self._ws.send(msgpack.packb({
                'action': 'subscribe',
                'trades': tuple(self._trade_handlers.keys()),
                'quotes': tuple(self._quote_handlers.keys()),
                'bars': tuple(self._bar_handlers.keys()),
            }))

    def _subscribe(self, handler, symbols, handlers):
        _ensure_coroutine(handler)
        for symbol in symbols:
            handlers[symbol] = handler
        if self._running:
            asyncio.run(self._subscribe_all())

    async def _unsubscribe(self, trades=(), quotes=(), bars=()):
        if trades or quotes or bars:
            await self._ws.send(msgpack.packb({
                'action': 'unsubscribe',
                'trades': trades,
                'quotes': quotes,
                'bars': bars,
            }))

    def subscribe_trades(self, handler, *symbols):
        self._subscribe(handler, symbols, self._trade_handlers)

    def subscribe_quotes(self, handler, *symbols):
        self._subscribe(handler, symbols, self._quote_handlers)

    def subscribe_bars(self, handler, *symbols):
        self._subscribe(handler, symbols, self._bar_handlers)

    def unsubscribe_trades(self, *symbols):
        if self._running:
            asyncio.run(self._unsubscribe(trades=symbols))
        for symbol in symbols:
            del self._trade_handlers[symbol]

    def unsubscribe_quotes(self, *symbols):
        if self._running:
            asyncio.run(self._unsubscribe(quotes=symbols))
        for symbol in symbols:
            del self._quote_handlers[symbol]

    def unsubscribe_bars(self, *symbols):
        if self._running:
            asyncio.run(self._unsubscribe(bars=symbols))
        for symbol in symbols:
            del self._bar_handlers[symbol]

    async def _start_ws(self):
        await self._connect()
        await self._auth()
        log.info(f'connected to: {self._endpoint}')
        await self._subscribe_all()

    async def _consume(self):
        while True:
            r = await self._ws.recv()
            msgs = msgpack.unpackb(r)
            for msg in msgs:
                await self._dispatch(msg)

    async def _run_forever(self):
        # do not start the websocket connection until we subscribe to something
        while not (self._trade_handlers or self._quote_handlers or self._bar_handlers):
            await asyncio.sleep(0.1)
        log.info('started data stream')
        retries = 0
        while True:
            self._running = False
            try:
                await self._start_ws()
                self._running = True
                retries = 0
                await self._consume()
            except asyncio.CancelledError:
                log.info('cancelled, closing data stream connection')
                return
            except websockets.WebSocketException as wse:
                retries += 1
                if retries > int(os.environ.get('APCA_RETRY_MAX', 3)):
                    raise ConnectionError("max retries exceeded")
                if retries > 1:
                    await asyncio.sleep(
                        int(os.environ.get('APCA_RETRY_WAIT', 3)))
                logging.warn(
                    'websocket error, restarting connection: ' + str(wse))
            finally:
                await self.close()

    def run(self):
        try:
            asyncio.run(self._run_forever())
        except KeyboardInterrupt:
            log.info('exited')

    async def close(self):
        if self._ws:
            await self._ws.close()
            self._ws = None


class TradingStream:
    def __init__(self, key_id: str, secret_key: str, base_url: URL):
        self._key_id = key_id
        self._secret_key = secret_key
        base_url = re.sub(r'^http', 'ws', base_url)
        self._endpoint = base_url + '/stream/'
        self._trade_updates_handler = None
        self._ws = None
        self._running = False

    async def _connect(self):
        self._ws = await websockets.connect(self._endpoint)

    async def _auth(self):
        await self._ws.send(json.dumps({
            'action': 'authenticate',
            'data': {
                'key_id': self._key_id,
                'secret_key': self._secret_key,
            }
        }))
        r = await self._ws.recv()
        msg = json.loads(r)
        if msg.get('data').get('status') != 'authorized':
            raise ValueError('failed to authenticate')

    async def _dispatch(self, msg):
        stream = msg.get('stream')
        if stream == 'trade_updates':
            if self._trade_updates_handler:
                await self._trade_updates_handler(Entity(msg.get('data')))

    async def _subscribe_trade_updates(self):
        if self._trade_updates_handler:
            await self._ws.send(json.dumps({
                'action': 'listen',
                'data': {
                    'streams': ['trade_updates']
                }
            }))

    def subscribe_trade_updates(self, handler):
        _ensure_coroutine(handler)
        self._trade_updates_handler = handler
        if self._running:
            asyncio.run(self._subscribe_trade_updates())

    async def _start_ws(self):
        await self._connect()
        await self._auth()
        log.info(f'connected to: {self._endpoint}')
        await self._subscribe_trade_updates()

    async def _consume(self):
        while True:
            r = await self._ws.recv()
            msg = json.loads(r)
            await self._dispatch(msg)

    async def _run_forever(self):
        # do not start the websocket connection until we subscribe to something
        while not self._trade_updates_handler:
            await asyncio.sleep(0.1)
        log.info('started trading stream')
        retries = 0
        while True:
            self._running = False
            try:
                await self._start_ws()
                self._running = True
                retries = 0
                await self._consume()
            except asyncio.CancelledError:
                log.info('cancelled, closing trading stream connection')
                return
            except websockets.WebSocketException as wse:
                retries += 1
                if retries > int(os.environ.get('APCA_RETRY_MAX', 3)):
                    raise ConnectionError("max retries exceeded")
                if retries > 1:
                    await asyncio.sleep(
                        int(os.environ.get('APCA_RETRY_WAIT', 3)))
                logging.warn(
                    'websocket error, restarting connection: ' + str(wse))
            finally:
                await self.close()

    def run(self):
        try:
            asyncio.run(self._run_forever())
        except KeyboardInterrupt:
            log.info('exited')

    async def close(self):
        if self._ws:
            await self._ws.close()
            self._ws = None


class Stream:
    def __init__(self,
                 key_id: str = None,
                 secret_key: str = None,
                 base_url: URL = None,
                 data_url: URL = None):
        self._key_id, self._secret_key, _ = get_credentials(key_id, secret_key)
        self._base_url = base_url or get_base_url()
        self._data_url = data_url or get_data_url()

        self._trading_ws = TradingStream(
            self._key_id, self._secret_key, self._base_url)
        self._data_ws = DataStream(
            self._key_id, self._secret_key, self._data_url)

    def subscribe_trade_updates(self, handler):
        self._trading_ws.subscribe_trade_updates(handler)

    def subscribe_trades(self, handler, *symbols):
        self._data_ws.subscribe_trades(handler, *symbols)

    def subscribe_quotes(self, handler, *symbols):
        self._data_ws.subscribe_quotes(handler, *symbols)

    def subscribe_bars(self, handler, *symbols):
        self._data_ws.subscribe_bars(handler, *symbols)

    def on_trade_update(self, func):
        self.subscribe_trade_updates(func)
        return func

    def on_trade(self, *symbols):
        def decorator(func):
            self.subscribe_trades(func, *symbols)
            return func
        return decorator

    def on_quote(self, *symbols):
        def decorator(func):
            self.subscribe_quotes(func, *symbols)
            return func
        return decorator

    def on_bar(self, *symbols):
        def decorator(func):
            self.subscribe_bars(func, *symbols)
            return func
        return decorator

    def unsubscribe_trades(self, *symbols):
        self._data_ws.unsubscribe_trades(*symbols)

    def unsubscribe_quotes(self, *symbols):
        self._data_ws.unsubscribe_quotes(*symbols)

    def unsubscribe_bars(self, *symbols):
        self._data_ws.unsubscribe_bars(*symbols)

    async def _run_forever(self):
        await asyncio.gather(
            self._trading_ws._run_forever(),
            self._data_ws._run_forever())

    def run(self):
        try:
            asyncio.run(self._run_forever())
        except KeyboardInterrupt:
            pass
