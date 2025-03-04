import datetime
from enum import Enum
from typing import List
import dateutil.parser
import requests
from .entity import (
    Entity, Aggsv2, Aggsv2Set, Trade, TradesV2, Quote, QuotesV2,
    Exchange, SymbolTypeMap, ConditionMap, Company, Dividends, Splits,
    Earnings, Financials, NewsList, Ticker, DailyOpenClose, Symbol
)
from alpaca_trade_api.common import get_polygon_credentials, URL, DATE


Exchanges = List[Exchange]
Tickers = List[Ticker]
Symbols = List[Symbol]


class FinancialsReportType(Enum):
    "https://polygon.io/docs/get_v2_reference_financials__stocksTicker__anchor"
    Y = "Year"
    YA = "Year annualized"
    Q = "Quarter"
    QA = "Quarter Annualized"
    T = "Trailing twelve months"
    TA = "trailing twelve months annualized"


class FinancialsSort(Enum):
    "https://polygon.io/docs/get_v2_reference_financials__stocksTicker__anchor"
    ReportPeriodAsc = "reportPeriod"
    ReportPeriodDesc = "-reportPeriod"
    CalendarDateAsc = "calendarDate"
    CalendarDateDesc = "-calendarDate"


def _is_list_like(o) -> bool:
    """
    returns True if o is either a list, a set or a tuple
    that way we could accept ['AAPL', 'GOOG'] or ('AAPL', 'GOOG') etc.
    """
    return isinstance(o, (list, set, tuple))


def format_date_for_api_call(date):
    """
    we support different date formats:
    - string
    - datetime.date
    - datetime.datetime
    - timestamp number
    - pd.Timestamp
    that gives the user the freedom to use the API in a very flexible way
    """
    if isinstance(date, datetime.datetime):
        return date.date().isoformat()
    elif isinstance(date, datetime.date):
        return date.isoformat()
    elif isinstance(date, str):  # string date
        return dateutil.parser.parse(date).date().isoformat()
    elif isinstance(date, int) or isinstance(date, float):
        # timestamp number
        return int(date)
    else:
        raise Exception(f"Unsupported date format: {date}")


def fix_daily_bar_date(date, timespan):
    """
    the polygon api does not include the end date for daily bars, or this:
    historic_agg_v2("SPY", 1, "day", _from="2020-07-22", to="2020-07-24").df
    results in this:
    timestamp
    2020-07-22 00:00:00-04:00  324.62  327.20  ...  57917101.0  325.8703
    2020-07-23 00:00:00-04:00  326.47  327.23  ...  75841843.0  324.3429

    the 24th data is missing
    for minute bars, it does include the end date

    so basically this method will add 1 day (if 'to' is not today, we don't
    have today's data until tomorrow) to the 'to' field
    """
    if not isinstance(date, int):
        if timespan == 'day':
            date = dateutil.parser.parse(date)
            today = datetime.datetime.utcnow().date()
            if today != date.date():
                date = date + datetime.timedelta(days=1)
            date = date.date().isoformat()
    return date


class REST(object):

    def __init__(self, api_key: str,
                 staging: bool = False,
                 raw_data: bool = False
                 ):
        """
        :param staging: do we work with the staging server
        :param raw_data: should we return api response raw or wrap it with
                         Entity objects.
        """
        self._api_key: str = get_polygon_credentials(api_key)
        self._staging: bool = staging
        self._use_raw_data: bool = raw_data
        self._session = requests.Session()

    def _request(self, method: str, path: str, params: dict = None,
                 version: str = 'v1'):
        """
        :param method: GET, POST, ...
        :param path: url part path (without the domain name)
        :param params: dictionary with params of the request
        :param version: v1 or v2
        :return: response
        """
        url: URL = URL('https://api.polygon.io/' + version + path)
        params = params or {}
        params['apiKey'] = self._api_key
        if self._staging:
            params['apiKey'] += '-staging'
        resp = self._session.request(method, url, params=params)
        resp.raise_for_status()
        return resp.json()

    def get(self, path: str, params: dict = None, version: str = 'v1'):
        return self._request('GET', path, params=params, version=version)

    def exchanges(self) -> Exchanges:
        path = '/meta/exchanges'
        resp = self.get(path)
        if self._use_raw_data:
            return resp
        else:
            return [self.response_wrapper(o, Exchange) for o in resp]

    def symbol_type_map(self) -> SymbolTypeMap:
        path = '/meta/symbol-types'
        return self.response_wrapper(self.get(path), SymbolTypeMap)

    def historic_trades_v2(self,
                           symbol: str,
                           date: DATE,
                           timestamp: int = None,
                           timestamp_limit: int = None,
                           reverse: bool = None,
                           limit: int = None
                           ) -> TradesV2:
        """
        polygon.io/docs/#get_v2_ticks_stocks_trades__ticker___date__anchor
        :param symbol
        :param date: DATE in this format YYYY-MM-DD
        :param timestamp: timestamp integer
        :param timestamp_limit: timestamp integer. offset, used for pagination.
        :param reverse: bool
        :param limit: max 50000
        :return:
        """
        path = '/ticks/stocks/trades/{}/{}'.format(symbol, date)
        params = {}
        if timestamp is not None:
            params['timestamp'] = timestamp
        if timestamp_limit is not None:
            params['timestampLimit'] = timestamp_limit
        if reverse is not None:
            params['reverse'] = reverse
        if limit is not None:
            params['limit'] = limit
        resp = self.get(path, params, 'v2')
        return self.response_wrapper(resp, TradesV2)

    def historic_quotes_v2(self,
                           symbol: str,
                           date: DATE,
                           timestamp: int = None,
                           timestamp_limit: int = None,
                           reverse: bool = None,
                           limit: int = None
                           ) -> QuotesV2:
        """
        polygon.io/docs/#get_v2_ticks_stocks_nbbo__ticker___date__anchor
        :param symbol
        :param date: DATE in this format YYYY-MM-DD
        :param timestamp: timestamp integer. offset, used for pagination.
        :param timestamp_limit: timestamp integer
        :param reverse: bool
        :param limit: max 50000
        :return:
        """
        path = '/ticks/stocks/nbbo/{}/{}'.format(symbol, date)
        params = {}
        if timestamp is not None:
            params['timestamp'] = timestamp
        if timestamp_limit is not None:
            params['timestampLimit'] = timestamp_limit
        if reverse is not None:
            params['reverse'] = reverse
        if limit is not None:
            params['limit'] = limit
        resp = self.get(path, params, 'v2')
        return self.response_wrapper(resp, QuotesV2)

    def historic_agg_v2(self,
                        symbol: str,
                        multiplier: int,
                        timespan: str,
                        _from,
                        to,
                        unadjusted: bool = False,
                        limit: int = None) -> Aggsv2:
        """
        :param symbol:
        :param multiplier: Size of the timespan multiplier (distance between
               samples. e.g if it's 1 we get for daily 2015-01-05, 2015-01-06,
                            2015-01-07, 2015-01-08.
                            if it's 3 we get 2015-01-01, 2015-01-04,
                            2015-01-07, 2015-01-10)
        :param timespan: Size of the time window: minute, hour, day, week,
               month, quarter, year
        :param _from: acceptable types: isoformat string, timestamp int,
               datetime object
        :param to: same as _from
        :param unadjusted
        :param limit: max samples to retrieve
        :return:
        """
        path_template = '/aggs/ticker/{symbol}/range/{multiplier}/' \
                        '{timespan}/{_from}/{to}'
        path = path_template.format(
            symbol=symbol,
            multiplier=multiplier,
            timespan=timespan,
            _from=format_date_for_api_call(_from),
            to=fix_daily_bar_date(format_date_for_api_call(to), timespan)
        )
        params = {'unadjusted': unadjusted}
        if limit:
            params['limit'] = limit
        resp = self.get(path, params, version='v2')
        return self.response_wrapper(resp, Aggsv2)

    def grouped_daily(self, date, unadjusted: bool = False) -> Aggsv2Set:
        path = f'/aggs/grouped/locale/us/market/stocks/{date}'
        params = {'unadjusted': unadjusted}
        resp = self.get(path, params, version='v2')
        return self.response_wrapper(resp, Aggsv2Set)

    def daily_open_close(self, symbol: str, date) -> DailyOpenClose:
        path = f'/open-close/{symbol}/{date}'
        resp = self.get(path)
        return self.response_wrapper(resp, DailyOpenClose)

    def last_trade(self, symbol: str) -> Trade:
        path = '/last/stocks/{}'.format(symbol)
        resp = self.get(path)['last']
        return self.response_wrapper(resp, Trade)

    def last_quote(self, symbol: str) -> Quote:
        path = '/last_quote/stocks/{}'.format(symbol)
        resp = self.get(path)['last']
        # TODO status check
        return self.response_wrapper(resp, Quote)

    def previous_day_bar(self, symbol: str) -> Aggsv2:
        path = '/aggs/ticker/{}/prev'.format(symbol)
        resp = self.get(path, version='v2')
        return self.response_wrapper(resp, Aggsv2)

    def condition_map(self, ticktype='trades') -> ConditionMap:
        path = '/meta/conditions/{}'.format(ticktype)
        return self.response_wrapper(self.get(path), ConditionMap)

    def company(self, symbol: str) -> Company:
        return self._get_symbol(symbol, 'company', Company)

    def _get_symbol(self, symbol: str, resource: str, entity):
        multi = _is_list_like(symbol)
        symbols = symbol if multi else [symbol]
        if len(symbols) > 50:
            raise ValueError('too many symbols: {}'.format(len(symbols)))
        params = {
            'symbols': ','.join(symbols),
        }
        path = '/meta/symbols/{}'.format(resource)
        res = self.get(path, params=params)
        if isinstance(res, list):
            res = {o['symbol']: o for o in res}
        if self._use_raw_data:
            retmap = res
        else:
            retmap = {sym: entity(res[sym]) for sym in symbols if sym in res}
        if not multi:
            return retmap.get(symbol)
        return retmap

    def dividends(self, symbol: str) -> Dividends:
        return self._get_symbol(symbol, 'dividends', Dividends)

    def splits(self, symbol: str) -> Splits:
        path = f'/reference/splits/{symbol}'
        resp = self.get(path, version='v2')['results']
        return self.response_wrapper(resp, Splits)

    def earnings(self, symbol: str) -> Earnings:
        return self._get_symbol(symbol, 'earnings', Earnings)

    def financials(self, symbol: str) -> Financials:
        return self._get_symbol(symbol, 'financials', Financials)

    def financials_v2(self, symbol: str,
                      limit: int,
                      report_type: FinancialsReportType,
                      sort: FinancialsSort
                      ) -> Financials:
        path = f'/reference/financials/{symbol}'
        params = {"limit":    limit,
                  "type": report_type.name,
                  "sort":  sort.value,
                  }
        resp = self.get(path, version='v2', params=params)['results']
        return self.response_wrapper(resp, Financials)

    def news(self, symbol: str) -> NewsList:
        path = '/meta/symbols/{}/news'.format(symbol)
        return self.response_wrapper(self.get(path), NewsList)

    def gainers_losers(self, direction: str = "gainers") -> Tickers:
        path = '/snapshot/locale/us/markets/stocks/{}'.format(direction)
        resp = self.get(path, version='v2')['tickers']
        if self._use_raw_data:
            return resp
        else:
            return [self.response_wrapper(o, Ticker) for o in resp]

    def all_tickers(self) -> Tickers:
        path = '/snapshot/locale/us/markets/stocks/tickers'
        resp = self.get(path, version='v2')['tickers']
        if self._use_raw_data:
            return resp
        else:
            return [self.response_wrapper(o, Ticker) for o in resp]

    def symbol_list_paginated(self, page: int = 1,
                              per_page: int = 50) -> Symbols:
        """
        this api /v2/reference/tickers returns paginated data.
        the user could specify the page to get data for
        :param page: page number
        :param per_page: number of results per page
        :return:
        """
        path = '/reference/tickers'
        resp = self.get(path,
                        version='v2',
                        params={
                            "page": page,
                            "active": "true",
                            "perpage": per_page,
                            "market": "STOCKS"
                        })['tickers']
        if self._use_raw_data:
            return resp
        else:
            return [self.response_wrapper(o, Symbol) for o in resp]

    def snapshot(self, symbol: str) -> Ticker:
        path = '/snapshot/locale/us/markets/stocks/tickers/{}'.format(symbol)
        resp = self.get(path, version='v2')
        return self.response_wrapper(resp, Ticker)
    
    def snapshot_all_tickers(self) -> Tickers:
        path = '/snapshot/locale/us/markets/stocks/tickers'
        resp = self.get(path, version='v2')['tickers']
        if self._use_raw_data:
            return resp
        else:
            return [self.response_wrapper(o, Ticker) for o in resp]

    def response_wrapper(self, obj, entity: Entity):
        """
        To allow the user to get raw response from the api, we wrap all
        functions with this method, checking if the user has set raw_data
        bool. if they didn't, we wrap the response with an Entity object.
        :param obj: response from server
        :param entity: derivative object of Entity
        :return:
        """
        if self._use_raw_data:
            return obj
        else:
            return entity(obj)
