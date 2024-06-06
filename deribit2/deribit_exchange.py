#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import requests

ORDER_BOOK_DEPTH = 5
HEADERS = {'Content-Type': "application/json"}
DERIBIT_WORKING_CURRENCIES = ["BTC", "ETH"]

class DeribitExchange(object):
    expiration_hour = 8
    url_currency = "https://www.deribit.com/api/v2/public/get_index_price?index_name={}_usd"
    url_book_summary = "https://www.deribit.com/api/v2/public/get_book_summary_by_currency?currency={}&kind={}"
    url_instruments = "https://www.deribit.com/api/v2/public/get_instruments?currency={}&expired=false&kind={}"
    url_order_book = "https://www.deribit.com/api/v2/public/get_order_book?depth={}&instrument_name={}"

    url_auth = "https://www.deribit.com/api/v2/public/auth?client_id={}&client_secret={}&grant_type=client_credentials"
    url_orders = "https://www.deribit.com/api/v2/private/get_open_orders_by_currency?currency={}&kind={}&type=all"
    url_order_state = "https://www.deribit.com/api/v2/private/get_order_state?order_id={}"
    url_trades = "https://www.deribit.com/api/v2/private/get_user_trades_by_instrument?count={}&instrument_name={}"
    url_positions = "https://www.deribit.com/api/v2/private/get_positions?currency={}&kind={}&type=all"
    url_wallet = "https://www.deribit.com/api/v2/private/get_account_summary?currency={}"
    url_settlements = "https://www.deribit.com/api/v2/private/get_settlement_history_by_instrument?count={}&instrument_name={}&type={}"

    url_buy = "https://www.deribit.com/api/v2/private/buy?amount={}&instrument_name={}&label={}&type={}&price={}"
    url_sell = "https://www.deribit.com/api/v2/private/sell?amount={}&instrument_name={}&label={}&type={}&price={}"
    url_buy_market = "https://www.deribit.com/api/v2/private/buy?amount={}&instrument_name={}&label={}&type={}"
    url_sell_market = "https://www.deribit.com/api/v2/private/sell?amount={}&instrument_name={}&label={}&type={}"
    url_edit = "https://www.deribit.com/api/v2/private/edit?order_id={}&amount={}&price={}"
    url_cancel = "https://www.deribit.com/api/v2/private/cancel?order_id={}"

    def __init__(self, client, key, markets=None):
        if markets is None:
            self.markets = DERIBIT_WORKING_CURRENCIES
        else:
            self.markets = markets
        self.client = client
        self.key = key

    def authenticate(self):
        self.status_code = None
        self.error_message = None
        if not self.client:
            return -1
        url = self.url_auth.format(self.client, self.key)
        res = requests.get(url, headers=HEADERS)
        if res.status_code != 200:
            self.status_code = res.status_code
            self.error_message = res.json()
            return res.status_code
        result = res.json()["result"]
        self.access_token = result["access_token"]
        self.refresh_token = result["refresh_token"]
        return 0

    def _private_request(self, url, *params):
        self.status_code = None
        self.error_message = None
        headers = HEADERS.copy()
        headers['Authorization'] = 'Bearer {}'.format(self.access_token)
        _url = url.format(*params)
        # print(_url)
        res = requests.get(_url, headers=headers)
        if res.status_code != 200:
            self.status_code = res.status_code
            self.error_message = res.json()
            return None
        # print(res.json())
        return res.json()["result"]

    def _public_request(self, url, *params):
        self.status_code = None
        self.error_message = None
        _url = url.format(*params)
        res = requests.get(_url, headers=HEADERS)
        if res.status_code != 200:
            self.status_code = res.status_code
            self.error_message = res.json()
            return None
        return res.json()["result"]

    def buy(self, amount, instrument, label, order_type, price=None):
        if order_type != "market":
            return self._private_request(self.url_buy, amount, instrument, label, order_type, price)
        else:
            return self._private_request(self.url_buy_market, amount, instrument, label, order_type)

    def sell(self, amount, instrument, label, order_type, price=None):
        if order_type != "market":
            return self._private_request(self.url_sell, amount, instrument, label, order_type, price)
        else:
            return self._private_request(self.url_sell_market, amount, instrument, label, order_type)

    def cancel_order(self, order_id):
        return self._private_request(self.url_cancel, order_id)

    def edit_order(self, order_id, amount, price):
        return self._private_request(self.url_edit, order_id, amount, price)

    def get_orders(self, currency=None):
        if currency:
            return self._private_request(self.url_orders, currency, "option")
        else:
            return sum([self._private_request(self.url_orders, c, "option") for c in self.markets], [])

    def get_trades(self, instrument, count=10):
        return self._private_request(self.url_trades, count, instrument)["trades"]

    def get_settlements(self, instrument):
        return self._private_request(self.url_settlements, 1, instrument, "delivery")["settlements"]

    def get_order_state(self, order_id):
        return self._private_request(self.url_order_state, order_id)

    def get_positions(self, currency=None):
        if currency:
            return self._private_request(self.url_positions, currency, "option")
        else:
            return sum([self._private_request(self.url_positions, c, "option") for c in self.markets], [])

    def get_wallet(self, currency=None):
        if currency:
            return self._private_request(self.url_wallet, currency)
        else:
            return {c: self._private_request(self.url_wallet, c) for c in self.markets + ["USDC"]}


    def get_currency(self, currency=None):
        if currency:
            return self._public_request(self.url_currency, currency.lower())["index_price"]
        else:
            return {c: self._public_request(self.url_currency, c.lower())["index_price"] for c in self.markets}

    def get_order_book(self, instrument):
        return self._public_request(self.url_order_book, ORDER_BOOK_DEPTH, instrument)

    def get_options_summary(self, currency=None):
        if currency:
            return self._public_request(self.url_book_summary, currency.value, "option")
        else:
            return {c: self._public_request(self.url_book_summary, c, "option") for c in self.markets}


    def get_option_instruments(self, req_currency):
        return self._public_request(self.url_instruments, req_currency, "option")

