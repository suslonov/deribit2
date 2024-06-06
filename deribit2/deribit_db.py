#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from datetime import datetime
import pickle
import zlib
import json
import MySQLdb
EXPIRATION_NOTIFYING_HOUR = 11

class DBMySQL(object):
    db_host="127.0.0.1"
    db_user="minimax"
    db_passwd="minimax"
    db_name="minimax"

    STATUS_STOP_LOSS = -3
    STATUS_TAKE_PROFIT = -2
    STATUS_TAKE_PROFIT_NEW = -4
    STATUS_EXPIRATION = -1
    STATUS_ALL_OUT = -5
    STATUS_MANUAL_STOP = -6
    STATUS_FINISHED = 0
    STATUS_OPENNING = 1
    STATUS_ACTIVE = 2

    ORDER_ACTION_CANCELLED = -1
    ORDER_ACTION_FILLED = 0
    ORDER_ACTION_OPEN = 1
    ORDER_ACTION_CLOSE = 2
    ORDER_ACTION_TAKE_PROFIT = 3
    ORDER_ACTION_STOP_LOSS = 4

    STATUS_ORDER = {STATUS_STOP_LOSS: ORDER_ACTION_STOP_LOSS,
                    STATUS_TAKE_PROFIT: ORDER_ACTION_TAKE_PROFIT,
                    STATUS_EXPIRATION: ORDER_ACTION_CLOSE,
                    STATUS_ALL_OUT: ORDER_ACTION_CLOSE,
                    STATUS_MANUAL_STOP: ORDER_ACTION_CLOSE,
                    STATUS_OPENNING: ORDER_ACTION_OPEN,
                    STATUS_ACTIVE: ORDER_ACTION_FILLED}
    ORDER_ACTION_TEXT = {ORDER_ACTION_CANCELLED: "cancelled",
                         ORDER_ACTION_FILLED: "filled",
                         ORDER_ACTION_OPEN: "open",
                         ORDER_ACTION_CLOSE: "close",
                         ORDER_ACTION_TAKE_PROFIT: "take profit",
                         ORDER_ACTION_STOP_LOSS: "stop loss"}

    def __init__(self, strategy_name, port=None, port1=None):
        self.strategy_name = strategy_name
        self.port = port
        self.port1 = port1

    def start(self):
        if self.port:
            self.db_connection = MySQLdb.connect(host=self.db_host, user=self.db_user, passwd=self.db_passwd, db=self.db_name, port=self.port)
        else:
            self.db_connection = MySQLdb.connect(host=self.db_host, user=self.db_user, passwd=self.db_passwd, db=self.db_name)
        self.cursor = self.db_connection.cursor()

        if self.port1 and self.port1 != self.port:
            self.db_connection1 = MySQLdb.connect(host=self.db_host, user=self.db_user, passwd=self.db_passwd, db=self.db_name, port=self.port1)
            self.cursor1 = self.db_connection1.cursor()
        else:
            self.db_connection1 = self.db_connection
            self.cursor1 = self.cursor

    def commit(self, all_connections=False):
        self.db_connection.commit()
        if all_connections:
            self.db_connection1.commit()

    def stop(self):
        if self.db_connection1 != self.db_connection:
            self.db_connection1.commit()
            self.db_connection1.close()
        self.db_connection.commit()
        self.db_connection.close()

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.stop()
        
    def _create_table(self, sql1, *sql):
            try:
                self.cursor.execute(sql1)
            except:
                pass
            for s in sql:
                self.cursor.execute(s)
        
    def _create_table1(self, sql1, *sql):
            try:
                self.cursor1.execute(sql1)
            except:
                pass
            for s in sql:
                self.cursor1.execute(s)

    def _create_tables(self, tables):

        if "orphaned_position" in tables:
            s12 = "drop table " + self.strategy_name + "_orphaned_position"
            s22 = "create table " + self.strategy_name + "_orphaned_position (instrument varchar(32), "
            s22 += "amount double, modified datetime)"
            self._create_table(s12, s22)

        if "market_state" in tables:
            s11 = "drop table " + self.strategy_name + "_market_state"
            s21 = "create table " + self.strategy_name + "_market_state (market varchar(12) NOT NULL, point datetime NOT NULL, state varchar(32))"
            self._create_table(s11, s21)

        if "global_state" in tables:
            s11 = "drop table " + self.strategy_name + "_global_state"
            s21 = "create table " + self.strategy_name + "_global_state (parameter varchar(32) NOT NULL, point datetime NOT NULL, state varchar(32))"
            self._create_table(s11, s21)

        if "currency" in tables:
            s11 = "drop table currency"
            s21 = "create table currency (currency varchar(12) NOT NULL, point datetime NOT NULL, rate decimal(18, 4))"
            s23 = "alter table currency add PRIMARY KEY (currency, point)"
            self._create_table(s11, s21, s23)

        if "bundles" in tables:
            # status = -3 stop_loss, -2 take_profit, -1 close on expiration, 0 finished, 1 to open, 2 active
            s11 = "drop table " + self.strategy_name + "_bundles"
            s21 = "create table " + self.strategy_name + "_bundles (id int NOT NULL AUTO_INCREMENT PRIMARY KEY, "
            s21 += "status int, term int, market varchar(12), open_date datetime, expiration datetime, close_date datetime)"
            self._create_table(s11, s21)
        if "position" in tables:
            s12 = "drop table " + self.strategy_name + "_positions"
            s22 = "create table " + self.strategy_name + "_positions (bundle_id int NOT NULL, instrument varchar(32) NOT NULL, "
            s22 += "status int, amount double, open_date datetime, close_date datetime, modified datetime)"
            s23 = "alter table " + self.strategy_name + "_positions add PRIMARY KEY (bundle_id, instrument)"
            self._create_table(s12, s22, s23)
        if "position_orders" in tables:
            # action = 1 open, 2 close, 3 take_profit, 4 stop_loss
            s13 = "drop table " + self.strategy_name + "_position_orders"
            s24 = "create table " + self.strategy_name + "_position_orders (bundle_id int NOT NULL, instrument varchar(32) NOT NULL, "
            s24 += "order_id varchar(32) NOT NULL, action int, modified datetime)"
            s25 = "alter table " + self.strategy_name + "_position_orders add PRIMARY KEY (bundle_id, instrument, order_id)"
            s26 = "alter table " + self.strategy_name + "_position_orders add index (order_id)"
            self._create_table(s13, s24, s25, s26)
        if "book_summary" in tables:
            s11 = "drop table " + self.strategy_name + "_book_summary"
            s21 = "create table " + self.strategy_name + "_book_summary (point datetime NOT NULL PRIMARY KEY, raw_data LONGBLOB)"
            self._create_table(s11, s21)


        if "wallet" in tables:
            s12 = "drop table " + self.strategy_name + "_wallet"
            s22 = "create table " + self.strategy_name + "_wallet (point datetime NOT NULL PRIMARY KEY, wallet_data text)"
            self._create_table1(s12, s22)
        if "exchange_positions" in tables:
            s12 = "drop table " + self.strategy_name + "_exchange_positions"
            s22 = "create table " + self.strategy_name + "_exchange_positions (point datetime NOT NULL PRIMARY KEY, position_data text)"
            self._create_table1(s12, s22)
        if "exchange_orders" in tables:
            s12 = "drop table " + self.strategy_name + "_exchange_orders"
            s22 = "create table " + self.strategy_name + "_exchange_orders (point datetime NOT NULL PRIMARY KEY, order_data text)"
            self._create_table1(s12, s22)

    def create_bundle(self, open_date, term, market, position_list, expiration=None, status=STATUS_OPENNING):
        s1 = "insert into " + self.strategy_name + "_bundles(status, term, market, open_date, expiration) values(%s, %s, %s, %s, %s)"
        self.cursor.execute(s1, (status, term, market, open_date, expiration))
        bundle_id = self.cursor.lastrowid
        for p in position_list:
            names = ""
            values = ""
            for f in p:
                names += f + ", "
                if type(p[f]) == int or type(p[f]) == float:
                    values += str(p[f]) + ", "
                else:
                    values += "'" + str(p[f]) + "', "
            names += "status, open_date, modified"
            values += str(status) + ", '" + str(open_date) + "', '" + str(open_date) + "'"
            s2 = "insert into " + self.strategy_name + "_positions(bundle_id, " + names + ") values("+ str(bundle_id) + ", " + values + ")"
            self.cursor.execute(s2)
        return bundle_id
    
    def clear_unxepected_opened_positions(self):
        s1 = "delete from " + self.strategy_name + "_orphaned_position"
        self.cursor.execute(s1)
        
    def add_unxepected_opened_position(self, instrument, amount, modified):
        s1 = "insert into " + self.strategy_name + "_orphaned_position(instrument, amount, modified) values(%s, %s, %s)"
        self.cursor.execute(s1, (instrument, amount, modified))

    def update_bundle_status(self, bundle_id, status):
        s1 = "UPDATE " + self.strategy_name + "_bundles SET status=" + str(status) + " WHERE id=" + str(bundle_id)
        self.cursor.execute(s1)
        s2 = "UPDATE " + self.strategy_name + "_positions SET status=" + str(status) + " WHERE status <> 0 and bundle_id=" + str(bundle_id)
        self.cursor.execute(s2)

    def update_position_status(self, bundle_id, instrument, status):
        s2 = "UPDATE " + self.strategy_name + "_positions SET status=" + str(status) + " WHERE bundle_id=" + str(bundle_id) + " and instrument='" + instrument + "'"
        self.cursor.execute(s2)

    def fetch_with_description(self, cursor):
        return [{n[0]: v for n, v in zip(cursor.description, row)} for row in cursor.fetchall()]

    def load_state(self, markets):
        s1 = "select * from " + self.strategy_name + "_bundles where status <> 0"
        self.cursor.execute(s1)
        l1 = self.fetch_with_description(self.cursor)

        s11 = "select * from " + self.strategy_name + "_bundles"  #!!! crutch
        self.cursor.execute(s11)
        l11 = self.fetch_with_description(self.cursor)

        s2 = "select * from " + self.strategy_name + "_positions where status <> 0"
        self.cursor.execute(s2)
        l2 = self.fetch_with_description(self.cursor)

        currency_history = {}
        s21 = "select point, rate from currency where currency = %s order by point desc limit 20"
        for c in markets:
            self.cursor.execute(s21, (c, ))
            l21 = self.fetch_with_description(self.cursor)
            currency_history[c] = l21
        
        market_state = {c: {"state": "STANDARD", "datetime": datetime.fromtimestamp(0)} for c in markets}
        s3 = "select * from " + self.strategy_name + "_market_state"
        self.cursor.execute(s3)
        l3 = self.fetch_with_description(self.cursor)
        for ll in l3:
            market_state[ll["market"]]["state"] = ll["state"]
            market_state[ll["market"]]["datetime"] = ll["point"]

        global_state = {}
        s4 = "select * from " + self.strategy_name + "_global_state"
        self.cursor.execute(s4)
        l3 = self.fetch_with_description(self.cursor)
        for ll in l3:
            global_state[ll["parameter"]] = {}
            global_state[ll["parameter"]]["state"] = ll["state"]
            global_state[ll["parameter"]]["datetime"] = ll["point"]

        return {'bundles': l1,
                'positions': l2,
                'global_state': global_state,
                'market_state': market_state,
                "bundles_unfiltered": l11,
                "currency_history": currency_history}

    
    def set_market_state(self, market, point, state):
        s1 = "delete from " + self.strategy_name + "_market_state where market = %s"
        s2 = "insert into " + self.strategy_name + "_market_state (market, point, state) values (%s, %s, %s)"
        self.cursor.execute(s1, (market, ))
        self.cursor.execute(s2, (market, point, state))
        
    def set_global_state(self, parameter, point, state):
        s1 = "delete from " + self.strategy_name + "_global_state where parameter = %s"
        s2 = "insert into " + self.strategy_name + "_global_state (parameter, point, state) values (%s, %s, %s)"
        self.cursor.execute(s1, (parameter, ))
        self.cursor.execute(s2, (parameter, point, state))
        
    def get_market_state(self, market):
        s1 = "select * from " + self.strategy_name + "_market_state where market = %s"
        self.cursor.execute(s1, (market, ))
        l = self.fetch_with_description(self.cursor)
        if l:
            return l[0]
        else:
            return None

    def get_global_state(self, parameter):
        s1 = "select * from " + self.strategy_name + "_global_state where parameter = %s"
        self.cursor.execute(s1, (parameter, ))
        l = self.fetch_with_description(self.cursor)
        if l:
            return l[0]
        else:
            return None

    def add_currency_record(self, currency, point, rate):
        s2 = "insert into currency (currency, point, rate) values (%s, %s, %s)"
        self.cursor.execute(s2, (currency, point, rate))

    def get_currency_point(self, point):
        s2 = "select * from currency where point = %s"
        self.cursor.execute(s2, (point, ))
        return {c["currency"]: float(c["rate"]) for c in self.fetch_with_description(self.cursor)}

    def add_book_summary_point(self, point, raw_data):
        s5 = """insert into """ + self.strategy_name + """_book_summary (point, raw_data) VALUES (%s, _binary "%s")"""
        self.cursor.execute(s5, (point, zlib.compress(pickle.dumps(raw_data))))

    def get_raw_data_points_from_db(self, N=9):
        s5 = "select point from " + self.strategy_name + "_book_summary order by point desc limit " + str(N)
        self.cursor.execute(s5)
        return [ll[0] for ll in self.cursor.fetchall()]

    def get_book_summary_point(self, point):
        s5 = "select raw_data from " + self.strategy_name + "_book_summary where point = '" + str(point) + "'"
        i = self.cursor.execute(s5)
        if not i:
            return None
        (r, ) = self.cursor.fetchone()
        return pickle.loads(zlib.decompress(r[1:-1]))

    def write_position_order(self, bundle_id, instrument, order_id, action, modified):
        s2 = "select count(*) from " + self.strategy_name + "_position_orders where order_id='" + order_id + "'"
        self.cursor.execute(s2)
        (i, ) = self.cursor.fetchone()
        if i == 0:
            s3 = "insert into " + self.strategy_name + "_position_orders(bundle_id, instrument, order_id, action, modified) values(%s, %s, %s, %s, %s)"
            self.cursor.execute(s3, (bundle_id, instrument, order_id, action, modified))
        else:
            s3 = "update " + self.strategy_name + "_position_orders set action=%s, modified=%s where order_id=%s"
            self.cursor.execute(s3, (action, modified, order_id))
            
    def update_position_order(self, order_id, action, modified):
        s3 = "update " + self.strategy_name + "_position_orders set action=%s, modified=%s where order_id=%s"
        self.cursor.execute(s3, (action, modified, order_id))

    def delete_position_orders(self, bundle_id, instrument):
        s2 = "delete from " + self.strategy_name + "_position_orders where bundle_id=" + str(bundle_id) + " and instrument='" + instrument + "'"
        try: 
            self.cursor.execute(s2)
        except:
            pass
        
    def move_orders_bb(self, bundle1, bundle2):
        s3 = "update " + self.strategy_name + "_position_orders set bundle_id=%s where bundle_id=%s and action>0"
        self.cursor.execute(s3, (bundle2, bundle1))
        
            
    def delete_position_order(self, order_id):
        s2 = "delete from " + self.strategy_name + "_position_orders where order_id='" + order_id + "'"
        try: 
            self.cursor.execute(s2)
        except:
            pass
            
    def get_position_orders(self, bundle_id, instrument=None):
        if instrument:
            s2 = "select * from " + self.strategy_name + "_position_orders where bundle_id=" + str(bundle_id) + " and instrument='" + instrument + "'"
        else:
            s2 = "select * from " + self.strategy_name + "_position_orders where bundle_id=" + str(bundle_id)
        self.cursor.execute(s2)
        return [ll for ll in self.fetch_with_description(self.cursor)]



    def add_wallet_data(self, point, data):
        s1 = "delete from " + self.strategy_name + "_wallet"
        s2 = """insert into """ + self.strategy_name + """_wallet (point, wallet_data) values (%s, '""" + json.dumps(data) + """')"""
        self.cursor1.execute(s1)
        self.cursor1.execute(s2, (point, ))

    def add_exchange_positions(self, point, data):
        s1 = "delete from " + self.strategy_name + "_exchange_positions"
        s2 = "insert into " + self.strategy_name + "_exchange_positions (point, position_data) values (%s, '""" + json.dumps(data) + """')"""
        self.cursor1.execute(s1)
        self.cursor1.execute(s2, (point, ))

    def add_exchange_orders(self, point, data):
        s1 = "delete from " + self.strategy_name + "_exchange_orders"
        s2 = "insert into " + self.strategy_name + "_exchange_orders (point, order_data) values (%s, '""" + json.dumps(data) + """')"""
        self.cursor1.execute(s1)
        self.cursor1.execute(s2, (point, ))
        
    def get_wallet_data(self):
        s1 = "select * from " + self.strategy_name + "_wallet order by point desc limit 1"
        self.cursor1.execute(s1)
        l = self.cursor1.fetchone()
        if l:
            return (l[0], json.loads(l[1]))
        else:
            return l

    def get_exchange_positions(self):
        s1 = "select * from " + self.strategy_name + "_exchange_positions order by point desc limit 1"
        self.cursor1.execute(s1)
        l = self.cursor1.fetchone()
        if l:
            return (l[0], json.loads(l[1]))
        else:
            return l

    def get_exchange_orders(self):
        s1 = "select * from " + self.strategy_name + "_exchange_orders order by point desc limit 1"
        self.cursor1.execute(s1)
        l = self.cursor1.fetchone()
        if l:
            return (l[0], json.loads(l[1]))
        else:
            return l
