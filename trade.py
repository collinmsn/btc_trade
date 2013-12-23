#!/usr/bin/python
# -*- coding: utf-8 -*-

import os
import sys 
import btcchina
import threading
import time
import httplib
import json
import datetime
import Queue
import logging
import logging.handlers

logger = logging.getLogger('btc_trade')

access_key=""
secret_key=""
host = "data.btcchina.com"
uri = "/data/ticker"
trade_retry = 5

(INIT, BUY, SELL) = range(3)
(LAST_PRICE, BUY_PRICE, SELL_PRICE) = range(3)
price_type_names = ("last", "buy", "sell")

cny_decimals = 2
btc_decimals = 5
min_cny_balance = 5
min_btc_balance = 0.0002
cny_amount_round = 10 * (0.1 ** cny_decimals)
btc_amount_round = 10 * (0.1 ** btc_decimals)

mq = Queue.Queue(5)

def init_logger():
    logger.setLevel(logging.INFO)
    fh = logging.handlers.TimedRotatingFileHandler('/root/btc/logs/btc_trade.log', when='D', interval=1, backupCount=15)
    fh.setLevel(logging.INFO)
    log_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    fh.setFormatter(log_formatter)
    logger.addHandler(fh)

    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(log_formatter)
    logger.addHandler(ch)

def daemonize():                                                                                                                     
    pid = os.fork()                                                                                                                  
    if pid == -1:                                                                                                                    
        sys.exit(-1)                                                                                                                 
    elif pid > 0:                                                                                                                    
        # parent                                                                                                                     
        sys.exit(0)                                                                                                                  
    elif pid == 0:                                                                                                                   
        # child                                                                                                                      
        pass                                                                                                                         
    os.setsid()                                                                                                                      
    os.chdir('/')                                                                                                                    
    os.umask(0)                                                                                                                      
    fd = os.open('/dev/null', os.O_RDWR)                                                                                             
    os.dup2(fd, 0)                                                                                                                   
    os.dup2(fd, 1)                                                                                                                   
    os.dup2(fd, 2)                                                                                                                   
    pass      

def do_create_conn():
    conn = None
    try:
        conn = httplib.HTTPSConnection(host = host, port = httplib.HTTPS_PORT)
    except Exception as e:
    	logger.error("conn exception: %s", str(e))
        return None
    return conn
        
def do_query_price(conn, price_type):
    try:
        conn.request("GET", uri)
        resp = conn.getresponse()
        if resp.status != 200:
    	    logger.error("query price status: %d", resp.status)
            return None
        # decode
        data = resp.read()
        obj = json.loads(data)
        price = obj["ticker"][price_type_names[price_type]]
        return price
    except Exception as e:
    	logger.error("query price exception: %s", str(e))
    return None

class PriceCenter(threading.Thread):
    def __init__(self, interval):
        threading.Thread.__init__(self)
    	self.interval = interval
    	self.conn = None
    	self.price = 0
    def get_interval(self):
	return self.interval

    def run(self):
        while True:
            threading.Timer(self.interval, self.OnTimer).start()
            time.sleep(self.interval)
    def OnTimer(self):
        if not self.conn:
            self.conn = do_create_conn()
        if not self.conn:
            return
        price = do_query_price(self.conn, LAST_PRICE)
        logger.info("price: %s", str(price))
        if price:
	       self.price = float(price)
        else:
            self.conn.close()
            self.conn = None
        pass

    def get_price(self):
        return self.price

    @staticmethod
    def query_price(price_type):
        conn = do_create_conn()
        if not self.conn:
            return None
        price = do_query_price(conn, price_type)
        conn.close()
        if price:
            price = float(price)
        return price


class MA(threading.Thread):
    def __init__(self, short_minutes, long_minutes, interval, price_center):
        threading.Thread.__init__(self)
    	self.short_minutes = short_minutes
    	self.long_minutes = long_minutes
    	self.interval = interval
    	self.num_short_price = self.short_minutes * 60 / self.interval
    	self.num_long_price = self.long_minutes * 60 / self.interval
        self.short_pos = 0 
        self.long_pos = 0 
	self.price_center = price_center
        self.cur_price = 0
        self.ma_short = 0
        self.ma_long = 0

    def run(self):
        while True:
            threading.Timer(self.interval, self.OnTimer).start()
            time.sleep(self.interval)

    def GetPrice(self):
        return (self.cur_price, self.ma_short, self.ma_long)

    def OnTimer(self):
    	price = self.price_center.get_price()
    	self.cur_price = price
    	self.OnNewPrice(self.cur_price)
        pass

    def OnNewPrice(self, price):
        logger.warning("not implemented")

class WMA(MA):
    def __init__(self, short_minutes, long_minutes, interval, price_center):
        MA.__init__(self, short_minutes, long_minutes, interval, price_center)
    	self.short_price = [0] * self.num_short_price
    	self.long_price = [0] * self.num_long_price

    def OnNewPrice(self, price):
        self.short_price[self.short_pos] = self.cur_price
        self.long_price[self.long_pos] = self.cur_price
        self.ma_short = self.WMAPrice(self.short_price, self.num_short_price, self.short_pos)
        self.ma_long = self.WMAPrice(self.long_price, self.num_long_price, self.long_pos)
        logger.info("price: %f, %s price: %f, %f", self.cur_price, self.__class__, self.ma_short, self.ma_long)
        self.short_pos += 1
        self.long_pos += 1
        if self.short_pos >= self.num_short_price:
            self.short_pos = 0
        if self.long_pos >= self.num_long_price:
            self.long_pos = 0
        pass        

    def WMAPrice(self, prices, num_price, cur):
        total = 0
        weight = num_price
        for i in range(cur, cur + num_price):
            total += weight * prices[i % num_price]
            weight -= 1
        div = num_price * (num_price + 1) / 2
        return total / div


class EMA(MA):
    def __init__(self, short_minutes, long_minutes, interval, price_center):
        MA.__init__(self, short_minutes, long_minutes, interval, price_center)

    def OnNewPrice(self, price):
        self.ma_short = self.EMAPrice(self.cur_price, self.ma_short, self.num_short_price, self.short_pos)          
        self.ma_long = self.EMAPrice(self.cur_price, self.ma_long, self.num_long_price, self.long_pos)          
        logger.info("price: %f, %s price: %f, %f", self.cur_price, self.__class__, self.ma_short, self.ma_long)
        self.short_pos += 1
        self.long_pos += 1
        pass    
    def EMAPrice(self, cur_price, last_ma, num_price, cur):
        N = min(num_price, cur + 1)
        # Y = [ 2 * X + (N - 1) * Y'] / (N + 1)
        price = (2 * cur_price + (N - 1) * last_ma) / (N + 1)
        return price

class MMA(EMA):
    def __init__(self, short_minutes, long_minutes, interval, price_center):
        EMA.__init__(self, short_minutes, long_minutes, interval, price_center)

    def EMAPrice(self, cur_price, last_ma, num_price, cur):
        N = min(num_price, cur + 1)
        # Y = [ X + (N - 1) * Y'] / N 
        price = (cur_price + (N - 1) * last_ma) / N
        return price

class Strategy(threading.Thread): 
    def __init__(self, short_minutes, long_minutes, interval, price_center):
        threading.Thread.__init__(self)
        self.long_minutes = long_minutes
        self.actions = [INIT] * 3
        self.moves = []
	# 按顺序 
    	self.moves.append(WMA(short_minutes, long_minutes, interval, price_center))
    	self.moves.append(EMA(short_minutes, long_minutes, interval, price_center))
    	self.moves.append(MMA(short_minutes, long_minutes, interval, price_center))
	self.profits = [0] * 3
	# 历史价格
	self.num_hist_price = int((3 * 60) / price_center.get_interval())
	self.history = [0] * self.num_hist_price
	self.history_pos = 0

    @staticmethod
    def fluct(hist, num):
	data = sorted(hist)
	logger.info("high: %f, low: %f", data[-2], data[1])
	return (data[-2] - data[1]) / hist[0]

    def run(self):
        for i in range(len(self.moves)):
            self.moves[i].start()
        # 让均线系统初始化
        time.sleep((self.long_minutes + 1) * 60)
        logger.info("init price done")
        while True:
            for i in range(len(self.moves)):
                action = self.actions[i]
            	(cur, s, l) = self.moves[i].GetPrice()
		if i == 0:
		    self.history[self.history_pos] = cur
		    self.history_pos = (self.history_pos + 1) % self.num_hist_price
		
            	if s > l:
               	    action = BUY
                elif s < l:
                    action = SELL

                if action != self.actions[i]:
                    self.actions[i] = action
		    if action == BUY:
			self.profits[i] -= cur
		    else:
			self.profits[i] += cur
		    fl = Strategy.fluct(self.history, self.num_hist_price) 
                    logger.info("%s add action: %d, fluct: %f, profits: %f", self.moves[i].__class__, action, fl, self.profits[i])
                    # 修改这里选择指标
                    #if i == 1:
                    if False:
                        try:
                    	    mq.put(action, True, 5) 
			except Queue.Full as e: 
			    logger.error("add to queue failed: %s, %d", str(e), action)
                    # endif
            time.sleep(1)
        pass

class Trade(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self.bc = None
        pass
    def run(self):
        while True:
            try:
                action = mq.get(block=True, timeout=1)
            except Queue.Empty as e:
                continue
            logger.info("get action: %d", action)
            self.trade(action)
            
    def sell(self, price, btc_amount, num_retry):
        price -= 2 ** num_retry
    	price = "%.2f" % price
    	btc_amount -= 0.0002
    	btc_amount = "%.4f" % btc_amount
        try:
            logger.info("sell: price: %s, amount: %s", price, btc_amount)
            ret = self.bc.sell(price, btc_amount)
            logger.info("sell ret: %s", str(ret))
        except Exception as e:
            logger.error("sell exception: %s", str(e))
        pass
    
    def buy(self, price, cny_amount, num_retry):
        price += 2 ** num_retry
        btc_amount = cny_amount/price
	# 舍入误差
        price -= 0.02
        price = "%.2f" % price
        btc_amount -= 0.0002
        btc_amount = "%.4f" % btc_amount
        try:
            logger.warning("buy: price: %s, amount: %s", price, btc_amount)
            ret = self.bc.buy(price, btc_amount)
            logger.info("buy ret: %s", str(ret))
        except Exception as e:
            logger.error("buy exception: %s", str(e))
        pass
    
    def cancel(self):
        for i in range(trade_retry):
            try:
                order_dict = self.bc.get_orders()
                write_log(str(order_dict))
                order_list = order_dict['order']
                for order in order_list:
                    logger.info(str(order))
                    ret = self.bc.cancel(order['id'])
                    logger.info("cancel ret: %s", str(ret))
            except Exception as e:
                logger.error("cancel order failed: %s", str(e))
                continue
            return 
    def create_bc_client(self):
        if self.bc:
            return
        while True:
            try:
                self.bc = btcchina.BTCChina(access_key,secret_key)
            except Exception as e:
                logger.error("create bc client failed: %s", str(e))
                time.sleep(2)
                continue
            logger.info("create bc client success")
            return 
        
    def trade(self, trade_type):
        for i in range(trade_retry):
            self.create_bc_client()
            # get info
            btc_amount = 0
            cny_amount = 0
            try:
                info = self.bc.get_account_info()
                btc_amount = float(info['balance']['btc']['amount'])
                cny_amount = float(info['balance']['cny']['amount'])
                logger.info("balance(btc, cny): %f, %f", btc_amount, cny_amount)
            except Exception as e:
                logger.error("get account info failed: %s", str(e))
                self.bc = None
                time.sleep(1)
                continue

            if trade_type == SELL and btc_amount < min_btc_balance:
                logger.info("sell but amount is too little: %f", btc_amount)
                return
            if trade_type == BUY and cny_amount < min_cny_balance:
                logger.info("buy but money is too little: %f", cny_amount)
                return

            if trade_type == SELL:
                price = PriceCenter.query_price(BUY_PRICE)
            else:
                price = PriceCenter.query_price(SELL_PRICE)
            if not price:
                logger.error("get price failed")
                time.sleep(1)
                continue
                        
            if trade_type == SELL:
                self.sell(price, btc_amount, i + 1)
            else:
                self.buy(price, cny_amount, i + 1)
            time.sleep(2)
            
            # 如果没成交，撤单
            self.cancel()
            time.sleep(1)
            pass
        
if __name__ == '__main__':
    global access_key, secret_key
    access_key=sys.argv[1]
    secret_key=sys.argv[2]
    daemonize()
    init_logger()
    price_center = PriceCenter(10)
    price_center.start()
    strategy = Strategy(30, 60, 30, price_center)
    strategy.start()
    trade = Trade()
    trade.start()
