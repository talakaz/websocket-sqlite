from websocket import create_connection
from datetime import datetime
from datetime import timedelta
import numpy as np
import timeloop
import sqlite3
import json
import time

symbol1 = {'s': 'BTCUSDT', 'last': 0, 'bid': 0, 'ask': 0, 'ema': 0}
symbol2 = {'s': 'BCHUSDT', 'last': 0, 'bid': 0, 'ask': 0, 'ema': 0}
bias = {'s1_bs': 0, 's2_bs': 0, 'bs': 0}
wss = {'ws': None}
sys_check = {'price': False, 'bs': False, 'wss': None, 'wssTimeOut': 0}

tl = timeloop.Timeloop()

# 建立 websocket 連線，訂閱 orderbook ticker user 等訊號
ws = create_connection("wss://fstream.binance.com/ws")  # realnet
ws.send(json.dumps({"method": "SUBSCRIBE", "params": [symbol1['s'].lower() + "@ticker", symbol2['s'].lower() + "@ticker"], "id": 99}))
ws.send(json.dumps({"method": "SET_PROPERTY", "params": ["combined", True], "id": 99}))


# 取得最新價格及平均價格 (Binance 的 ws 可取得日均價，接近 ema24)
@tl.job(interval=timedelta(seconds=0.1))
def realtime_info():
    try:
        # 第一次連線
        if sys_check['wss'] is None:
            sys_check['wss'] = create_connection("wss://fstream.binance.com/ws")
            print('Binance Future Websocket 連線中...')
            time.sleep(0.5)
            sys_check['wss'].send(json.dumps({"method": "SUBSCRIBE", "params": [symbol1['s'].lower() + "@ticker", symbol2['s'].lower() + "@ticker"], "id": 99}))
            sys_check['wss'].send(json.dumps({"method": "SET_PROPERTY", "params": ["combined", True], "id": 99}))
            time.sleep(0.5)
            if sys_check['wss'].sock:
                print('Binance Future Websocket 已連線。')

        # wss 物件存在時
        elif sys_check['wss']:
            # 連線狀態時更新價格
            if sys_check['wss'].sock:
                try:
                    raw = json.loads(ws.recv())
                    if raw['stream'] == symbol1['s'].lower() + '@ticker':
                        symbol1['last'] = np.round((float(raw['data']['c'])) + 0.00001, decimals=4)
                        symbol1['ema'] = np.round((float(raw['data']['w'])) + 0.00001, decimals=4)
                    if raw['stream'] == symbol2['s'].lower() + '@ticker':
                        symbol2['last'] = np.round((float(raw['data']['c'])) + 0.00001, decimals=4)
                        symbol2['ema'] = np.round((float(raw['data']['w'])) + 0.00001, decimals=4)
                    if 'closed' in raw or '強制關閉' in raw:
                        print('遠端主機已強制關閉一個現存的連線，重新啟動!')
                        system_stop()
                    if sys_check['price'] is False:
                        sys_check['price'] = True
                        print('price checked!')
                except Exception as e:
                    if e.args == ('e',) or e.args == ('stream',):
                        pass
                    else:
                        print('Binance Future Websocket 已斷線:' + str(e.args))
                        sys_check['wss'].close()
                        sys_check['wss'] = None
                        time.sleep(5)

            # 斷線時重新連線
            elif sys_check['wss'].connected is False or sys_check['wss'].sock is None:
                print('Binance Future Websocket 已斷線，%s秒後重新連線...' % (str(5 - sys_check['wssTimeOut'])))
                sys_check['wssTimeOut'] += 1
                time.sleep(1)
                if sys_check['wssTimeOut'] == 5:
                    sys_check['wss'].close()
                    sys_check['wss'] = None
                    sys_check['wssTimeOut'] = 0

    except Exception as e:
        print(e.args)


# 計算背離率
@tl.job(interval=timedelta(seconds=0.1))
def calculate_bs():
    try:
        if sys_check['price'] and symbol1['ema'] != 0 and symbol2['ema'] != 0:
            s1_bias = -np.round(((symbol1['ema'] - symbol1['last']) / symbol1['ema']) + 0.00001, decimals=4)
            s2_bias = np.round(((symbol2['ema'] - symbol2['last']) / symbol2['ema']) + 0.00001, decimals=4)
            bs = np.round((s1_bias + s2_bias) + 0.00001, decimals=4)
            bias['s1_bs'] = s1_bias
            bias['s2_bs'] = s2_bias
            bias['bs'] = bs

            if sys_check['bs'] is False:
                sys_check['bs'] = True

    except Exception as e:
        print(e.args)


@tl.job(interval=timedelta(seconds=1))
def store_to_db():
    # 將目前資訊存入資料庫
    try:
        db_index = 'bnc_bs_btc_bch_'
        current = datetime.now()
        cur_year = str(current.year)
        cur_month = str(current.month) if len(str(current.month)) == 2 else '0' + str(current.month)
        cur_day = str(current.day) if len(str(current.day)) == 2 else '0' + str(current.day)
        cur_db = db_index + cur_year + '.db'
        cur_table = db_index + cur_year + cur_month + cur_day

        writer = sqlite3.connect(r'../\SQLite\%s' % cur_db, check_same_thread=False, isolation_level=None)
        writer.execute('pragma journal_mode=wal;')

        cmd = '''CREATE TABLE IF NOT EXISTS %s (date TEXT, s1_c REAL, s2_c REAL, s1_ema REAL, s2_ema REAL, s1_bs REAL, s2_bs REAL, bs REAL)''' % cur_table
        writer.cursor().execute(cmd)

        if sys_check['bs']:
            cmd = "INSERT INTO %s (date, s1_c, s2_c, s1_ema, s2_ema, s1_bs, s2_bs, bs) VALUES (?, ?, ?, ?, ?, ?, ?, ?)" % cur_table
            com = (datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S'), symbol1['last'], symbol2['last'], symbol1['ema'], symbol2['ema'], bias['s1_bs'], bias['s2_bs'], bias['bs'])
            c = writer.cursor()
            c.execute(cmd, com)

    except Exception as e:
        print(e.args)





@tl.job(interval=(timedelta(seconds=1)))
def print_log():
    try:
        if sys_check['bs']:
            print('============================ %s【 Binance BS %s 】============================' % (datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S'), symbol1['s'] + '/' + symbol2['s']))
            print(' s1_price:%.2f s2_price:%.2f s1_ema:%.4f s2_ema:%.4f s1_bs:%.4f s2_bs:%.4f【 bs:%.4f 】' % (symbol1['last'], symbol2['last'], symbol1['ema'], symbol2['ema'], bias['s1_bs'], bias['s2_bs'], bias['bs']))

    except Exception as e:
        print(e.args)


# 定時回報 websocket Ping Pong
@tl.job(interval=(timedelta(seconds=30)))
def ping_pong():
    try:
        sys_check['wss'].pong('pong')
    except Exception as e:
        if 'closed' in str(e.args) or '強制關閉' in str(e.args):
            print('遠端主機已強制關閉一個現存的連線，重新啟動!')
            system_stop()
        else:
            print('心跳錯誤，重新啟動!')
            system_stop()


tl.start()


def system_stop():
    time.sleep(3)
    import subprocess
    subprocess.call("taskkill /F /IM pythonbncbsbtcbch.exe /T")


while True:
    try:
        time.sleep(0.1)
    except Exception as e:
        print(e.args)
