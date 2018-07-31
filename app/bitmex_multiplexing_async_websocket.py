import asyncio
import logging
import json
import threading
import time
import traceback
import urllib
import codecs
import hashlib
import hmac
from collections import defaultdict

import janus
import requests
import websockets


class BitmexMultiplexingAsyncWebsocket(object):
    VERB = "GET"
    AUTH_ENDPOINT = "/realtime"
    ENDPOINT = "/realtimemd?transport=websocket&b64=1"
    MAINNET_ENDPOINT = "wss://www.bitmex.com/realtimemd"
    TESTNET_ENDPOINT = "wss://testnet.bitmex.com/realtimemd"

    MESSAGE_TYPE = 0
    SUBSCRIBE_TYPE = 1
    UNSUBSCRIBE_TYPE = 2

    MAX_TABLE_LEN = 200

    PUBLIC_TOPICS = [
        "announcement",# Site announcements
        "chat",        # Trollbox chat
        "connected",   # Statistics of connected users/bots
        "funding",     # Updates of swap funding rates. Sent every funding interval (usually 8hrs)
        "instrument",  # Instrument updates including turnover and bid/ask
        "insurance",   # Daily Insurance Fund updates
        "liquidation", # Liquidation orders as they're entered into the book
        "orderBookL2", # Full level 2 orderBook
        "orderBook10", # Top 10 levels using traditional full book push
        "publicNotifications", # System-wide notifications (used for short-lived messages)
        "quote",       # Top level of the book
        "quoteBin1m",  # 1-minute quote bins
        "quoteBin5m",  # 5-minute quote bins
        "quoteBin1h",  # 1-hour quote bins
        "quoteBin1d",  # 1-day quote bins
        "settlement",  # Settlements
        "trade",       # Live trades
        "tradeBin1m",  # 1-minute trade bins
        "tradeBin5m",  # 5-minute trade bins
        "tradeBin1h",  # 1-hour trade bins
        "tradeBin1d",  # 1-day trade bins
    ]

    PRIVATE_TOPICS = [
        "affiliate",   # Affiliate status, such as total referred users & payout %
        "execution",   # Individual executions; can be multiple per order
        "order",       # Live updates on your orders
        "margin",      # Updates on your current account balance and margin requirements
        "position",    # Updates on your positions
        "privateNotifications", # Individual notifications - currently not used
        "transact"     # Deposit/Withdrawal updates
        "wallet"       # Bitcoin address balance data, including total deposits & withdrawals
    ]

    NO_SYMBOL_TABLES = [
        'account',
        'affiliate',
        'funds',
        'insurance',
        'margin',
        'transact',
        'wallet',
        'announcement',
        'connected',
        'chat',
        'publicNotifications',
        'privateNotifications'
    ]

    def __init__(self, testnet=False, logger=None, logger_level=logging.INFO):
        self.endpoint = self.MAINNET_ENDPOINT if testnet is False else self.TESTNET_ENDPOINT
        self.logger = logger if logger is not None else self._setup_logger(logger_level)

        self.channels = defaultdict(dict)
        self.accounts = {}
        self.public_account = "public"

        self.closed = False
        self.connected = False
        self.expires = 60 * 60
        self.timeout = 2
        self.maxretry = 10
        self.tried = 0

        self.loop = asyncio.get_event_loop()
        self.queue = janus.Queue(loop=self.loop)

    def get_all_symbol(self):
        mainnet = "https://www.bitmex.com/api/bitcoincharts"
        testnet = "https://testnet.bitmex.com/api/bitcoincharts"

        api = testnet if 'testnet' in self.endpoint else mainnet
        r = requests.get(api)
        data = r.json()
        return data["all"]

    def open(self):
        self.thread = threading.Thread(target=self._open, daemon=True)
        self.thread.start() 

        # workaround to wait websocket
        time.sleep(self.timeout*1.2)
        if not self.connected:
            raise Exception("connect %s timeoutd" % (self.endpoint))

    def close(self):
        if self.closed:
            raise Exception("client has closed")
        self.queue.close()
        self.closed = True

    def add_account(self, name, key, secret):
        self.accounts[name] = {
            'key': key,
            'secret': secret,
            'verified': False,
        }

    def _clean(self):
        # clean account state
        for _, account in self.accounts.items():
            account["verified"] = False

        # clean connect state
        self.connected = False
        self.closed = False

    def _open(self):
        while True:
            try:
                rc = self.loop.run_until_complete(self._run()) 
                if rc:
                    self.logger.info("Exited, because of actived close")
                    break
            except Exception as e:
                self.logger.info("Got exception: %s" % repr(e))

            self.tried += 1
            if self.tried < self.maxretry:
                self.logger.info("reconnect(%d/%d) to %s", self.tried, self.maxretry, self.endpoint)
            else:
                self.logger.error("attempt to reconnect(%d/%d) %s failed", self.tried, self.maxretry, self.endpoint)
                break

            time.sleep(1)
            self._clean()

        self.loop.close()

    async def _run(self):
        async with websockets.connect(self.endpoint, timeout=self.timeout) as ws:
            self.connected = True
            self.logger.info("connected websocket: %s" % self.endpoint)

            recv_task = asyncio.ensure_future(
                self._recv(ws))
            send_task = asyncio.ensure_future(
                self._send(ws))
            ping_task = asyncio.ensure_future(
                self._ping(ws))
            subscribe_task = asyncio.ensure_future(
                self._subscribe(ws))
            done, pending = await asyncio.wait(
                [recv_task, send_task, ping_task, subscribe_task],
                return_when=asyncio.FIRST_EXCEPTION,
            )

            exit = False
            for task in done:
                e = task.exception()
                if isinstance(e, RuntimeError) and str(e) == "Modification of closed queue is forbidden":
                    exit = True
                if e:
                    self.logger.error("websocket %s exited with %s" % (self.endpoint, repr(e)))

            for task in pending:
                task.cancel()

            await ws.close()

        self.connected = False

        return exit

    async def _ping(self, ws):
        while True:
            await asyncio.sleep(5)
            await self.queue.async_q.put("ping")

    async def _subscribe(self, ws):
        # resubscribe all topics
        for channel, topics in self.channels.items():
            for topic, d in topics.items():
                topic = d["topic"]
                symbol = d["symbol"]
                keysecret = None
                if channel in self.accounts:
                    key = self.accounts[channel]["key"]
                    secret = self.accounts[channel]["secret"]
                    keysecret = (key, secret)
                self._open_subscription(topic, symbol, channel, keysecret)

    async def _recv(self, ws):
        async for message in ws:
            await self._dispatch(message)

    async def _dispatch(self, message):
        if message == "pong":
            self.logger.debug("recev pong frame")
            return

        message = json.loads(message)
        self.logger.debug("recv %s" % (json.dumps(message)))
        if len(message) != 4:
            self.logger.error("unknow message format: %s", json.dumps(message))
            return

        t, channel, cid, payload = message

        if "error" in payload:
            self.logger.error("'channel:%s' got the error:%s", channel, payload["error"])
            self.accounts[channel]["verified"] = False
            return

        if channel not in self.channels:
            self.logger.info("unknow channel %s" % channel)
            return

        if "subscribe" in payload:
            self.logger.info("'channel:%s' subscribed %s" % (channel, payload["subscribe"]))
        elif "info" in payload:
            self.logger.info("'channel:%s' connected: %s" % (channel, payload["info"]))
        elif "success" in payload:
            self.logger.info("'channel:%s' was verified" % (channel))
            self.accounts[channel]["verified"] = True
        else:
            table = payload["table"] if "table" in payload else None
            action = payload["action"] if "action" in payload else None

            if not table:
                return

            # data = self._parse(channel, table, action, payload)
            await self.channels[channel][table]["handler"](channel, table, payload)

    async def _send(self, ws):
        while True:
            message = await self.queue.async_q.get()
            if not message:
                break
            self.logger.debug("send %s" % (json.dumps(message)))
            message = json.dumps(message) if not isinstance(message, str) else message
            await ws.send(message)

    # def _parse(self, channel, table, action, message):
    #     if action == "partial":
    #         self.logger.info("'channel:%s' %s: partial" % (channel, table))

    #     elif action == "insert":
    #         self.logger.info("'channel:%s' %s: insert %s" % (channel, table, message["data"]))
    #         if channel not in self.data:
    #             self.data[channel] = {}
    #         if table not in self.data[channel]:
    #             self.data[channel][table] = []

    #         self.data[channel][table] +=  message

    #         if len(self.data[channel][table]) > self.MAX_TABLE_LEN:
    #             self.data[channel][table] = self.data[channel][table][int(self.MAX_TABLE_LEN/2):]

    #     elif action == "delete":
    #         self.logger.info("'channel:%s' %s: delete %s" % (channel, table, message["data"]))

    #     # update
    #     else:
    #         self.logger.info("'channel:%s' %s: update %s" % (channel, table, message["data"]))

    def subscribe_public_topic(self, topic, symbol=None, handler=None):
        if not self.connected:
            raise Exception("websocket was disconnected")

        self.channels['public'][topic] = {
            'topic': topic,
            'symbol': symbol,
            'handler': handler,
        }

        self._open_subscription(topic, symbol, self.public_account)

    def subscribe_private_topic(self, account, topic, symbol=None, handler=None):
        if not self.connected:
            raise Exception("websocket was disconnected")

        if account not in self.accounts:
            raise Exception("account do not found")

        self.channels[account][topic] = {
            'topic': topic,
            'symbol': symbol,
            'handler': handler,
        }

        keysecret = (self.accounts[account]['key'], self.accounts[account]['secret'])

        self._open_subscription(topic, symbol, account, keysecret=keysecret)

    def _open_subscription(self, topic, symbol, channel, keysecret=None):

        # open channel
        req = [self.SUBSCRIBE_TYPE, channel, channel]
        self.queue.sync_q.put(req)

        if keysecret and not self.accounts[channel]["verified"]:
            key, secret = keysecret
            expires = int(round(time.time()) + self.expires) * 1000 * 1000
            signature = bitmex_signature(secret, self.VERB, self.AUTH_ENDPOINT, expires)
            req = [self.MESSAGE_TYPE, channel, channel, {'op': 'authKey', 'args': [key, expires, signature]}]
            self.queue.sync_q.put(req)

            # assume it's verified
            self.accounts[channel]["verified"] = True


        subscription = [topic if not symbol else topic + ":" + symbol]
        payload = {"op": "subscribe", "args": subscription}
        req = [self.MESSAGE_TYPE, channel, channel, payload]
        self.queue.sync_q.put(req)

    def _setup_logger(self, level):
        logger = logging.getLogger(self.__class__.__name__)
        logger.setLevel(level)
        ch = logging.StreamHandler()
        ch.setLevel(level)
        formatter = logging.Formatter("[%(asctime)s %(levelname)s %(filename)s@%(lineno)d]:  %(message)s")
        ch.setFormatter(formatter)
        logger.addHandler(ch)
        return logger


def bitmex_signature(secret, verb, url, nonce, postdict=None):
    """Given an API Secret and data, create a BitMEX-compatible signature."""
    data = ''
    if postdict:
        # separators remove spaces from json
        # BitMEX expects signatures from JSON built without spaces
        data = json.dumps(postdict, separators=(',', ':'))
    parsedURL = urllib.parse.urlparse(url)
    path = parsedURL.path

    if parsedURL.query:
        path = path + '?' + parsedURL.query
    # print("Computing HMAC: %s" % verb + path + str(nonce) + data)
    message = (verb + path + str(nonce) + data).encode('utf-8')
    signature = hmac.new(secret.encode('utf-8'), message, digestmod=hashlib.sha256).hexdigest()

    return signature


if __name__ == "__main__":
    class BitmexHandler(object):
        def __init__(self):
            pass

        def chat_handler(self, channel, table, data):
            print("channel:%s table:%s data:%s" % (channel, table, data))

        def trade_handler(self, channel, table, data):
            print("channel:%s table:%s data:%s" % (channel, table, data))

        def announcement_handler(self, channel, table, data):
            print("channel:%s table:%s data:%s" % (channel, table, data))

        def instrument_handler(self, channel, table, data):
            print("channel:%s table:%s data:%s" % (channel, table, data))

        def quote_handler(self, channel, table, data):
            print(channel, table, data)

        def order_handler(self, channel, table, data):
            print(channel, table, data)

        def execution_handler(self, channel, table, data):
            print(channel, table, data)

    bh = BitmexHandler()

    key = "0qtGfIyjcuVJUfEyqnbAzv5W"
    secret = "mrxYe9h_pbhAtq_Bs4Ne_AHL0g09Ryg9cNx_FIx8rutL_euU"
    key1 = "_KOEM78pB54uI6kn_5iDbd4k"
    secret1 = "ZlVal2lr4Egc245xoPDCnLzFvuW4xDWQanunnv6s-1_whbUP"

    bm = BitmexMultiplexingAsyncWebsocket(testnet=True, logger_level=logging.INFO)
    print(bm.get_all_symbol())
    bm.add_account('testaccount0', key, secret)
    bm.add_account('testaccount1', key1, secret1)
    bm.open()
    bm.subscribe_public_topic("chat", handler=bh.chat_handler)
    bm.subscribe_public_topic("trade", symbol="ADAU18", handler=bh.trade_handler)
    bm.subscribe_public_topic("trade", symbol="XBTUSD", handler=bh.trade_handler)
    bm.subscribe_public_topic("quote", handler=bh.quote_handler)
    bm.subscribe_public_topic("announcement", handler=bh.announcement_handler)
    bm.subscribe_public_topic("instrument", symbol="XBTUSD", handler=bh.instrument_handler)
    bm.subscribe_private_topic("testaccount1", "execution", handler=bh.execution_handler)
    bm.subscribe_private_topic("testaccount0", "execution", handler=bh.execution_handler)
    bm.subscribe_private_topic("testaccount1", "order", handler=bh.order_handler)
    bm.subscribe_private_topic("testaccount0", "order", handler=bh.order_handler)
    while True:
        pass