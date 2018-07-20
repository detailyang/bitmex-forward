import json
import sys
import logging
import urllib


import aiohttp
from websockets import connect
import click

from util import generate_nonce, generate_signature


logger = logging.getLogger("bitmex")
logger.setLevel(logging.INFO)
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
formatter = logging.Formatter("[%(asctime)s %(levelname)s %(filename)s@%(lineno)d]:  %(message)s")
ch.setFormatter(formatter)
logger.addHandler(ch)


class BitMEXAsyncWebsocket:
    def __init__(self, endpoint, symbols, api_key=None, api_secret=None):
        if api_key is not None and api_secret is None:
            raise ValueError('api_secret is required if api_key is provided')
        if api_key is None and api_secret is not None:
            raise ValueError('api_key is required if api_secret is provided')

        logger.debug("Initializing WebSocket.")
        self.api_key = api_key
        self.api_secret = api_secret
        self.endpoint = endpoint
        self.symbols = symbols
        self.url = self.__get_url()
        self.headers = self.__get_auth()

    def __get_auth(self):
        '''Return auth headers. Will use API Keys if present in settings.'''
        if self.api_key:
            # To auth to the WS using an API key, we generate a signature of a nonce and
            # the WS API endpoint.
            nonce = generate_nonce()
            return {
                "api-nonce":  str(nonce),
                "api-signature":  generate_signature(self.api_secret, 'GET', '/realtime', nonce, ''),
                "api-key":  self.api_key
            }
        else:
            return {}

    def __get_url(self):
        '''
        Generate a connection URL. We can define subscriptions right in the querystring.
        Most subscription topics are scoped by the symbol we're listening to.
        '''

        # You can sub to orderBookL2 for all levels, or orderBook10 for top 10 levels & save bandwidth
        symbolSubs = ["execution", "instrument", "order", "orderBookL2", "position", "quote", "trade"]
        genericSubs = ["margin"]

        subscriptions = []
        for symbol in self.symbols:
            subscriptions += [sub + ':' + symbol for sub in symbolSubs]
        subscriptions += genericSubs

        urlParts = list(urllib.parse.urlparse(self.endpoint))
        urlParts[0] = urlParts[0].replace('http', 'ws')
        urlParts[2] = "/realtime?subscribe={}".format(','.join(subscriptions))
        return urllib.parse.urlunparse(urlParts)


    def __await__(self):
        # see: http://stackoverflow.com/a/33420721/1113207
        return self.__async_init().__await__()

    async def __async_init(self):
        self.conn = connect(self.url, extra_headers=self.headers)
        self.ws = await self.conn.__aenter__()
        return self

    async def close(self):
        await self.conn.__aexit__(*sys.exc_info())

    async def send(self, message):
        await self.ws.send(message)

    async def recv(self):
        return await self.ws.recv()  