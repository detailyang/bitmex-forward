import json

from bitmex_async_websocket import BitMEXAsyncWebsocket
from notifier import discord
from logger import logger


async def process_new_order(o, log):
    symbol = o["symbol"]
    price = o["price"]
    side = o["side"]
    order_qty = o["orderQty"]
    order_type = o["ordType"]
    simple_leaves_qty = o["simpleLeavesQty"]
    text = o["text"]
    ex_destination = o["exDestination"]
    stop_price = o["stopPx"]

    title = "%s Order Submitted" % (order_type)
    if order_type == "Stop":
        direction = "above" if side == "buy" else "below"
        content = "%s %f Contracts of %s at Market. Trigger: Last Price @%f and %s. %s" % (side, order_qty, symbol, stop_price, direction, text)
    else:
        content = "%s %f Contracts of %s at %f. %s" % (side, order_qty, symbol, price, text)

    await log(title, content)


async def process_restated_order(o, log):
    symbol = o["symbol"]
    price = o["price"]
    side = o["side"]
    order_qty = o["orderQty"]
    order_type = o["ordType"]
    simple_leaves_qty = o["simpleLeavesQty"]
    text = o["text"]
    ex_destination = o["exDestination"]

    title = "%s Order Restated" % (order_type)
    content = "%s %f Contracts of %s at %f. %s" % (side, order_qty, symbol, price, text)

    await log(title, content)


async def process_trade_order(o, log):
    symbol = o["symbol"]
    price = o["price"]
    side = o["side"]
    ex_destination = o["exDestination"]
    order_qty = o["orderQty"]
    order_type = o["ordType"]
    order_status = o["ordStatus"]
    last_qty = o["lastQty"]
    leaves_qty = o["leavesQty"]
    text = o["text"]

    if order_status == "Filled":
        title = "%s Order Filled" % (order_type) 
        body = "%f Contracts of %s %s at %f. The order has fully filled. %s" % (order_qty, symbol, side, price, text)
    elif order_status == "PartiallyFilled":
        title = "%s %f Contracts %s" % (order_type, last_qty, side)
        body = "%f Contracts of %s %s at %f. %f contracts remain in the order. %s" % (last_qty, symbol, side, price, leaves_qty, text)

    content = "%s:%s" % (title, body)

    await log(title, content)


async def process_cancel_order(o, log):
    symbol = o["symbol"]
    price = o["price"]
    side = o["side"]
    ex_destination = o["exDestination"]
    order_qty = o["orderQty"]
    order_type = o["ordType"]
    order_status = o["ordStatus"]
    last_qty = o["lastQty"]
    leaves_qty = o["leavesQty"]    
    text = o['text']

    title = "%s Order Canceled" % (order_type)
    content = "%s %f Contract of %s at %f. %s" %(side, order_qty, symbol, price, text)

    await log(title, content)


async def forwarder(endpoint, symbols, api_key, api_secret, discordwebhook):
    async def log(title, content):
        if discordwebhook:
            await discord(discordwebhook, title, content)
        logger.info("%s:%s" % (title, content))

    bm = await BitMEXAsyncWebsocket(endpoint=endpoint, symbols=symbols, api_key=api_key, api_secret=api_secret)

    logger.info("Start up bitmex-forwarder from %s" % (bm.url))

    try:
        while True:
            data = await bm.recv()
            msg = json.loads(data)

            table = msg['table'] if 'table' in msg else None
            action = msg['action'] if 'action' in msg else None
            data = msg['data'] if 'data' in msg else None

            # Care about order created canceled and filled only.
            if action != 'insert' or table != 'execution':
               continue 

            logger.debug(json.dumps(msg))

            for o in data:
                exec_type = o["execType"] 
                if exec_type == "New":
                    await process_new_order(o, log)
                elif exec_type == "Trade":
                    await process_trade_order(o, log)
                elif exec_type == "Canceled":
                    await process_cancel_order(o, log)
                elif exec_type == "Restated":
                    await process_restated_order(o, log)
                else:
                    logger.warning("unknow order", json.dumps(o))
                
    finally:
        await bm.close()
