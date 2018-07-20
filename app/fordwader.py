import json

from bitmex_async_websocket import BitMEXAsyncWebsocket
from notifier import discord
from logger import logger


async def process_new_order(o, log):
    symbol = o["symbol"]
    price = o["price"]
    side = o["side"]
    order_qty = o["orderQty"]
    simple_leaves_qty = o["simpleLeavesQty"]
    ex_destination = o["exDestination"]

    title = "Order Submitted"
    content = "Order Submitted: %s %f Contracts of %s at %f" % (side, order_qty, symbol, price)

    await log(title, content)


async def process_trade_order(o, log):
    symbol = o["symbol"]
    price = o["price"]
    side = o["side"]
    ex_destination = o["exDestination"]
    order_qty = o["orderQty"]
    order_status = o["ordStatus"]
    last_qty = o["lastQty"]
    leaves_qty = o["leavesQty"]

    if order_status == "Filled":
        title = "Order Filled"
        body = "%f Contracts of %s %s at %f. The order has fully filled" % (order_qty, symbol, side, price)
    elif order_status == "PartiallyFilled":
        title = "%f Contracts %s" % (last_qty, side)
        body = "%f Contracts of %s %s at %f. %f contracts remain in the order" % (last_qty, symbol, side, price, leaves_qty)

    content = "%s:%s" % (title, body)

    await log(title, content)


async def process_cancel_order(o, log):
    symbol = o["symbol"]
    price = o["price"]
    side = o["side"]
    ex_destination = o["exDestination"]
    order_qty = o["orderQty"]
    order_status = o["ordStatus"]
    last_qty = o["lastQty"]
    leaves_qty = o["leavesQty"]    
    text = o['text']

    title = "Order Canceled"
    content = "Order Canceled: %s %f Contract of %s at %f. %s" %(side, order_qty, symbol, price, text)

    await log(title, content)


async def forwader(endpoint, symbols, api_key, api_secret, discordwebhook):
    async def log(title, content):
        if discordwebhook:
            await discord(discordwebhook, title, content)
        logger.info("%s:%s" % (title, content))

    bm = await BitMEXAsyncWebsocket(endpoint=endpoint, symbols=symbols, api_key=api_key, api_secret=api_secret)

    logger.info("Start up bitmex-fordwader from %s" % (bm.url))

    try:
        while True:
            data = await bm.recv()
            msg = json.loads(data)
            if msg is None:
               continue

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
                else:
                    logger.warning("unknow order", json.dumps(o))
                
    finally:
        await bm.close()