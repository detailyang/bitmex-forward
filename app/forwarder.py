import json

from bitmex_async_websocket import BitMEXAsyncWebsocket
from notifier import discord
from logger import logger
from bitmex_multiplexing_async_websocket import BitmexMultiplexingAsyncWebsocket


async def process_new_order(channel, o, log):
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
        direction = "above" if side == "Buy" else "below"
        content = "%s %d Contracts of %s at Market. Trigger: Last Price @%f and %s. %s" % (side, order_qty, symbol, stop_price, direction, text)
    else:
        content = "%s %d Contracts of %s at %.8f. %s" % (side, order_qty, symbol, price, text)

    content = "%s: %s" % (channel, content)

    await log(title, content)


async def process_restated_order(channel, o, log):
    symbol = o["symbol"]
    price = o["price"]
    side = o["side"]
    order_qty = o["orderQty"]
    order_type = o["ordType"]
    simple_leaves_qty = o["simpleLeavesQty"]
    text = o["text"]
    ex_destination = o["exDestination"]

    title = "%s Order Restated" % (order_type)
    content = "%s %d Contracts of %s at %.8f. %s" % (side, order_qty, symbol, price, text)
    content = "%s: %s" % (channel, content)

    await log(title, content)


async def process_trigger_order(channel, o, log):
    symbol = o["symbol"]
    price = o["price"]
    side = o["side"]
    order_qty = o["orderQty"]
    order_type = o["ordType"]
    simple_leaves_qty = o["simpleLeavesQty"]
    text = o["text"]
    ex_destination = o["exDestination"]

    title = "Stop Triggered"
    content = "A stop to %s %d contracts of %s at %.8f has been triggered. %s" % (side, order_qty, symbol, price, text)
    content = "%s: %s" % (channel, content)

    await log(title, content)


async def process_trade_order(channel, o, log):
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
        body = "%d Contracts of %s %s at %.8f. The order has fully filled. %s" % (order_qty, symbol, side, price, text)
    elif order_status == "PartiallyFilled":
        title = "%d Contracts %s" % (last_qty, side)
        body = "%d Contracts of %s %s at %.8f. %d contracts remain in the order. %s" % (last_qty, symbol, side, price, leaves_qty, text)

    content = body
    content = "%s: %s" % (channel, content)

    await log(title, content)


async def process_cancel_order(channel, o, log):
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
    stop_price = o["stopPx"]

    title = "%s Order Canceled" % (order_type)
    if order_type == "Stop":
        direction = "above" if side == "Buy" else "below"
        content = "%s %d Contract of %s at Market. Trigger: Last Price @%f and %s. %s" %(side, order_qty, symbol, stop_price, direction, text)
    else:
        content = "%s %d Contract of %s at %.8f. %s" %(side, order_qty, symbol, price, text)

    content = "%s: %s" % (channel, content)

    await log(title, content)

async def process_rejected_order(channel, o, log):
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
    stop_price = o["stopPx"]

    title = "%s Order Rejected" % (order_type)
    content = text

    content = "%s: %s" % (channel, content)

    await log(title, content)


async def process_funding_order(channel, o, log):
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
    stop_price = o["stopPx"]
    exec_comm = o["execComm"] / 10000

    title = "%s Order" % (order_type)
    content = "Pay %.8f %s" %(exec_comm, symbol)

    content = "%s: %s" % (channel, content)

    await log(title, content)
    

def forwarder(testnet, symbols, accounts, discordwebhook):
    async def log(title, content):
        if discordwebhook:
            await discord(discordwebhook, title, content)
        logger.info("%s:%s" % (title, content))

    async def execution_handler(channel, table, data):
        action = data["action"]
        if action != 'insert':
            return

        logger.debug(json.dumps(data))

        data = data['data']

        for o in data:
            exec_type = o["execType"] 
            if exec_type == "New":
                await process_new_order(channel, o, log)
            elif exec_type == "Trade":
                await process_trade_order(channel, o, log)
            elif exec_type == "Canceled":
                await process_cancel_order(channel, o, log)
            elif exec_type == "Restated":
                await process_restated_order(channel, o, log)
            elif exec_type == "TriggeredOrActivatedBySystem":
                await process_trigger_order(channel, o, log)
            elif exec_type == "Rejected":
                await process_rejected_order(channel, o, log)
            elif exec_type == "Funding":
                await process_funding_order(channel, o, log)
            else:
                logger.warning("unknow order", json.dumps(o))

    bm = BitmexMultiplexingAsyncWebsocket(testnet=testnet, logger=logger)
    for account in accounts:
        name = account['name']
        key = account['key']
        secret = account['secret']
        bm.add_account(name, key, secret)

    bm.open()

    for account in accounts:
        name = account['name']
        bm.subscribe_private_topic(name, "execution", handler=execution_handler)

    while True:
        pass
