import json

import aiohttp


async def discord(webhook, title, content):
    payload = json.dumps({'username': title, 'content': content})
    headers = {'content-type': "application/json"}
    async with aiohttp.ClientSession() as session:
        html = await session.post(webhook, data=payload, headers=headers)
