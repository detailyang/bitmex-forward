import asyncio
import click
import requests


from forwarder import forwarder


def get_bitmex_symbol(endpoint):
    mainnet = "https://www.bitmex.com/api/bitcoincharts"
    testnet = "https://testnet.bitmex.com/api/bitcoincharts"

    api = testnet if 'testnet' in endpoint else mainnet
    r = requests.get(api)
    data = r.json()
    return data["all"]


@click.command()
@click.option(
    '--endpoint',
    '-e',
    default="https://wwww.bitmex.com/api/v1",
    help='the endpoint of bitmex.')
@click.option(
    '--symbol',
    '-m',
    multiple=True,
    required=False,
    help="bitmex contract symbol if none it will be all contract")
@click.option('--apikey', '-k', default=None, help='api key of bitmex.')
@click.option('--apisecret', '-s', default=None, help="api secret of bitmex")
@click.option(
    '--discordwebhook', '-d', default=None, help="discord channel webhook")
def main(endpoint, symbol, apikey, apisecret, discordwebhook):
    symbol = get_bitmex_symbol(endpoint) if symbol is None else symbol
    loop = asyncio.get_event_loop()
    loop.run_until_complete(
        forwarder(endpoint, symbol, apikey, apisecret, discordwebhook))
    loop.close()


if __name__ == '__main__':
    main(auto_envvar_prefix="BITMEX_FORDWARDER")
