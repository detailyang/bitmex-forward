import asyncio

import click

from forwarder import forwarder


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
    required=True,
    help="bitmex contract symbol")
@click.option('--apikey', '-k', default=None, help='api key of bitmex.')
@click.option('--apisecret', '-s', default=None, help="api secret of bitmex")
@click.option(
    '--discordwebhook', '-d', default=None, help="discord channel webhook")
def main(endpoint, symbol, apikey, apisecret, discordwebhook):
    loop = asyncio.get_event_loop()
    loop.run_until_complete(
        forwarder(endpoint, symbol, apikey, apisecret, discordwebhook))
    loop.close()


if __name__ == '__main__':
    main(auto_envvar_prefix="BITMEX_FORDWARDER")
