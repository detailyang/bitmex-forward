import click
import requests
import json


from forwarder import forwarder


def get_bitmex_symbol(is_testnet):
    mainnet = "https://www.bitmex.com/api/bitcoincharts"
    testnet = "https://testnet.bitmex.com/api/bitcoincharts"

    api = testnet if is_testnet else mainnet
    r = requests.get(api)
    data = r.json()
    return data["all"]


@click.command()
@click.argument('f', type=click.Path(exists=True))
def main(f):
    file = click.format_filename(f)
    with open(file, "r") as f:
        data = json.load(f)
        accounts = data["accounts"]
        if not len(accounts):
            raise Exception("not accounts found in %s" % file)
        testnet = data["testnet"]
        discordwebhook = data["discordwebhook"]
        symbol = get_bitmex_symbol(testnet)
        forwarder(testnet, symbol, accounts, discordwebhook)


if __name__ == '__main__':
    main()
