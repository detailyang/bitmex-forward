# Requires
* Python >= 3.5

# How to install
Install requirements as the following:

```bash
git clone https://github.com/detailyang/bitmex-forward.git
cd bitmex-forward
virtualenv env
source env/bin/active
pip install -r requirements.txt
```

# How to use
Pass the options to app.py as the following:
```bash
python app/app.py --symbol XBTUSD --symbol LTCU18 --symbol ETHXBT --apikey xxxx --apisecret yyyy --discordwebhook https://discordapp.com/api/webhooks/yyy
--endpoint https://www.bitmex.com/api/v1
```

PS: the bitmex apikey should be readonly