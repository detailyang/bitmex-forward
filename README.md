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
cp default.json config.json
vim config.json
python app/app.py  config.json
```

PS: the bitmex apikey should be readonly