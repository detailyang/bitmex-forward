import logging


logger = logging.getLogger("bitmex-fordwader")
logger.setLevel(logging.INFO)
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
formatter = logging.Formatter("[%(asctime)s %(levelname)s %(filename)s@%(lineno)d]:  %(message)s")
ch.setFormatter(formatter)
logger.addHandler(ch)
