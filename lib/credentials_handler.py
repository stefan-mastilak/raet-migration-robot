# REF: stefan.mastilak@visa.com

import config as cfg
import json
import logging
from os import path


def get_credentials(item):
    """
    Get credentials stored in the credentials.json file locally on the runtime server (LastPass not used anymore)
    """
    if path.isfile(path.join(cfg.ROOT_DIR, cfg.CREDENTIALS)):
        with open(path.join(cfg.ROOT_DIR, cfg.CREDENTIALS), encoding='utf-8') as creds_file:
            return json.load(creds_file).get(item)
    else:
        logging.critical(msg=f' Credentials file doesnt exist in path: {path.join(cfg.ROOT_DIR, cfg.CREDENTIALS)}!')
