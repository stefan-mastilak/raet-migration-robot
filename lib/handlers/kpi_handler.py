# REF: stefan.mastilak@visma.com

from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime
import config as cfg
from lib.handlers.lastpass_handler import get_lp_creds
from os import path
import logging


def _get_secret():
    """
    Get GCP service account secret key from LastPass and save it locally to file.
    Use previously stored credentials if LastPass is unreachable
    :return: path to JSON file that is storing a GCP service account secret locally
    """
    sc_path = path.join(cfg.ROOT_DIR, cfg.GCP_SECRET_FILE)

    # get gcp service account secret key from LastPass and save it locally to file:
    try:
        creds = get_lp_creds(folder=cfg.LP_FOLDER, item=cfg.LP_GCP_SECRET, user=cfg.LP_USER_ACC)
        with open(sc_path, 'w') as f:
            f.write(creds.get('notes'))
        return sc_path

    # use previously stored credentials if LastPass is unreachable:
    except Exception as error:
        print(f' Lastpass Error: {error}')
        if path.exists(sc_path):
            return sc_path
        else:
            raise FileNotFoundError(f' GCP credentials file not found in {sc_path}')


class Kpi(object):
    """
    Class for Migration robot to communication with AutoMate KPI framework (Google BigQuery database).
    GCP Project: prod-amkpi-cm.
    BigQuery Database: transaction_items_kpi.
    """
    PROCESS = cfg.ID_PROCESS
    ROBOT = cfg.ID_ROBOT
    QUEUE = cfg.ID_QUEUE
    TABLE_TEST = cfg.TABLE_TEST
    TABLE_PROD = cfg.TABLE_PROD
    SCOPE = cfg.SCOPE
    CUSTOMER = cfg.ID_CUSTOMER

    def __init__(self):
        self.credentials = service_account.Credentials.from_service_account_file(filename=_get_secret(),
                                                                                 scopes=[Kpi.SCOPE])
        self.client = bigquery.Client(credentials=self.credentials,
                                      project=self.credentials.project_id)

    def insert(self, start, end, inp, out, status, mark, db_prod=True):
        """
        Insert into the TRANSACTION_ITEMS table
        :param start: transaction start date
        :param end: transaction end date
        :param inp: transaction input
        :param out: transaction output
        :param status: transaction status (NULL, IN_PROGRESS, DONE)
        :param mark: transaction mark (SUCCESS, BUSINESS EXCEPTION, APPLICATION EXCEPTION)
        :param db_prod: (True by default) if True - logging into production DB to TRANSACTION_ITEMS table.
                        TEST_TRANSACTION_ITEMS otherwise
        :return: result
        """
        table = Kpi.TABLE_PROD if db_prod else Kpi.TABLE_TEST
        query = (
            f'''
            INSERT INTO `{table}`
            (
                ID_PROCESS,
                ID_ROBOT,
                ID_QUEUE,
                ID_CUSTOMER,
                TIMESTAMP,
                START_DATE,
                END_DATE,
                INPUT,
                OUTPUT,
                TRANSACTION_STATUS,
                MARK
            )
            VALUES
            (
                "{Kpi.PROCESS}",
                "{Kpi.ROBOT}",
                {Kpi.QUEUE},
                "{Kpi.CUSTOMER}",
                DATETIME(CURRENT_TIMESTAMP),
                DATETIME(TIMESTAMP("{datetime.strftime(start, '%Y-%m-%d %H:%M:%S')}", "Europe/Bratislava")),
                DATETIME(TIMESTAMP("{datetime.strftime(end, '%Y-%m-%d %H:%M:%S')}", "Europe/Bratislava")),
                "{inp}",
                "{out}",
                "{status}",
                "{mark}"
            )
            '''
        )

        result = self.client.query(query)
        result.result()
        if result.errors:
            logging.critical(msg=f' KPI framework error: BiqQuery Exception: {result.errors}')
            raise Exception(f' KPI framework error: BiqQuery Exception: {result.errors}')
        logging.info(msg=f' New entry added to KPI {table.split(".")[-1] if "." in table else table} table')
        return result
