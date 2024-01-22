# REF: stefan.mastilak@visma.com

import config as cfg
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime
from lib.credentials_handler import get_credentials
import logging


def _get_secret():
    """
    Get GCP service account secret key credentials.json file
    :return: path to JSON file that is storing a GCP service account secret locally
    """
    return get_credentials(item=cfg.GCP_SECRET).get('notes')


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
    HOST = cfg.RUNTIME_HOSTNAME
    MONITORING_TABLE = cfg.TABLE_MONITORING

    def __init__(self):
        self.credentials = service_account.Credentials.from_service_account_info(_get_secret(), scopes=[Kpi.SCOPE])
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

    def __get_last_health(self):
        """
        Helper for getting last health and status from monitoring table.
        :return: Health
        """
        last_health = 0
        query = (
            f'''
            SELECT STATUS, HEALTH
            FROM `{Kpi.MONITORING_TABLE}`
            WHERE ID_PROCESS = "{Kpi.PROCESS}" 
            ORDER BY JOB_FINISHED DESC LIMIT 9
            '''
        )
        bq_result = self.client.query(query)
        for row in bq_result.result():
            if row.get("STATUS") in ('green', "GREEN"):
                last_health += 1
        return last_health

    def insert_to_monitoring(self, start, status):
        """
        Insert into the monitoring table
        :param start: transaction start date
        :param status: transaction status
        """
        query = (
            f'''
                INSERT INTO `{Kpi.MONITORING_TABLE}`
                (
                    ID_PROCESS,
                    HOST,
                    STATUS,
                    JOB_DURATION,
                    JOB_START,
                    JOB_FINISHED,
                    HEALTH
                )
                VALUES
                (
                   "{Kpi.PROCESS}",
                   "{Kpi.HOST}",
                   "{status}",
                    {(datetime.now() - start).seconds},
                    "{start}",
                    "{datetime.now()}",
                    {self.__get_last_health() +1 if status.lower() == 'green' else self.__get_last_health() +0}
                )
                ''')

        result = self.client.query(query)
        result.result()
        if result.errors:
            logging.critical(msg=f' Monitoring error: BiqQuery Exception: {result.errors}')
            raise Exception(f' Monitoring error: BiqQuery Exception: {result.errors}')
        logging.info(
            msg=f' New entry added to monitoring table {Kpi.MONITORING_TABLE.split(".")[-1] if "." in Kpi.MONITORING_TABLE else Kpi.MONITORING_TABLE} table')
        logging.info(msg=f' Monitoring table updated with new status: {status}')
        return result
