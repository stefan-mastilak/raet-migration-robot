# REF: stefan.mastilak@visma.com

import config as cfg
import logging
import os
from mlm.mlm_migration import MlmMigration
from lib.base_migration import get_unprocessed_dirs
from lib.handlers.slack_handler import SlackLogger
from lib.handlers.lastpass_handler import get_lp_creds
from lib.handlers.kpi_handler import Kpi
from datetime import datetime


if __name__ == '__main__':

    # Enable/Disable features - for Testing purposes:
    slack_prod_logging = True  # True for '#ipa-mig-raet-reports', False for '#ipa-test-reports'
    sftp_prod = True           # True for uploading to 'robot_files' folder, False for 'robot_test_files' folder
    db_prod = True             # True for TRANSACTION_ITEMS KPI table, False for TEST_TRANSACTION_ITEMS KPI table

    # Create robot logfile:
    mlm_logfile = os.path.join(cfg.ROOT_DIR, cfg.MLM_LOGFILE)
    logging.basicConfig(filename=mlm_logfile,
                        filemode='w',
                        level=logging.INFO,
                        force=True)
    logging.getLogger("paramiko").setLevel(logging.WARNING)

    # Robot start:
    logging.info(msg=f' Starting MLM migration robot')
    start_time = datetime.now()
    logging.info(msg=f' Start: {start_time}')

    # Fetch Slack OAuth Token from LastPass:
    slack_item = cfg.LP_SLACK_PROD_CREDS if slack_prod_logging else cfg.LP_SLACK_TEST_CREDS
    slack_channel = cfg.SLACK_PROD_CHANNEL if slack_prod_logging else cfg.SLACK_TEST_CHANNEL

    # Slack credentials:
    slack_creds = get_lp_creds(folder=cfg.LP_FOLDER,
                               item=slack_item,
                               user=cfg.LP_USER_ACC)

    # Slack instance:
    slack = SlackLogger(lastpass_creds=slack_creds,
                        channel=slack_channel)

    # Get unprocessed dirs:
    mlm_folders = get_unprocessed_dirs(mig_type='MLM')

    # Proceed if any:
    if mlm_folders:
        logging.info(msg=f' Unprocessed MLM folders: ' + ', '.join(mlm_folders))
        processed = []
        failed = []

        # Processing:
        for mlm_folder in mlm_folders:
            mig_start_time = datetime.now()
            logging.info(msg=f' {cfg.DELIMITER}')
            logging.info(msg=f' Starting the migration for CustomerID: {mlm_folder}')
            current = MlmMigration(customer_dir=mlm_folder)
            try:
                # Run migration process for current folder
                migration = current.run_migration(sftp_prod=sftp_prod)

                if migration:
                    # CASE1: Process finished successfully
                    current.rename_if_success_migration()
                    processed.append(mlm_folder)
                    mig_end_time = datetime.now()

                    # Log success migration to AutoMate KPI:
                    logging.info(msg=f' Logging successful migration of {mlm_folder} customer to the KPI framework')
                    kpi = Kpi()
                    kpi.insert(start=mig_start_time,
                               end=mig_end_time,
                               inp='MLM',
                               out=f'CustomerID: {mlm_folder}',
                               status='DONE',
                               mark='SUCCESS',
                               db_prod=db_prod)

                else:
                    # CASE2: Process stopped due to expected error
                    current.rename_if_failed_migration()
                    failed.append(mlm_folder)
                    mig_end_time = datetime.now()

                    # Log failed migration due to business exception to AutoMate KPI:
                    logging.info(msg=f' Logging failed migration of {mlm_folder} customer to the KPI framework')
                    kpi = Kpi()
                    kpi.insert(start=mig_start_time,
                               end=mig_end_time,
                               inp='MLM',
                               out=f'CustomerID: {mlm_folder}',
                               status='DONE',
                               mark='BUSINESS EXCEPTION',
                               db_prod=db_prod)

            # CASE3: Process stopped due to unexpected error
            except Exception as error:
                current.rename_if_failed_migration()
                failed.append(mlm_folder)
                mig_end_time = datetime.now()
                logging.critical(f' Exception while processing {mlm_folder}: {error}')

                # Log failed migration due to application exception to AutoMate KPI:
                logging.info(msg=f' Logging failed migration of {mlm_folder} customer to the KPI framework')
                kpi = Kpi()
                kpi.insert(start=mig_start_time,
                           end=mig_end_time,
                           inp='MLM',
                           out=f'CustomerID: {mlm_folder}',
                           status='DONE',
                           mark='APPLICATION EXCEPTION',
                           db_prod=db_prod)
                continue

        # Robot end:
        end_time = datetime.now()
        duration = str(end_time - start_time).split(".")[0]
        logging.info(msg=f' {cfg.DELIMITER}')
        logging.info(msg=f' All MLM folders processed')

        # Log summary:
        logging.info(msg=(f' Summary:\n' +
                          f'\n Processed: {len(mlm_folders)} folders' if len(mlm_folders) > 1
                          else f'\n Processed: {len(mlm_folders)} folder') +
                         (f'\n Success: ' + f', '.join(processed) if processed
                          else f'\n Success: None') +
                         (f'\n Failed: ' + f', '.join(failed) if failed
                          else f'\n Failed: None') +
                         f'\n Start: {start_time}' +
                         f'\n End: {end_time}' +
                         f'\n Duration: {duration}\n')

        # Termination
        logging.info(msg=f' Terminating MLM migration robot')

        # Log to Slack:
        slack.upload_message(msg=f'*MLM migration:*\n' +
                                 (f'\n`Processed: {len(mlm_folders)} folders`' if len(mlm_folders) > 1
                                  else f'\n`Processed: {len(mlm_folders)} folder`') +
                                 (f'\n`Success: ' + f', '.join(processed) + '`' if processed
                                  else f'\n`Success: None`') +
                                 (f'\n`Failed: ' + f', '.join(failed) + '`' if failed
                                  else f'\n`Failed: None`') +
                                 f'\n`Duration: {duration}`')

        # Upload execution log file to the slack:
        slack.upload_file(filepath=mlm_logfile)

    else:
        # Robot end:
        logging.info(msg=f' No unprocessed customer files found')
        logging.info(msg=f' {cfg.DELIMITER}')
        logging.info(msg=f' Terminating MLM migration robot')

        # Log to Slack:
        slack.upload_message(msg=f'*MLM migration:*\n\n'
                                 f'`No unprocessed customer files found`\n'
                                 f'\n')