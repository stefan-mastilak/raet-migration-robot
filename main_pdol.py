# REF: stefan.mastilak@visma.com

import config as cfg
import logging
import pdol.lib.pdol_migration as pdol
import common.lib.actions.base_actions as actions
from common.lib.handlers.slack_handler import SlackLogger
from common.lib.handlers.lastpass_handler import get_lp_creds


if __name__ == '__main__':

    # Enable/Disable features - for Testing purposes:
    slack_logging = True  # True for reports to 'ipa-mig-raet-reports' channel on Slack, False otherwise
    sftp_prod = False  # True for uploading to 'robot_files' folder, False for uploading to 'robot_test_files'

    # Robot logfile:
    logging.basicConfig(filename=f'{cfg.ROOT_DIR}\\{cfg.PDOL_LOGFILE}',
                        filemode='w',
                        level=logging.INFO)

    # Slack credentials:
    lp_creds = get_lp_creds(folder=cfg.LP_FOLDER,
                            item=cfg.LP_SLACK_CREDS,
                            user=cfg.LP_USER_ACC)

    # Slack instance:
    slack = SlackLogger(lastpass_creds=lp_creds,
                        channel=cfg.CHANNEL)

    # Robot start:
    logging.info(msg=f' Starting PDOL migration robot')

    # Get unprocessed dirs:
    pdol_folders = pdol.get_unprocessed_dirs()

    # Proceed if any:
    if pdol_folders:
        logging.info(msg=f' Unprocessed PDOL folders: ' + ', '.join(pdol_folders))
        processed = []
        failed = []

        # Processing:
        for folder in pdol_folders:
            logging.info(msg=f' {cfg.DELIMITER}')
            logging.info(msg=f' Starting the migration for customer {folder}')
            try:
                # Run migration process for current folder
                current = pdol.run_migration(customer_dir=folder,
                                             sftp_prod=sftp_prod)
                if current:
                    # CASE1: Process finished successfully
                    actions.rename_if_success(customer_dir=folder)
                    processed.append(folder)
                else:
                    # CASE2: Process stopped due to error
                    actions.rename_if_failed(customer_dir=folder)
                    failed.append(folder)
            # CASE3: Process failed on exception
            except Exception as error:
                actions.rename_if_failed(customer_dir=folder)
                failed.append(folder)
                print(f' Exception while processing {folder}: {error}')
                continue

        # Robot end:
        logging.info(msg=f' {cfg.DELIMITER}')
        logging.info(msg=f' All PDOL folders processed')
        logging.info(msg=f' Terminating PDOL migration robot')

        # Log to Slack:
        if slack_logging:
            # Log execution status to slack
            slack.upload_message(msg=f'*PDOL migration:*\n'
                                     f'\nProcessed: {len(pdol_folders)}'
                                     f'\nSuccess: ' + f', '.join(processed) +
                                     f'\nFailed: ' + f', '.join(failed) +
                                     f'\n')

            # Upload execution log file to the slack:
            slack.upload_file(filepath=f'{cfg.ROOT_DIR}\\{cfg.PDOL_LOGFILE}')

    else:
        # Robot end:
        logging.info(msg=f' No unprocessed customer files found')
        logging.info(msg=f' {cfg.DELIMITER}')
        logging.info(msg=f' Terminating PDOL migration robot')

        # Log to Slack - No PDOL folders to be migrated:
        if slack_logging:
            slack.upload_message(msg=f'*PDOL migration:*\n\n'
                                     f'No unprocessed customer files found\n'
                                     f'\n')
