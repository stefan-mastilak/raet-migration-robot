# REF: stefan.mastilak@visma.com

from common.lib.handlers.lastpass_handler import get_lp_creds
from common.lib.handlers.slack_handler import SlackLogger
import config as cfg
import common.lib.actions.base_actions as actions
import logging
import sdol.lib.sdol_migration as sdol


if __name__ == "__main__":

    # Enable/Disable features - for Testing purposes:
    slack_logging = True    # True for reports to 'ipa-mig-raet-reports' channel on Slack, False otherwise
    sftp_prod = False       # True for uploading to 'robot_files' folder, False for uploading to 'robot_test_files'

    # Robot logfile:
    logging.basicConfig(filename=f'{cfg.ROOT_DIR}\\{cfg.SDOL_LOGFILE}',
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
    logging.info(msg=f' Starting SDOL migration robot')

    # Get unprocessed dirs:
    sdol_folders = sdol.get_unprocessed_dirs()

    # Proceed if any:
    if sdol_folders:
        logging.info(msg=f' Unprocessed SDOL folders: ' + ', '.join(sdol_folders))
        processed = []
        failed = []

        # Processing:
        for folder in sdol_folders:
            logging.info(msg=f' {cfg.DELIMITER}')
            logging.info(msg=f' Starting the migration for customer {folder}')
            try:
                # Run migration process for current folder
                current = sdol.run_migration(customer_dir=folder,
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
        logging.info(msg=f' All SDOL folders processed')
        logging.info(msg=f' Terminating SDOL migration robot')

        # Log to Slack:
        if slack_logging:
            # Log execution status to slack
            slack.upload_message(msg=f'*SDOL migration:*\n'
                                     f'\nProcessed: {len(sdol_folders)}'
                                     f'\nSuccess: ' + f', '.join(processed) +
                                     f'\nFailed: ' + f', '.join(failed) +
                                     f'\n')

            # Upload execution log file to the slack:
            slack.upload_file(filepath=f'{cfg.ROOT_DIR}\\{cfg.SDOL_LOGFILE}')

    else:
        # Robot end:
        logging.info(msg=f' No unprocessed customer files found')
        logging.info(msg=f' {cfg.DELIMITER}')
        logging.info(msg=f' Terminating SDOL migration robot')

        # Log to Slack - no SDOL folders to be migrated:
        if slack_logging:
            slack.upload_message(msg=f'*SDOL migration:*\n\n'
                                     f'No unprocessed customer files found\n'
                                     f'\n')

