# REF: stefan.mastilak@visma.com

import logging
import config as cfg
import common.lib.checks.base_checks as base_checks
import common.lib.actions.base_actions as base_actions
import common.lib.handlers.zip_handler as zipper
import common.lib.handlers.batch_handler as batcher
import sdol.lib.sdol_checks.pre_checks as pre_checks
import sdol.lib.sdol_actions.pre_actions as pre_actions
import sdol.lib.sdol_checks.post_checks as post_checks
import sdol.lib.sdol_actions.post_actions as post_actions
import sdol.lib.sdol_handlers.cmd_handler as commander
from os import path, listdir


def get_unprocessed_dirs():
    """
    Function for getting unprocessed SDOL dirs.
    - Considering reserved directories (config.py - RESERVED_DIRS)
    - Unprocessed also means that directory name doesn't have '_processed_robot' or '_failed_robot' in it's name.
    :return: list of unprocessed SDOL directories
    """
    dirs = [i for i in listdir(cfg.MIG_ROOT) if path.isdir(path.join(cfg.MIG_ROOT, i))]
    # consider reserved directories:
    dirs = [i for i in dirs if i not in cfg.RESERVED_DIRS]
    # consider already processed directories:
    dirs = [i for i in dirs if '_robot' not in i]

    unprocessed = []
    for i in dirs:
        if pre_checks.mig_sdol_dir_check(i):
            unprocessed.append(i)

    return unprocessed


def run_migration(customer_dir, sftp_prod, mig_type='SDOL', job_id='6'):
    """
    Run SDOL migration for specific customer
    :param customer_dir: directory that contains all files that belongs to the specific customer
                   NOTE: This folder name also serves as migration ID.
    :param sftp_prod: True for uploading to the sftp 'robot_files' folder, False for uploading to 'robot_test_folder'
    :param mig_type: SDOL - default value for SDOL migration
    :param job_id: 6 - default value for SDOL job in migration tool batch
    """
    while True:

        # check if Pentaho is installed in the provided path:
        if not base_checks.pentaho_path_check():
            break

        # check is MigVisma folder exists:
        if not base_checks.mig_base_path_check():
            break

        # check if Kitchen.bat script exists in pentaho dir:
        if not base_checks.kitchen_bat_check():
            break

        # check if customer folder path exists:
        if not base_checks.mig_dir_check(customer_dir=customer_dir):
            break

        # check if customer folder is not in reserved list:
        if not base_checks.not_reserved_check(customer_dir=customer_dir):
            break

        # check if properties file exists in customer folder:
        if not base_checks.mig_props_check(customer_dir=customer_dir):
            break

        # check if parameters file exists in customer folder:
        if not base_checks.mig_params_check(customer_dir=customer_dir):
            break

        # check if PDOL folder exists in customer folder:
        if not pre_checks.mig_sdol_dir_check(customer_dir=customer_dir):
            break

        # read password for zipped files:
        pwd = base_actions.get_zip_pwd(customer_dir=customer_dir, mig_type=mig_type)

        # create DOCS folder if it doesn't already exist:
        docs_dir = base_actions.create_docs_dir(customer_dir=customer_dir, mig_type=mig_type)

        # create index folder if it doesn't already exist:
        idx_dir = base_actions.create_index_dir(customer_dir=customer_dir, mig_type=mig_type)

        # create migration log folder if it doesn't already exist:
        base_actions.create_log_dir(customer_dir=customer_dir)

        # unpack customer files:
        if not zipper.unpack_files(customer_dir=customer_dir, mig_type=mig_type, docs_dir=docs_dir, pwd=pwd):
            break

        # unzip files inside DOCS:
        pre_actions.unzip_docs_files(customer_dir=customer_dir, docs_dir=docs_dir)

        # rename and move index files to index folder:
        if not pre_actions.move_index_files(customer_dir=customer_dir, docs_dir=docs_dir, index_dir=idx_dir):
            break

        # checksum - index files count vs zip files count in DOCS:
        if not pre_checks.checksum_index_vs_zip(docs_dir=docs_dir, index_dir=idx_dir):
            break

        # run Migration:
        if not batcher.execute_batch_job(customer_dir=customer_dir, job_id=job_id):
            break

        # checksum counters vs cmd file rows:
        if not post_checks.chsum_counters_vs_cmd(customer_dir=customer_dir, mig_type=mig_type):
            break

        # execute cmd files:
        if not commander.execute_cmd_files(customer_dir=customer_dir, mig_type=mig_type):
            break

        # checksum counters vs dossier files:
        if not post_checks.chsum_counters_vs_dossiers(customer_dir=customer_dir, mig_type=mig_type):
            break

        # rename dossier directories:
        renamed_dirs = post_actions.rename_dossier_dirs(customer_dir=customer_dir, mig_type=mig_type)
        if not renamed_dirs:
            break

        # zip dossiers:
        zipped_files = base_actions.zip_dossier_dirs(dossier_dirs=renamed_dirs, pwd=pwd)
        if not zipped_files:
            break

        # Upload to SFTP:
        upload = base_actions.upload_dossiers(files=zipped_files, customer_dir=customer_dir, sftp_prod=sftp_prod)
        if not upload:
            break

        # PDOL migration succeeded:
        logging.info(msg=f' Migration for {customer_dir} finished successfully')
        return True
