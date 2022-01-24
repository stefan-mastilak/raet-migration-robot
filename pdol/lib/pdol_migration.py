# REF: stefan.mastilak@visma.com

import logging
import config as cfg
import common.lib.handlers.batch_handler as batcher
import common.lib.checks.base_checks as base_checks
import common.lib.actions.base_actions as base_actions
import common.lib.handlers.zip_handler as zipper
import pdol.lib.pdol_actions.pre_actions as pre_actions
import pdol.lib.pdol_actions.post_actions as post_actions
import pdol.lib.pdol_checks.pre_checks as pre_checks
import pdol.lib.pdol_checks.post_checks as post_checks
import pdol.lib.pdol_handlers.cmd_handler as commander
from os import path, listdir


def get_unprocessed_dirs():
    """
    Function for getting unprocessed PDOL dirs.
    - Considering reserved directories (config.py - RESERVED_DIRS)
    - Unprocessed also means that directory name doesn't have '_processed_robot' or '_failed_robot' in it's name.
    :return: list of unprocessed PDOL directories
    """
    dirs = [i for i in listdir(cfg.MIG_ROOT) if path.isdir(path.join(cfg.MIG_ROOT, i))]
    # consider reserved directories:
    dirs = [i for i in dirs if i not in cfg.RESERVED_DIRS]
    # consider already processed directories:
    dirs = [i for i in dirs if '_robot' not in i]

    unprocessed = []
    for i in dirs:
        if pre_checks.mig_pdol_dir_check(i):
            unprocessed.append(i)

    return unprocessed


def run_migration(customer_dir, sftp_prod, mig_type='PDOL', job_id='7'):
    """
    Run PDOL migration for specific customer.
    :param customer_dir: directory that contains all files that belongs to the specific customer
                   NOTE: This folder name also serves as migration ID.
    :param sftp_prod: True for uploading to the sftp 'robot_files' folder, False for uploading to 'robot_test_folder'
    :param mig_type: PDOL - default value for PDOL migration
    :param job_id: 7 - default value for PDOL job in migration tool batch
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
        if not pre_checks.mig_pdol_dir_check(customer_dir=customer_dir):
            break

        # read password for zipped files:
        pwd = base_actions.get_zip_pwd(customer_dir=customer_dir, mig_type=mig_type)

        # create DOCS folder if it doesn't exist:
        docs_dir = base_actions.create_docs_dir(customer_dir=customer_dir, mig_type=mig_type)

        # create migration log folder if doesn't exist:
        base_actions.create_log_dir(customer_dir=customer_dir)

        # unpack customer files:
        if not zipper.unpack_files(customer_dir=customer_dir, mig_type=mig_type, docs_dir=docs_dir, pwd=pwd):
            break

        # move index.xml to the PDOL folder:
        pre_actions.move_index_file(customer_dir=customer_dir, mig_type=mig_type)

        # execute PDOL migration job:
        if not batcher.execute_batch_job(customer_dir=customer_dir, job_id=job_id):
            break

        # pre-checksum:
        if pre_checks.checksum_cmd_vs_docs(customer_dir=customer_dir):
            # Execute cmd file:
            if not commander.execute_cmd_file(customer_dir=customer_dir):
                break
        else:
            break

        # post-checksum:
        if not post_checks.checksum_cmd_vs_dossiers(customer_dir=customer_dir):
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
