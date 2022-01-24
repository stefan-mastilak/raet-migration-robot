# REF: stefan.mastilak@visma.com

import logging
from os import path
import config as cfg
import subprocess


def call_mig_bat(customer_dir, job_type):
    """
    Method for calling the MigrationTool_Robot.bat script stored in MigVisma root folder.
    This is a batch script dedicated for the robot to run migration tasks.
    Script takes two parameters:
        - customer directory: which is also used as Migration ID
        - job_type: 1=Customer, 2=Servicetime, 3=Sickness, 4=MappingDossier, 6=SDOL, 7=PDOL
    :param customer_dir: customer directory
    :param job_type: job type
    :return: output, errors
    """
    batch_path = f'{cfg.MIG_ROOT}\\MigrationTool_Robot.bat'

    if path.isfile(batch_path):
        cmd = [batch_path, customer_dir, job_type]
        process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, cwd=cfg.PENTAHO_DIR)
        stdout, stderr = process.communicate()
        return stdout.decode('utf-8').splitlines(), stderr.decode('utf-8')
    else:
        logging.critical(msg=f' MigrationTool_Robot.bat no found in {cfg.MIG_ROOT}')
        raise FileNotFoundError(f' Path {batch_path} doesnt exist')


def execute_batch_job(customer_dir, job_id):
    """
    Run migration tool batch script - specify job type based on the purpose
    :param job_id: (pass it as a string) 1=Customer, 2=Servicetime, 3=Sickness, 4=MappingDossier, 6=SDOL, 7=PDOL
    :param customer_dir: customer directory
    :return: True if batch executed without errors
    """
    logging.info(msg=f' Starting the migration process')
    logging.info(msg=f' Executing the MigrationTool. MigID: {customer_dir} and JobID: {job_id}')
    pdol_result = call_mig_bat(customer_dir=customer_dir, job_type=job_id)
    out = pdol_result[0]
    err = pdol_result[1]

    if err:
        logging.critical(msg=f' Migration batch job failed with errors: {err}')
        return
    else:
        logging.info(msg=f' Migration job info:')
        for line in out:
            if '_MIGRATE_' in line and '-logfile:' not in line:
                logging.info(msg=f' {line}')
        logging.info(msg=f' Migration batch job finished')
        return True
