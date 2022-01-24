# REF: stefan.mastilak@visma.com

import config as cfg
import glob
import subprocess
import logging
from os import path
from common.lib.handlers.cmd_handler import add_cmd_encoding


def get_cmd_files(customer_dir, mig_type):
    """
    Get all .cmd files (their paths) inside customer folder.
    :param customer_dir: customer directory
    :param mig_type: migration type
    :return: cmd file paths
    """
    return glob.glob(f'{cfg.MIG_ROOT}\\{customer_dir}\\{mig_type}\\*.cmd')


def execute_cmd_files(customer_dir, mig_type):
    """
    Execute all .cmd files inside customer folder.
    :param customer_dir: customer directory
    :param mig_type: migration type
    :return: True if all cmd executed successfully
    :rtype: bool
    """
    cmd_files = get_cmd_files(customer_dir=customer_dir, mig_type=mig_type)

    for cmd_file_path in cmd_files:
        # split path:
        root_path, cmd_file_name = path.split(cmd_file_path)

        if path.isfile(cmd_file_path):
            # Add encoding (UTF8):
            add_cmd_encoding(cmd_file_path=cmd_file_path)
            # Execute cmd file:
            logging.info(msg=f' Executing cmd file {cmd_file_name}')
            process = subprocess.Popen(cmd_file_path, stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding='utf-8')
            stdout, stderr = process.communicate()
            if stderr:
                logging.critical(msg=f' Cmd file {cmd_file_name} execution failed with errors: {stderr}')
                return
            else:
                logging.info(msg=f' Cmd file {cmd_file_name} executed successfully')
                continue
        else:
            logging.critical(msg=f' Cmd file {cmd_file_name} not found in {cmd_file_path}')
            raise FileNotFoundError(f' {cmd_file_name} file not found in {cmd_file_path}')

    return True
