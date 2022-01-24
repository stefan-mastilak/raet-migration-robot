# REF: stefan.mastilak@visma.com

from os import path
import config as cfg
import subprocess
import logging
from common.lib.handlers.cmd_handler import add_cmd_encoding


def execute_cmd_file(customer_dir):
    """
    Call the .cmd file after migration tool job creates this file.
    :param customer_dir: customer directory name
    :return: output, errors
    """
    cmd_file_path = f'{cfg.MIG_ROOT}\\{customer_dir}\\PDOL\\{customer_dir}_Move_PDOL_files.cmd'

    if path.isfile(cmd_file_path):
        # Add encoding (UTF8):
        add_cmd_encoding(cmd_file_path=cmd_file_path)
        # Execute cmd file:
        logging.info(msg=f' Executing cmd file..')
        process = subprocess.Popen(cmd_file_path, stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding='utf-8')
        stdout, stderr = process.communicate()
        if stderr:
            logging.critical(msg=f' Cmd file execution failed with errors: {stderr}')
            return
        else:
            logging.info(msg=f' Cmd file executed successfully')
            return True
    else:
        logging.critical(msg=f' Cmd file not found in {cmd_file_path}')
        raise FileNotFoundError(f' File not found in {cmd_file_path}')


def get_cmd_rows_count(customer_dir):
    """
    Read .cmd file and get its rows count
    :param customer_dir: customer directory name
    :return: rows count
    """
    cmd_file_path = f'{cfg.MIG_ROOT}\\{customer_dir}\\PDOL\\{customer_dir}_Move_PDOL_files.cmd'

    if path.isfile(cmd_file_path):
        # get rows count:
        with open(cmd_file_path) as cmd_file:
            rows = cmd_file.readlines()
        return len(rows)
    else:
        logging.critical(msg=f' Cmd file not found in {cmd_file_path}')
        raise FileNotFoundError(f' File not found in {cmd_file_path}')
