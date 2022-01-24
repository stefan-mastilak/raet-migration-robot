# REF: stefan.mastilak@visma.com

import logging
import config as cfg
from os import path, listdir
from pdol.lib.pdol_handlers.cmd_handler import get_cmd_rows_count


def mig_pdol_dir_check(customer_dir):
    """
    Check if PDOL folder exists in customer folder.
    :param customer_dir: customer directory name
    :return: True if PDOL folder exists
    """
    pdol_dir = f'{cfg.MIG_ROOT}\\{customer_dir}\\PDOL'

    if path.exists(pdol_dir):
        return True
    else:
        return False


def get_docs_files_count(customer_dir):
    """
    Get count of files in DOCS folder inside customer folder.
    :param customer_dir: customer directory name
    :return: files count of DOCS folder
    """
    docs_folder_path = f'{cfg.MIG_ROOT}\\{customer_dir}\\PDOL\\DOCS'

    if path.exists(docs_folder_path):
        count = 0
        for _path in listdir(docs_folder_path):
            if path.isfile(path.join(docs_folder_path, _path)):
                count += 1
        return count
    else:
        logging.critical(msg=f' DOCS path doesnt exists in {docs_folder_path}')
        raise NotADirectoryError(f' Path doesnt exist: {docs_folder_path}')


def checksum_cmd_vs_docs(customer_dir):
    """
    Check if number or rows in .cmd file divided by two is te same as number of file in DOCS folder
    :param customer_dir: customer directory name
    :return: True if count matches
    """
    docs_count = get_docs_files_count(customer_dir=customer_dir)
    cmd_count = get_cmd_rows_count(customer_dir=customer_dir)

    if docs_count and cmd_count:
        # do comparison:
        if docs_count == (cmd_count/2):
            logging.info(msg=f' Checksum passed. Docs files count: {docs_count}. Cmd count: {cmd_count/2}')
            return True
        else:
            logging.critical(msg=f' Checksum failed. Docs files count: {docs_count}. Cmd count: {cmd_count/2}')
            return False
    else:
        logging.critical(msg=f' Zero rows count error. Docs files count: {docs_count}. Cmd count: {cmd_count/2}')
        raise AssertionError(f' Zero values found. Docs count: {docs_count}, Cmd count: {cmd_count}')
