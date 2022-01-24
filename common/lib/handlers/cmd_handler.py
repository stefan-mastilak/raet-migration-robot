# REF: stefan.mastilak@visma.com

from os import path
import logging


def add_cmd_encoding(cmd_file_path):
    """
    Add "chcp 65001" to the very top of the .cmd file. This will tell windows to use UTF8 encoding for cmd execution.
    NOTE: This action will add one extra line to the .cmd file - This needs to be considered in the checksum!
    :param cmd_file_path: cmd file path
    :return:
    """
    if path.isfile(cmd_file_path):
        with open(cmd_file_path, 'r') as contents:
            save = contents.read()

        with open(cmd_file_path, 'w') as contents:
            # att UTF8 encoding at the top of cmd file:
            contents.write('chcp 65001\n')
            contents.write(save)

        logging.info(msg=f' Cmd file encoding changed to UTF8. NOTE: One row added to cmd file.')
    else:
        logging.critical(msg=f' cmd file not found in {cmd_file_path}')
        raise FileNotFoundError(f' File not found in: {cmd_file_path}')
