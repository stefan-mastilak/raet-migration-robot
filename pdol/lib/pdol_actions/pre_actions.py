# REF: stefan.mastilak@visma.com

import config as cfg
import logging
import shutil
from os import path


def move_index_file(customer_dir, mig_type):
    """
    Move index.xml file from customer's DOCS folder to PDOL folder
    :param customer_dir: customer directory name
    :param mig_type: migration type
    :return: index file path
    """
    index_src_path = f'{cfg.MIG_ROOT}\\{customer_dir}\\{mig_type}\\DOCS\\index.xml'
    index_dst_path = f'{cfg.MIG_ROOT}\\{customer_dir}\\{mig_type}\\index.xml'

    if path.isfile(index_src_path):
        logging.info(msg=f' Index file found in {index_src_path}')
        # move to PDOL folder:
        shutil.move(src=index_src_path, dst=index_dst_path)
        logging.info(msg=f' Index.xml file moved to {index_dst_path}')
        return index_dst_path
    else:
        logging.critical(msg=f' Index file not found in {index_src_path}')
        raise FileNotFoundError(f' Index file not found in {index_src_path}')
