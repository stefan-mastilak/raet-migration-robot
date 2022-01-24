# REF: stefan.mastilak@visma.com

import config as cfg
import glob
import logging
from os import path, listdir


def mig_sdol_dir_check(customer_dir):
    """
    Check if SDOL folder exists in customer folder.
    :param customer_dir: customer directory name
    :return: True if SDOL folder exists
    """
    pdol_dir = f'{cfg.MIG_ROOT}\\{customer_dir}\\SDOL'

    if path.exists(pdol_dir):
        return True
    else:
        return False


def get_index_files_count(index_dir):
    """
    Get number of files inside index folder.
    :param index_dir: index directory
    :return: number of index files
    :rtype: int
    """
    return len([i for i in listdir(index_dir) if path.isfile(path.join(index_dir, i))])


def get_zip_files_count(docs_dir):
    """
    Get zip files count inside DOCS folder.
    :param docs_dir: DOCS directory
    :return: number of zip files
    :rtype: int
    """
    return len(glob.glob(f"{docs_dir}\\*.zip"))


def get_unzipped_dir_count(docs_dir):
    """
    Get unzipped folders count inside DOCS folder
    :param docs_dir: DOCS directory
    :return:
    """
    return len([i for i in glob.glob(f'{docs_dir}\\*') if '.zip' not in i])


def checksum_index_vs_zip(docs_dir, index_dir):
    """
    Compare number of index files vs number of zip files inside DOCS. Count should match.
    :param docs_dir: DOCS directory
    :param index_dir: Index directory
    :return: checksum result
    :rtype: bool
    """
    idx_count = get_index_files_count(index_dir)
    zip_count = get_zip_files_count(docs_dir)

    if idx_count:
        if zip_count:
            if idx_count == zip_count:
                logging.info(msg=f' Checksum passed: index files count: {idx_count}, zip files count: {zip_count}')
                return True
            else:
                logging.critical(msg=f' Checksum failed: index files count: {idx_count}, zip files count: {zip_count}')
                return False
        else:
            logging.critical(msg=f' Zero count of zip files')
            raise ValueError(f' Zero count of zip files')
    else:
        logging.critical(msg=f' Zero count of index files')
        raise ValueError(f' Zero count of index files')
