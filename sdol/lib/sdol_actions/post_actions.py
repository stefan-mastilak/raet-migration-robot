# REF: stefan.mastilak@visma.com

import logging
import config as cfg
from sdol.lib.sdol_handlers.counters_handler import get_mig_folders
from os import path, rename
import time


def rename_dossier_dirs(customer_dir, mig_type):
    """
    Rename e-dossier migration folders like: {CustomerID}_{CompanyID}_S
    :param customer_dir: customer directory
    :param mig_type: migration type
    :return: renamed mig folders full-paths
    """
    mig_folders = get_mig_folders(customer_dir=customer_dir, mig_type=mig_type)
    renamed = []

    for dir_name in mig_folders:
        mig_folder = f'{cfg.MIG_ROOT}\\{customer_dir}\\{mig_type}\\{dir_name}'
        new_name = f'{cfg.MIG_ROOT}\\{customer_dir}\\{mig_type}\\{customer_dir}_{dir_name}_S'

        if path.isdir(mig_folder):
            retry = 5
            while retry:
                time.sleep(0.5)
                try:
                    rename(src=mig_folder, dst=new_name)
                    renamed.append(new_name)
                    retry = 0
                except Exception as err:
                    if retry:
                        retry = retry - 1
                    else:
                        logging.critical(msg=f' Renaming process failed for {mig_folder} >> {new_name}\n Error: {err}')
                        raise PermissionError(f' Renaming of {mig_folder} to {new_name} failed')
        else:
            raise NotADirectoryError(f' Directory {mig_folder} doesnt exist.')

    if len(mig_folders) == len(renamed):
        return renamed
    else:
        logging.critical(msg=f' Error while renaming migration folders - not all folders were renamed')
        raise AssertionError(f' Renaming process failed - not all folders were renamed')
