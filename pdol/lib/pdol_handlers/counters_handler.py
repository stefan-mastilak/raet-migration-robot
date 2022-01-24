# REF: stefan.mastilak@visma.com

import csv
import config as cfg
import logging
from os import path, walk


def get_counters(customer_dir):
    """
    Read counters.csv file contend from customers PDOL folder
    :param customer_dir: customer directory
    :return: counters as dictionary
    """
    counters_path = f'{cfg.MIG_ROOT}\\{customer_dir}\\PDOL\\Counters.csv'

    if path.isfile(counters_path):
        counters = {}
        with open(counters_path, mode='r') as csv_file:
            csv_reader = csv.reader(csv_file, delimiter=';')
            for row in csv_reader:
                if len(row) == 2:
                    counters[row[0]] = row[1]
                else:
                    logging.critical(msg=f' Counters.csv was not created properly - missing keys or values.')
                    raise ValueError
        return counters
    else:
        logging.critical(msg=f' Counters.csv not found in {counters_path}')
        return


def get_mig_folders(customer_dir):
    """
    Get Migration target folder names (only folder name, not full path).
    :param customer_dir: customer directory
    :return: migration folder names
    """
    counters = get_counters(customer_dir)
    mig_folders = [key.replace('PDOL_Migrated_', '') for key in counters.keys() if 'PDOL_Migrated_' in key]
    return mig_folders


def get_dossiers_count(customer_dir):
    """
    Get count of all e-dossier files inside customer's PDOL folder.
    :param customer_dir: customer directory
    :return: total count of all e-dossier files inside PDOL
    """
    mig_folders = get_mig_folders(customer_dir)

    total_count = 0
    for dir_name in mig_folders:
        mig_folder = f'{cfg.MIG_ROOT}\\{customer_dir}\\PDOL\\{dir_name}'
        if path.isdir(mig_folder):
            for root, dirs, files in walk(mig_folder):
                for file in files:
                    total_count += 1
    return total_count
