# REF: stefan.mastilak@visma.com

import csv
import config as cfg
import glob
import logging
from os import path, walk


def get_counters(customer_dir, mig_type):
    """
    Read Counters.csv file created by MigrationTool - get only Migrated counts.
    :param customer_dir: customer directory
    :param mig_type: migration type
    """
    counters_path = f'{cfg.MIG_ROOT}\\{customer_dir}\\{mig_type}\\Counters.csv'

    if path.isfile(counters_path):
        counters = {}
        with open(counters_path, mode='r') as csv_file:
            csv_reader = csv.reader(csv_file, delimiter=';')
            for row in csv_reader:
                if len(row) == 2:
                    # get only migrated:
                    if '_Migrated' in row[0]:
                        counters[row[0]] = row[1]
                else:
                    logging.critical(msg=f' Counters.csv was not created properly')
                    raise ValueError(f' Counters.csv was not created properly')
        return counters
    else:
        logging.critical(msg=f' Counters.csv not found')
        raise FileNotFoundError(f' Counters.csv not found')


def get_migrated_count(customer_dir, mig_type):
    """
    Get sum count of '_Migrated' documents for each unique customerID from Counters.csv file
    :param customer_dir: customer directory
    :param mig_type: migration type
    :return: counts dict (unique_id: count_of_items_to_be_migrated)
    """
    counters = get_counters(customer_dir=customer_dir, mig_type=mig_type)
    unique_ids = []

    if counters:
        # get unique IDs:
        for key, val in counters.items():
            current = key.split('_')[:2]
            if current[1] not in unique_ids:
                unique_ids.append(current[1])

        # get filtered counts:
        counts = {uid: 0 for uid in unique_ids}
        for uid in unique_ids:
            for key, val in counters.items():
                if f'_{uid}_' in key:
                    counts[uid] += int(val)
        return counts
    else:
        logging.critical(msg=f' No Migrated entries found in Counters.csv file')
        raise ValueError(f' No Migrated entries found in Counters.csv file')


def get_mig_folders(customer_dir, mig_type):
    """
    Get Migration target folder names
    :param customer_dir: customer directory
    :param mig_type: migration type
    :return: folder names
    """
    counters = get_migrated_count(customer_dir=customer_dir, mig_type=mig_type)
    folders = [i for i in counters.keys()]
    return folders


def get_cmd_rows_count(customer_dir, mig_type):
    """
    Read all .cmd files and get count of all 'move' rows inside cmd files.
    Returns dictionary with ID: count of cmd rows
    :param customer_dir: customer directory
    :param mig_type: migration type
    :return: counts dict (unique_id: count_of_move_cmd_rows)
    """
    cmd_files = glob.glob(f'{cfg.MIG_ROOT}\\{customer_dir}\\{mig_type}\\*.cmd')
    counts = {}

    for file in cmd_files:
        curr_cmd_count = 0
        with open(file) as cmd_file:
            rows = cmd_file.readlines()
            for row in rows:
                if 'move' in row:
                    curr_cmd_count += 1
        root_path, file_name = path.split(file)
        counts[file_name.split("_")[2]] = curr_cmd_count

    return counts


def get_dossiers_count(customer_dir, mig_type):
    """
    Get number of files in Dossiers folders.
    :param customer_dir: customer directory
    :param mig_type: migration type
    :return: dossiers count
    :rtype: dict
    """
    counters_count = get_migrated_count(customer_dir=customer_dir, mig_type=mig_type)
    dossiers_count = {}

    # get dossiers count:
    for key in counters_count.keys():
        if key not in dossiers_count.keys():
            doss_dir = f'{cfg.MIG_ROOT}\\{customer_dir}\\{mig_type}\\{key}'
            dossiers_count[key] = 0
            if path.isdir(doss_dir):
                for root, dirs, files in walk(doss_dir):
                    for file in files:
                        dossiers_count[key] += 1

    return dossiers_count
