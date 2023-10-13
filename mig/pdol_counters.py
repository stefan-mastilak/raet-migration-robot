# REF: stefan.mastilak@visma.com

import csv
import config as cfg
import logging
import os
from lib.base_migration import Migration


class PdolCounters(Migration):
    """
    Counters handling for SDOL migration type.
    """

    def __get_counters(self):
        """
        Read counters.csv file contend from customers PDOL folder.
        :return: counters for PDOL
        :rtype: dict
        """
        counters_path = os.path.join(cfg.MIG_ROOT, self.customer_dir, self.mig_type, 'Counters.csv')

        if os.path.isfile(counters_path):
            counters = {}
            with open(counters_path, mode='r') as csv_file:
                csv_reader = csv.reader(csv_file, delimiter=';')
                for row in csv_reader:
                    if len(row) == 2:
                        counters[row[0]] = row[1]
                    else:
                        logging.critical(msg=f' Counters.csv was not created properly. Missing keys or values')
                        raise ValueError(f' Counters.csv was not created properly. Missing keys or values')
            if counters:
                return counters
            else:
                logging.critical(msg=f' No data found in counters.csv file')
                raise ValueError(f' No data found in counters.csv file')
        else:
            logging.critical(msg=f' Counters.csv file not found in {self.mig_type} folder')
            raise FileNotFoundError(f' Counters.csv file not found in {self.mig_type} folder')

    def get_mig_folders(self):
        """
        Get Migration target folder names (only folder name, not full path).
        :return: migration folder names
        :rtype: list
        """
        counters = self.__get_counters()
        mig_folders = [key.replace('PDOL_Migrated_', '') for key in counters.keys() if 'PDOL_Migrated_' in key]
        if mig_folders:
            return mig_folders
        else:
            logging.critical(msg=f' No PDOL_Migrated entries found in counters.csv file')
            raise AssertionError(f' No PDOL_Migrated entries found in counters.csv file')

    def get_dossiers_count(self):
        """
        Get count of all e-dossier files inside customer's PDOL folder.
        :return: total count of all e-dossier files inside PDOL folder
        :rtype: int
        """
        mig_folders = self.get_mig_folders()

        total_count = 0
        for dir_name in mig_folders:
            mig_folder = os.path.join(cfg.MIG_ROOT, self.customer_dir, self.mig_type, dir_name)
            if os.path.isdir(mig_folder):
                for root, dirs, files in os.walk(mig_folder):
                    for file in files:
                        total_count += 1
            else:
                logging.critical(msg=f" Migration folder {mig_folder} doesn't exist")
                raise NotADirectoryError(f" Migration folder {mig_folder} doesn't exist")
        if total_count:
            return total_count
        else:
            logging.critical(msg=f' No e-dossier files found')
            raise FileNotFoundError(f' No e-dossier files found')
