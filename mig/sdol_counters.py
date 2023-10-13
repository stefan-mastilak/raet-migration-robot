# REF: stefan.mastilak@visma.com

import csv
import config as cfg
import glob
import logging
import os
from lib.base_migration import Migration


class SdolCounters(Migration):
    """
    Counters handling for SDOL migration type.
    """

    def __get_counters(self):
        """
        Read Counters.csv file created by MigrationTool and get only Migrated counts.
        :return: counters of '_Migrated' entries
        :rtype: dict
        """
        counters_path = os.path.join(cfg.MIG_ROOT, self.customer_dir, self.mig_type, 'Counters.csv')

        if os.path.isfile(counters_path):
            counters = {}
            with open(counters_path, mode='r') as csv_file:
                csv_reader = csv.reader(csv_file, delimiter=';')
                for row in csv_reader:
                    if len(row) == 2:
                        # get only migrated:
                        if '_Migrated' in row[0]:
                            counters[row[0]] = row[1]
                    else:
                        logging.critical(msg=f' Counters.csv file was not created properly. Missing keys or values')
                        raise ValueError(f' Counters.csv file was not created properly. Missing keys or values')
            return counters
        else:
            logging.critical(msg=f' Counters.csv file not found in {self.mig_type} folder')
            raise FileNotFoundError(f' Counters.csv file not found in {self.mig_type} folder')

    def get_migrated_count(self):
        """
        Get sum count of '_Migrated' documents for each unique customerID from Counters.csv file.
        :return: counts dict (unique_id: count_of_items_to_be_migrated)
        :rtype: dict
        """
        counters = self.__get_counters()
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

    def get_mig_dirs_from_counters(self):
        """
        Get Migration target folder names from counters.csv file.
        :return: folder names
        :rtype: list
        """
        counters = self.get_migrated_count()
        return [i for i in counters.keys()]

    def get_cmd_rows_count(self):
        """
        Read all cmd files and get count of all 'move' rows inside cmd files.
        Returns dictionary containing {CustomerID: cmd_rows_count}
        :return: counts dict (unique_id: count_of_move_cmd_rows)
        :rtype: dict
        """
        cmd_files = glob.glob(os.path.join(cfg.MIG_ROOT, self.customer_dir, self.mig_type, '*.cmd'))
        counts = {}

        for file in cmd_files:
            curr_cmd_count = 0
            with open(file) as cmd_file:
                rows = cmd_file.readlines()
                for row in rows:
                    if 'move' in row:
                        curr_cmd_count += 1
            root_path, file_name = os.path.split(file)
            counts[file_name.split("_")[2]] = curr_cmd_count
        return counts

    def get_dossiers_count(self):
        """
        Get number of files in Dossiers folders.
        :return: dossiers count
        :rtype: dict
        """
        counters_count = self.get_migrated_count()
        dossiers_count = {}

        # get dossiers count:
        for key in counters_count.keys():
            if key not in dossiers_count.keys():
                doss_dir = os.path.join(cfg.MIG_ROOT, self.customer_dir, self.mig_type, key)
                dossiers_count[key] = 0
                if os.path.isdir(doss_dir):
                    for root, dirs, files in os.walk(doss_dir):
                        for file in files:
                            dossiers_count[key] += 1

        return dossiers_count
