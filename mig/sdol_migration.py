# REF: stefan.mastilak@visma.com

import csv
import config as cfg
import glob
import logging
import os
import shutil
import zipfile
from lib.base_migration import Migration
from lib.base_checks import Checks
from lib.base_actions import Actions
from lib.zip_handler import Zipper


class SdolMigration(Checks, Actions, Zipper, Migration):
    """
    SDOL migration class.
    """

    def __init__(self, customer_dir):
        super().__init__(customer_dir=customer_dir,
                         mig_type='SDOL',
                         job_id='6')
        self.logs_dir = None
        self.docs_dir = None
        self.idx_dir = None
        self.password = None

    def __unzip_docs_files(self):
        """
        Unzip all the zip files found inside DOCS folder. It's needed because when we execute sfx files,
        we will get set of zip files inside DOCS folder, which needs to be extracted as well in order to be able
        to manipulate with its content.
        :return: True if all zip files unzipped successfully
        :rtype: bool
        """
        if self.mig_type_dir_check():
            zip_files = glob.glob(os.path.join(self.docs_dir, '*.zip'), recursive=True)

            if zip_files:
                logging.info(msg=f' Unzipping archives inside DOCS folder')
                for file in zip_files:
                    root, ext = os.path.splitext(file)
                    with zipfile.ZipFile(file, "r") as zip_ref:
                        zip_ref.extractall(path=root)

                logging.info(msg=f' All archives unzipped successfully')
                return True

            else:
                logging.critical(msg=f' No zip files found in {self.docs_dir} folder')
                raise FileNotFoundError(f' No zip files found in {self.docs_dir} folder')

    def __move_index_files(self):
        """
        Rename and move all index files from DOCS folder into the index folder.
        :return: True if all files moved
        :rtype: bool
        """
        if self.mig_type_dir_check():
            index_files = glob.glob(os.path.join(self.docs_dir, '**', 'index.xml'), recursive=True)

            if index_files:
                counter = 0
                for file in index_files:
                    root_path, file_name = os.path.split(file)
                    new_file_name = os.path.join(root_path, f'{os.path.basename(os.path.dirname(file))}-index.xml')

                    # rename index file:
                    try:
                        os.rename(src=file, dst=new_file_name)
                    except Exception as err:
                        logging.critical(f' Renaming process failed for {file}. Error: {err}')
                        raise OSError(f' Renaming process failed for {file}')

                    # move index file:
                    try:
                        shutil.move(src=new_file_name, dst=self.idx_dir)
                    except Exception as err:
                        logging.critical(f' Moving of {file} failed. Error: {err}')
                        raise OSError(f' Moving of {file} failed')
                    counter += 1

                if counter != 1:
                    logging.info(f' {counter} index files renamed and moved to index folder')
                else:
                    logging.info(f' {counter} index file renamed and moved to index folder')
                return True
            else:
                logging.critical(msg=f' No index files found in {self.docs_dir} folder')
                raise FileNotFoundError(f' No index files found in {self.docs_dir} folder')

    def __get_index_files_count(self):
        """
        Get number of files inside index folder.
        :return: number of index files
        :rtype: int
        """
        return len([i for i in os.listdir(self.idx_dir) if os.path.isfile(os.path.join(self.idx_dir, i))])

    def __get_zip_files_count(self):
        """
        Get zip files count inside DOCS folder.
        :return: number of zip files
        :rtype: int
        """
        return len(glob.glob(os.path.join(self.docs_dir, "*.zip")))

    def __get_unzipped_dir_count(self):
        """
        Get unzipped folders count inside DOCS folder.
        :return: count of unzipped directories
        :rtype: int
        """
        return len([i for i in glob.glob(os.path.join(self.docs_dir, '*')) if '.zip' not in i])

    def __get_migrated_count(self):
        """
        Read Counters.csv file created by MigrationTool and get 'SDOL_Migrated_TotalFiles' number from it.
        :return: total migrated files count
        :rtype: int
        """
        counters_path = os.path.join(cfg.MIG_ROOT, self.customer_dir, self.mig_type, 'Counters.csv')

        if os.path.isfile(counters_path):
            counter = None
            with open(counters_path, mode='r') as csv_file:
                csv_reader = csv.reader(csv_file, delimiter=';')
                for row in csv_reader:
                    if len(row) == 2:
                        if 'SDOL_Migrated_TotalFiles' in row:
                            counter = int(row[1]) if row[1].isdecimal() else 0
                            return counter
            if not counter:
                logging.critical(msg=f" No keyword like 'SDOL_Migrated_TotalFiles' found in Counters.csv file")
                raise ValueError(f' Unable to fetch SDOL_Migrated_TotalFiles count from Counters.csv file')
        else:
            logging.critical(msg=f' Counters.csv file not found in {self.mig_type} folder')
            raise FileNotFoundError(f' Counters.csv file not found in {self.mig_type} folder')

    def __checksum_index_vs_zip(self):
        """
        Compare number of index files inside index folder with number of zip files inside DOCS folder.
        :return: comparison result
        :rtype: bool
        """
        idx_count = self.__get_index_files_count()
        zip_count = self.__get_zip_files_count()

        if idx_count:
            if zip_count:
                if idx_count == zip_count:
                    logging.info(msg=f' Checksum passed: index files count: {idx_count}, zip files count: {zip_count}')
                    return True
                else:
                    logging.critical(
                        msg=f' Checksum failed: index files count: {idx_count}, zip files count: {zip_count}')
                    return False
            else:
                logging.critical(msg=f' Zero count of zip files')
                raise ValueError(f' Zero count of zip files')
        else:
            logging.critical(msg=f' Zero count of index files')
            raise ValueError(f' Zero count of index files')

    def __checksum_counters_vs_cmd(self, cmd_file):
        """
        Compare total migrated files count with number of 'move' rows from .cmd file
        :param cmd_file: cmd file path
        :return: comparison result
        :rtype: bool
        """
        counters_count = self.__get_migrated_count()
        cmd_count = self.get_cmd_move_rows_count(cmd_file=cmd_file)

        if counters_count == cmd_count:
            logging.info(msg=f' Checksum passed: Counters count: {counters_count}, Cmd move rows count: {cmd_count}')
            return True
        else:
            logging.critical(msg=f' Checksum failed: Counters count: {counters_count}, Cmd move rows count: {cmd_count}')
            return False

    def __checksum_counters_vs_dossiers(self, cmd_file):
        """
        Checksum counters vs e-dossier files count.
        NOTE:
        1) One file with SQL query created in e-dossier folder
        2) One file BulkInsert created in e-dossier folder
        So we need to subtract -2  from total e-dossier files count to have a correct number for comparison
        :param cmd_file: cmd file path
        :return: comparison result (counters count vs e-dossier files count)
        :rtype: bool
        """
        counters_count = self.__get_migrated_count()
        dossiers_count = self.get_dossiers_count(cmd_file=cmd_file)

        if counters_count == (dossiers_count - 2):
            logging.info(msg=f' Checksum passed: Counters count: {counters_count}, Dossiers count: {dossiers_count}')
            logging.info(msg=f" NOTE: Considering 2 new extra files created in e-dossier folder (SQL and BulkInsert)")
            return True
        else:
            logging.critical(msg=f' Checksum failed: Counters count: {counters_count}, Dossiers count: {dossiers_count}')
            logging.info(msg=f" NOTE: Considering 2 new extra files created in e-dossier folder (SQL and BulkInsert)")
            return False

    def run_migration(self, sftp_prod: bool):
        """
        Run SDOL migration for specific customer.
        NOTE: name of the customer directory is used as CustomerID
        :param sftp_prod: True for uploading to 'robot_files' folder, False for uploading to 'robot_test_files'
        :return: True if successful migration
        :rtype: bool
        """
        while True:
            # check if Pentaho is installed in the provided path:
            if not self.pentaho_check():
                break

            # check is MigVisma root folder exists:
            if not self.mig_root_check():
                break

            # check if Kitchen.bat script exists in pentaho dir:
            if not self.kitchen_check():
                break

            # check if customer folder path exists:
            if not self.customer_dir_check():
                break

            # check if customer folder is not in reserved list:
            if not self.not_reserved_check():
                break

            # check if properties file exists in customer folder:
            if not self.props_check():
                pass

            # check if parameters file exists in customer folder:
            if not self.params_check():
                break

            # check if SDOL folder exists in customer folder:
            if not self.mig_type_dir_check():
                break

            # check if SDOL_parameters.xlsx file exists in customer folder:
            if not self.mig_params_xlsx_check():
                break

            # read password for zipped files:
            self.password = self.get_password()

            # create DOCS folder if it doesn't already exist:
            self.docs_dir = self.create_docs_dir()

            # create index folder if it doesn't already exist:
            self.idx_dir = self.create_index_dir()

            # create migration log folder if it doesn't already exist:
            self.logs_dir = self.create_log_dir()

            # unpack customer files to DOCS folder:
            if not self.unpack_sfx_archive(destination=self.docs_dir, pwd=self.password):
                break

            # unzip files inside DOCS folder:
            if not self.__unzip_docs_files():
                break

            # rename and move index files to index folder:
            if not self.__move_index_files():
                break

            # checksum - index files count vs zip files count in DOCS:
            if not self.__checksum_index_vs_zip():
                break

            # run Migration:
            if not self.execute_migration_job():
                break

            # get cmd file (there should always be only one for SDOL):
            cmd_file = self.get_cmd_file()

            # checksum counters vs cmd file move rows:
            if not self.__checksum_counters_vs_cmd(cmd_file=cmd_file):
                break

            # execute cmd file:
            if not self.execute_cmd_file(cmd_file=cmd_file):
                break

            # checksum counters vs e-dossier files:
            if not self.__checksum_counters_vs_dossiers(cmd_file=cmd_file):
                break

            # rename e-dossier folder:
            renamed_dir = self.rename_dossier_folder(cmd_file=cmd_file)
            if not renamed_dir:
                break

            # zip folder containing all e-dossiers:
            zipped_folder = self.zip_single_dossier(folder=renamed_dir, pwd=self.password)
            if not zipped_folder:
                break

            # Upload zipped folder to SFTP:
            if not self.upload_single_dossier(file=zipped_folder, sftp_prod=sftp_prod):
                break

            # SDOL migration succeeded:
            logging.info(msg=f' Migration for {self.customer_dir} finished successfully')
            return True
