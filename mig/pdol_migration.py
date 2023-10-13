# REF: stefan.mastilak@visma.com

from mig.pdol_counters import PdolCounters
from lib.base_migration import Migration
from lib.base_checks import Checks
from lib.base_actions import Actions
from lib.zip_handler import Zipper
import config as cfg
import logging
import os
import shutil
import time


class PdolMigration(Checks, Actions, Zipper, PdolCounters, Migration):
    """
    PDOL migration class.
    """

    def __init__(self, customer_dir):
        super().__init__(customer_dir=customer_dir,
                         mig_type='PDOL',
                         job_id='7')
        self.logs_dir = None
        self.docs_dir = None
        self.password = None

    def __move_index_file(self):
        """
        Move index.xml file from customer's DOCS folder to PDOL folder.
        NOTE: There is always only one index file for PDOL migration.
        :return: index file path
        :rtype: str
        """
        index_src_path = os.path.join(self.docs_dir, 'index.xml')
        index_dst_path = os.path.join(cfg.MIG_ROOT, self.customer_dir, self.mig_type, 'index.xml')

        if os.path.isfile(index_src_path):
            logging.info(msg=f' Index file found in {index_src_path} path')
            # move to PDOL folder:
            shutil.move(src=index_src_path, dst=index_dst_path)
            logging.info(msg=f' Index.xml file moved to {self.mig_type} folder')
            return True
        else:
            logging.critical(msg=f' Index file not found in {self.docs_dir} folder')
            raise FileNotFoundError(f' Index file not found in {self.docs_dir} folder')

    def __get_docs_files_count(self):
        """
        Get count of files in DOCS folder inside customer folder.
        :return: files count in DOCS folder
        :rtype: int
        """
        if os.path.exists(self.docs_dir):
            count = 0
            for _path in os.listdir(self.docs_dir):
                if os.path.isfile(os.path.join(self.docs_dir, _path)):
                    count += 1
            if count:
                return count
            else:
                logging.critical(msg=f' No files found inside DOCS folder')
                raise FileNotFoundError(f' No files found inside DOCS folder')
        else:
            logging.critical(msg=f" DOCS folder doesn't exists in {self.docs_dir} path")
            raise NotADirectoryError(f" DOCS folder doesn't exist in {self.docs_dir} path")

    def __get_cmd_rows_count(self, cmd_files):
        """
        Read cmd file and get its rows count.
        NOTE: There is always only one cmd file for PDOL migration.
        :return: cmd file rows count
        :rtype: int
        """
        if len(cmd_files) == 1:
            cmd_file = cmd_files[0]

            if os.path.isfile(cmd_file):
                # get rows count:
                with open(cmd_file) as cmd_file:
                    rows = cmd_file.readlines()
                if len(rows) > 0:
                    return len(rows)
                else:
                    logging.critical(msg=f' Zero rows count in cmd file')
                    raise ValueError(f' Zero rows count in cmd file')
            else:
                logging.critical(msg=f' Cmd file not found in {cmd_file} path')
                raise FileNotFoundError(f' Cmd file not found in {cmd_file} path')
        elif len(cmd_files) > 1:
            logging.critical(msg=f' More than one cmd file found in {self.customer_dir} folder')
            raise AssertionError(f' More than one cmd file found in {self.customer_dir} folder')
        else:
            logging.critical(msg=f' No cmd files found in {self.customer_dir} folder')
            raise AssertionError(f' No cmd files found in {self.customer_dir} folder')

    def __checksum_cmd_vs_docs(self, cmd_files):
        """
        Check if number or rows in cmd file divided by two is te same as number of file in DOCS folder.
        :param: cmd_files: list of found cmd files
        :return: True if count matches
        :rtype: bool
        """
        docs_count = self.__get_docs_files_count()
        cmd_count = self.__get_cmd_rows_count(cmd_files)

        if docs_count == (cmd_count / 2):
            logging.info(msg=f' Checksum passed: DOCS files count: {docs_count}, Cmd rows count: {cmd_count / 2}')
            return True
        else:
            logging.critical(msg=f' Checksum failed: DOCS files count: {docs_count}, Cmd rows count: {cmd_count / 2}')
            return False

    def __checksum_cmd_vs_dossiers(self, cmd_files):
        """
        Post-Checksum: Check if number of e-dossier files is the same as cmd file rows divided by two.
        NOTE: We added one extra line to the cmd file to specify te encoding, so we need to subtract 1 from cmd_count.
        :return: checksum result
        """
        eds_count = self.get_dossiers_count()
        cmd_count = self.__get_cmd_rows_count(cmd_files=cmd_files)

        if eds_count == ((cmd_count - 1) / 2):
            logging.info(msg=f' Checksum passed: Dossiers count: {eds_count}, Cmd rows count: {(cmd_count - 1) / 2}')
            return True
        else:
            logging.critical(msg=f' Checksum failed: Dossiers count: {eds_count}, Cmd rows count: {(cmd_count - 1) / 2}')
            logging.critical(msg=f' Checksum error: {((cmd_count - 1) / 2) - eds_count} dossiers are missing')
            return False

    def __rename_dossier_dirs(self):
        """
        Rename e-dossier migration folders according to '{CustomerID}_{CompanyID}_P' pattern.
        :return: renamed folders paths
        :rtype: list
        """
        mig_folders = self.get_mig_folders()
        renamed = []

        for dir_name in mig_folders:
            mig_folder = os.path.join(cfg.MIG_ROOT, self.customer_dir, self.mig_type, dir_name)
            new_name = os.path.join(cfg.MIG_ROOT, self.customer_dir, self.mig_type, f'{self.customer_dir}_{dir_name}_P')

            if os.path.isdir(mig_folder):
                retry = 5
                while retry:
                    time.sleep(0.5)
                    try:
                        os.rename(src=mig_folder, dst=new_name)
                        renamed.append(new_name)
                        retry = 0
                    except Exception as err:
                        if retry:
                            retry = retry - 1
                        else:
                            logging.critical(
                                msg=f' Renaming process failed for {mig_folder} >> {new_name}. Error: {err}')
                            raise PermissionError(f' Renaming of {mig_folder} to {new_name} failed')
            else:
                logging.critical(msg=f" Directory {mig_folder} doesn't exist")
                raise NotADirectoryError(f" Directory {mig_folder} doesn't exist")

        if len(mig_folders) == len(renamed):
            return renamed
        else:
            logging.critical(msg=f' Renaming process failed')
            raise AssertionError(f' Renaming process failed')

    def run_migration(self, sftp_prod: bool):
        """
        Run PDOL migration for specific customer.
        NOTE: name of the customer directory is used as CustomerID.
        :param sftp_prod: True for uploading to 'robot_files' folder, False for uploading to 'robot_test_files'
        :return: True if successful migration
        :rtype: bool
        """
        while True:
            # check if Pentaho is installed in the provided path:
            if not self.pentaho_check():
                break

            # check is MigVisma folder exists:
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
                break

            # check if parameters file exists in customer folder:
            if not self.params_check():
                break

            # check if PDOL folder exists in customer folder:
            if not self.mig_type_dir_check():
                break

            # read password for zipped files:
            self.password = self.get_password()

            # create DOCS folder if it doesn't exist:
            self.docs_dir = self.create_docs_dir()

            # create migration log folder if it doesn't already exist:
            self.logs_dir = self.create_log_dir()

            # unpack customer files:
            if not self.unpack_sfx_archive(destination=self.docs_dir, pwd=self.password):
                break

            # move index.xml to the PDOL folder:
            if not self.__move_index_file():
                break

            # run Migration:
            if not self.execute_migration_job():
                break

            # get .cmd files (there should be always only one for PDOL):
            cmd_files = self.get_cmd_files()

            # checksum cmd vs docs:
            if not self.__checksum_cmd_vs_docs(cmd_files=cmd_files):
                break

            # Execute cmd files (there should be always only one for PDOL):
            if not self.execute_cmd_files(cmd_files=cmd_files):
                break

            # checksum cmd vs dossiers:
            if not self.__checksum_cmd_vs_dossiers(cmd_files=cmd_files):
                break

            # rename dossier directories:
            renamed = self.__rename_dossier_dirs()
            if not renamed:
                break

            # zip dossiers:
            zipped = self.zip_dossiers(folders=renamed, pwd=self.password)
            if not zipped:
                break

            # Upload to SFTP:
            if not self.upload_dossiers(files=zipped, sftp_prod=sftp_prod):
                break

            # PDOL migration succeeded:
            logging.info(msg=f' Migration for {self.customer_dir} finished successfully')
            return True
