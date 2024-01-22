# REF: stefan.mastilak@visma.com

from lib.base_migration import Migration
from lib.base_checks import Checks
from lib.base_actions import Actions
from lib.zip_handler import Zipper
import config as cfg
import logging
import os
import shutil


class PdolMigration(Checks, Actions, Zipper, Migration):
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

    def __checksum_cmd_vs_docs(self, cmd_file):
        """
        Check if count of cmd move rows is te same as count of files inside DOCS folder.
        :param: cmd_file: cmd file
        :return: True if count matches
        :rtype: bool
        """
        docs_count = self.__get_docs_files_count()
        cmd_count = self.get_cmd_move_rows_count(cmd_file=cmd_file)

        if docs_count == cmd_count:
            logging.info(msg=f' Checksum passed: DOCS files count: {docs_count}, Cmd move rows count: {cmd_count}')
            return True
        else:
            logging.critical(msg=f' Checksum failed: DOCS files count: {docs_count}, Cmd move rows count: {cmd_count}')
            return False

    def __checksum_cmd_vs_dossiers(self, cmd_file):
        """
        Post-Checksum: Check if number of e-dossier files is the same as cmd file move rows count.
        NOTE:
        1) One file with SQL query created in e-dossier folder
        2) One file BulkInsert created in e-dossier folder
        So we need to subtract -2 from total e-dossier files count to have a correct number for comparison
        :param cmd_file: cmd file path
        :return: checksum result
        :rtype: bool
        """
        dossiers_count = self.get_dossiers_count(cmd_file=cmd_file)
        cmd_count = self.get_cmd_move_rows_count(cmd_file=cmd_file)

        if (dossiers_count - 2) == cmd_count:
            logging.info(msg=f' Checksum passed: Dossiers count: {dossiers_count}, Cmd move rows count: {cmd_count}')
            logging.info(msg=f" NOTE: Considering 2 new extra files created in e-dossier folder (SQL and BulkInsert)")
            return True
        else:
            logging.critical(msg=f' Checksum failed: Dossiers count: {dossiers_count}, Cmd move rows count: {cmd_count}')
            logging.info(msg=f" NOTE: Considering 2 new extra files created in e-dossier folder (SQL and BulkInsert)")
            return False

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
                pass

            # check if parameters file exists in customer folder:
            if not self.params_check():
                break

            # check if PDOL folder exists in customer folder:
            if not self.mig_type_dir_check():
                break

            # check if PDOL_parameters.xlsx file exists in customer folder:
            if not self.mig_params_xlsx_check():
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

            # get cmd file (there should always be only one for PDOL):
            cmd_file = self.get_cmd_file()

            # checksum cmd move rows count vs docs files count:
            if not self.__checksum_cmd_vs_docs(cmd_file=cmd_file):
                break

            # execute cmd file:
            if not self.execute_cmd_file(cmd_file=cmd_file):
                break

            # checksum cmd move rows count vs e-dossier files count:
            if not self.__checksum_cmd_vs_dossiers(cmd_file=cmd_file):
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

            # PDOL migration succeeded:
            logging.info(msg=f' Migration for {self.customer_dir} finished successfully')
            return True
