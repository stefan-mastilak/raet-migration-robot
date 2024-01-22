# REF: stefan.mastilak@visma.com

from lib.base_migration import Migration
from lib.base_checks import Checks
from lib.base_actions import Actions
from lib.zip_handler import Zipper
from pathlib import Path
import glob
import logging
import os
import shutil


class MlmMigration(Checks, Actions, Zipper, Migration):
    """
    MLM migration class.
    """

    def __init__(self, customer_dir):
        super().__init__(customer_dir=customer_dir,
                         mig_type='MLM',
                         job_id='5')
        self.logs_dir = None
        self.docs_dir = None
        self.password = None

    def __find_bestanden(self):
        """
        Find Bestanden folder inside DOCS folder.
        :return: Bestanden folder path
        :rtype: str
        """
        bestanden = glob.glob(os.path.join(self.docs_dir, '**\\Bestanden'), recursive=True)
        if bestanden:
            if len(bestanden) == 1:
                matched_path = bestanden[0]
                if os.path.exists(matched_path):
                    logging.info(msg=f" Bestanden path found in {matched_path}")
                    return os.path.normpath(matched_path)
                else:
                    logging.critical(msg=f" Bestanden folder doesn't exist in {self.customer_dir} folder")
                    raise OSError(f" Bestanden path doesn't exist")
            else:
                logging.critical(msg=f' More than one Bestanden folder found inside {self.docs_dir} folder')
                raise AssertionError(f' More than one Bestanden folder found inside {self.docs_dir} folder')
        else:
            logging.critical(msg=f' Bestanden folder not found inside {self.docs_dir} folder')
            raise NotADirectoryError(f' Bestanden folder not found inside {self.docs_dir} folder')

    def __move_unzipped_files(self):
        """
        Move everything on the Bestanden folder level to Customer MLM folder.
        :return True if all files moved correctly
        :rtype: bool
        """
        bestanden_folder = self.__find_bestanden()
        source = Path(bestanden_folder).parent
        destination = Path(self.docs_dir).parent
        files_to_move = os.listdir(source)
        moved = 0

        for file in files_to_move:
            try:
                shutil.move(src=os.path.join(source, file), dst=os.path.join(destination, file))
                moved += 1
            except Exception as error:
                logging.critical(msg=f' Error while moving file {file}. Error: {error}')
                raise

        if moved == len(files_to_move):
            logging.info(msg=f' Checksum passed: All files successfully moved to the MLM folder')
            return True
        else:
            logging.critical(msg=f' Moving process failed')
            raise AssertionError(f' Moving process failed')

    def __remove_docs_folder(self):
        """
        Delete DOCS dir after all files are moved to the customer MLM folder.
        :return True if DOCS successfully removed
        :rtype: bool
        """
        if os.path.isdir(self.docs_dir):
            if not os.listdir(self.docs_dir):
                # CASE 1: DOCS folder is empty
                shutil.rmtree(self.docs_dir)
                logging.info(msg=f' Empty DOCS folder removed')
            else:
                # CASE 2: DOCS folder is not empty
                sub_dirs = [x[0] for x in os.walk(self.docs_dir)]
                for d in reversed(sub_dirs):
                    if not os.listdir(d):
                        shutil.rmtree(d)
                        root_path, dir_name = os.path.split(d)
                        logging.info(f' Empty folder {dir_name} removed')
                        return True
                    else:
                        logging.critical(msg=' DOCS folder is not empty - some files has not been moved')
                        raise FileExistsError(' DOCS folder is not empty - some files has not been moved')
        else:
            logging.critical(msg=f" DOCS folder doesn't exist in {self.docs_dir} folder")
            raise NotADirectoryError(f" DOCS folder doesn't exist in {self.docs_dir} folder")

    def __checksum_cmd_vs_dossiers(self, cmd_file):
        """
        Checksum: count of 'move' rows in cmd file vs number of files in e-dossier folder.
        :param cmd_file: cmd file path
        :return: True if both counts matches
        :rtype: bool
        """
        move_count = self.get_cmd_move_rows_count(cmd_file=cmd_file)
        doss_count = self.get_dossiers_count(cmd_file=cmd_file)

        if move_count == doss_count:
            logging.info(msg=f' Checksum passed: Dossier files count: {doss_count}, Cmd move rows count: {move_count}')
            return True
        elif abs(move_count - doss_count) < 11:
            logging.info(msg=f' Acceptable data loss for MLM migration: {abs(move_count - doss_count)} files diff')
            logging.info(msg=f' Checksum passed: Dossier files count: {doss_count}, Cmd move rows count: {move_count}')
            return True
        else:
            logging.info(msg=f' Checksum failed: Dossier files count: {doss_count}, Cmd move rows count: {move_count}')
            return False

    def run_migration(self, sftp_prod: bool):
        """
        Run MLM migration for specific customer.
        NOTE: name of the customer directory is used as CustomerID.
        :param sftp_prod: True for uploading to 'robot_files' folder, False for uploading to 'robot_test_files'
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
                break

            # check if parameters file exists in customer folder:
            if not self.params_check():
                break

            # check if MLM folder exists in customer folder:
            if not self.mig_type_dir_check():
                break

            # check if MLM_parameters.xlsx file exists in customer folder:
            if not self.mig_params_xlsx_check():
                break

            # read password for zipped files:
            self.password = self.get_password()

            # create DOCS folder if it doesn't already exist:
            self.docs_dir = self.create_docs_dir()

            # create migration log folder if it doesn't already exist:
            self.logs_dir = self.create_log_dir()

            # unpack customer files:
            if not self.unpack_split_archive(destination=self.docs_dir, pwd=self.password):
                break

            # move unzipped files to customer root folder:
            if not self.__move_unzipped_files():
                break

            # delete empty DOCS folder after files are moved:
            if not self.__remove_docs_folder():
                break

            # run migration tool:
            if not self.execute_migration_job():
                break

            # get cmd file (there should always be only one for MLM):
            cmd_file = self.get_cmd_file()

            # execute cmd file:
            if not self.execute_cmd_file(cmd_file=cmd_file):
                break

            # checksum cmd move rows count vs e-dossier files count:
            if not self.__checksum_cmd_vs_dossiers(cmd_file=cmd_file):
                break

            # rename dossier folder:
            renamed = self.rename_dossier_folder(cmd_file=cmd_file)
            if not renamed:
                break

            # zip e-dossier folder:
            zipped = self.zip_single_dossier(folder=renamed, pwd=self.password)
            if not zipped:
                break

            # Upload zipped folder to sftp:
            if not self.upload_single_dossier(file=zipped, sftp_prod=sftp_prod):
                break

            # MLM migration succeeded:
            logging.info(msg=f' Migration for {self.customer_dir} finished successfully')
            return True
