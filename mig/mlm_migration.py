# REF: stefan.mastilak@visma.com

from lib.base_migration import Migration
from lib.base_checks import Checks
from lib.base_actions import Actions
from lib.zip_handler import Zipper
from pathlib import Path
import config as cfg
import glob
import logging
import os
import shutil
import time


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
                if os.path.exists(bestanden[0]):
                    return bestanden[0]
                else:
                    logging.critical(msg=f" Bestanden path {bestanden[0]} doesn't exist")
                    raise OSError(f" Bestanden path doesn't exist")
            else:
                logging.critical(msg=f' More than one Bestanden folder found in {self.docs_dir} folder')
                raise AssertionError(f' More than one Bestanden folder found in {self.docs_dir} folder')
        else:
            logging.critical(msg=f' Bestanden folder not found in {self.docs_dir} folder')
            raise NotADirectoryError(f' Bestanden folder not found in {self.docs_dir} folder')

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

    def __get_cmd_files(self):
        """
        Get cmd file from customer folder.
        IMPORTANT: for MLM it should be always only one cmd file
        :return: cmd file path
        :rtype: list
        """
        cmd_files = self.get_cmd_files()
        if len(cmd_files) == 1:
            return cmd_files
        else:
            logging.critical(msg=f' More than one MLM cmd file found in {self.customer_dir} folder')
            raise AssertionError(f' More than one MLM cmd file found in {self.customer_dir} folder')

    def __get_dossier_folder(self):
        """
        Find 'Elektronisch Dossier' folder inside customer folder.
        :return: path to e-dossier folder
        :rtype: str
        """
        pattern = os.path.join(cfg.MIG_ROOT, self.customer_dir, '**Elektronisch Dossier*')
        matched_folder = glob.glob(pattern)

        if len(matched_folder) == 1:
            if os.path.isdir(matched_folder[0]):
                return matched_folder[0]
            else:
                logging.critical(msg=f" Elektronisch Dossier folder doesn't exist in {self.customer_dir} folder")
                raise NotADirectoryError(f" Elektronisch Dossier folder doesn't exist in {self.customer_dir} folder")
        elif len(matched_folder) == 0:
            logging.critical(msg=f' Elektronisch Dossier folder not created in {self.customer_dir} folder')
            move_rows = self.__get_move_rows_count()
            if move_rows == 0:
                logging.critical(msg=f' Elektronisch Dossier folder not created due to blank cmd file')
                raise SystemError(f' Blank cmd file created')
            raise AssertionError(f' Elektronisch Dossier folder not created in {self.customer_dir} folder')
        else:
            logging.critical(msg=f' Multiple Elektronisch Dossier folders found in {self.customer_dir} folder')
            raise AssertionError(f' Multiple Elektronisch Dossier folders found in {self.customer_dir} folder')

    def __get_move_rows_count(self):
        """
        Get number of 'move' rows from cmd file.
        :return: number of 'move' rows in cmd file
        :rtype: int
        """
        cmd_file = self.__get_cmd_files()[0]  # NOTE: for MLM there is always only one cmd file
        count = 0
        with open(cmd_file) as f:
            content = f.readlines()
            for row in content:
                if 'move ' in row:
                    count += 1
        return count

    def __get_dossiers_count(self):
        """
        Get count of files inside e-dossier folder.
        :return: count of e-dossier documents
        :rtype: int
        """
        dossier_folder = self.__get_dossier_folder()

        total_count = 0
        for root, dirs, files in os.walk(dossier_folder):
            for file in files:
                total_count += 1
        return total_count

    def __checksum_cmd_vs_dossiers(self):
        """
        Checksum: count of 'move' rows in cmd file vs number of files in e-dossier folder.
        :return: True if both counts matches
        :rtype: bool
        """
        move_count = self.__get_move_rows_count()
        doss_count = self.__get_dossiers_count()

        if move_count == doss_count:
            logging.info(msg=f' Checksum passed: Dossier files count: {doss_count}, Cmd move rows count: {move_count}')
            return True
        else:
            logging.info(msg=f' Checksum failed: Dossier files count: {doss_count}, Cmd move rows count: {move_count}')
            return False

    def __rename_dossier_dir(self):
        """
        Rename e-dossier folder for MLM according to '{CustomerID}_M' pattern.
        :return: path to renamed directory
        """
        original_name = self.__get_dossier_folder()
        org_root, org_dir = os.path.split(original_name)
        new_name = os.path.join(org_root, f'{self.customer_dir}_M')

        # rename e-dossier folder:
        new_root, new_dir = os.path.split(new_name)
        retry = 5
        while retry:
            time.sleep(0.5)
            try:
                os.rename(src=original_name, dst=new_name)
                logging.info(msg=f' E-dossier folder renamed from {org_dir} to {new_dir}')
                retry = 0
                return new_name
            except Exception as err:
                if retry:
                    retry = retry - 1
                else:
                    logging.critical(msg=f' Renaming process failed for {org_dir} >> {new_dir}. Error: {err}')
                    raise PermissionError(f' Renaming of {org_dir} to {new_dir} failed')

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

            # execute cmd file:
            cmd_files = self.__get_cmd_files()
            if not self.execute_cmd_files(cmd_files=cmd_files):
                break

            # checksum - cmd 'move' rows count vs number of files in E-dossier folder:
            if not self.__checksum_cmd_vs_dossiers():
                break

            # rename dossier folder:
            renamed = self.__rename_dossier_dir()
            if not renamed:
                break

            # zip dossier folder:
            zipped = self.zip_single_dossier(folder=renamed, pwd=self.password)
            if not zipped:
                break

            # Upload zipped dossier folder to sftp:
            if not self.upload_single_dossier(file=zipped, sftp_prod=sftp_prod):
                break

            # MLM migration succeeded:
            logging.info(msg=f' Migration for {self.customer_dir} finished successfully')
            return True
