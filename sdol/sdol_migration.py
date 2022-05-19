# REF: stefan.mastilak@visma.com

from sdol.sdol_counters import SdolCounters
from lib.base_migration import Migration
from lib.checks.base_checks import Checks
from lib.actions.base_actions import Actions
from lib.zipper.zip_handler import Zipper
import config as cfg
import glob
import logging
import os
import shutil
import time
import zipfile


class SdolMigration(Checks, Actions, Zipper, SdolCounters, Migration):
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
        to manipulate with it's content.
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

    def __checksum_index_vs_zip(self):
        """
        Compare number of index files vs number of zip files inside DOCS.
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

    def __checksum_counters_vs_cmd(self):
        """
        Compare count of '_Migrated' customer files from Counters.csv with number of 'move' rows from customer cmd file
        :return: comparison result
        :rtype: bool
        """
        counters_count = self.get_migrated_count()
        cmd_count = self.get_cmd_rows_count()
        error = False

        for uid, count in counters_count.items():
            for _uid, _count in cmd_count.items():
                if uid == _uid:
                    if count == _count:
                        logging.info(msg=f' Checksum for customer ID {uid} passed.'
                                         f' Cmd count: {_count}, Counters count: {count}')
                    else:
                        logging.error(msg=f' Checksum for customer ID {uid} failed.'
                                          f' Cmd count: {_count}, Counters count: {count}')
                        error = True
        if not error:
            logging.info(msg=f' Checksum passed: All counters counts matches all cmd file rows counts')
            return True
        else:
            logging.critical(msg=f" Checksum failed: Some counters counts doesn't match cmd file rows counts")
            return False

    def __checksum_counters_vs_dossiers(self):
        """
        Checksum counters vs dossiers count.
        :return: comparison result (counters count vs dossiers count)
        :rtype: bool
        """
        counters_count = self.get_migrated_count()
        dossiers_count = self.get_dossiers_count()
        error = False

        for uid, count in counters_count.items():
            for _uid, _count in dossiers_count.items():
                if uid == _uid:
                    if count == _count:
                        logging.info(msg=f' Checksum for customer ID {uid} passed.'
                                         f' Dossiers count: {_count}, Counters count: {count}')
                    else:
                        logging.error(msg=f' Checksum for customer ID {uid} failed.'
                                          f' Dossiers count: {_count}, Counters count: {count}')
                        error = True
        if not error:
            logging.info(msg=f' Checksum passed: All dossier files moved correctly')
            return True
        else:
            logging.critical(msg=f' Checksum failed: Some dossier files not moved')
            return False

    def __rename_dossier_dirs(self):
        """
        Rename e-dossier migration folders according to '{CustomerID}_{CompanyID}_S' pattern.
        :return: renamed mig folders full-paths
        :rtype: list
        """
        mig_folders = self.get_mig_dirs_from_counters()
        renamed = []

        for dir_name in mig_folders:
            mig_folder = os.path.join(cfg.MIG_ROOT, self.customer_dir, self.mig_type, dir_name)
            new_name = os.path.join(cfg.MIG_ROOT, self.customer_dir, self.mig_type, f'{self.customer_dir}_{dir_name}_S')

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
                raise NotADirectoryError(f" Directory {mig_folder} doesn't exist")

        if len(mig_folders) == len(renamed):
            return renamed
        else:
            logging.critical(msg=f' Renaming process failed - not all folders were renamed')
            raise AssertionError(f' Renaming process failed - not all folders were renamed')

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
                break

            # check if parameters file exists in customer folder:
            if not self.params_check():
                break

            # check if SDOL folder exists in customer folder:
            if not self.mig_type_dir_check():
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

            # checksum counters vs cmd file rows:
            if not self.__checksum_counters_vs_cmd():
                break

            # get .cmd files:
            cmd_files = self.get_cmd_files()

            # execute cmd files:
            if not self.execute_cmd_files(cmd_files=cmd_files):
                break

            # checksum counters vs dossier files:
            if not self.__checksum_counters_vs_dossiers():
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

            # SDOL migration succeeded:
            logging.info(msg=f' Migration for {self.customer_dir} finished successfully')
            return True
