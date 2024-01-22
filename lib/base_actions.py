# REF: stefan.mastilak@visma.com

import config as cfg
import glob
import logging
import os
import pandas as pd
import re
import subprocess
import time
from lib.base_migration import Migration
from lib.sftp_handler import SftpHandle


class Actions(Migration):
    """
    Base Actions class containing common methods for all supported migration types.
    """
    def create_log_dir(self):
        """
        Create migration 'Log' directory inside customer's folder if doesn't already exist.
        :return: log folder path
        :rtype: str
        """
        log = os.path.join(cfg.MIG_ROOT, self.customer_dir, 'Log')
        if os.path.exists(log):
            logging.info(msg=f' Log folder already exists in {log}')
            return log
        else:
            try:
                os.mkdir(log)
            except Exception as err:
                logging.error(msg=f' Failed to create log folder in {log}\n Error:{err}')
                raise OSError(f' Failed to create log folder in {log}')
            else:
                logging.info(msg=f' Log folder created')
                return log

    def create_docs_dir(self):
        """
        Create DOCS folder inside customer folder if doesn't already exist.
        :return: docs folder path
        :rtype: str
        """
        docs = os.path.join(cfg.MIG_ROOT, self.customer_dir, self.mig_type, 'DOCS')
        if os.path.exists(docs):
            logging.info(msg=f' DOCS folder already exists in {docs}')
            return docs
        else:
            try:
                os.mkdir(docs)
            except Exception as err:
                logging.error(msg=f' Failed to create docs folder in {docs}\n Error: {err}')
                raise OSError(f' Failed to create docs folder in {docs}')
            else:
                logging.info(msg=f' DOCS folder created')
                return docs

    def create_index_dir(self,):
        """
        Create index folder inside customer folder if it doesn't already exist.
        :return: index folder path
        :rtype: str
        """
        idx = os.path.join(cfg.MIG_ROOT, self.customer_dir, self.mig_type, 'index')
        if os.path.exists(idx):
            logging.info(msg=f' Index folder already exists in {idx}')
            return idx
        else:
            try:
                os.mkdir(idx)
            except Exception as err:
                logging.error(msg=f' Failed to create index folder in {idx}\n Error: {err}')
                raise OSError(f' Failed to create index folder in {idx}')
            else:
                logging.info(msg=f' Index folder created')
                return idx

    def get_password(self):
        """"
        Read the zip/unzip password from the file saved in customer folder.
        :return: zip password
        :rtype: str
        """
        path_1 = os.path.join(cfg.MIG_ROOT, self.customer_dir, self.mig_type, 'PW.txt')
        path_2 = os.path.join(cfg.MIG_ROOT, self.customer_dir, self.mig_type, 'PW.txt.txt')

        if os.path.isfile(path_1):
            logging.info(msg=f' Password file found in {self.customer_dir} folder')
            with open(path_1) as f:
                pwd = f.read()
                return pwd
        else:
            if os.path.isfile(path_2):
                logging.info(msg=f' Password file found in {self.customer_dir} folder')
                with open(path_2) as f:
                    pwd = f.read()
                    return pwd
            logging.critical(msg=f' Password file not found in {self.customer_dir} folder')
            raise FileNotFoundError(f' Password file not found in {self.customer_dir} folder')

    def __call_migration_bat(self):
        """
        Method for calling the MigrationTool_Robot.bat script stored in D:/MigVisma root folder.
        This is a batch script dedicated for the robot to run migration tasks defined by CustomerID and JobID.
        Script takes two parameters:
            1) customer directory name: which is also used as Migration ID
            2) job_type: 1=Customer, 2=Servicetime, 3=Sickness, 4=MappingDossier, 5=MLM, 6=SDOL, 7=PDOL
        :return: stdout, stderr
        """
        batch_path = os.path.join(cfg.MIG_ROOT, cfg.MIG_BATCH_SCRIPT)

        if os.path.isfile(batch_path):
            cmd = [batch_path, self.customer_dir, self.job_id]
            process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, cwd=cfg.PENTAHO_DIR)
            stdout, stderr = process.communicate()
            return stdout.decode('utf-8').splitlines(), stderr.decode('utf-8')
        else:
            logging.critical(msg=f' {cfg.MIG_BATCH_SCRIPT} not found in {cfg.MIG_ROOT} folder')
            raise FileNotFoundError(f' Path {batch_path} doesnt exist')

    def execute_migration_job(self):
        """
        Run migration tool batch script for specific CustomerID and JobID.
        :return: True if batch executed without errors
        :rtype: bool
        """
        logging.info(msg=f' Starting the migration process')
        logging.info(msg=f' Executing the MigrationTool. MigrationID: {self.customer_dir} and JobID: {self.job_id}')
        pdol_result = self.__call_migration_bat()
        out = pdol_result[0]
        err = pdol_result[1]

        if err:
            logging.critical(msg=f' Migration job failed with errors: {err}')
            return
        else:
            logging.info(msg=f' Migration job execution details:')
            for line in out:
                if '_MIGRATE_' in line and '-logfile:' not in line:
                    logging.info(msg=f' {line}')
            logging.info(msg=f' Migration process finished')
            return True

    @staticmethod
    def __add_cmd_encoding(cmd_file):
        """
        Add "chcp 65001" to the very top line of the cmd file.
        This will tell windows to use UTF8 encoding for cmd execution.
        NOTE: This action will add one extra line to the cmd file.
        IMPORTANT: This needs to be considered in the checksum!
        :param cmd_file: cmd file path
        :return: None
        """
        if os.path.isfile(cmd_file):
            with open(cmd_file, 'r', encoding='utf-8') as contents:
                save = contents.read()

            with open(cmd_file, 'w', encoding='utf-8') as contents:
                # att UTF8 encoding at the top of cmd file:
                contents.write('chcp 65001\n')
                contents.write(save)

            # split path:
            root_path, cmd_file_name = os.path.split(cmd_file)
            logging.info(msg=f' Cmd file {cmd_file_name} encoding changed to UTF8')
            logging.info(msg=f' NOTE: One row added to the cmd file')
        else:
            logging.critical(msg=f' Cmd file not found in {cmd_file} path')
            raise FileNotFoundError(f' Cmd file not found in {cmd_file} path')

    def get_cmd_file(self):
        """
        Get cmd file from customer folder.
        There should always be only one cmd file for PDOL, SDOL and MLM migration
        :return: cmd file path
        :rtype: str
        """
        cmd_files = glob.glob(os.path.join(cfg.MIG_ROOT, self.customer_dir, '**/*.cmd'), recursive=True)
        if cmd_files:
            if len(cmd_files) == 1:
                # there should always be only one cmd file for PDOL, SDOL and MLM migration
                cmd_path = os.path.normpath(cmd_files[0])
                if os.path.isfile(cmd_path):
                    return cmd_path
                else:
                    logging.critical(msg=f" No cmd file found in path {cmd_path}")
                    raise FileNotFoundError(f" No cmd file found in path {cmd_path}")
            else:
                logging.critical(f" More than one cmd file found in {self.customer_dir}")
                raise AssertionError(f" More than one cmd file found in {self.customer_dir}")
        else:
            logging.critical(msg=f' No cmd file found in {self.customer_dir} folder')
            raise FileNotFoundError(f' No cmd file found in {self.customer_dir} folder')

    @staticmethod
    def get_cmd_move_rows_count(cmd_file):
        """
        Read cmd file and get its 'move' rows count.
        :return: cmd file rows count
        :rtype: int
        """
        count = 0

        if os.path.isfile(cmd_file):
            with open(cmd_file, encoding='utf-8') as cmd_file:
                rows = cmd_file.readlines()
                if len(rows) > 0:
                    for row in rows:
                        if 'move' in row:
                            count += 1
                else:
                    logging.critical(msg=f' Zero rows count in cmd file')
                    raise ValueError(f' Zero rows count in cmd file')
            if count:
                return count
            else:
                logging.critical(msg=f' Zero move rows count in cmd file')
                raise ValueError(f' Zero move rows count in cmd file')
        else:
            logging.critical(msg=f' Cmd file not found in {cmd_file} path')
            raise FileNotFoundError(f' Cmd file not found in {cmd_file} path')

    def execute_cmd_file(self, cmd_file):
        """
        Execute cmd file inside customer folder.
        :param cmd_file: cmd file to be executed
        :return: True if cmd file was executed without errors
        :rtype: bool
        """
        missing_files_count = 0
        root_path, file_name = os.path.split(cmd_file)
        self.__add_cmd_encoding(cmd_file=cmd_file)

        logging.info(msg=f' Executing cmd file {file_name}..')
        process = subprocess.Popen(cmd_file,
                                   stdout=subprocess.PIPE,
                                   stderr=subprocess.STDOUT,
                                   encoding='utf-8')
        stdout, stderr = process.communicate()

        out = [line for line in stdout.split("\n") if line != '']
        nf_errs = [out[idx - 1] + f' {out[idx]}' for idx, i in enumerate(out) if 'cannot find the file' in i]

        # If MLM - Check missing files error:
        if self.mig_type == 'MLM':
            if nf_errs:
                logging.critical(msg=f' Cmd file {file_name} execution failed due to missing files')
                logging.critical(msg=f' Missing files count: {len(nf_errs)}')
                missing_files_count += int(len(nf_errs))

                if 0 < missing_files_count <= 10:
                    for err in nf_errs:
                        missing_file = err.split('"')[1]
                        if missing_file:
                            logging.warning(msg=f' Missing file: {missing_file}')

                    logging.info(msg=f' NOTE: Acceptable data loss ({missing_files_count} files) for MLM migration')
                    logging.info(msg=f' Cmd file {file_name} execution considered as successful')
                    return True  # MLM migration accepts maximum of 10 missing files
                else:
                    return False  # More than 10 missing files is considered as failure for MLM

        if stderr:
            logging.critical(msg=f' Cmd file {file_name} execution failed')
            return False
        else:
            logging.info(msg=f' Cmd file {file_name} executed successfully')
            return True

    def __find_target_path_in_cmd_file(self, cmd_file):
        """
        Alternative way of finding migration target folder in the case there is no "TargetPath" in
        PDOL_parameters.xlsx file. There should be migration target path in the cmd file at the beginning like:
        1) FOR PDOL: if not exist <target_path> mkdir <target_path>
        2) FOR SDOL: md <target_path> 2>nul
        3) FOR MLM: if not exist <target_path/Elektronisch Dossier/file> mkdir <target_path/Elektronisch Dossier/file>
        :param cmd_file: cmd file path
        :return: target path as string
        :rtype: str
        """
        with open(cmd_file, encoding='utf-8') as cmd_file:
            if self.mig_type == 'PDOL':
                for row in cmd_file.readlines():
                    if 'if not exist' in row and 'mkdir' in row:
                        path = row.split('mkdir')[1]
                        return os.path.normpath(path) if path else None
            if self.mig_type == 'SDOL':
                for row in cmd_file.readlines():
                    if 'md' in row:
                        path = re.findall(r'"(.*?)"', row)
                        return os.path.normpath(path[0]) if path else None
            if self.mig_type == 'MLM':
                for row in cmd_file.readlines():
                    if "if not exist" in row and 'mkdir' in row:
                        target_path = re.search(r"(.*?Elektronisch Dossier)",
                                                os.path.normpath(row.split('"')[-2])).group(1)
                        if target_path:
                            return os.path.normpath(target_path)
                        else:
                            return

    def get_target_folder(self, cmd_file):
        """
        Get Migration target folder path (There should be only one such folder according to the new rules).
        :param cmd_file: cmd file path
        :return: migration target folder full path
        :rtype: str
        """
        mig_target = os.path.join(cfg.MIG_ROOT, self.customer_dir, self.mig_type, f'{self.mig_type}_parameters.xlsx')
        target_path = None

        if os.path.exists(mig_target):
            # try to find target path in parameters file:
            data = pd.read_excel(mig_target).to_dict()
            for key in data.keys():
                if not target_path:
                    if 'target' in key.lower():  # there should be a TargetPath column in parameters excel file
                        for item in data.get(key).values():
                            if 'migvisma' in item.lower():  # MigVisma should be in the path for e-dossier target folder
                                target_path = item
            if target_path:
                logging.info(msg=f" Target path fetched from {self.mig_type}_parameters.xlsx file")
                logging.info(msg=f" Target folder: {target_path}")
                return os.path.normpath(target_path)
            else:
                logging.warning(msg=f" No target path found in {self.mig_type}_parameters.xlsx file")
        else:
            logging.warning(msg=f" File {self.mig_type}_parameters.xlsx doesn't exist in {self.mig_type} folder")

        if not target_path:
            # try to find target path in cmd file:
            logging.info(msg=f" Finding target path in cmd file..")
            target_path = self.__find_target_path_in_cmd_file(cmd_file=cmd_file)
            if target_path:
                logging.info(msg=f" Target path fetched from cmd file")
                logging.info(msg=f" Target folder: {target_path}")
                return os.path.normpath(target_path)
            else:
                raise AssertionError(f" Unable to find target path in {self.mig_type}_parameters.xlsx or cmd file")

    def get_dossiers_count(self, cmd_file):
        """
        Get count of all e-dossier files inside E-dossier target folder (there should be only one such folder)
        :param cmd_file: cmd file path
        :return: total count of all e-dossier files inside PDOL folder
        :rtype: int
        """
        target_path = self.get_target_folder(cmd_file=cmd_file)

        total_count = 0

        if os.path.isdir(target_path):
            for root, dirs, files in os.walk(target_path):
                for _ in files:
                    total_count += 1
        else:
            logging.critical(msg=f" Migration target folder doesn't exist")
            raise NotADirectoryError(f" Path {target_path} doesn't exist")

        if total_count:
            return total_count
        else:
            logging.critical(msg=f' No e-dossier files found')
            raise FileNotFoundError(f' No e-dossier files found')

    def upload_single_dossier(self, file: str, sftp_prod=True):
        """
        Upload single file to SFTP server.
        :param file: zipped file e-dossier path
        :param sftp_prod: True by default, set to False if you want to upload to Test folder during testing phase
        :return: True if uploaded
        :rtype: bool
        """
        logging.info(msg=f' Starting the uploading process')
        sftp_folder = cfg.SFTP_DIR if sftp_prod else cfg.SFTP_TEST_DIR

        with SftpHandle() as sftp:
            # create remote directory to upload file to:
            sftp.mk_dir(remote_path=f'{sftp_folder}/{self.customer_dir}')

            # split path:
            root_path, file_name = os.path.split(file)

            # upload file to sftp:
            logging.info(msg=f' Uploading file {file_name}...')
            sftp.upload(local_path=file, remote_path=f'{sftp_folder}/{self.customer_dir}/{file_name}')

            # check if file uploaded:
            sftp_content = sftp.ls_dir(remote_path=f'/{sftp_folder}/{self.customer_dir}/')
            if file_name in sftp_content:
                logging.info(msg=f' File {file_name} uploaded to sftp folder {sftp_folder}/{self.customer_dir}')
            else:
                logging.critical(msg=f' Uploading process failed for {file_name} file')
                raise FileNotFoundError(f' Uploading process failed for {file_name} file')

            logging.info(msg=f' Uploading process finished')
            return True

    def rename_dossier_folder(self, cmd_file):
        """
        Rename e-dossier migration target folder according to:
         1) '{CustomerID}_P' pattern for PDOL ('P' stands for PDOL).
         2) '{CustomerID}_S' pattern for SDOL ('S' stands for SDOL).
         3) '{CustomerID}_P' pattern for MLM ('M' stands for MLM).
         Postfix letter is basically first letter of migration type.
        :param cmd_file: cmd file path
        :return: renamed folder path (there should be only one e-dossier folder)
        :rtype: str
        """
        original_name = self.get_target_folder(cmd_file=cmd_file)
        original_root_path, _ = os.path.split(original_name)
        new_name = os.path.join(original_root_path, f'{self.customer_dir}_{self.mig_type[0]}')

        if os.path.isdir(original_name):
            retry = 5
            while retry:
                time.sleep(0.5)
                try:
                    os.rename(src=original_name, dst=new_name)
                    retry = 0
                    logging.info(msg=f" Dossier folder {original_name} renamed to {new_name}")
                    return new_name
                except Exception as err:
                    if retry:
                        retry = retry - 1
                    else:
                        logging.critical(
                            msg=f' Renaming process failed for {original_name} >> {new_name}. Error: {err}')
                        raise PermissionError(f' Renaming of {original_name} to {new_name} failed')
        else:
            logging.critical(msg=f" Directory {original_name} doesn't exist")
            raise NotADirectoryError(f" Directory {original_name} doesn't exist")

    def rename_if_success_migration(self):
        """
        Rename customer directory like: {CustomerDirectory}_success_robot if migration succeeded.
        :return: None
        """
        old = os.path.join(cfg.MIG_ROOT, self.customer_dir)
        new = os.path.join(cfg.MIG_ROOT, f'{self.customer_dir}_success_robot')

        if os.path.isdir(old):
            retry = 10
            while retry:
                time.sleep(1)
                try:
                    os.rename(src=old, dst=new)
                    retry = 0
                    logging.info(msg=f' {old} folder renamed to {new}')
                except Exception as err:
                    if retry:
                        retry = retry - 1
                    else:
                        logging.critical(msg=f' Renaming failed for directory {self.customer_dir}\n Error: {err}')
                        raise PermissionError(f' Renaming failed for directory {self.customer_dir}')
        else:
            raise NotADirectoryError(f" Directory {self.customer_dir} doesn't exist")

    def rename_if_failed_migration(self):
        """
        Rename customer directory like: {CustomerDirectory}_failed_robot if migration failed.
        :return: None
        """
        old = os.path.join(cfg.MIG_ROOT, self.customer_dir)
        new = os.path.join(cfg.MIG_ROOT, f'{self.customer_dir}_failed_robot')

        if os.path.isdir(old):
            retry = 10
            while retry:
                time.sleep(1)
                try:
                    os.rename(src=old, dst=new)
                    retry = 0
                    logging.info(msg=f' {old} folder renamed to {new}')
                except Exception as err:
                    if retry:
                        retry = retry - 1
                    else:
                        logging.critical(msg=f' Renaming failed for directory {self.customer_dir}\n Error: {err}')
                        raise PermissionError(f' Renaming failed for directory {self.customer_dir}')
        else:
            raise NotADirectoryError(f" Directory {self.customer_dir} doesn't exist")
