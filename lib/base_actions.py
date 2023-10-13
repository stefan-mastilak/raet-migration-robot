# REF: stefan.mastilak@visma.com

from lib.base_migration import Migration
from lib.sftp_handler import SftpHandle
import config as cfg
import glob
import logging
import os
import subprocess
import time


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
    def add_cmd_encoding(cmd_file):
        """
        Add "chcp 65001" to the very top line of the cmd file.
        This will tell windows to use UTF8 encoding for cmd execution.
        NOTE: This action will add one extra line to the cmd file.
        IMPORTANT: This needs to be considered in the checksum!
        :param cmd_file: cmd file path
        :return: None
        """
        if os.path.isfile(cmd_file):
            with open(cmd_file, 'r') as contents:
                save = contents.read()

            with open(cmd_file, 'w') as contents:
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

    def get_cmd_files(self):
        """
        Get all cmd files inside customer folder and its sub-folders.
        :return: cmd file paths
        :rtype: list
        """
        cmd_files = glob.glob(os.path.join(cfg.MIG_ROOT, self.customer_dir, '**/*.cmd'), recursive=True)
        if cmd_files:
            return cmd_files
        else:
            logging.critical(msg=f' No cmd files found in {self.customer_dir} folder')
            raise FileNotFoundError(f' No cmd files found in {self.customer_dir} folder')

    def execute_cmd_files(self, cmd_files: list):
        """
        Execute all cmd files inside customer folder.
        :param cmd_files: list of customer cmd files to be executed
        :return: True if all cmd files executed successfully
        :rtype: bool
        """
        executed = 0
        for cmd_file in cmd_files:
            # check if cmd file path exists:
            if os.path.isfile(cmd_file):
                # split path:
                root_path, file_name = os.path.split(cmd_file)
                # add encoding (UTF8):
                self.add_cmd_encoding(cmd_file=cmd_file)

                # Execute cmd file:
                logging.info(msg=f' Executing cmd file {file_name}')
                process = subprocess.Popen(cmd_file,
                                           stdout=subprocess.PIPE,
                                           stderr=subprocess.PIPE,
                                           encoding='utf-8')
                stdout, stderr = process.communicate()

                if stderr:
                    # Report errors count:
                    error_list = stderr.split("\n")
                    for line in error_list:
                        if line == "":
                            error_list.remove(line)
                    logging.critical(msg=f' Cmd file {file_name} execution failed')
                    logging.critical(msg=f' Errors in total: {len(error_list)}')

                    # Report 'File not found errors' count:
                    nf_counter = 0
                    for line in error_list:
                        if 'cannot find the file' in line:
                            nf_counter += 1
                    if nf_counter:
                        logging.critical(msg=f' Cannot find the file errors: {nf_counter}')

                    return
                else:
                    logging.info(msg=f' Cmd file {file_name} executed successfully')
                    executed += 1
                    continue
            else:
                logging.critical(msg=f' Cmd file not found in {cmd_file} path')
                raise FileNotFoundError(f' Cmd file not found in {cmd_file} path')

        if executed == len(cmd_files):
            logging.info(msg=f' Checksum passed: All cmd files executed successfully')
            return True
        else:
            logging.critical(msg=f' Checksum failed: Cmd files execution failed')
            return False

    def upload_dossiers(self, files: list, sftp_prod=True):
        """
        Upload dossier files to SFTP server.
        :param files: files to be uploaded to SFTP server
        :param sftp_prod: True by default, set to False if you want to upload to Test folder during testing phase
        :return: True if all files uploaded
        :rtype: bool
        """
        logging.info(msg=f' Starting the uploading process')
        sftp_folder = cfg.SFTP_DIR if sftp_prod else cfg.SFTP_TEST_DIR

        sftp = SftpHandle()
        sftp.mk_dir(remote_path=f'{sftp_folder}/{self.customer_dir}')
        logging.info(msg=f' Customer folder {self.customer_dir} created inside {sftp_folder} folder')
        uploaded = 0

        for file_path in files:
            # split path:
            root_path, file_name = os.path.split(file_path)
            # upload file to sftp:
            logging.info(msg=f' Uploading file {file_name}...')
            sftp.upload(local_path=file_path, remote_path=f'{sftp_folder}/{self.customer_dir}/{file_name}')

            # check if file uploaded:
            sftp_content = sftp.ls_dir(remote_path=f'/{sftp_folder}/{self.customer_dir}/')
            if file_name in sftp_content:
                logging.info(msg=f' File {file_name} successfully uploaded to {sftp_folder}/{self.customer_dir}')
                uploaded += 1
            else:
                logging.critical(msg=f' Uploading failed for {file_name} Error: No such file found')
                break

        if uploaded == len(files):
            logging.info(msg=f' Checksum passed: All files successfully uploaded to the SFTP server')
            logging.info(msg=f' Uploading process finished')
            return True
        else:
            logging.critical(msg=f" Checksum failed: Uploading process failed")
            raise AssertionError(f" Checksum failed: Uploading process failed")

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

        sftp = SftpHandle()
        sftp.mk_dir(remote_path=f'{sftp_folder}/{self.customer_dir}')
        logging.info(msg=f' Customer folder {self.customer_dir} created inside {sftp_folder} folder')

        # split path:
        root_path, file_name = os.path.split(file)
        # upload file to sftp:
        logging.info(msg=f' Uploading file {file_name}...')
        sftp.upload(local_path=file, remote_path=f'{sftp_folder}/{self.customer_dir}/{file_name}')

        # check if file uploaded:
        sftp_content = sftp.ls_dir(remote_path=f'/{sftp_folder}/{self.customer_dir}/')
        if file_name in sftp_content:
            logging.info(msg=f' File {file_name} successfully uploaded to sftp folder {sftp_folder}/{self.customer_dir}')
        else:
            logging.critical(msg=f' Uploading process failed for {file_name} file')
            raise FileNotFoundError(f' Uploading process failed for {file_name} file')

        logging.info(msg=f' Uploading process finished')
        return True

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
