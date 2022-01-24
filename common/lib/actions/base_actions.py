# REF: stefan.mastilak@visma.com

from common.lib.handlers.zip_handler import run_7z_pack
from common.lib.handlers.sftp_handler import SftpHandle
import config as cfg
import logging
from os import path, mkdir, rename
import time


def create_log_dir(customer_dir):
    """
    Create migration 'Log' directory inside customer's folder if doesn't already exist.
    :param customer_dir: customer directory name
    :return: log folder path
    """
    log_dir = f'{cfg.MIG_ROOT}\\{customer_dir}\\Log'

    if path.exists(log_dir):
        logging.info(msg=f' Log folder already exists in {log_dir}')
        return log_dir
    else:
        try:
            mkdir(log_dir)
        except Exception as err:
            logging.error(msg=f' Failed to create log folder in {log_dir}\n Error:{err}')
            raise OSError(f' Failed to create log folder in {log_dir}')
        else:
            logging.info(msg=f' Log folder created')
            return log_dir


def create_docs_dir(customer_dir, mig_type):
    """
    Create DOCS folder inside customer folder if doesn't already exist.
    :return: docs folder path
    """
    docs_folder = f'{cfg.MIG_ROOT}\\{customer_dir}\\{mig_type}\\DOCS'

    if path.exists(docs_folder):
        logging.info(msg=f' DOCS folder already exists in {docs_folder}')
        return docs_folder
    else:
        try:
            mkdir(docs_folder)
        except Exception as err:
            logging.error(msg=f' Failed to create docs folder in {docs_folder}\n Error: {err}')
            raise OSError(f' Failed to create docs folder in {docs_folder}')
        else:
            logging.info(msg=f' DOCS folder created')
            return docs_folder


def create_index_dir(customer_dir, mig_type):
    """
    Create index folder inside customer folder if it doesn't already exist.
    :param customer_dir: customer directory
    :param mig_type: migration type (like PDOL, SDOL, etc..)
    :return: index folder path
    """
    idx_folder = f'{cfg.MIG_ROOT}\\{customer_dir}\\{mig_type}\\index'

    if path.exists(idx_folder):
        logging.info(msg=f' Index folder already exists in {idx_folder}')
        return idx_folder
    else:
        try:
            mkdir(idx_folder)
        except Exception as err:
            logging.error(msg=f' Failed to create index folder in {idx_folder}\n Error: {err}')
            raise OSError(f' Failed to create index folder in {idx_folder}')
        else:
            logging.info(msg=f' Index folder created')
            return idx_folder


def get_zip_pwd(customer_dir, mig_type):
    """"
    Read the zip/unzip password from the file in customer PDOL folder.
    :param customer_dir: customer directory
    :param mig_type: migration type (like PDOL, SDOL, etc..)
    :return: password if exists
    """
    path_alternate_1 = f'{cfg.MIG_ROOT}\\{customer_dir}\\{mig_type}\\PW.txt'
    path_alternate_2 = f'{cfg.MIG_ROOT}\\{customer_dir}\\{mig_type}\\PW.txt.txt'

    if path.isfile(path_alternate_1):
        logging.info(msg=f' Password found')
        with open(path_alternate_1) as f:
            pwd = f.read()
            return pwd
    else:
        if path.isfile(path_alternate_2):
            logging.info(msg=f' Password found')
            with open(path_alternate_2) as f:
                pwd = f.read()
                return pwd
        logging.critical(msg=f' Password file not found in {cfg.MIG_ROOT}\\{customer_dir}\\{mig_type}')
        raise FileNotFoundError(f' Password file not found in {cfg.MIG_ROOT}\\{customer_dir}\\{mig_type}')


def rename_if_success(customer_dir):
    """
    Rename customer directory like: {CustomerDirectory}_success_robot if PDOL migration succeeded.
    :param customer_dir: customer directory name
    :return: None
    """
    old = f'{cfg.MIG_ROOT}\\{customer_dir}'
    new = f'{cfg.MIG_ROOT}\\{customer_dir}_success_robot'

    if path.isdir(old):
        retry = 3
        while retry:
            time.sleep(0.5)
            try:
                rename(src=old, dst=new)
                retry = 0
            except Exception as err:
                if retry:
                    retry = retry - 1
                else:
                    logging.critical(msg=f' Renaming process failed for directory {customer_dir}\n Error: {err}')
                    raise PermissionError(f' Renaming of {customer_dir} failed')
    else:
        raise NotADirectoryError(f' Directory {customer_dir} doesnt exist.')


def rename_if_failed(customer_dir):
    """
    Rename customer directory like: {CustomerDirectory}_failed_robot if PDOL migration failed.
    :param customer_dir: customer directory name
    :return: None
    """
    old = f'{cfg.MIG_ROOT}\\{customer_dir}'
    new = f'{cfg.MIG_ROOT}\\{customer_dir}_failed_robot'

    if path.isdir(old):
        retry = 3
        while retry:
            time.sleep(0.5)
            try:
                rename(src=old, dst=new)
                retry = 0
            except Exception as err:
                if retry:
                    retry = retry - 1
                else:
                    logging.critical(msg=f' Renaming process failed for customer directory {customer_dir}\n Error: {err}')
                    raise PermissionError(f' Renaming of {customer_dir} failed')
    else:
        raise NotADirectoryError(f' Directory {customer_dir} doesnt exist.')


def zip_dossier_dirs(dossier_dirs, pwd):
    """
    Zip e-dossiers with password.
    :param dossier_dirs: list of e-dossier directories
    :param pwd: password
    :return: zipped files list
    """
    logging.info(msg=f' Starting the zipping process for e-dossiers')
    zipped_files = []

    for doss_path in dossier_dirs:
        # split path:
        doss_root_path, doss_file_name = path.split(doss_path)

        logging.info(msg=f' Zipping of {doss_file_name} in progress..')
        pack_result = run_7z_pack(folder=doss_path, pwd=pwd)
        out = pack_result[0]
        errs = pack_result[1]
        zipfile = pack_result[2]
        if errs:
            logging.critical(msg=f' Zipping of {doss_file_name} failed.')
            return
        else:
            logging.info(f' Zipping of {doss_file_name} done')
            zipped_files.append(zipfile)

    logging.info(msg=f' Zipping finished successfully')
    return zipped_files


def upload_dossiers(files, customer_dir, sftp_prod=True):
    """
    Upload files to SFTP server.
    :param files: files to be uploaded to SFTP server
    :param customer_dir: customer directory
    :param sftp_prod: True by default, set to False if you want to upload to Test folder during testing phase
    :return:
    """
    sftp_folder = cfg.SFTP_DIR if sftp_prod else cfg.SFTP_TEST_DIR

    sftp = SftpHandle()
    sftp.mk_dir(remote_path=f'{sftp_folder}/{customer_dir}')
    logging.info(msg=f' Customer folder {customer_dir} created inside {sftp_folder} folder')

    for file_path in files:
        # split path:
        root_path, file_name = path.split(file_path)
        # upload file to sftp:
        logging.info(msg=f' Uploading file {file_name}...')
        sftp.upload(local_path=file_path, remote_path=f'{sftp_folder}/{customer_dir}/{file_name}')

        # check if file uploaded:
        sftp_content = sftp.ls_dir(remote_path=f'/{sftp_folder}/{customer_dir}/')
        if file_name in sftp_content:
            logging.info(msg=f' File {file_name} successfully uploaded to sftp folder {sftp_folder}/{customer_dir}')
        else:
            logging.critical(msg=f' Uploading failed for {file_name} file - no such file found')
            break

    return True
