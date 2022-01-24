# REF: stefan.mastilak@visma.com

import glob
import logging
import config as cfg
import subprocess
from os import path, listdir
import time


def find_sfx_files(customer_dir, mig_type):
    """
    Find sfx files in customer SDOL folder (sfx file is .exe file that will extract zip files)
    :param customer_dir: customer directory
    :param mig_type: migration type (like PDOL, SDOL, etc..)
    :return: sfx files paths
    """
    if mig_type == 'PDOL':
        sfx_files = glob.glob(f'{cfg.MIG_ROOT}\\{customer_dir}\\{mig_type}\\*ExportPersonnelFile*.exe')
    elif mig_type == 'SDOL':
        sfx_files = glob.glob(f'{cfg.MIG_ROOT}\\{customer_dir}\\{mig_type}\\*ExportPayrollFile*.exe')
    else:
        logging.error(f' Not implemented migration type: {mig_type}')
        raise NotImplementedError(f' Not implemented migration type: {mig_type}')

    if sfx_files:
        return sfx_files
    else:
        logging.critical(msg=f' No SFX files found in {customer_dir}')
        raise FileNotFoundError(f' No SFX files found in {customer_dir}')


def unpack_files(customer_dir, mig_type, docs_dir, pwd):
    """
    Find SFX files that will be used for unzipping all files to the SDOL DOCS folder.
    :param customer_dir: customer directory
    :param mig_type: migration type (like PDOL, SDOL, etc..)
    :param docs_dir: DOCS folder directory - unzipping destination folder
    :param pwd: password for unpacking files
    """
    sfx_files = find_sfx_files(customer_dir=customer_dir, mig_type=mig_type)
    logging.info(msg=' Unzipping files into DOCS folder')

    for index, sfx in enumerate(sfx_files):
        root_path, sfx_file_name = path.split(sfx)
        # Execute SFX file:
        logging.info(msg=f' Processing 7z sfx file: {sfx_file_name}')
        # Check unzipping progress only for the first sfx file:
        if index == 0:
            unzip_result = run_7z_unpack(sfx_path=sfx, pwd=pwd, out_dir=docs_dir, check_progress=True)
        else:
            unzip_result = run_7z_unpack(sfx_path=sfx, pwd=pwd, out_dir=docs_dir, check_progress=False)
        out = unzip_result[0]
        err = unzip_result[1]
        if err:
            logging.critical(msg=f' Processing of {sfx_file_name} finished with errors: {err}')
            return
        else:
            logging.info(msg=f' Sfx processed successfully')
    logging.info(msg=' Files unzipped into the DOCS folder')
    return True


def run_7z_unpack(sfx_path, out_dir, pwd, check_progress):
    """
    Unzip password protected 7zip files by executing sfx .exe file with python subprocess lib.
    :param sfx_path: full path to sfx file
    :param out_dir: unzip destination folder
    :param pwd: password to access zip file
    :param check_progress: True/False
            - compare size of output dir before unzipping and during the unzipping process
            - just to know if the unzipping started or not.
            - it's not possible to get console output when running 7zip SFX .exe file with subprocess.popen
            - so check progress control will just verify if unzipping started (if not - there's problem with password)
    :return: stdout, stderr
    """
    cmd = [sfx_path, '-y', '-gm2', '-r', f'-p{pwd}', f'-o{out_dir}']

    initial_size = (sum(path.getsize(out_dir) for f in listdir('.') if path.isfile(f)))

    with subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE) as process:
        if check_progress:
            retry = 10
            while retry:
                time.sleep(3)
                actual_size = (sum(path.getsize(out_dir) for f in listdir('.') if path.isfile(f)))
                if actual_size > initial_size:
                    break
                else:
                    retry = retry - 1
                if retry == 0:
                    logging.critical(f' Unzipping process failed')
                    process.terminate()
                    return None, 'Unzipping process stopped - Wrong password or corrupted file'

        stdout, stderr = process.communicate()

    return stdout.decode('utf-8'), stderr.decode('utf-8')


def run_7z_pack(folder, pwd):
    """
    Zip specific folder with password
    :param folder: customer directory
    :param pwd: folder full-path
    :return: stdout, stderr, zipfile path
    """
    cmd = [f'{cfg.SEVEN_ZIP_PATH}\\7z', 'a', f'{folder}.7z', f'{folder}', f'-p{pwd}', '-mhe=on']
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = process.communicate()

    return stdout.decode('utf-8'), stderr.decode('utf-8'), f'{folder}.7z'
