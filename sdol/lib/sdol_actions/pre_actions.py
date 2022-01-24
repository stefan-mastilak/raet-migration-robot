# REF: stefan.mastilak@visma.com

import glob
import logging
from sdol.lib.sdol_checks.pre_checks import mig_sdol_dir_check
import shutil
import os
import zipfile


def unzip_docs_files(customer_dir, docs_dir):
    """
    Unzip all of the.zip files inside DOCS folder. It's needed because when we execute sfx files, we will get set of
    zip files inside DOCS folder, which needs to be extracted as well
    in order to be able to manipulate with it's content.
    :param customer_dir: customer directory
    :param docs_dir: customer DOCS directory
    :return: True if all zip files unzipped successfully
    """
    if mig_sdol_dir_check(customer_dir=customer_dir):
        zip_files = glob.glob(f'{docs_dir}\\*.zip', recursive=True)

        if zip_files:
            logging.info(msg=f' Unzipping archives inside DOCS folder')
            for file in zip_files:
                root, ext = os.path.splitext(file)
                with zipfile.ZipFile(file, "r") as zip_ref:
                    zip_ref.extractall(path=root)

            logging.info(msg=f' All archives unzipped successfully')
            return True

        else:
            logging.critical(msg=f' No zip files found in {docs_dir}')
            raise FileNotFoundError(f' No zip files found in {docs_dir}')


def move_index_files(customer_dir, docs_dir, index_dir):
    """
    Rename and move all index files from DOCS folder into the index folder.
    :param customer_dir: customer directory
    :param docs_dir: customer DOCS directory
    :param index_dir: index directory
    :return:
    """
    if mig_sdol_dir_check(customer_dir=customer_dir):
        index_files = glob.glob(f'{docs_dir}\\**\\index.xml', recursive=True)

        if index_files:
            counter = 0
            for file in index_files:
                root_path, file_name = os.path.split(file)
                new_file_name = f'{root_path}\\{os.path.basename(os.path.dirname(file))}-index.xml'

                # rename index file:
                try:
                    os.rename(src=file, dst=new_file_name)
                except Exception as err:
                    logging.critical(f' Renaming process failed for {file}\n Error: {err}')
                    raise OSError(f' Renaming process failed for {file}')

                # move index file:
                try:
                    shutil.move(src=new_file_name, dst=index_dir)
                except Exception as err:
                    logging.critical(f' Moving of {file} failed\n Error: {err}')
                    raise OSError(f' Moving of {file} failed')
                counter += 1

            if counter != 1:
                logging.info(f' {counter} index files renamed and moved to index folder')
            else:
                logging.info(f' {counter} index file renamed and moved to index folder')
            return True
        else:
            logging.critical(msg=f' No index files found in {docs_dir}')
            raise FileNotFoundError(f' No index files found in {docs_dir}')
