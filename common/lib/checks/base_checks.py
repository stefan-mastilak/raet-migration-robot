# REF: stefan.mastilak@visma.com

import logging
import config as cfg
from os import path


def pentaho_path_check():
    """
    Check if Pentaho installation path exists
    :return: True if Pentaho installation path exists
    """
    if path.exists(cfg.PENTAHO_DIR):
        return True
    else:
        logging.fatal(msg=f' Pentaho installation path {cfg.PENTAHO_DIR} doesnt exist')
        raise NotADirectoryError(f' Path doesnt exist: {cfg.PENTAHO_DIR}')


def mig_base_path_check():
    """
    Check if MigVisma folder exists.
    :return: True if MigVisma folder exists
    """
    if path.exists(cfg.MIG_ROOT):
        return True
    else:
        logging.error(msg=f' MigVisma path {cfg.PENTAHO_DIR} doesnt exist')
        raise NotADirectoryError(f' Path doesnt exist: {cfg.PENTAHO_DIR}')


def kitchen_bat_check():
    """
    Check if Kitchen.bat script exists in Pentaho installation directory.
    :return: True if Kitchen.bat script exists
    """
    kitchen_path = f'{cfg.PENTAHO_DIR}\\Kitchen.bat'

    if path.isfile(kitchen_path):
        return True
    else:
        logging.critical(msg=f' Kitchen.bat script doesnt exist in {kitchen_path}')
        raise FileNotFoundError(f' File doesnt exist: {kitchen_path}')


def mig_dir_check(customer_dir):
    """
    Check if customer dir path exists.
    :param customer_dir: customer directory name
    :return: True if directory exists
    """
    customer_folder = f'{cfg.MIG_ROOT}\\{customer_dir}'

    if path.exists(customer_folder):
        return True
    else:
        logging.critical(msg=f' Customer directory doesnt exist in {customer_folder}')
        raise NotADirectoryError(f' Path doesnt exist: {customer_folder}')


def not_reserved_check(customer_dir):
    """
    Check if customer dir name is not in reserved names list stored in config.py
    :param customer_dir: customer directory name
    :return: True if customer directory name is in reserved names list.
    """
    if customer_dir not in cfg.RESERVED_DIRS:
        return True
    else:
        logging.critical(msg=f' Customer folder {customer_dir} is in the reserved list {cfg.RESERVED_DIRS}')
        raise NameError(f' Folder {customer_dir} is in the reserved list')


def mig_props_check(customer_dir):
    """
    Check if customer dir has config.properties file inside.
    :param customer_dir: customer directory name
    :return: True if file exists
    """
    props_path = f'{cfg.MIG_ROOT}\\{customer_dir}\\config.properties'

    if path.isfile(props_path):
        return True
    else:
        logging.critical(msg=f' File config.properties doesnt exists in {props_path}')
        raise FileNotFoundError(f' File doesnt exists: {props_path}')


def mig_params_check(customer_dir):
    """
    Check if customer dir has MigVisma_parameters.xlsx file inside.
    :param customer_dir: customer directory name
    :return: True if file exists
    """
    params_path = f'{cfg.MIG_ROOT}\\{customer_dir}\\MigVisma_parameters.xlsx'

    if path.isfile(params_path):
        return True
    else:
        logging.critical(msg=f' File MigVisma_parameters.xlsx doesnt exists in {params_path}')
        raise FileNotFoundError(f' File doesnt exists: {params_path}')