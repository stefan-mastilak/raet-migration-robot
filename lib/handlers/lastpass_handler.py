# REF: stefan.mastilak@visa.com

import logging
import lastpass
import config as cfg
from os import path
from retry import retry


def _get_master_pw():
    """
    Get LastPass master password that is stored locally on the server under the robot project
    directory (stored in file lp_pwd.txt).
    File storing the lastpass master pwd is added to .gitignore and will not be visible anywhere except
    robot execution server.
    :return: lastpass master pwd
    """
    pw_path = path.join(cfg.ROOT_DIR, cfg.LP_MASTER_PWD_FILE)

    if path.isfile(pw_path):
        with open(pw_path) as f:
            pwd = f.read()
            return pwd
    else:
        logging.critical(msg=f' Unable to fetch LP master password from {pw_path}')
        raise FileNotFoundError(f' Unable to fetch LP master password from {pw_path}')


@retry(Exception, delay=10, tries=5)
def get_lp_creds(folder, item, user):
    """
    Get credentials from the LastPass secure password manager.
    :param folder: lastpass folder name - Name of LastPass folder where credentials are stored
    :param item: lastpass item name - Name of item we want to retrieve credentials for, as defined in LastPass
    :param user: User account for LastPass and KeyRing access
    :return: Credential properties as dictionary with 'name', 'url', 'username', 'password' and 'notes' as keys
    :rtype: dict
    """
    try:
        logging.info(msg=f' Calling LastPass. Folder: {folder}, Item: {item}')
        creds = None
        vault = lastpass.Vault.open_remote(username=user,
                                           password=_get_master_pw())

        for i in vault.accounts:
            if i.group.decode('utf-8') == folder and i.name.decode('utf-8') == item:
                creds = {'name': i.name.decode('utf-8'),
                         'url': i.url.decode('utf-8'),
                         'username': i.username.decode('utf-8'),
                         'password': i.password.decode('utf-8'),
                         'notes': i.notes.decode('utf-8')}
                break
    except Exception as error:
        logging.error(msg=f' Error while calling the lastpass.\n Message: {error}')
        raise
    else:
        logging.info(msg=f' Credentials for {item} obtained successfully')
        return creds
