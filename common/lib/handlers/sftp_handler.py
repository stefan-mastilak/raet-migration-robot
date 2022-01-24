# REF: stefan.mastilak@visma.com

import config as cfg
import logging
from common.lib.handlers.lastpass_handler import get_lp_creds
import pysftp


class SftpHandle(object):
    """
    SFTP handler class. Credentials for SFTP are obtained from LastPass secure password manager.
    """

    def __init__(self):
        # Collect SFTP credentials from secure password manager:
        credentials = get_lp_creds(folder=cfg.LP_FOLDER, item=cfg.LP_SFTP_CREDS, user=cfg.LP_USER_ACC)
        self.host = credentials.get('notes')
        self.user = credentials.get('username')
        self.pwd = credentials.get('password')
        self.cnopts = pysftp.CnOpts()
        self.cnopts.hostkeys = None

    def ls_dir(self, remote_path):
        """
        Return a list of files/directories for the given remote path.
        :param remote_path: path to list on the server (path format like: /path/to/list)
        :return: list result
        """
        with pysftp.Connection(host=self.host, username=self.user, password=self.pwd, cnopts=self.cnopts) as sftp:
            if sftp.exists(remote_path):
                return sftp.listdir(remote_path)
            else:
                logging.critical(msg=f'Remote path {remote_path} doesnt exist')
                raise OSError(f' Remote path {remote_path} doesnt exist')

    def mk_dir(self, remote_path, mode=777):
        """
        Create a directory and all it's sub-folders defined in remote_dir path on the sftp server.
        :param remote_path: path to create (path format like: /path/to/create)
        :param mode: int mode: *Default: 777* - int representation of octal mode for directory
        :return:
        """
        with pysftp.Connection(host=self.host, username=self.user, password=self.pwd, cnopts=self.cnopts) as sftp:
            if not sftp.exists(remote_path):
                sftp.makedirs(remotedir=remote_path, mode=mode)
            else:
                logging.error(f' Remote dir {remote_path} already exists')
                raise OSError(f' Remote dir {remote_path} already exists')

    def rm_dir(self, remote_path):
        """
        Remove remote directory (only if it's empty).
        :param str remote_path: the remote directory to remove (path format like: /path/to/dir)
        :returns: None
        """
        with pysftp.Connection(host=self.host, username=self.user, password=self.pwd, cnopts=self.cnopts) as sftp:
            if sftp.exists(remote_path):
                sftp.rmdir(remotepath=remote_path)
            else:
                logging.critical(msg=f'Remote dir {remote_path} doesnt exist')
                raise OSError(f' Remote path {remote_path} doesnt exist')

    def rm_file(self, remote_path):
        """
        Remove remote file from the sftp server.
        :param remote_path: remote file path (path format like: /path/to/file)
        :return: None
        """
        with pysftp.Connection(host=self.host, username=self.user, password=self.pwd, cnopts=self.cnopts) as sftp:
            if sftp.exists(remotepath=remote_path):
                sftp.remove(remotefile=remote_path)
            else:
                logging.critical(msg=f'Remote file {remote_path} doesnt exist')
                raise OSError(f' Remote file {remote_path} doesnt exist')

    def upload(self, local_path, remote_path):
        """
        Upload file to the SFTP server.
        :param local_path: local path to file
        :param remote_path: remote path (path format like: /path/to/file)
        :return:
        """
        with pysftp.Connection(host=self.host, username=self.user, password=self.pwd, cnopts=self.cnopts) as sftp:
            if not sftp.exists(remote_path):
                try:
                    sftp.put(localpath=local_path, remotepath=remote_path)
                except Exception as err:
                    logging.critical(msg=f' Uploading failed with error: {err}')
                    raise err
                # verify if uploaded filepath exists:
                if sftp.exists(remote_path):
                    return True
                else:
                    return
            else:
                logging.critical(msg=f' Remote file {remote_path} already exists')
                raise OSError(f' Remote file {remote_path} already exists')
