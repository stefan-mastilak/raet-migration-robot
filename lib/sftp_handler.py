# REF: stefan.mastilak@visma.com

import config as cfg
import logging
import pysftp
from lib.credentials_handler import get_credentials
from retry import retry


class SftpHandle(object):
    """
    SFTP handler class.
    Credentials for SFTP are obtained from LastPass secure password manager.
    """

    def __init__(self):
        # Collect SFTP credentials from secure password manager:
        credentials = get_credentials(item=cfg.SFTP_CREDS)
        self.host = credentials.get('notes')
        self.user = credentials.get('username')
        self.pwd = credentials.get('password')
        self.cnopts = pysftp.CnOpts()
        self.cnopts.hostkeys = None
        self.sftp = None

        # Establish the SFTP connection
        self.connect()

    @retry(Exception, delay=20, tries=3)
    def connect(self):
        """Establish the SFTP connection."""
        try:
            self.sftp = pysftp.Connection(host=self.host, username=self.user, password=self.pwd, cnopts=self.cnopts)
        except Exception as error:
            logging.warning(msg=f" Connection to sftp failed due to error: {error}")

    def disconnect(self):
        """Close the SFTP connection."""
        if self.sftp:
            self.sftp.close()

    def __enter__(self):
        """For use in a with statement."""
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """For use in a with statement. Closes the connection when exiting the 'with' block."""
        self.disconnect()

    def ls_dir(self, remote_path):
        """
        Return a list of files/directories for the given remote path.
        :param remote_path: path to list on the server (path format like: /path/to/list)
        :return: list result
        """
        if self.sftp.exists(remote_path):
            return self.sftp.listdir(remote_path)
        else:
            logging.critical(msg=f'Remote path {remote_path} doesnt exist')
            raise AssertionError(f' Remote path {remote_path} doesnt exist')

    def mk_dir(self, remote_path, mode=777):
        """
        Create a directory and all it's sub-folders defined in remote_dir path on the sftp server.
        :param remote_path: path to create (path format like: /path/to/create)
        :param mode: int mode: *Default: 777* - int representation of octal mode for directory
        :return:
        """
        if not self.sftp.exists(remote_path):
            self.sftp.makedirs(remotedir=remote_path, mode=mode)
            logging.info(msg=f" Folder {remote_path} created")
        else:
            logging.error(f' Remote dir {remote_path} already exists')
            raise AssertionError(f' Remote dir {remote_path} already exists')

    def rm_dir(self, remote_path):
        """
        Remove remote directory (only if it's empty).
        :param str remote_path: the remote directory to remove (path format like: /path/to/dir)
        :returns: None
        """
        if self.sftp.exists(remote_path):
            self.sftp.rmdir(remotepath=remote_path)
        else:
            logging.critical(msg=f'Remote dir {remote_path} doesnt exist')
            raise AssertionError(f' Remote path {remote_path} doesnt exist')

    def rm_file(self, remote_path):
        """
        Remove remote file from the sftp server.
        :param remote_path: remote file path (path format like: /path/to/file)
        :return: None
        """
        if self.sftp.exists(remotepath=remote_path):
            self.sftp.remove(remotefile=remote_path)
        else:
            logging.critical(msg=f'Remote file {remote_path} doesnt exist')
            raise AssertionError(f' Remote file {remote_path} doesnt exist')

    def upload(self, local_path, remote_path):
        """
        Upload file to the SFTP server.
        :param local_path: local path to file
        :param remote_path: remote path (path format like: /path/to/file)
        :return:
        """
        if not self.sftp.exists(remote_path):
            try:
                self.sftp.put(localpath=local_path, remotepath=remote_path)
            except Exception as err:
                logging.critical(msg=f' Uploading failed with error: {err}')
                raise err
            # verify if uploaded filepath exists:
            if self.sftp.exists(remote_path):
                return True
            else:
                return
        else:
            logging.critical(msg=f' Remote file {remote_path} already exists')
            raise AssertionError(f' Remote file {remote_path} already exists')
