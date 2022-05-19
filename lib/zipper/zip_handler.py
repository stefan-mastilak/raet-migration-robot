# REF: stefan.mastilak@visma.com

import config as cfg
from lib.base_migration import Migration
import glob
import logging
import subprocess
import os
import time


class Zipper(Migration):
    """
    7zip operations handling class containing common methods for all migration types.
    """
    @staticmethod
    def __get_size(folder: str):
        """
        Go trough folder path and return size of it's content (including it's sub-folders).
        :param folder: folder path
        :return: folder size
        :rtype: int
        """
        total = 0
        for dir_path, dir_names, filenames in os.walk(folder):
            for f in filenames:
                fp = os.path.join(dir_path, f)
                # skip if it is symbolic link
                if not os.path.islink(fp):
                    total += os.path.getsize(fp)
        return total

    def __find_sfx_files(self):
        """
        Find SFX files inside customer folder.
        NOTE: SFX file is an exe file that is used for extraction of zip archive.
        :return: SFX file paths
        :rtype: list
        """
        if self.mig_type == 'PDOL':
            sfx_files = glob.glob(os.path.join(cfg.MIG_ROOT, self.customer_dir, self.mig_type,
                                               '*ExportPersonnelFile*.exe'))
        elif self.mig_type == 'SDOL':
            sfx_files = glob.glob(os.path.join(cfg.MIG_ROOT, self.customer_dir, self.mig_type,
                                               '*ExportPayrollFile*.exe'))
        elif self.mig_type == 'MLM':
            sfx_files = []  # NOTE: MLM migration doesn't use sfx files
        else:
            logging.error(f' Unsupported migration type: {self.mig_type}')
            raise NotImplementedError(f' Unsupported migration type: {self.mig_type}')

        if sfx_files:
            return sfx_files
        else:
            logging.critical(msg=f' No SFX files found in {self.customer_dir}')
            raise FileNotFoundError(f' No SFX files found in {self.customer_dir}')

    def __find_split_archive_start(self):
        """
        Find split archive 'start file' in customer directory.
        It's something like: 'filename.zip.001' in the customer directory.
        :return: zip.001 file path
        :rtype: str
        """
        zip_files = glob.glob(os.path.join(cfg.MIG_ROOT, self.customer_dir, self.mig_type, '*.zip*'))

        if len(zip_files) == 1:
            return zip_files[0]
        elif len(zip_files) > 1:
            for file in zip_files:
                if 'zip.001' in file:
                    return zip_files[zip_files.index(file)]
                else:
                    logging.critical(msg=f' Unable to find split zip archive start in {self.customer_dir} folder')
                    raise FileNotFoundError(f' Unable to find split zip archive start in {self.customer_dir} folder')
        else:
            logging.critical(msg=f' No split zip archive files found in {self.customer_dir} folder')
            raise FileNotFoundError(f' No split zip archive files found in {self.customer_dir} folder')

    def __run_sfx(self, sfx_path: str, out_dir: str, pwd: str, check_progress: bool):
        """
        Unzip password protected 7zip files by executing sfx exe file with python subprocess library.
        :param sfx_path: full path to sfx file
        :param out_dir: unzip destination folder
        :param pwd: password to access zip file
        :param check_progress: True/False
                - compare size of output dir before unzipping and during the unzipping process
                - just to know if the unzipping started or not.
                - it's not possible to get console output when running 7zip SFX .exe file with subprocess.popen
                - so check progress control will just verify if unzipping started (if not there's problem with password)
        :return: stdout, stderr
        :rtype: tuple
        """
        cmd = [sfx_path, '-y', '-gm2', '-r', f'-p{pwd}', f'-o{out_dir}']
        initial_size = self.__get_size(out_dir)

        with subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE) as process:
            if check_progress:
                retry = 10
                while retry:
                    time.sleep(5)
                    actual_size = self.__get_size(out_dir)
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

    def __run_7zip_file(self, file_path: str, out_dir: str, pwd: str, check_progress: bool):
        """
        Run unzipping process for 7zip file.
        :param file_path: path to 7z file
        :param out_dir: output directory
        :param pwd: password
        :param check_progress: True/False
                - compare size of output dir before unzipping and during the unzipping process
                - just to know if the unzipping started or not
        :return: stdout, stderr
        :rtype: tuple
        """
        cmd = [os.path.join(cfg.SEVEN_ZIP_PATH, '7z'), 'x', file_path, '-y', '-r', f'-p{pwd}', f'-o{out_dir}']
        initial_size = self.__get_size(out_dir)

        with subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE) as process:
            if check_progress:
                retry = 10
                while retry:
                    time.sleep(5)
                    actual_size = self.__get_size(out_dir)
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

    @staticmethod
    def __zip_folder(folder: str, pwd: str):
        """
        Zip specific folder with password.
        :param folder: folder path
        :param pwd: password
        :return: stdout, stderr, 7zip_file_path
        :rtype: tuple
        """
        cmd = [os.path.join(cfg.SEVEN_ZIP_PATH, '7z'), 'a', f'{folder}.7z', f'{folder}', f'-p{pwd}', '-mhe=on']
        process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = process.communicate()

        return stdout.decode('utf-8'), stderr.decode('utf-8'), f'{folder}.7z'

    def unpack_sfx_archive(self, destination: str, pwd: str):
        """
        Unzip all customer files with SFX exe file.
        :param destination: unzipping destination path (should be DOCS folder for all migration types)
        :param pwd: password
        :return: True if all sfx files processed successfully
        :rtype: bool
        """
        root_path, destination_folder = os.path.split(destination)
        sfx_files = self.__find_sfx_files()
        logging.info(msg=' Starting the unzipping process')
        processed = 0

        for index, sfx in enumerate(sfx_files):
            root_path, file_name = os.path.split(sfx)
            # Execute SFX file:
            logging.info(msg=f' Processing 7z sfx file {file_name}')
            # Check unzipping progress only for the first sfx file:
            if index == 0:
                unzip_result = self.__run_sfx(sfx_path=sfx,
                                              pwd=pwd,
                                              out_dir=destination,
                                              check_progress=True)
            else:
                unzip_result = self.__run_sfx(sfx_path=sfx,
                                              pwd=pwd,
                                              out_dir=destination,
                                              check_progress=False)
            out = unzip_result[0]
            err = unzip_result[1]
            if err:
                logging.critical(msg=f' Processing of {file_name} finished with errors: {err}')
                return
            else:
                logging.info(msg=f' {file_name} processed successfully')
                processed += 1

        if processed == len(sfx_files):
            logging.info(msg=f' Checksum passed: All sfx files processed correctly')
            logging.info(msg=f' Unzipping process finished')
            logging.info(msg=f' Customer data were unzipped into the {destination_folder} folder')
            return True
        else:
            logging.critical(msg=f' Checksum failed: Unzipping process failed')
            return False

    def unpack_split_archive(self, destination: str, pwd: str):
        """
        Unzip all customer files using 7z split archive.
        :param destination: unzipping destination path (should be DOCS folder for all migration types)
        :param pwd: password
        :return: True if unzipping process finish successfully
        :rtype: bool
        """
        root_path, destination_folder = os.path.split(destination)
        start_file = self.__find_split_archive_start()
        logging.info(msg=' Starting the unzipping process')

        unzip_result = self.__run_7zip_file(file_path=start_file,
                                            out_dir=destination,
                                            pwd=pwd,
                                            check_progress=True)
        out = unzip_result[0]
        err = unzip_result[1]
        if err:
            logging.critical(msg=f' Processing of {start_file} finished with errors: {err}')
            logging.critical(msg=f' Unzipping process failed')
            return False
        else:
            logging.info(msg=f' Unzipping process finished')
            logging.info(msg=f' Customer data were unzipped into the {destination_folder} folder')
            return True

    def zip_dossiers(self, folders: list, pwd: str):
        """
        Zip multiple e-dossiers with password.
        :param folders: list of e-dossiers paths
        :param pwd: password
        :return: zipped files
        :rtype: list
        """
        logging.info(msg=f' Starting the zipping process')
        zipped = []

        for dossier in folders:
            # split path:
            doss_root_path, doss_file_name = os.path.split(dossier)

            logging.info(msg=f' Zipping of {doss_file_name} in progress')
            pack_result = self.__zip_folder(folder=dossier, pwd=pwd)
            out = pack_result[0]
            errs = pack_result[1]
            zipfile = pack_result[2]
            if errs:
                logging.critical(msg=f' Zipping of {doss_file_name} failed')
                return
            else:
                logging.info(f' Zipping of {doss_file_name} done')
                zipped.append(zipfile)

        if len(zipped) == len(folders):
            logging.info(msg=f' Checksum passed: All folders zipped successfully')
            logging.info(msg=f' Zipping process finished')
            return zipped
        else:
            logging.critical(msg=f' Checksum failed: Zipping process finished with errors')
            raise AssertionError(f' Checksum failed: Zipping process finished with errors')

    def zip_single_dossier(self, folder: str, pwd: str):
        """
        Zip single e-dossier folder with password.
        :param folder: e-dossier folder path
        :param pwd: password
        :return: zipped folder path
        """
        # split folder path:
        root_path, dir_name = os.path.split(folder)

        logging.info(msg=f' Starting the zipping process')
        logging.info(msg=f' Zipping of {dir_name} in progress')
        zip_result = self.__zip_folder(folder=folder, pwd=pwd)
        out = zip_result[0]
        err = zip_result[1]
        zipped = zip_result[2]

        if err:
            logging.critical(msg=f' Zipping of {dir_name} failed')
            return
        else:
            logging.info(msg=f' Zipping of {dir_name} done')
            logging.info(msg=f' Zipping process finished')
            return zipped
