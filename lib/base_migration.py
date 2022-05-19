# REF: stefan.mastilak@visma.com

import config as cfg
import os


def get_unprocessed_dirs(mig_type: str):
    """
    Function for getting unprocessed migration dirs.
    It's considering reserved directories (config.py - RESERVED_DIRS).
    Unprocessed also means that directory name doesn't have '_processed_robot' or '_failed_robot' in its name.
    NOTE: Robot is renaming all processed files with name containing postfix '_robot'
    :param mig_type: migration type (PDOL, SDOL, MLM, etc..)
    :return: list of unprocessed <mig_type> directories
    """
    dirs = [i for i in os.listdir(cfg.MIG_ROOT) if os.path.isdir(os.path.join(cfg.MIG_ROOT, i))]
    # consider reserved directories:
    dirs = [i for i in dirs if i not in cfg.RESERVED_DIRS]
    # consider already processed directories:
    dirs = [i for i in dirs if '_robot' not in i]

    unprocessed = []
    for _dir in dirs:
        if os.path.exists(os.path.join(cfg.MIG_ROOT, _dir, mig_type.upper())):
            unprocessed.append(_dir)

    return unprocessed


class Migration(object):
    """
    Base Migration class.
    All specific migration types inherits from this class.
    """
    def __init__(self, customer_dir, mig_type, job_id):
        """
        :param customer_dir: customer directory
        :param mig_type: migration type (PDOL, SDOL, MLM, etc..)
        :param job_id: migration tool job ID (7 for PDOL, 6 for SDOL, 5 for MLM, etc..)
        """
        self.customer_dir = customer_dir
        self.mig_type = mig_type
        self.job_id = job_id
