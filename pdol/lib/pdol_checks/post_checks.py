# REF: stefan.mastilak@visma.com

import logging
import config as cfg
from pdol.lib.pdol_handlers.counters_handler import get_dossiers_count
from pdol.lib.pdol_handlers.cmd_handler import get_cmd_rows_count


def checksum_cmd_vs_dossiers(customer_dir):
    """
    Post-Checksum: Check if number of e-dossier files is the same as cmd file rows divided by two.
    NOTE: Since we added one extra line to the .cmd file to specify te encoding, we need to subtract 1 from cmd_count
    :param customer_dir: customer directory
    :return: checksum result
    """
    eds_count = get_dossiers_count(customer_dir)
    cmd_count = get_cmd_rows_count(customer_dir)

    if eds_count:
        if eds_count == ((cmd_count-1)/2):
            logging.info(msg=f' Checksum passed: Dossiers count: {eds_count}. Cmd count: {(cmd_count-1)/2}')
            return True
        else:
            logging.critical(msg=f' Checksum failed: {((cmd_count-1)/2) - eds_count} dossiers are missing')
            return False
    else:
        logging.critical(msg=f' No e-dossier files found inside {cfg.MIG_ROOT}\\{customer_dir}\\PDOL folder')
        return False
