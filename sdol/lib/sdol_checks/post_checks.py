# REF: stefan.mastilak@visma.com

import logging
import sdol.lib.sdol_handlers.counters_handler as counter


def chsum_counters_vs_cmd(customer_dir, mig_type):
    """
    Compare count of '_Migrated' customer files from Counters.csv with number of 'move' rows from customer .cmd file
    :param customer_dir: customer directory
    :param mig_type: migration type (PDOL, SDOL, etc.)
    :return: comparison result
    :rtype: bool
    """
    counters_count = counter.get_migrated_count(customer_dir=customer_dir, mig_type=mig_type)
    cmd_count = counter.get_cmd_rows_count(customer_dir=customer_dir, mig_type=mig_type)
    error = False

    for uid, count in counters_count.items():
        for _uid, _count in cmd_count.items():
            if uid == _uid:
                if count == _count:
                    logging.info(msg=f' Checksum for customer ID {uid} passed.'
                                     f' Cmd count: {_count}, Counters count: {count}')
                else:
                    logging.error(msg=f' Checksum for customer ID {uid} failed.'
                                      f' Cmd count: {_count}, Counters count: {count}')
                    error = True
    if not error:
        logging.info(msg=f' Checksum passed: Counters count matches cmd file rows')
        return True
    else:
        logging.critical(msg=f' Checksum failed: Counters count doesnt match cmd file rows')
        return False


def chsum_counters_vs_dossiers(customer_dir, mig_type):
    """
    :param customer_dir: customer directory
    :param mig_type: migration type (PDOL, SDOL, etc.)
    :return: comparison result (counters count vs dossiers count)
    :rtype: bool
    """
    counters_count = counter.get_migrated_count(customer_dir=customer_dir, mig_type=mig_type)
    dossiers_count = counter.get_dossiers_count(customer_dir=customer_dir, mig_type=mig_type)
    error = False

    for uid, count in counters_count.items():
        for _uid, _count in dossiers_count.items():
            if uid == _uid:
                if count == _count:
                    logging.info(msg=f' Checksum for customer ID {uid} passed.'
                                     f' Dossiers count: {_count}, Counters count: {count}')
                else:
                    logging.error(msg=f' Checksum for customer ID {uid} failed.'
                                      f' Dossiers count: {_count}, Counters count: {count}')
                    error = True
    if not error:
        logging.info(msg=f' Checksum passed: All dossier files moved correctly')
        return True
    else:
        logging.critical(msg=f' Checksum failed: Some dossier files not moved')
        return False
