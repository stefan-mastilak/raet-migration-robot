# REF: stefan.mastilak@visma.com

"""
Project constants
"""
from pathlib import Path

# ROBOT MAIN DIR PATH:
ROOT_DIR = Path(__file__).parent.__str__()

# ROBOT LOGFILE:
PDOL_LOGFILE = 'robot_log_pdol.log'
SDOL_LOGFILE = 'robot_log_sdol.log'

# DELIMITER:
DELIMITER = '#'*100

# ROBOT SLACK CHANNEL:
CHANNEL = '#ipa-mig-raet-reports'

# PENTAHO PATH:
PENTAHO_DIR = 'C:\\Pentaho\\data-integration'

# MIGVISMA PATH:
MIG_ROOT = 'D:\\MigVisma'

# SFTP FOLDERS:
SFTP_TEST_DIR = 'robot_test_files'
SFTP_DIR = 'robot_files'

# PDOL:
MIG_JOB_PDOL = '7_MIGRATE_PDOL.kjb'
MIG_LOG_PDOL = 'MigrationTool7.log'
MIG_DOSLOG_PDOL = 'MigrationTool7.doslog'

# SDOL:
MIG_JOB_SDOL = '6_MIGRATE_SDOL.kjb'
MIG_LOG_SDOL = 'MigrationTool6.log'
MIG_DOSLOG_SDOL = 'MigrationTool6.doslog'

# 7-ZIP PATH:
SEVEN_ZIP_PATH = 'C:\\Program Files\\7-Zip'

# RESERVED DIRECTORIES:
RESERVED_DIRS = ['Templates', 'Transformations', 'MappingFixedAllowances']

# LASTPASS:
LP_MASTER_PWD = 'lp_pwd.txt'
LP_FOLDER = 'PROD_VISMA_RAET'
LP_USER_ACC = 'cipo.robot@visma.com'
LP_SLACK_CREDS = 'Raet_slack_credentials'
LP_SFTP_CREDS = 'Visma_Raet_SFTP'
