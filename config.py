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
MLM_LOGFILE = 'robot_log_mlm.log'

# DELIMITER:
DELIMITER = '#'*100

# KPI:
ID_CUSTOMER = 'NULL'
ID_ROBOT = 'IPA-0019'
ID_PROCESS = 'BNL-0003'
ID_QUEUE = 0
RUNTIME_HOSTNAME = 'NL-D-RAET00000'
TABLE_TEST = 'prod-amkpi-cm.transaction_items_kpi.TEST_TRANSACTION_ITEMS'
TABLE_PROD = 'prod-amkpi-cm.transaction_items_kpi.TRANSACTION_ITEMS'
TABLE_MONITORING = 'prod-amkpi-cm.automate_monitoring.monitoring'
SCOPE = 'https://www.googleapis.com/auth/cloud-platform'

# ROBOT SLACK CHANNELS:
SLACK_PROD_CHANNEL = '#ipa-mig-raet-reports'
SLACK_TEST_CHANNEL = '#ipa-test-reports'

# PENTAHO PATH:
PENTAHO_DIR = 'C:\\Pentaho\\data-integration'

# MIGVISMA PATH:
MIG_ROOT = 'D:\\MigVisma'

# ROBOT MIGRATION BATCH SCRIPT:
MIG_BATCH_SCRIPT = 'MigrationTool_Robot.bat'

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

# MLM:
MIG_JOB_MLM = '5_MIGRATE_Medicalleave_dossiers.kjb'
MIG_LOG_MLM = 'MigrationTool5.log'
MIG_DOSLOG_MLM = 'MigrationTool5.doslog'

# 7-ZIP PATH:
SEVEN_ZIP_PATH = 'C:\\Program Files\\7-Zip'

# RESERVED DIRECTORIES:
RESERVED_DIRS = ['Templates', 'Transformations', 'MappingFixedAllowances']

# CREDENTIALS:
CREDENTIALS = 'credentials.json'

# CREDENTIAL ITEMS:
SLACK_PROD_CREDS = 'Slack_prod_credentials'
SLACK_TEST_CREDS = 'Slack_test_credentials'
SFTP_CREDS = 'Visma_Raet_SFTP'
GCP_SECRET = 'GCP_SECRET'
