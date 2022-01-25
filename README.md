## visma-raet-migration
Data migration robot for Visma Raet. Robot is transforming data exported from old vendor (Pentaho Data Integration ETL) and uploading them to the Visma Raet SFTP server. 
### General: 
* Robot type: On-Prem solution
* Customer: Visma Raet Netherlands
* Trigerring: schedule based - every working day
* Author: stefan.mastilak@visma.com
### Visma Raet migration robot tasks:
* PDOL migration
* SDOL migration

### Repository content:
* common\lib --> common functions for all migration types
* pdol\lib --> specific functions for PDOL migration type
* sdol\lib --> specific functions for SDOL migration type
* config.py --> robot configuration file (paths, etc.)
* main_pdol.py --> main PDOL migration script
* main_sdol.py --> main SDOL migration script
* MigrationToolRobot.bat --> MigrationTool modified script used by robot
* requirements.txt --> Python requirements file


### Robot runtime environment:
* NL-D-RAET00000 (10.5.94.29) - raet.local domain
* Windows server 
  * logon via https://remote.raet.nl
* Installed components:
  * Python 
    * version: 3.8.10 
  * OpenJDK
    * version: jdk-11.0.13.8-hotspot
  * Jenkins
    * version: 2.319.2
    * Home: D:\Jenkins
    * admin account: ipa_adm
    * pdol-robot job --> triggering main_pdol.py
    * sdol-robot job --> triggering main_sdol.py
    * mig-robot-pipeline --> trigerring pdol-robot and sdol-robot jobs based on the schedule
 * Robot home: 
    * D:\Robots\visma-raet-migration
 * Robot venv: 
    * D:\Robots\venvs\visma-raet-migration-venv
 * Migration files home: 
    * D:\MigVisma 

### Robot workflow schema:
![raet_migration_schema](https://user-images.githubusercontent.com/74961891/150784643-f6ce4dff-8a15-46f3-890c-1b32d1968a58.png)
