@ECHO OFF


SET mypentahodir=C:\Pentaho\data-integration
SET M_root=D:\MigVisma
SET MIG_ID=%~1
SET MIG_TYPE=%2
SET MIG_JOB=
SET MIG_LOG=
SET MIG_DOSLOG=

REM Determine which batch job type to run

IF "%MIG_TYPE%"=="1" GOTO MigType1
IF "%MIG_TYPE%"=="2" GOTO MigType2
IF "%MIG_TYPE%"=="3" GOTO MigType3
IF "%MIG_TYPE%"=="4" GOTO MigType4
IF "%MIG_TYPE%"=="7" GOTO MigType7
IF "%MIG_TYPE%"=="6" GOTO MigType6

ECHO Type Migration "!MIG_TYPE!" invalid.
GOTO End

:MigType1
SET MIG_JOB=1_MIGRATE CUSTOMER.kjb
SET MIG_LOG=MigrationTool1.log
SET MIG_DOSLOG=MigrationTool1.doslog
GOTO TypeDone

:MigType2
SET MIG_JOB=2_MIGRATE_Servicetime_additional.kjb
SET MIG_LOG=MigrationTool2.log
SET MIG_DOSLOG=MigrationTool2.doslog
GOTO TypeDone

:MigType3
SET MIG_JOB=3_MIGRATE_Sickness_additional.kjb
SET MIG_LOG=MigrationTool3.log
SET MIG_DOSLOG=MigrationTool3.doslog
GOTO TypeDone

:MigType4
SET MIG_JOB=4_MIGRATE_MappingDossier_additional.kjb
SET MIG_LOG=MigrationTool4.log
SET MIG_DOSLOG=MigrationTool4.doslog
GOTO TypeDone

:MigType6
SET MIG_JOB=6_MIGRATE_SDOL.kjb
SET MIG_LOG=MigrationTool6.log
SET MIG_DOSLOG=MigrationTool6.doslog
GOTO TypeDone

:MigType7
SET MIG_JOB=7_MIGRATE_PDOL.kjb
SET MIG_LOG=MigrationTool7.log
SET MIG_DOSLOG=MigrationTool7.doslog
GOTO TypeDone

:TypeDone


REM Start Pentaho job

cd /d %M_root%
ECHO Start migration %MIG_ID% type %MIG_TYPE% > %M_root%\%MIG_ID%\Log\%MIG_LOG%
ECHO Start migration %MIG_ID% type %MIG_TYPE%
ECHO See %M_root%\%MIG_ID%

cd /d %mypentahodir%
call Kitchen.bat "-file:%M_root%\%MIG_JOB%" "-param:MIG_ID=%MIG_ID%" "-logfile:%M_root%\%MIG_ID%\Log\%MIG_LOG%" 2>%M_root%\%MIG_ID%\Log\%MIG_DOSLOG%

cd /d %M_root%
ECHO Finished migration %MIG_ID% type %MIG_TYPE% >> %M_root%\%MIG_ID%\Log\%MIG_LOG%
ECHO Finished migration %MIG_ID% type %MIG_TYPE%
ECHO See %M_root%\%MIG_ID%
GOTO End


REM Finished

:End
SET mypentahodir=
SET M_root=
SET MIG_ID=
SET MIG_TYPE=
SET MIG_JOB=
SET MIG_LOG=
SET MIG_DOSLOG=
