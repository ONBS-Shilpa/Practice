#------------------------------------------------------------------------
#Team    - NBS OCI Migration Team
#Comment - Remediate code that references file paths, Port/Server Name, 
#          Database Name in the on-premise environment for OCI Migration
#Date    - 02-Oct-2023
#-------------------------------------------------------------------------
#!/bin/ksh
echo " I am here...."

#. /oraappl/od-nbs/a159prod/a159prodscr/bin/profile.a159prod.db

#ORACLE_SID=a159prod
#ORACLE_BASE=/oraappl/od-nbs/a159prod

#. $ORACLE_BASE/${ORACLE_SID}scr/bin/profile.*.master
#. $ORACLE_BASE/${ORACLE_SID}scr/bin/profile.${ORACLE_SID}.db
#. $ORACLE_BASE/${ORACLE_SID}scr/bin/.${ORACLE_SID}acc

echo '************************************************************'
echo '** Get Passwords'
echo '************************************************************'
ORACLE_SID=`echo $TWO_TASK`
ORACLE_SID_LOWER=$(echo $TWO_TASK | tr '[:upper:]' '[:lower:]')
. $SCRIPTS_TOP/.${ORACLE_SID}acc
. $SCRIPTS_TOP/profile.${ORACLE_SID_LOWER}.app

echo "ORACLE_HOME = $ORACLE_HOME "

echo
sqlplus -s /nolog <<EOF
PROMPT ************************************************************
PROMPT ** Connect to APPS schema
PROMPT ************************************************************
CONNECT apps/$APPS_PW

INSERT INTO T_NBS_FLASH_INDICATOR
VALUES ('FLASHSNAP COMPLETED', SYSDATE);

COMMIT ;

EOF


exit 0;
