--/*===================================================================================================================
-- File Name      : NBSCH0003596_NIHAP_PC_DAILY_TXN_UPD.sql
-- Purpose        : This datafix script used for update nihap_pc_daily_txns_all.
--                  Reference NBSCH0003596-PRB0028730-INC9483955-RITM0641874-Unable to find credit that was issued .
--                  in Sept 25 statement.Datafix nihap_pc_daily_txns_all to clear the credit flag on PCard Credit 
--                  transactions unmatched by the PFT clearing. 
--                  Similar datafix executed before for old CR#NBSCH0003027, NBSCH0002009, NBSCH0001887 and 43778. 
-- Execution Note : Execute the script from APPS schema in NBS EBS database from SQL window or FlexDeploy Release.
--
-- Update History:
-- Date          Author               Change Description
-- -----------  -------------------  ----------------------------------------------------------------------------------
-- 04/21/2026   RemedyBiz Team       CR# NBSCH0003596-PRB0028730-INC9483955-RITM0641874-Initial Creation                             
-- =================================================================================================================*/
--
SET SERVEROUTPUT ON SIZE 1000000;
DEFINE SCRIPT="NBSCH0003596_NIHAP_PC_DAILY_TXN_UPD.sql"
SET DEFINE ON
SET VERIFY OFF
SET TERMOUT OFF
CLEAR BUFFER
CLEAR COLUMNS
COLUMN splflnm new_value splflnm 
COLUMN currdate new_value SYSDATE noprint
COLUMN dcol new_value dcol 
SELECT 'NBSCH0003596_APPC_'||to_char(SYSDATE,'YYMMDDHH24MISS')||'_'||db_unique_name||'.log' splflnm, to_char(SYSDATE,'YYMMDDHH24MISS') dcol 
FROM   v$database;
SET TERMOUT ON
SET TRIMSPOOL ON
WHENEVER SQLERROR EXIT SQL.SQLCODE ROLLBACK
WHENEVER OSERROR EXIT ROLLBACK
SPOOL  &splflnm 
PROMPT 1.Creates a spool file &splflnm.
---
--- Create a backup table in nihtmp schama for datafix Rollback purpose
---
PROMPT 2.creating a backup table NIHTMP.NBSCH0003596_APP&dcol for nihap_pc_daily_txns_all.
CREATE TABLE NIHTMP.NBSCH0003596_APP&dcol 
AS
SELECT npdt.* 
  FROM apps.nihap_pc_daily_txns_all npdt
 WHERE npdt.transaction_type = 'CREDIT'
   AND npdt.transaction_id IN
       (SELECT npah.transaction_id
          FROM apps.nihap_pc_action_history npah
         WHERE npah.action_reason like 'Set to Unmatched by PFT Waiver run for trx_id 24487902%');

COMMENT ON TABLE NIHTMP.NBSCH0003596_APP&dcol IS 'CR#NBSCH0003596-PRB0028730-INC9483955-RITM0641874-Backup table for nihap_pc_daily_txns_all.';
---
DECLARE
   l_count          PLS_INTEGER := 0;      
   l_comment        VARCHAR2 (100) := '3.Start update of nihap_pc_daily_txns_all.';
BEGIN
   dbms_output.enable(1000000);
   DBMS_OUTPUT.PUT_LINE (l_comment);
   -- 
   UPDATE apps.nihap_pc_daily_txns_all npdt
      SET npdt.dispute_txn_id = NULL,
          npdt.clear_disp_cr_flag = 'N'
    WHERE npdt.transaction_type = 'CREDIT'
      AND npdt.transaction_id IN
               (SELECT a.transaction_id
                  FROM NIHTMP.NBSCH0003596_APP&dcol a);
   --
   l_count   := SQL%ROWCOUNT;
   l_comment := '4.Total number of records updated:'|| NVL(l_count, 0);
   DBMS_OUTPUT.PUT_LINE (l_comment);
   --
   IF NVL(l_count, 0) = 1  THEN         
      COMMIT;	 
      l_comment := '5.Successful! Commit completed.';
   ELSE
      ROLLBACK;
      l_comment := '6.Warning!! Invalid records updated/inserted.';
   END IF;
   DBMS_OUTPUT.PUT_LINE (l_comment);
EXCEPTION
   WHEN OTHERS THEN
       ROLLBACK;
       DBMS_OUTPUT.PUT_LINE ('7.Failed at :' ||l_comment);       
       DBMS_OUTPUT.PUT_LINE ('8.Oracle Error:' || SQLERRM);
END;
/
SELECT '&&SCRIPT' SCRIPT_NAME, TO_CHAR(SYSDATE,'DD-MON-RRRR HH24:MI:SS') END_DATE
  FROM DUAL
/
SPOOL OFF;