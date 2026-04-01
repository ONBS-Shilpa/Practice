--/*===================================================================================================================
--  FILE NAME    : NBSCH0002856_NIHGL_GINV_DF7.sql
-- 
--  DESCRIPTION  : Data fixes for month end issues related issues for MAR-2025.
--                 1) Resolve PA and AR Month End issues for MAR_25, There are Revenue Adjustment Transactions which did not create accounting events due to a bug in Oracle code.
--                 2) G-invoicing Orders data fixes
--                 3) Import and Approve Fed Admin documents stuck in Interface and Base tables
--                 4) The Advance and Expenditure transactions that were interfaced from PMS on 17th March got stuck in the import status as Grants SLA was not in a valid status. 
--                  Once the SLA was validated, the future transactions got imported successfully. The status on the 17th March transactions must be updated to Error, in order to process them.
--                 Similar updates executed before old CR# NBSCH0002644, NBSCH0002598.
--  Execution Notes:  Must be connected to EBS APPS schema to run this script.
--
--  Update History:
--  Date          Author                         Change Description
--  -----------  -----------------------------  ------------------------------------------------------------------------
--  04/01/2025    RemedyBiz Team                CR# NBSCH0002856-PRB0028163-Initial Revision.
--  =================================================================================================================*/
SET SERVEROUTPUT ON SIZE 1000000;
DEFINE SCRIPT="NBSCH0002856_NIHGL_GINV_DF7.sql"
SET DEFINE ON
SET VERIFY OFF
SET TERMOUT OFF
CLEAR BUFFER
CLEAR COLUMNS
COLUMN splflnm new_value splflnm 
COLUMN currdate new_value SYSDATE noprint
COLUMN dcol new_value dcol 
SELECT 'NBSCH0002856_GLUPD7_'||to_char(SYSDATE,'YYMMDDHH24MISS')||'_'||db_unique_name||'.log' splflnm, to_char(SYSDATE,'YYMMDDHH24MISS') dcol 
FROM   v$database;
SET TERMOUT ON
SET TRIMSPOOL ON
WHENEVER SQLERROR EXIT SQL.SQLCODE ROLLBACK
WHENEVER OSERROR EXIT ROLLBACK
SPOOL  &splflnm 
PROMPT Create spool file &splflnm
--========================
--
-- Update Ledger id for viewing and Posting NETS Journal Batch
PROMPT 1.Correct the ledger_id from -1 to 1 in gl_je_headers for je_batch_id in 7162257;
UPDATE gl_je_headers
   SET ledger_id = 1 
 WHERE 1=1 
  AND je_batch_id IN (7162257)
--  and ledger_id = -1
;
PROMPT 1a.Expected Result:1 Record updated
PROMPT 2.Correct the ledger_id from -1 to 1 in gl_je_headers for je_header_id in 101874738;
UPDATE gl_je_lines
   SET ledger_id = 1 
 WHERE je_header_id IN  (101874738)
--  and ledger_id = -1
; 
PROMPT 2a.Expected Result:8 Records updated

COMMIT;

SPOOL OFF;
 