--/*===================================================================================================================
--  FILE NAME:      NIHINV_QP_ITEM_SALEPRICE.sql
--  
--  DESCRIPTION :    This script used to create table nihinv_qp_item_saleprice and perform below operations 
--                   1) This table used to save the Item details with sale Price,which will be used by nVision for reporting from Readonly Reporting DB
--                   2) This will be used in a concurrent program "NIHINV Populate QP Sale Price"
--
--  DEPLOYMENT NOTES: Execute this SQL script from APPS schema in NBS EBS database in SQL window or FlexDeploy release.
--  UPDATE HISTORY:
--   Date         Author                Change Description
--  -----------  --------------------  -------------------------------------------------------------------------------
--  07/21/2025   RemedyBiz Team        CR# NBSCH0003054-PRB0028468-Initial Creation 
--  ================================================================================================================= */
--
SET SERVEROUTPUT ON SIZE 1000000
SET ESCAPE OFF
SET DEFINE OFF
SET TERMOUT ON
SET TRIMSPOOL ON
WHENEVER SQLERROR EXIT SQL.SQLCODE ROLLBACK
WHENEVER OSERROR EXIT ROLLBACK
SPOOL NIHINV_QP_ITEM_SALEPRICE.log
--
DECLARE
l_edition_flag      PLS_INTEGER := 1;
l_ora00955_exp      EXCEPTION; -- table or view does not exist
PRAGMA              EXCEPTION_INIT(l_ora00955_exp, -955);
l_ora01430_exp      EXCEPTION; -- column being added already exists in table
PRAGMA              EXCEPTION_INIT(l_ora01430_exp, -1430);
BEGIN
   --
   DBMS_OUTPUT.PUT_LINE('1. Create Table NIHINV.NIHINV_QP_ITEM_SALEPRICE.');
   EXECUTE IMMEDIATE 'CREATE TABLE NIHINV.NIHINV_QP_ITEM_SALEPRICE (
  INVENTORY_ITEM_ID                 NUMBER NOT NULL,
  ORGANIZATION_ID                   NUMBER NOT NULL,
  ITEM_NUMBER                       VARCHAR2(40) NOT NULL,
  ITEM_DESCRIPTION                  VARCHAR2(240),
  INVENTORY_ITEM_STATUS_CODE        VARCHAR2(10),
  UNIT_OF_ISSUE                     VARCHAR2(25),
  PRIMARY_UOM_CODE                  VARCHAR2(3),
  QP_SALE_PRICE                     NUMBER,
  QP_FINAL_SALE_PRICE               NUMBER,
  ITEM_QP_LAST_UPDATE_DATE          DATE,  
  LAST_UPDATE_DATE                  DATE,
  LAST_UPDATED_BY                   NUMBER,
  CREATION_DATE                     DATE,
  CREATED_BY                        NUMBER,  
  REQUEST_ID                        NUMBER,
  REQUEST_DATE                      DATE  
   ) ';
   l_edition_flag := 2;  
   --
   DBMS_OUTPUT.PUT_LINE('2.Editioning of the custom table');
   AD_ZD_TABLE.UPGRADE('NIHINV','NIHINV_QP_ITEM_SALEPRICE');
EXCEPTION
   WHEN l_ora00955_exp THEN
       DBMS_OUTPUT.PUT_LINE('1.Exception:'||SQLERRM);
   WHEN l_ora01430_exp THEN
       DBMS_OUTPUT.PUT_LINE('2.Exception:'||SQLERRM);         
   WHEN OTHERS THEN
       DBMS_OUTPUT.PUT_LINE('3.Oracle Error:'||SQLERRM); 
       RAISE;
END;
/
SELECT 'NIHINV_QP_ITEM_SALEPRICE' SCRIPT, TO_CHAR(SYSDATE,'DD-MON-RRRR HH:MI:SS AM') "END DATE"
  FROM DUAL
/
SPOOL OFF;