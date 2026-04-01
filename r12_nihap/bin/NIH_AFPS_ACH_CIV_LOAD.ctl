--============================================================================
-- File Name            : NIH_AFPS_ACH_CIV_LOAD.ctl
-- Revision             : 1.0
-- Date                 : 17-MAY-2011
-- Original Author      : Syed Alam
--
-- Description          : SQL*Loader script to load AFPS ACH Civilion Data
--                        Into table nihap_vendor_ach_staging.
--
-- Change History
-- Version      When            Who                What
-- =========    ==============  ================= ============================
-- 1.0          17-MAY-2011     Syed Alam         Initial Creation
--============================================================================
--OPTIONS(BINDSIZE=10000000,READSIZE=5120000,ROWS=15000)
OPTIONS (BINDSIZE 512000,ROWS= 10000,SKIP= 1)
LOAD DATA
APPEND
INTO TABLE nihap_vendor_ach_staging
--WHEN (1282:1290 <> ' ')
TRAILING NULLCOLS
(
 vendor_ach_staging_id          "nihap_vendor_ach_staging_s.nextval",
 record_status                  CONSTANT "U",
 employee_type                  constANT "O",
 num_1099                       POSITION(10:18),
 pay_date                       POSITION(27:32) "To_char(TO_date(:pay_date,'MMDDYY'),'MMDDYYYY')",
 name                           POSITION(58:75),
routing_number                 POSITION(1323: 1331),
account_number                 POSITION(1332: 1348),
account_type                   POSITION(1349: 1349),
 last_update_date               SYSDATE,
 last_updated_by                constant "1179")
