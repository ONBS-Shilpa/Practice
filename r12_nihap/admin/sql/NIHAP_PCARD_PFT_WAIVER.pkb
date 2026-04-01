CREATE OR REPLACE PACKAGE BODY APPS.NIHAP_PCARD_PFT_WAIVER
AS
--$Header: svn+ssh://nbssvnprod.nbscloud.nih.gov/r12_nihap/admin/sql/NIHAP_PCARD_PFT_WAIVER.pkb 15064 2021-06-15 19:50:25Z nainarkulampoj2 $
--/*========================================================================
-- File Name      : NIHAP_PCARD_PFT_WAIVER.pkb
-- Description    : This file creates Package body NIHAP_PCARD_PFT_WAIVERa package that contains
--                  procedures and functions of the Waiver Program to remove the PFT flag because
--                  of any P-Card exception.
--
-- Execution Notes: Execute this SQL script from APPS schema in NBS EBS Database.
--
-- Modification History:
-- Author                 Date          Change Description
-- --------------------  ------------  ---------------------------
-- Animesh Singh         25-JUN-2015   Added this History comment section.
--                                     CR 34394 Name: PRB0008616 - R12- PFT checkbox of PCARD Log will not clear
--                                     Modified to select the card holder name/card number from iby_credit_card table
-- Krishna Aravapalli    06-OCT-2017   Added Pcard_adjustment procedure
-- Renu R                06-OCT-2018   Modifications for SPIII
--                                     CR No: 41903 - 12.2.9 Upgrade changes
--                                     - Online Patching Owner_name.Table_name modified to apps.table_name
-- Pradeep Mantena       23-Apr-2024   NBSCH0002026-PRB0026663 - Fix for P-Card duplicate lines due to PFT not clearing view lines.
-- Srinivasa Rayankula   05-SEP-2025   NBSCH0003062 -PRB0028494 P-Card enhancement for PFT clearing job  
--                                     NIHAP P-Card Clear PFT(PO Not Yet Approved) to clear all required flags 
--=========================================================================*/
--
   gpcardprogramcontextid   NUMBER
                           := NVL (fnd_profile.VALUE ('NIH_PCARD_PROGRAM_CONTEXT_ID'),
                                   -1); 
   PROCEDURE pft_tracking_table
   IS
   BEGIN
      fnd_file.put_line (fnd_file.LOG,
                         'Loading the Data into PFT Tracking Table');

      INSERT INTO readonly.nihap_pc_track_pft (SHORT_ERROR,
                                               PFT_DATE,
                                               STMT_PERIOD,
                                               TRANSACTION_ID,
                                               CH_ACCOUNT_NUM,
                                               CARDMEMBER_NAME,
                                               REQ_NUM,
                                               PO_NUM,
                                               ERROR_MESSAGE,
                                               PFT_Run_date)
         SELECT 'Purchase order not yet created' short_error,
                rc.CREATION_DATE pft_date,
                --rc.ERROR_CODE,
                stmt_period,
                dt.transaction_id,
                dt.CH_ACCOUNT_NUM,
                --ac.CARDMEMBER_NAME, CR34394
                ibycc.chname CARDMEMBER_NAME,
                dt.REQ_NUM,
                dt.PO_NUM,
                error_message,
                SYSDATE
           FROM apps.nihap_pc_req_change_stg rc,
                apps.nihap_pc_daily_txns_all dt,
                apps.ap_cards_all ac,
                iby_creditcard ibycc
          WHERE     dt.transaction_id = rc.transaction_id
                AND record_status != 'P'
                AND error_message LIKE '%Purchase order not yet created%'
                AND dt.ch_account_num = ibycc.ccnumber--ac.card_number --CR34394
                AND ac.card_reference_id=ibycc.instrid
				AND ac.card_program_id = gpcardprogramcontextid
         UNION
         SELECT 'Purchase order not yet created' short_error,
                rc.CREATION_DATE pft_date,
                --rc.ERROR_CODE,
                stmt_period,
                dt.transaction_id,
                dt.CH_ACCOUNT_NUM,
                --ac.CARDMEMBER_NAME, CR34394
                ibycc.chname CARDMEMBER_NAME,
                dt.REQ_NUM,
                dt.PO_NUM,
                error_message,
                SYSDATE
           FROM apps.nihap_pc_req_change_stg rc,
                apps.nihap_pc_daily_txns_all dt,
                apps.ap_cards_all ac,
                iby_creditcard ibycc
          WHERE     dt.transaction_id = rc.transaction_id
                AND record_status != 'P'
                AND error_message LIKE '%PO%is not yet created%'
                AND dt.ch_account_num = ibycc.ccnumber--ac.card_number --CR34394
                AND ac.card_reference_id=ibycc.instrid
				AND ac.card_program_id = gpcardprogramcontextid
         UNION
         SELECT 'PO is not yet approved' short_error,
                rc.CREATION_DATE pft_date,
                --rc.ERROR_CODE,
                stmt_period,
                dt.transaction_id,
                dt.CH_ACCOUNT_NUM,
                --ac.CARDMEMBER_NAME, CR34394
                ibycc.chname CARDMEMBER_NAME,
                dt.REQ_NUM,
                dt.PO_NUM,
                error_message,
                SYSDATE
           FROM apps.nihap_pc_req_change_stg rc,
                apps.nihap_pc_daily_txns_all dt,
                apps.ap_cards_all ac,
                iby_creditcard ibycc
          WHERE     dt.transaction_id = rc.transaction_id
                AND record_status != 'P'
                AND error_message LIKE '%PO%is not yet approved%'
                AND dt.ch_account_num = ibycc.ccnumber--ac.card_number --CR34394
                AND ac.card_reference_id=ibycc.instrid
				AND ac.card_program_id = gpcardprogramcontextid
         UNION
         SELECT 'Requisition is not yet approved' short_error,
                rc.CREATION_DATE pft_date,
                --rc.ERROR_CODE,
                stmt_period,
                dt.transaction_id,
                dt.CH_ACCOUNT_NUM,
                --ac.CARDMEMBER_NAME, CR34394
                ibycc.chname CARDMEMBER_NAME,
                dt.REQ_NUM,
                dt.PO_NUM,
                error_message,
                SYSDATE
           FROM apps.nihap_pc_req_change_stg rc,
                apps.nihap_pc_daily_txns_all dt,
                apps.ap_cards_all ac,
                iby_creditcard ibycc
          WHERE     dt.transaction_id = rc.transaction_id
                AND record_status != 'P'
                AND error_message LIKE '%Requisition%is not yet approved%'
                and dt.ch_account_num = ibycc.ccnumber--ac.card_number --CR34394
                AND ac.card_reference_id=ibycc.instrid
				AND ac.card_program_id = gpcardprogramcontextid
         UNION
         SELECT 'The requisition number is in use' short_error,
                rc.CREATION_DATE pft_date,
                --rc.ERROR_CODE,
                stmt_period,
                dt.transaction_id,
                dt.CH_ACCOUNT_NUM,
                --ac.CARDMEMBER_NAME, CR34394
                ibycc.chname CARDMEMBER_NAME,
                dt.REQ_NUM,
                dt.PO_NUM,
                error_message,
                SYSDATE
           FROM apps.nihap_pc_req_change_stg rc,
                apps.nihap_pc_daily_txns_all dt,
                apps.ap_cards_all ac,
                iby_creditcard ibycc
          WHERE     dt.transaction_id = rc.transaction_id
                AND record_status != 'P'
                AND error_message LIKE '%The requisition number is in use%'
                AND dt.ch_account_num = ibycc.ccnumber--ac.card_number --CR34394
                AND ac.card_reference_id=ibycc.instrid
				AND ac.card_program_id = gpcardprogramcontextid
         UNION
         SELECT 'The requester is not an active employee' short_error,
                rc.CREATION_DATE pft_date,
                --rc.ERROR_CODE,
                stmt_period,
                dt.transaction_id,
                dt.CH_ACCOUNT_NUM,
                --ac.CARDMEMBER_NAME, CR34394
                ibycc.chname CARDMEMBER_NAME,
                dt.REQ_NUM,
                dt.PO_NUM,
                error_message,
                SYSDATE
           FROM apps.nihap_pc_req_change_stg rc,
                apps.nihap_pc_daily_txns_all dt,
                apps.ap_cards_all ac,
                iby_creditcard ibycc
          WHERE     dt.transaction_id = rc.transaction_id
                AND record_status != 'P'
                AND error_message LIKE '%The requester is not an active employee%'
                AND dt.ch_account_num = ibycc.ccnumber--ac.card_number --CR34394
                AND ac.card_reference_id=ibycc.instrid
				AND ac.card_program_id = gpcardprogramcontextid
         UNION
         SELECT 'Preparer is not an active employee' short_error,
                rc.CREATION_DATE pft_date,
                --rc.ERROR_CODE,
                stmt_period,
                dt.transaction_id,
                dt.CH_ACCOUNT_NUM,
                --ac.CARDMEMBER_NAME, CR34394
                ibycc.chname CARDMEMBER_NAME,
                dt.REQ_NUM,
                dt.PO_NUM,
                error_message,
                SYSDATE
           FROM apps.nihap_pc_req_change_stg rc,
                apps.nihap_pc_daily_txns_all dt,
                apps.ap_cards_all ac,
                iby_creditcard ibycc
          WHERE     dt.transaction_id = rc.transaction_id
                AND record_status != 'P'
                AND error_message LIKE '%Preparer is not an active employee%'
                AND dt.ch_account_num = ibycc.ccnumber--ac.card_number --CR34394
                AND ac.card_reference_id=ibycc.instrid
				AND ac.card_program_id = gpcardprogramcontextid
         UNION
         SELECT 'Preparer is not an on-line user' short_error,
                rc.CREATION_DATE pft_date,
                --rc.ERROR_CODE,
                stmt_period,
                dt.transaction_id,
                dt.CH_ACCOUNT_NUM,
                --ac.CARDMEMBER_NAME, CR34394
                ibycc.chname CARDMEMBER_NAME,
                dt.REQ_NUM,
                dt.PO_NUM,
                error_message,
                SYSDATE
           FROM apps.nihap_pc_req_change_stg rc,
                apps.nihap_pc_daily_txns_all dt,
                apps.ap_cards_all ac,
                iby_creditcard ibycc
          WHERE     dt.transaction_id = rc.transaction_id
                AND record_status != 'P'
                AND error_message LIKE '%Preparer is not an on-line user%'
                AND dt.ch_account_num = ibycc.ccnumber--ac.card_number --CR34394
                AND ac.card_reference_id=ibycc.instrid
				AND ac.card_program_id = gpcardprogramcontextid
         UNION
         SELECT 'No active P-card found' short_error,
                rc.CREATION_DATE pft_date,
                --rc.ERROR_CODE,
                stmt_period,
                dt.transaction_id,
                dt.CH_ACCOUNT_NUM,
                --ac.CARDMEMBER_NAME, CR34394
                ibycc.chname CARDMEMBER_NAME,
                dt.REQ_NUM,
                dt.PO_NUM,
                error_message,
                SYSDATE
           FROM apps.nihap_pc_req_change_stg rc,
                apps.nihap_pc_daily_txns_all dt,
                apps.ap_cards_all ac,
                iby_creditcard ibycc
          WHERE     dt.transaction_id = rc.transaction_id
                AND record_status != 'P'
                AND error_message LIKE '%No active P-card found%'
                AND dt.ch_account_num = ibycc.ccnumber--ac.card_number --CR34394
                AND ac.card_reference_id=ibycc.instrid
				AND ac.card_program_id = gpcardprogramcontextid
         UNION
         SELECT 'GL date is not in a valid period' short_error,
                rc.CREATION_DATE pft_date,
                --rc.ERROR_CODE,
                stmt_period,
                dt.transaction_id,
                dt.CH_ACCOUNT_NUM,
                --ac.CARDMEMBER_NAME, CR34394
                ibycc.chname CARDMEMBER_NAME,
                dt.REQ_NUM,
                dt.PO_NUM,
                error_message,
                SYSDATE
           FROM apps.nihap_pc_req_change_stg rc,
                apps.nihap_pc_daily_txns_all dt,
                apps.ap_cards_all ac,
                iby_creditcard ibycc
          WHERE     dt.transaction_id = rc.transaction_id
                AND record_status != 'P'
                AND error_message LIKE '%GL date is not in a valid period%'
                AND dt.ch_account_num = ibycc.ccnumber--ac.card_number --CR34394
                AND ac.card_reference_id=ibycc.instrid
				AND ac.card_program_id = gpcardprogramcontextid
         UNION
         SELECT 'Attribute does not exist for item POWFPOAG' short_error,
                rc.CREATION_DATE pft_date,
                --rc.ERROR_CODE,
                stmt_period,
                dt.transaction_id,
                dt.CH_ACCOUNT_NUM,
                --ac.CARDMEMBER_NAME, CR34394
                ibycc.chname CARDMEMBER_NAME,
                dt.REQ_NUM,
                dt.PO_NUM,
                error_message,
                SYSDATE
           FROM apps.nihap_pc_req_change_stg rc,
                apps.nihap_pc_daily_txns_all dt,
                apps.ap_cards_all ac,
                iby_creditcard ibycc
          WHERE     dt.transaction_id = rc.transaction_id
                AND record_status != 'P'
                AND error_message LIKE
                       '%Attribute%does not exist for item%POWFPOAG%'
                AND dt.ch_account_num = ibycc.ccnumber--ac.card_number --CR34394
                AND ac.card_reference_id=ibycc.instrid
				AND ac.card_program_id = gpcardprogramcontextid
         UNION
         SELECT 'POETT must be valid' short_error,
                rc.CREATION_DATE pft_date,
                --rc.ERROR_CODE,
                stmt_period,
                dt.transaction_id,
                dt.CH_ACCOUNT_NUM,
                --ac.CARDMEMBER_NAME, CR34394
                ibycc.chname CARDMEMBER_NAME,
                dt.REQ_NUM,
                dt.PO_NUM,
                error_message,
                SYSDATE
           FROM apps.nihap_pc_req_change_stg rc,
                apps.nihap_pc_daily_txns_all dt,
                apps.ap_cards_all ac,
                iby_creditcard ibycc
          WHERE     dt.transaction_id = rc.transaction_id
                AND record_status != 'P'
                AND error_message LIKE
                       '%If Oracle Projects is installed and PROJECT_ACCOUNTING_CONTEXT is%then project, task, expenditure type, expenditure item date and expenditure organization must be valid%'
                AND dt.ch_account_num = ibycc.ccnumber--ac.card_number --CR34394
                AND ac.card_reference_id=ibycc.instrid
				AND ac.card_program_id = gpcardprogramcontextid
         UNION
         SELECT 'Expenditure Type is not active on the expenditure item date'
                   short_error,
                rc.CREATION_DATE pft_date,
                --rc.ERROR_CODE,
                stmt_period,
                dt.transaction_id,
                dt.CH_ACCOUNT_NUM,
                --ac.CARDMEMBER_NAME, CR34394
                ibycc.chname CARDMEMBER_NAME,
                dt.REQ_NUM,
                dt.PO_NUM,
                error_message,
                SYSDATE
           FROM apps.nihap_pc_req_change_stg rc,
                apps.nihap_pc_daily_txns_all dt,
                apps.ap_cards_all ac,
                iby_creditcard ibycc
          WHERE     dt.transaction_id = rc.transaction_id
                AND record_status != 'P'
                AND error_message LIKE
                       '%The expenditure type is not active on the expenditure Item date%'
                AND dt.ch_account_num = ibycc.ccnumber--ac.card_number --CR34394r
                AND ac.card_reference_id=ibycc.instrid
				AND ac.card_program_id = gpcardprogramcontextid
         UNION
         SELECT 'Error message is null, probably Amendment is in-process or never kicked-off'
                   short_error,
                rc.CREATION_DATE pft_date,
                --rc.ERROR_CODE,
                stmt_period,
                dt.transaction_id,
                dt.CH_ACCOUNT_NUM,
                --ac.CARDMEMBER_NAME, CR34394
                ibycc.chname CARDMEMBER_NAME,
                dt.REQ_NUM,
                dt.PO_NUM,
                error_message,
                SYSDATE
           FROM apps.nihap_pc_req_change_stg rc,
                apps.nihap_pc_daily_txns_all dt,
                apps.ap_cards_all ac,
                iby_creditcard ibycc
          WHERE     dt.transaction_id = rc.transaction_id
                AND record_status != 'P'
                AND error_message IS NULL
                AND dt.CH_ACCOUNT_NUM = ibycc.ccnumber--ac.card_number --CR34394
                AND rc.CREATION_DATE < TRUNC (SYSDATE)
                AND ac.card_reference_id=ibycc.instrid
				AND ac.card_program_id = gpcardprogramcontextid

         UNION
         SELECT error_message short_error,
                rc.CREATION_DATE pft_date,
                --rc.ERROR_CODE,
                stmt_period,
                dt.transaction_id,
                dt.CH_ACCOUNT_NUM,
                --ac.CARDMEMBER_NAME, CR34394
                ibycc.chname CARDMEMBER_NAME,
                dt.REQ_NUM,
                dt.PO_NUM,
                error_message,
                SYSDATE
           FROM apps.nihap_pc_req_change_stg rc,
                apps.nihap_pc_daily_txns_all dt,
                apps.ap_cards_all ac,
                iby_creditcard ibycc
          WHERE     dt.transaction_id = rc.transaction_id
                AND dt.ch_account_num = ibycc.ccnumber--ac.card_number --CR34394
                AND record_status != 'P'
                AND ac.card_reference_id=ibycc.instrid
                AND error_message NOT LIKE '%PO%is not yet approved%'
                AND error_message NOT LIKE '%Purchase order not yet created%'
                AND error_message NOT LIKE '%PO%is not yet created%'
                AND error_message NOT LIKE
                       '%Requisition%is not yet approved%'
                AND error_message NOT LIKE
                       '%The requester is not an active employee%'
                AND error_message NOT LIKE
                       '%Preparer is not an active employee%'
                AND error_message NOT LIKE '%No active P-card found%'
                AND error_message NOT LIKE
                       '%GL date is not in a valid period%'
                AND error_message NOT LIKE
                       '%The requisition number is in use%'
                AND error_message NOT LIKE
                       '%The requisition number is in use%'
                AND error_message NOT LIKE
                       '%Attribute%does not exist for item%POWFPOAG%'
                AND error_message NOT LIKE
                       '%If Oracle Projects is installed and PROJECT_ACCOUNTING_CONTEXT is%then project, task, expenditure type, expenditure item date and expenditure organization must be valid%'
                AND error_message NOT LIKE
                       '%Preparer is not an on-line user%'
                AND error_message NOT LIKE
                       '%The expenditure type is not active on the expenditure Item date%'
                AND error_message IS NOT NULL
				AND ac.card_program_id = gpcardprogramcontextid;
				
      fnd_file.put_line (fnd_file.LOG,
                         'End of Loading the Data into PFT Tracking Table');
   EXCEPTION
      WHEN OTHERS
      THEN
         fnd_file.put_line (fnd_file.LOG,
                            'Error PFT Tracking TABLe :- ' || SQLERRM);
   END;

   --
   --

   PROCEDURE pft_pcard_logs (errbuf OUT VARCHAR2, retcode OUT NUMBER)
   IS
   BEGIN
      fnd_file.put_line (fnd_file.LOG,
                         'Start Calling Pcard PFT LOG Procesing');

      --Loading Data into Pcard PFT Log
      pft_tracking_table;


      fnd_file.put_line (fnd_file.LOG, 'Update nihap_pc_daily_txns_all ');

--Added new fields for update statement as per NBSCH0003062 on 05-Sep-2025
      UPDATE nihap_pc_daily_txns_all dt
         SET txn_status = 'NEW',
             po_num = NULL,
             req_num = NULL,
             dispute_flag = 'N',
             dispute_reason = NULL,
             dispute_txn_id = NULL,
             clear_disp_cr_flag = 'N',
             partial_flag = 'N',
             auto_match_flag = 'N',
             auto_match_date = NULL,
             last_update_date = SYSDATE
       WHERE (transaction_id IN
                 (SELECT transaction_id
                    FROM nihap_pc_req_change_Stg rc
                   WHERE     record_status != 'P'
                         AND (   error_message LIKE
                                    '%The requester is not an active employee%'
                              OR error_message LIKE
                                    '%Preparer is not an active employee%'
                              OR error_message LIKE
                                    '%No active P-card found%'
                              OR error_message LIKE
                                    '%GL date is not in a valid period%'
                              OR error_message LIKE
                                    '%The requisition number is in use%'
                              OR error_message LIKE
                                    '%If Oracle Projects is installed and PROJECT_ACCOUNTING_CONTEXT is%then project%must be valid%'
                              OR error_message LIKE
                                    '%Preparer is not an active employee%'
                              OR error_message LIKE
                                    '%Preparer is not an on-line user%'
                              OR error_message LIKE
                                    '%The requester is not an active employee%'
                              OR error_message LIKE '%PO%is not yet created%'
                              --OR error_message LIKE '%PO%is not yet approved%'  --- *** This is required only once on 10/25/2010. It should be commented for all future waivers ***
                              OR error_message LIKE
                                    '%Purchase order not yet created%'
                              OR error_message LIKE
                                    'Requisition%is not yet approved%'
                              OR error_message LIKE
                                    '%Attribute%does not exist for item%POWFPOAG%'
                              OR error_message LIKE
                                    'Unknown exception%ORA-01502%' ---- Added on 08/12/2010
                              OR error_message LIKE
                                    '%Please enter a valid Deliver to Location%' ---- Added on 03/07/2011
                              OR error_message LIKE
                                    '%Category is invalid, or category is invalid for the item%' ---- Added on 10/19/2011
                              OR error_message LIKE
                                    '%Multiple Task Numbers found for the Project%' ---- Added on 10/19/2011
                              OR error_message LIKE
                                    '%The suggested buyer is not a valid buyer%' ---- Added on 01/10/2012
                              OR error_message LIKE
                                    '%NO MESSAGE IN MESSAGE DICTIONARY%' ---- Added on 04/26/2012
                              OR error_message LIKE
                                    '%Accrual account is invalid%' ---- Added on 04/26/2012
                              OR error_message LIKE
                                    '%The expenditure type is not active on the expenditure Item date%' --Added on 02/14/2013
                              OR error_message LIKE
                                    '%NO MESSAGE IN MESSAGE DICTIONARY%')))
		AND dt.pcard_program_context_id = gpcardprogramcontextid;

	  --NBSCH0002026 PRB0026663 - Start - Fix for P-Card duplicate lines due to PFT not clearing view lines
      fnd_file.put_line (fnd_file.LOG, 'delete nihap_pc_recon_view_lines ');

      DELETE FROM nihap_pc_recon_view_lines dt
       WHERE (cc_trx_id IN
                 (SELECT transaction_id
                    FROM nihap_pc_req_change_Stg rc
                   WHERE     record_status != 'P'
                         AND (   error_message LIKE
                                    '%The requester is not an active employee%'
                              OR error_message LIKE
                                    '%Preparer is not an active employee%'
                              OR error_message LIKE
                                    '%No active P-card found%'
                              OR error_message LIKE
                                    '%GL date is not in a valid period%'
                              OR error_message LIKE
                                    '%The requisition number is in use%'
                              OR error_message LIKE
                                    '%If Oracle Projects is installed and PROJECT_ACCOUNTING_CONTEXT is%then project%must be valid%'
                              OR error_message LIKE
                                    '%Preparer is not an active employee%'
                              OR error_message LIKE
                                    '%Preparer is not an on-line user%'
                              OR error_message LIKE
                                    '%The requester is not an active employee%'
                              OR error_message LIKE '%PO%is not yet created%'
                              --OR error_message LIKE '%PO%is not yet approved%'  --- *** This is required only once on 10/25/2010. It should be commented for all future waivers ***
                              OR error_message LIKE
                                    '%Purchase order not yet created%'
                              OR error_message LIKE
                                    'Requisition%is not yet approved%'
                              OR error_message LIKE
                                    '%Attribute%does not exist for item%POWFPOAG%'
                              OR error_message LIKE
                                    'Unknown exception%ORA-01502%' ---- Added on 08/12/2010
                              OR error_message LIKE
                                    '%Please enter a valid Deliver to Location%' ---- Added on 03/07/2011
                              OR error_message LIKE
                                    '%Category is invalid, or category is invalid for the item%' ---- Added on 10/19/2011
                              OR error_message LIKE
                                    '%Multiple Task Numbers found for the Project%' ---- Added on 10/19/2011
                              OR error_message LIKE
                                    '%The suggested buyer is not a valid buyer%' ---- Added on 01/10/2012
                              OR error_message LIKE
                                    '%NO MESSAGE IN MESSAGE DICTIONARY%' ---- Added on 04/26/2012
                              OR error_message LIKE
                                    '%Accrual account is invalid%' ---- Added on 04/26/2012
                              OR error_message LIKE
                                    '%The expenditure type is not active on the expenditure Item date%' --Added on 02/14/2013
                              OR error_message LIKE
                                    '%NO MESSAGE IN MESSAGE DICTIONARY%')));
	  --NBSCH0002026 PRB0026663 - End - Fix for P-Card duplicate lines due to PFT not clearing view lines

      fnd_file.put_line (fnd_file.LOG,
                         'Inserting into  nihap_pc_action_history ');


      INSERT INTO nihap_pc_action_history (transaction_id,
                                           action_date,
                                           action,
                                           action_reason,
                                           created_by,
                                           creation_date)
         SELECT transaction_id,
                SYSDATE,
                'NEW',
                'Manually fixed PFT issue, Ticket# PRB0001649',
                -1,
                SYSDATE
           FROM nihap_pc_req_change_Stg rc
          WHERE     record_status != 'P'
                AND ( (   error_message LIKE
                             '%The requester is not an active employee%'
                       OR error_message LIKE
                             '%Preparer is not an active employee%'
                       OR error_message LIKE '%No active P-card found%'
                       OR error_message LIKE
                             '%GL date is not in a valid period%'
                       OR error_message LIKE
                             '%The requisition number is in use%'
                       OR error_message LIKE
                             '%If Oracle Projects is installed and PROJECT_ACCOUNTING_CONTEXT is%then project%must be valid%'
                       OR error_message LIKE
                             '%Preparer is not an active employee%'
                       OR error_message LIKE
                             '%Preparer is not an on-line user%'
                       OR error_message LIKE
                             '%The requester is not an active employee%'
                       OR error_message LIKE '%PO%is not yet created%'
                       --OR error_message LIKE '%PO%is not yet approved%'  --- *** This is required only once on 10/25/2010. It should be commented for all future waivers ***
                       OR error_message LIKE
                             '%Purchase order not yet created%'
                       OR error_message LIKE
                             'Requisition%is not yet approved%'
                       OR error_message LIKE
                             '%Attribute%does not exist for item%POWFPOAG%'
                       OR error_message LIKE 'Unknown exception%ORA-01502%' ---- Added on 08/12/2010
                       OR error_message LIKE
                             '%Please enter a valid Deliver to Location%' ---- Added on 03/07/2011
                       OR error_message LIKE
                             '%Category is invalid, or category is invalid for the item%' ---- Added on 10/19/2011
                       OR error_message LIKE
                             '%Multiple Task Numbers found for the Project%' ---- Added on 10/19/2011
                       OR error_message LIKE
                             '%The suggested buyer is not a valid buyer%' ---- Added on 01/10/2012
                       OR error_message LIKE
                             '%NO MESSAGE IN MESSAGE DICTIONARY%' ---- Added on 04/26/2012
                       OR error_message LIKE '%Accrual account is invalid%' ---- Added on 04/26/2012
                       OR error_message LIKE
                             '%The expenditure type is not active on the expenditure Item date%' --Added on 02/14/2013
                       OR error_message LIKE
                             '%NO MESSAGE IN MESSAGE DICTIONARY%'));


      fnd_file.put_line (fnd_file.LOG, 'Delete from nihap_pc_req_change_Stg');

      -- Delete PFT records as Users will be creating new log and complete recociliation.
      DELETE FROM nihap_pc_req_change_Stg
            WHERE     record_status != 'P'
                  AND ( (   error_message LIKE
                               '%The requester is not an active employee%'
                         OR error_message LIKE
                               '%Preparer is not an active employee%'
                         OR error_message LIKE '%No active P-card found%'
                         OR error_message LIKE
                               '%GL date is not in a valid period%'
                         OR error_message LIKE
                               '%The requisition number is in use%'
                         OR error_message LIKE
                               '%If Oracle Projects is installed and PROJECT_ACCOUNTING_CONTEXT is%then project%must be valid%'
                         OR error_message LIKE
                               '%Preparer is not an active employee%'
                         OR error_message LIKE
                               '%Preparer is not an on-line user%'
                         OR error_message LIKE
                               '%The requester is not an active employee%'
                         OR error_message LIKE '%PO%is not yet created%'
                         --OR error_message LIKE '%PO%is not yet approved%'  --- *** This is required only once on 10/25/2010. It should be commented for all future waivers ***
                         OR error_message LIKE
                               '%Purchase order not yet created%'
                         OR error_message LIKE
                               'Requisition%is not yet approved%'
                         OR error_message LIKE
                               '%Attribute%does not exist for item%POWFPOAG%'
                         OR error_message LIKE 'Unknown exception%ORA-01502%' ---- Added on 08/12/2010
                         OR error_message LIKE
                               '%Please enter a valid Deliver to Location%' ---- Added on 03/07/2011
                         OR error_message LIKE
                               '%Category is invalid, or category is invalid for the item%' ---- Added on 10/19/2011
                         OR error_message LIKE
                               '%Multiple Task Numbers found for the Project%' ---- Added on 10/19/2011
                         OR error_message LIKE
                               '%The suggested buyer is not a valid buyer%' ---- Added on 01/10/2012
                         OR error_message LIKE
                               '%NO MESSAGE IN MESSAGE DICTIONARY%' ---- Added on 04/26/2012
                         OR error_message LIKE '%Accrual account is invalid%' ---- Added on 04/26/2012
                         OR error_message LIKE
                               '%The expenditure type is not active on the expenditure Item date%' --Added on 02/14/2013
                         OR error_message LIKE
                               '%NO MESSAGE IN MESSAGE DICTIONARY%'));

      COMMIT;
   EXCEPTION
      WHEN OTHERS
      THEN
         fnd_file.put_line (fnd_file.LOG, 'PCARd_PFT_LOG :- ' || SQLERRM);
         retcode := 2;
   END pft_pcard_logs;

   --
   --

   PROCEDURE po_not_yet_approved (errbuf                  OUT VARCHAR2,
                                  retcode                 OUT NUMBER,
                                  pft_transaction_id   IN     NUMBER,
                                  p_req_num            IN     VARCHAR2)
   IS
      CURSOR pft_cr
      IS
         SELECT 'Purchase order not yet created' short_error,
                rc.CREATION_DATE pft_date,
                --rc.ERROR_CODE,
                stmt_period,
                dt.transaction_id,
                dt.CH_ACCOUNT_NUM,
                --ac.CARDMEMBER_NAME, CR34394
                ibycc.chname CARDMEMBER_NAME,
                dt.REQ_NUM,
                dt.PO_NUM,
                error_message,
                SYSDATE
           FROM apps.nihap_pc_req_change_stg rc,
                apps.nihap_pc_daily_txns_all dt,
                apps.ap_cards_all ac,
                iby_creditcard ibycc
          WHERE     dt.transaction_id = rc.transaction_id
                AND record_status != 'P'
                AND error_message LIKE '%Purchase order not yet created%'
                AND dt.ch_account_num = ibycc.ccnumber--ac.card_number --CR34394
                AND ac.card_reference_id=ibycc.instrid
				AND ac.card_program_id = gpcardprogramcontextid
         UNION
         SELECT 'Purchase order not yet created' short_error,
                rc.CREATION_DATE pft_date,
                --rc.ERROR_CODE,
                stmt_period,
                dt.transaction_id,
                dt.CH_ACCOUNT_NUM,
                --ac.CARDMEMBER_NAME, CR34394
                ibycc.chname CARDMEMBER_NAME,
                dt.REQ_NUM,
                dt.PO_NUM,
                error_message,
                SYSDATE
           FROM apps.nihap_pc_req_change_stg rc,
                apps.nihap_pc_daily_txns_all dt,
                apps.ap_cards_all ac,
                iby_creditcard ibycc
          WHERE     dt.transaction_id = rc.transaction_id
                AND record_status != 'P'
                AND error_message LIKE '%PO%is not yet created%'
                AND dt.ch_account_num = ibycc.ccnumber--ac.card_number --CR34394
                AND ac.card_reference_id=ibycc.instrid
				AND ac.card_program_id = gpcardprogramcontextid				
         UNION
         SELECT 'PO is not yet approved' short_error,
                rc.CREATION_DATE pft_date,
                --rc.ERROR_CODE,
                stmt_period,
                dt.transaction_id,
                dt.CH_ACCOUNT_NUM,
                --ac.CARDMEMBER_NAME, CR34394
                ibycc.chname CARDMEMBER_NAME,
                dt.REQ_NUM,
                dt.PO_NUM,
                error_message,
                SYSDATE
           FROM apps.nihap_pc_req_change_stg rc,
                apps.nihap_pc_daily_txns_all dt,
                apps.ap_cards_all ac,
                iby_creditcard ibycc
          WHERE     dt.transaction_id = rc.transaction_id
                AND record_status != 'P'
                AND error_message LIKE '%PO%is not yet approved%'
                AND dt.ch_account_num = ibycc.ccnumber--ac.card_number --CR34394
                AND ac.card_reference_id=ibycc.instrid
				AND ac.card_program_id = gpcardprogramcontextid;
				
      CURSOR pft_info (
         i_po_num VARCHAR2)
      IS
           SELECT poh.WF_ITEM_KEY po_WF_ITEM_KEY,
                  poh.po_header_id,
                  poh.segment1 PO_num,
                  prh.segment1 Req_num,
                  poh.pcard_id po_pcard_id,
                  prh.pcard_id req_pcard_id,
                  poh.authorization_status po_status,
                  prh.AUTHORIZATION_STATUS req_status,
                  prh.CANCEL_FLAG req_cancel_flag,
                  poh.CANCEL_FLAG po_cancel_flag,
                  poh.CLOSED_CODE,
                  prh.CREATION_DATE,
                  prl.CREATION_DATE req_line_cr_date,
                  prd.CREATION_DATE req_dist_cr_date,
                  poh.CREATION_DATE po_creation_date,
                  prl.LINE_LOCATION_ID,
                  prl.CANCEL_FLAG,
                  prl.CLOSED_CODE l_close_code,
                  prh.CLOSED_CODE h_close_code,
                  poh.INTERFACE_SOURCE_CODE po_source_code,
                  -- ac.CARD_NUMBER CR34394,
                  ibycc.ccnumber CARD_NUMBER,
                  ac.CARD_PROGRAM_ID,
                  --ac.CARDMEMBER_NAME, CR34394
                  ibycc.chname CARDMEMBER_NAME,
                  prh.CREATED_BY,
                  prh.PREPARER_ID,
                  prd.EXPENDITURE_ITEM_DATE,
                  pd.po_distribution_id,
                  prh.INTERFACE_SOURCE_CODE req_source_code,
                  prh.REQUISITION_HEADER_ID,
                  poh.PO_HEADER_ID po_id,
                  poh.last_update_date
             FROM apps.po_headers_all poh,
                  apps.po_distributions_all pd,
                  apps.po_requisition_headers_all prh,
                  apps.po_requisition_lines_all prl,
                  apps.po_req_distributions_all prd,
                  apps.ap_cards_all ac,
                  iby_creditcard ibycc
            WHERE     prl.requisition_header_id = prh.requisition_header_id
                  AND prd.requisition_line_id = prl.requisition_line_id
                  AND pd.req_distribution_id = prd.distribution_id
                  AND poh.po_header_id = pd.po_header_id
                  -- AND poh.segment1 = '2532086'
                  --AND REGEXP_LIKE(prh.segment1,'^2699208')
                  AND prh.segment1 LIKE i_po_num || '%'
                  AND poh.PCARD_ID = ac.CARD_ID(+)
                  AND ac.card_reference_id=ibycc.instrid
         ORDER BY prh.creation_date DESC;

      pft_rec_info   pft_info%ROWTYPE;
      p_po_num       VARCHAR2 (240);
   BEGIN
      fnd_file.put_line (
         fnd_file.LOG,
         '-----------------------------------------------------------');

      fnd_file.put_line (
         fnd_file.LOG,
         '                              Parameters                   ');
      fnd_file.put_line (fnd_file.LOG,
                         'pft_transaction_id :- ' || pft_transaction_id);
      fnd_file.put_line (fnd_file.LOG, 'p_req_num          :- ' || p_req_num);
      fnd_file.put_line (fnd_file.LOG, 'p_po_num           :- ' || p_po_num);

      fnd_file.put_line (
         fnd_file.LOG,
         '-----------------------------------------------------------');



      fnd_file.put_line (
         fnd_file.LOG,
         '        Backing PCARd Logs into PFT Tracking Table     ');


      INSERT INTO readonly.nihap_pc_track_pft (SHORT_ERROR,
                                               PFT_DATE,
                                               STMT_PERIOD,
                                               TRANSACTION_ID,
                                               CH_ACCOUNT_NUM,
                                               CARDMEMBER_NAME,
                                               REQ_NUM,
                                               PO_NUM,
                                               ERROR_MESSAGE,
                                               PFT_Run_date)
         SELECT 'PO is not yet approved' short_error,
                rc.CREATION_DATE pft_date,
                --rc.ERROR_CODE,
                stmt_period,
                dt.transaction_id,
                dt.CH_ACCOUNT_NUM,
                --ac.CARDMEMBER_NAME, CR34394
                ibycc.chname CARDMEMBER_NAME,
                dt.REQ_NUM,
                dt.PO_NUM,
                error_message,
                SYSDATE
           FROM apps.nihap_pc_req_change_stg rc,
                apps.nihap_pc_daily_txns_all dt,
                apps.ap_cards_all ac,
                iby_creditcard ibycc
          WHERE     dt.transaction_id = rc.transaction_id
                AND record_status != 'P'
                AND error_message LIKE '%PO%is not yet approved%'
                AND dt.ch_account_num = ibycc.ccnumber--ac.card_number --CR34394
                AND ac.card_reference_id=ibycc.instrid
				AND ac.card_program_id = gpcardprogramcontextid;

      fnd_file.put_line (fnd_file.LOG,
                         'No of rows that got inserted :- ' || SQL%ROWCOUNT);


      FOR rec IN pft_cr
      LOOP
         IF (rec.transaction_id = pft_transaction_id)
         THEN
            OPEN pft_info (rec.REQ_NUM);

            FETCH pft_info INTO pft_rec_info;

            CLOSE pft_info;


            fnd_file.put_line (fnd_file.LOG,
                               'Updating nihap_pc_daily_txns_all');

            fnd_file.put_line (fnd_file.LOG,
                               'Transaction ID :- ' || rec.transaction_id);
            fnd_file.put_line (fnd_file.LOG,
                               'Old Req Num  :- ' || rec.REQ_NUM);
            fnd_file.put_line (fnd_file.LOG,
                               'New Req Num  :- ' || pft_rec_info.req_num);
            fnd_file.put_line (fnd_file.LOG,
                               'New Po  Num  :- ' || pft_rec_info.po_num);


            BEGIN
               UPDATE nihap_pc_daily_txns_all
                  SET req_num = pft_rec_info.req_num,
                      po_num = pft_rec_info.po_num,
                      last_update_date = SYSDATE
                WHERE     po_num IN
                             (SELECT po_num
                                FROM nihap_pc_daily_txns_all dt,
                                     nihap_pc_req_change_Stg rc
                               WHERE     dt.transaction_id =
                                            rec.transaction_id
                                     AND dt.transaction_id =
                                            rc.transaction_id
                                     AND error_message LIKE
                                            '%PO%is not yet approved%')
                      AND paid_status = 'N'
                      AND req_num LIKE rec.REQ_NUM || '%'
					  AND pcard_program_context_id = gpcardprogramcontextid;
            EXCEPTION
               WHEN OTHERS
               THEN
                  fnd_file.put_line (
                     fnd_file.LOG,
                     'Error Updating nihap_pc_daily_txns_all :- ' || SQLERRM);
            END;

            fnd_file.put_line (fnd_file.LOG,
                               'Updating nihap_pc_req_change_Stg');
            fnd_file.put_line (fnd_file.LOG,
                               'Transaction ID :- ' || rec.transaction_id);

            BEGIN
               UPDATE nihap_pc_req_change_Stg
                  SET record_status = 'P',
                      error_message = NULL,
                      ERROR_CODE = NULL
                WHERE     record_status = 'I'
                      AND transaction_id = rec.transaction_id;

               --AND error_message LIKE '%PO%is not yet approved%';

               fnd_file.put_line (
                  fnd_file.LOG,
                  'No of rows that got updated :- ' || SQL%ROWCOUNT);
            EXCEPTION
               WHEN OTHERS
               THEN
                  fnd_file.put_line (
                     fnd_file.LOG,
                     'Error Updating nihap_pc_req_change_Stg :- ' || SQLERRM);
            END;

            --NBSCH0002026 PRB0026663 - 07/18/2024 - Start - Fix for P-Card duplicate lines due to PFT not clearing view lines
            IF    NVL (pft_rec_info.po_status, 'CANCELLED') != 'APPROVED'
               OR NVL (pft_rec_info.req_num, rec.REQ_NUM) = rec.REQ_NUM
            THEN
               fnd_file.put_line (fnd_file.LOG,
                                  'Delete from nihap_pc_req_change_Stg');

               -- Delete PFT records as Users will be creating new log and complete recociliation.
               DELETE FROM nihap_pc_req_change_Stg
                     WHERE transaction_id = rec.transaction_id;

               fnd_file.put_line (fnd_file.LOG,
                                  'delete nihap_pc_recon_view_lines ');

               DELETE FROM nihap_pc_recon_view_lines dt
                     WHERE    cc_trx_id = rec.transaction_id
                           OR cc_trx_id IN
                                 (SELECT transaction_id
                                    FROM nihap_pc_daily_txns_all dt
                                   WHERE     paid_status = 'N'
                                         AND req_num =
                                                NVL (pft_rec_info.req_num,
                                                     rec.REQ_NUM));

               fnd_file.put_line (fnd_file.LOG,
                                  'Inserting into  nihap_pc_action_history ');

               INSERT INTO nihap_pc_action_history (transaction_id,
                                                    action_date,
                                                    action,
                                                    action_reason,
                                                    created_by,
                                                    creation_date)
                  SELECT transaction_id,
                         SYSDATE,
                         'NEW',
                            'Set to Unmatched by PFT Waiver run for trx_id '
                         || rec.transaction_id,
                         FND_GLOBAL.USER_ID,
                         SYSDATE
                    FROM nihap_pc_daily_txns_all dt
                   WHERE     paid_status = 'N'
                         AND (   transaction_id = rec.transaction_id
                              OR req_num =
                                    NVL (pft_rec_info.req_num, rec.REQ_NUM));

               fnd_file.put_line (
                  fnd_file.LOG,
                  'Update nihap_pc_daily_txns_all to status NEW');
               --Added new fields for update statement as per NBSCH0003062 on 05-Sep-2025
               UPDATE nihap_pc_daily_txns_all dt
                  SET txn_status = 'NEW',
                      po_num = NULL,
                      req_num = NULL,
                      dispute_flag = 'N',
                      dispute_reason = NULL,
                      dispute_txn_id = NULL,
                      clear_disp_cr_flag = 'N',
                      partial_flag = 'N',
                      auto_match_flag = 'N',
                      auto_match_date = NULL,
                      last_updated_by = FND_GLOBAL.USER_ID,
                      last_update_date = SYSDATE
                WHERE     paid_status = 'N'
                      AND (   transaction_id = rec.transaction_id
                           OR req_num =
                                 NVL (pft_rec_info.req_num, rec.REQ_NUM));
            END IF;
         --NBSCH0002026 PRB0026663 - 07/18/2024 - End - Fix for P-Card duplicate lines due to PFT not clearing view lines
         END IF;
      END LOOP;

      COMMIT;
   EXCEPTION
      WHEN OTHERS
      THEN
         fnd_file.put_line (fnd_file.LOG,
                            'PO_NOT_YET_APPROVED:- ' || SQLERRM);
   END po_not_yet_approved;
--
--

   PROCEDURE Pcard_adjustment (errbuf        OUT VARCHAR2,
                               retcode       OUT NUMBER,
                               p_period   IN     VARCHAR2)
   IS
      CURSOR C1
      IS
         SELECT TRUNC (SYSDATE) ACCOUNTING_DATE,
                aid.ACCRUAL_POSTED_FLAG,
                aid.ASSETS_ADDITION_FLAG,
                aid.ASSETS_TRACKING_FLAG,
                aid.CASH_POSTED_FLAG,
                NULL DISTRIBUTION_LINE_NUMBER,              -- Reversal record
                aid.DIST_CODE_COMBINATION_ID,
                aid.INVOICE_ID,
                -1 LAST_UPDATED_BY,
                SYSDATE LAST_UPDATE_DATE,
                aid.LINE_TYPE_LOOKUP_CODE,
                aid.PERIOD_NAME,
                aid.SET_OF_BOOKS_ID,
                aid.ACCTS_PAY_CODE_COMBINATION_ID,
                -1 * aid.amount AMOUNT, -- Reversal record, so multiplying with -1
                NULL BASE_AMOUNT,
                NULL BASE_INVOICE_PRICE_VARIANCE,
                aid.BATCH_ID,
                -1 CREATED_BY,
                SYSDATE CREATION_DATE,
                --'P-Card QTY-ORD Hold Release Adjustment - Reversal of matched line'
                'P-Card FINAL MATCHING Hold Release Adjustment - Reversal of matched line' --R12TU
                   DESCRIPTION,
                aid.EXCHANGE_RATE_VARIANCE,
                aid.FINAL_MATCH_FLAG,
                aid.INCOME_TAX_REGION,
                aid.INVOICE_PRICE_VARIANCE,
                aid.LAST_UPDATE_LOGIN,
                aid.MATCH_STATUS_FLAG,
                'N' POSTED_FLAG,
                aid.PO_DISTRIBUTION_ID, -- Reveral record, so copying PO link --- null,  ---
                aid.PROGRAM_APPLICATION_ID,
                aid.PROGRAM_ID,
                SYSDATE PROGRAM_UPDATE_DATE,
                -1 * QUANTITY_INVOICED QUANTITY_INVOICED, -- Reversal record, so multiplying with -1
                aid.RATE_VAR_CODE_COMBINATION_ID,
                NULL REQUEST_ID,
                'Y' REVERSAL_FLAG,       -- Reversal record, so populating 'Y'
                aid.TYPE_1099,
                UNIT_PRICE,
                --aid.VAT_CODE, -- R12TU
                aid.AMOUNT_ENCUMBERED,
                aid.BASE_AMOUNT_ENCUMBERED,
                aid.ENCUMBERED_FLAG,
                aid.EXCHANGE_DATE,
                aid.EXCHANGE_RATE,
                aid.EXCHANGE_RATE_TYPE,
                aid.PRICE_ADJUSTMENT_FLAG,
                aid.PRICE_VAR_CODE_COMBINATION_ID,
                aid.QUANTITY_UNENCUMBERED,
                aid.STAT_AMOUNT,
                aid.AMOUNT_TO_POST,
                aid.ATTRIBUTE1,
                aid.ATTRIBUTE10,
                aid.ATTRIBUTE11,
                aid.ATTRIBUTE12,
                aid.ATTRIBUTE13,
                aid.ATTRIBUTE14,
                aid.ATTRIBUTE15,
                aid.ATTRIBUTE2,
                aid.ATTRIBUTE3,
                aid.ATTRIBUTE4,
                aid.ATTRIBUTE5,
                aid.ATTRIBUTE6,
                aid.ATTRIBUTE7,
                aid.ATTRIBUTE8,
                aid.ATTRIBUTE9,
                aid.ATTRIBUTE_CATEGORY,
                aid.BASE_AMOUNT_TO_POST,
                aid.CASH_JE_BATCH_ID,
                aid.EXPENDITURE_ITEM_DATE,
                aid.EXPENDITURE_ORGANIZATION_ID,
                aid.EXPENDITURE_TYPE,
                aid.JE_BATCH_ID,
                aid.PARENT_INVOICE_ID,
                aid.PA_ADDITION_FLAG,
                -1 * aid.PA_QUANTITY PA_QUANTITY, -- Reversal record, so * with -1
                aid.POSTED_AMOUNT,
                aid.POSTED_BASE_AMOUNT,
                aid.PREPAY_AMOUNT_REMAINING,
                aid.PROJECT_ACCOUNTING_CONTEXT,
                aid.PROJECT_ID,
                aid.TASK_ID,
                aid.USSGL_TRANSACTION_CODE,
                aid.USSGL_TRX_CODE_CONTEXT,
                aid.EARLIEST_SETTLEMENT_DATE,
                aid.REQ_DISTRIBUTION_ID,
                -1 * aid.QUANTITY_VARIANCE QUANTITY_VARIANCE, -- Reversal record, so * with -1
                -1 * aid.BASE_QUANTITY_VARIANCE BASE_QUANTITY_VARIANCE, -- Reversal record, so * with -1
                aid.PACKET_ID,
                aid.AWT_FLAG,
                aid.AWT_GROUP_ID,
                aid.AWT_TAX_RATE_ID,
                aid.AWT_GROSS_AMOUNT,
                aid.AWT_INVOICE_ID,
                aid.AWT_ORIGIN_GROUP_ID,
                aid.REFERENCE_1,
                aid.REFERENCE_2,
                aid.ORG_ID,
                aid.OTHER_INVOICE_ID,
                aid.AWT_INVOICE_PAYMENT_ID,
                aid.GLOBAL_ATTRIBUTE_CATEGORY,
                aid.GLOBAL_ATTRIBUTE1,
                aid.GLOBAL_ATTRIBUTE2,
                aid.GLOBAL_ATTRIBUTE3,
                aid.GLOBAL_ATTRIBUTE4,
                aid.GLOBAL_ATTRIBUTE5,
                aid.GLOBAL_ATTRIBUTE6,
                aid.GLOBAL_ATTRIBUTE7,
                aid.GLOBAL_ATTRIBUTE8,
                aid.GLOBAL_ATTRIBUTE9,
                aid.GLOBAL_ATTRIBUTE10,
                aid.GLOBAL_ATTRIBUTE11,
                aid.GLOBAL_ATTRIBUTE12,
                aid.GLOBAL_ATTRIBUTE13,
                aid.GLOBAL_ATTRIBUTE14,
                aid.GLOBAL_ATTRIBUTE15,
                aid.GLOBAL_ATTRIBUTE16,
                aid.GLOBAL_ATTRIBUTE17,
                aid.GLOBAL_ATTRIBUTE18,
                aid.GLOBAL_ATTRIBUTE19,
                aid.GLOBAL_ATTRIBUTE20,
                -- aid.AMOUNT_INCLUDES_TAX_FLAG, --R12TU
                -- aid.TAX_CALCULATED_FLAG, -- R12TU
                aid.LINE_GROUP_NUMBER,
                aid.RECEIPT_VERIFIED_FLAG,
                aid.RECEIPT_REQUIRED_FLAG,
                aid.RECEIPT_MISSING_FLAG,
                aid.JUSTIFICATION,
                aid.EXPENSE_GROUP,
                aid.START_EXPENSE_DATE,
                aid.END_EXPENSE_DATE,
                aid.RECEIPT_CURRENCY_CODE,
                aid.RECEIPT_CONVERSION_RATE,
                aid.RECEIPT_CURRENCY_AMOUNT,
                aid.DAILY_AMOUNT,
                aid.WEB_PARAMETER_ID,
                aid.ADJUSTMENT_REASON,
                aid.AWARD_ID,
                aid.MRC_ACCRUAL_POSTED_FLAG,               --R12TU New columns
                aid.MRC_CASH_POSTED_FLAG,                  --R12TU New columns
                aid.MRC_DIST_CODE_COMBINATION_ID,
                aid.MRC_AMOUNT,                            -- R12TU New column
                aid.MRC_BASE_AMOUNT,
                aid.MRC_BASE_INV_PRICE_VARIANCE,
                aid.MRC_EXCHANGE_RATE_VARIANCE,
                aid.MRC_POSTED_FLAG,                       --R12TU New columns
                aid.MRC_PROGRAM_APPLICATION_ID,           -- R12TU New columns
                aid.MRC_PROGRAM_ID,                        --R12TU New columns
                aid.MRC_PROGRAM_UPDATE_DATE,              -- R12TU New columns
                aid.MRC_RATE_VAR_CCID,
                aid.MRC_REQUEST_ID,                        -- R12TU New column
                aid.MRC_EXCHANGE_DATE,
                aid.MRC_EXCHANGE_RATE,
                aid.MRC_EXCHANGE_RATE_TYPE,
                aid.MRC_AMOUNT_TO_POST,                    -- R12TU New column
                aid.MRC_BASE_AMOUNT_TO_POST,                --R12TU New column
                aid.MRC_CASH_JE_BATCH_ID,                  -- R12TU New column
                aid.MRC_JE_BATCH_ID,                        --R12TU New column
                aid.MRC_POSTED_AMOUNT,                     -- R12TU New column
                aid.MRC_POSTED_BASE_AMOUNT,                -- R12TU New column
                aid.MRC_RECEIPT_CONVERSION_RATE,
                aid.CREDIT_CARD_TRX_ID,                    -- R12TU New column
                aid.DIST_MATCH_TYPE,
                aid.RCV_TRANSACTION_ID,
                AP_INVOICE_DISTRIBUTIONS_S.NEXTVAL INVOICE_DISTRIBUTION_ID,
                aid.INVOICE_DISTRIBUTION_ID PARENT_REVERSAL_ID, -- Reversal, so populating parent's distr. id
                --aid.TAX_RECOVERY_RATE, --R12TU Obsolete Column
                --aid.TAX_RECOVERY_OVERRIDE_FLAG,--R12TU Obsolete Column
                aid.TAX_RECOVERABLE_FLAG,
                --aid.TAX_CODE_OVERRIDE_FLAG,--R12TU Obsolete Column
                --aid.TAX_CODE_ID,--R12TU Obsolete Column
                aid.PA_CC_AR_INVOICE_ID,
                aid.PA_CC_AR_INVOICE_LINE_NUM,
                aid.PA_CC_PROCESSED_CODE,
                aid.MERCHANT_DOCUMENT_NUMBER,
                aid.MERCHANT_NAME,
                aid.MERCHANT_REFERENCE,
                aid.MERCHANT_TAX_REG_NUMBER,
                aid.MERCHANT_TAXPAYER_ID,
                aid.COUNTRY_OF_SUPPLY,
                aid.MATCHED_UOM_LOOKUP_CODE,
                aid.GMS_BURDENABLE_RAW_COST,
                aid.ACCOUNTING_EVENT_ID,                          ---- ???????
                aid.PREPAY_DISTRIBUTION_ID,
                --aid.CREDIT_CARD_TRX_ID,
                aid.UPGRADE_POSTED_AMT,
                aid.UPGRADE_BASE_POSTED_AMT,
                aid.INVENTORY_TRANSFER_STATUS,
                aid.COMPANY_PREPAID_INVOICE_ID,
                aid.CC_REVERSAL_FLAG,
                --aid.PREPAY_TAX_PARENT_ID,--R12TU Obsolete
                aid.AWT_WITHHELD_AMT,
                aid.INVOICE_INCLUDES_PREPAY_FLAG,
                aid.PRICE_CORRECT_INV_ID,
                aid.PRICE_CORRECT_QTY,
                aid.PA_CMT_XFACE_FLAG,
                aid.CANCELLATION_FLAG,
                --aid.FULLY_PAID_ACCTD_FLAG,--R12TU Obsolete
                --aid.ROOT_DISTRIBUTION_ID, -- R12TU Obsolete
                --aid.XINV_PARENT_REVERSAL_ID,-- R12TU Obsolete
                aid.INVOICE_LINE_NUMBER,                   -- R12TU New Column
                aid.CORRECTED_INVOICE_DIST_ID,             -- R12TU New Column
                aid.ROUNDING_AMT,                          -- R12TU New Column
                aid.CHARGE_APPLICABLE_TO_DIST_ID,          -- R12TU New Column
                aid.CORRECTED_QUANTITY,                    -- R12TU New Column
                aid.RELATED_ID,                            -- R12TU New Column
                aid.ASSET_BOOK_TYPE_CODE,                  -- R12TU New Column
                aid.ASSET_CATEGORY_ID,                     -- R12TU New Column
                aid.DISTRIBUTION_CLASS,                    -- R12TU New Column
                aid.FINAL_PAYMENT_ROUNDING,                -- R12TU New Column
                aid.FINAL_APPLICATION_ROUNDING,            -- R12TU New Column
                aid.AMOUNT_AT_PREPAY_XRATE,                -- R12TU New Column
                aid.CASH_BASIS_FINAL_APP_ROUNDING,         -- R12TU New Column
                aid.AMOUNT_AT_PREPAY_PAY_XRATE,            -- R12TU New Column
                aid.INTENDED_USE,                          -- R12TU New Column
                aid.DETAIL_TAX_DIST_ID,                    -- R12TU New Column
                aid.REC_NREC_RATE,                         -- R12TU New Column
                aid.RECOVERY_RATE_ID,                      -- R12TU New Column
                aid.RECOVERY_RATE_NAME,                    -- R12TU New Column
                aid.RECOVERY_TYPE_CODE,                    -- R12TU New Column
                aid.RECOVERY_RATE_CODE,                    -- R12TU New Column
                aid.WITHHOLDING_TAX_CODE_ID,               -- R12TU New Column
                aid.TAX_ALREADY_DISTRIBUTED_FLAG,          -- R12TU New Column
                aid.SUMMARY_TAX_LINE_ID,                   -- R12TU New Column
                aid.TAXABLE_AMOUNT,                        -- R12TU New Column
                aid.TAXABLE_BASE_AMOUNT,                   -- R12TU New Column
                aid.EXTRA_PO_ERV,                          -- R12TU New Column
                aid.PREPAY_TAX_DIFF_AMOUNT,                -- R12TU New Column
                aid.TAX_CODE_ID,                           -- R12TU New Column
                aid.VAT_CODE,                              -- R12TU New Column
                aid.AMOUNT_INCLUDES_TAX_FLAG,              -- R12TU New Column
                aid.TAX_CALCULATED_FLAG,                   -- R12TU New Column
                aid.TAX_RECOVERY_RATE,                     -- R12TU New Column
                aid.TAX_RECOVERY_OVERRIDE_FLAG,            -- R12TU New Column
                aid.TAX_CODE_OVERRIDE_FLAG,                -- R12TU New Column
                aid.TOTAL_DIST_AMOUNT,                     -- R12TU New Column
                aid.TOTAL_DIST_BASE_AMOUNT,                -- R12TU New Column
                aid.PREPAY_TAX_PARENT_ID,                  -- R12TU New Column
                aid.CANCELLED_FLAG,                        -- R12TU New Column
                aid.OLD_DISTRIBUTION_ID,                   -- R12TU New Column
                aid.OLD_DIST_LINE_NUMBER,                  -- R12TU New Column
                aid.AMOUNT_VARIANCE,
                aid.BASE_AMOUNT_VARIANCE,
                aid.HISTORICAL_FLAG,                       -- R12TU New Column
                aid.RCV_CHARGE_ADDITION_FLAG,              -- R12TU New Column
                aid.AWT_RELATED_ID,                        -- R12TU New Column
                aid.RELATED_RETAINAGE_DIST_ID,             -- R12TU New Column
                aid.RETAINED_AMOUNT_REMAINING,             -- R12TU New Column
                aid.BC_EVENT_ID,                           -- R12TU New Column
                aid.RETAINED_INVOICE_DIST_ID,              -- R12TU New Column
                aid.FINAL_RELEASE_ROUNDING,                -- R12TU New Column
                aid.FULLY_PAID_ACCTD_FLAG,                 -- R12TU New Column
                aid.ROOT_DISTRIBUTION_ID,                  -- R12TU New Column
                aid.XINV_PARENT_REVERSAL_ID,               -- R12TU New Column
                aid.RECURRING_PAYMENT_ID,
                aid.RELEASE_INV_DIST_DERIVED_FROM,         -- R12TU New Column
                aid.PAY_AWT_GROUP_ID                       -- R12TU New Column
           FROM ap_invoices_all ai, ap_invoice_distributions_all aid
          WHERE     ai.invoice_id = aid.invoice_id
                AND ai.source IN
                       ('PCARD_PARTIAL', 'PCARD_FINAL', 'PCARD_CREDIT')
                AND NVL (aid.reversal_flag, 'N') = 'N'
                AND aid.po_distribution_id IS NOT NULL -- This is to pick up only the lines matched to a PO
                AND EXISTS
                       (SELECT 'x'
                          FROM ap_holds_all ah2,
                               po_line_locations_all pll,
                               po_distributions_all pod
                         WHERE     ah2.invoice_id = ai.invoice_id
                               AND ah2.hold_lookup_code = 'FINAL MATCHING'
                               AND ah2.release_lookup_code IS NULL -------------------------------
                               AND ah2.line_location_id =
                                      pll.line_location_id
                               AND pod.line_location_id =
                                      pll.line_location_id
                               AND aid.po_distribution_id =
                                      pod.po_distribution_id
                               AND pll.closed_code = 'FINALLY CLOSED')
                --------------------------- and ai.invoice_id IN (423433)
                AND EXISTS
                       (SELECT 'x'
                          FROM nihap_pc_daily_txns_all ds
                         WHERE     ai.invoice_num = ds.invoice_num
                               AND ds.stmt_period = p_period
							   AND pcard_program_context_id = gpcardprogramcontextid);


      CURSOR c2
      IS
         SELECT TRUNC (SYSDATE) ACCOUNTING_DATE,
                aid.ACCRUAL_POSTED_FLAG,
                aid.ASSETS_ADDITION_FLAG,
                aid.ASSETS_TRACKING_FLAG,
                aid.CASH_POSTED_FLAG,
                NULL DISTRIBUTION_LINE_NUMBER,
                aid.DIST_CODE_COMBINATION_ID,
                aid.INVOICE_ID,
                -1 LAST_UPDATED_BY,
                SYSDATE LAST_UPDATE_DATE,
                aid.LINE_TYPE_LOOKUP_CODE,
                aid.PERIOD_NAME,
                aid.SET_OF_BOOKS_ID,
                aid.ACCTS_PAY_CODE_COMBINATION_ID,
                aid.amount AMOUNT,  -- Reversal record, so multiplying with -1
                NULL BASE_AMOUNT,
                NULL BASE_INVOICE_PRICE_VARIANCE,
                aid.BATCH_ID,
                -1 CREATED_BY,
                SYSDATE CREATION_DATE,
                --'P-Card QTY-ORD Hold Release Adjustment - Unmatched Distirbution line'
                'P-Card FINAL MATCHING Hold Release Adjustment - Unmatched Distribution line' --R12TU
                   DESCRIPTION,
                aid.EXCHANGE_RATE_VARIANCE,
                aid.FINAL_MATCH_FLAG,
                aid.INCOME_TAX_REGION,
                aid.INVOICE_PRICE_VARIANCE,
                aid.LAST_UPDATE_LOGIN,
                aid.MATCH_STATUS_FLAG,
                'N' POSTED_FLAG,
                NULL PO_DISTRIBUTION_ID, -- Reveral record, so copying PO link --- null,  ---
                aid.PROGRAM_APPLICATION_ID,
                aid.PROGRAM_ID,
                SYSDATE PROGRAM_UPDATE_DATE,
                NULL QUANTITY_INVOICED, -- Reversal record, so multiplying with -1
                aid.RATE_VAR_CODE_COMBINATION_ID,
                NULL REQUEST_ID,
                NULL REVERSAL_FLAG,
                aid.TYPE_1099,
                NULL UNIT_PRICE,
                -- aid.VAT_CODE, --R12TU
                aid.AMOUNT_ENCUMBERED,
                aid.BASE_AMOUNT_ENCUMBERED,
                aid.ENCUMBERED_FLAG,
                aid.EXCHANGE_DATE,
                aid.EXCHANGE_RATE,
                aid.EXCHANGE_RATE_TYPE,
                aid.PRICE_ADJUSTMENT_FLAG,
                aid.PRICE_VAR_CODE_COMBINATION_ID,
                aid.QUANTITY_UNENCUMBERED,
                aid.STAT_AMOUNT,
                aid.AMOUNT_TO_POST,
                aid.ATTRIBUTE1,
                aid.ATTRIBUTE10,
                aid.ATTRIBUTE11,
                aid.ATTRIBUTE12,
                aid.ATTRIBUTE13,
                aid.ATTRIBUTE14,
                aid.ATTRIBUTE15,
                aid.ATTRIBUTE2,
                aid.ATTRIBUTE3,
                aid.ATTRIBUTE4,
                aid.ATTRIBUTE5,
                aid.ATTRIBUTE6,
                aid.ATTRIBUTE7,
                aid.ATTRIBUTE8,
                aid.ATTRIBUTE9,
                aid.ATTRIBUTE_CATEGORY,
                aid.BASE_AMOUNT_TO_POST,
                aid.CASH_JE_BATCH_ID,
                aid.EXPENDITURE_ITEM_DATE,
                aid.EXPENDITURE_ORGANIZATION_ID,
                aid.EXPENDITURE_TYPE,
                aid.JE_BATCH_ID,
                aid.PARENT_INVOICE_ID,
                aid.PA_ADDITION_FLAG,
                aid.PA_QUANTITY,              -- Reversal record, so * with -1
                aid.POSTED_AMOUNT,
                aid.POSTED_BASE_AMOUNT,
                aid.PREPAY_AMOUNT_REMAINING,
                aid.PROJECT_ACCOUNTING_CONTEXT,
                aid.PROJECT_ID,
                aid.TASK_ID,
                aid.USSGL_TRANSACTION_CODE,
                aid.USSGL_TRX_CODE_CONTEXT,
                aid.EARLIEST_SETTLEMENT_DATE,
                aid.REQ_DISTRIBUTION_ID,
                NULL QUANTITY_VARIANCE,       -- Reversal record, so * with -1
                NULL BASE_QUANTITY_VARIANCE,  -- Reversal record, so * with -1
                aid.PACKET_ID,
                aid.AWT_FLAG,
                aid.AWT_GROUP_ID,
                aid.AWT_TAX_RATE_ID,
                aid.AWT_GROSS_AMOUNT,
                aid.AWT_INVOICE_ID,
                aid.AWT_ORIGIN_GROUP_ID,
                aid.REFERENCE_1,
                aid.REFERENCE_2,
                aid.ORG_ID,
                aid.OTHER_INVOICE_ID,
                aid.AWT_INVOICE_PAYMENT_ID,
                aid.GLOBAL_ATTRIBUTE_CATEGORY,
                aid.GLOBAL_ATTRIBUTE1,
                aid.GLOBAL_ATTRIBUTE2,
                aid.GLOBAL_ATTRIBUTE3,
                aid.GLOBAL_ATTRIBUTE4,
                aid.GLOBAL_ATTRIBUTE5,
                aid.GLOBAL_ATTRIBUTE6,
                aid.GLOBAL_ATTRIBUTE7,
                aid.GLOBAL_ATTRIBUTE8,
                aid.GLOBAL_ATTRIBUTE9,
                aid.GLOBAL_ATTRIBUTE10,
                aid.GLOBAL_ATTRIBUTE11,
                aid.GLOBAL_ATTRIBUTE12,
                aid.GLOBAL_ATTRIBUTE13,
                aid.GLOBAL_ATTRIBUTE14,
                aid.GLOBAL_ATTRIBUTE15,
                aid.GLOBAL_ATTRIBUTE16,
                aid.GLOBAL_ATTRIBUTE17,
                aid.GLOBAL_ATTRIBUTE18,
                aid.GLOBAL_ATTRIBUTE19,
                aid.GLOBAL_ATTRIBUTE20,
                -- aid.AMOUNT_INCLUDES_TAX_FLAG,-- R12TU
                --aid.TAX_CALCULATED_FLAG,-- R12TU
                aid.LINE_GROUP_NUMBER,
                aid.RECEIPT_VERIFIED_FLAG,
                aid.RECEIPT_REQUIRED_FLAG,
                aid.RECEIPT_MISSING_FLAG,
                aid.JUSTIFICATION,
                aid.EXPENSE_GROUP,
                aid.START_EXPENSE_DATE,
                aid.END_EXPENSE_DATE,
                aid.RECEIPT_CURRENCY_CODE,
                aid.RECEIPT_CONVERSION_RATE,
                aid.RECEIPT_CURRENCY_AMOUNT,
                aid.DAILY_AMOUNT,
                aid.WEB_PARAMETER_ID,
                aid.ADJUSTMENT_REASON,
                aid.AWARD_ID,
                aid.MRC_ACCRUAL_POSTED_FLAG,               --R12TU New columns
                aid.MRC_CASH_POSTED_FLAG,                  --R12TU New columns
                aid.MRC_DIST_CODE_COMBINATION_ID,
                aid.MRC_AMOUNT,                            -- R12TU New column
                aid.MRC_BASE_AMOUNT,
                aid.MRC_BASE_INV_PRICE_VARIANCE,
                aid.MRC_EXCHANGE_RATE_VARIANCE,
                aid.MRC_POSTED_FLAG,                       --R12TU New columns
                aid.MRC_PROGRAM_APPLICATION_ID,           -- R12TU New columns
                aid.MRC_PROGRAM_ID,                        --R12TU New columns
                aid.MRC_PROGRAM_UPDATE_DATE,              -- R12TU New columns
                aid.MRC_RATE_VAR_CCID,
                aid.MRC_REQUEST_ID,                        -- R12TU New column
                aid.MRC_EXCHANGE_DATE,
                aid.MRC_EXCHANGE_RATE,
                aid.MRC_EXCHANGE_RATE_TYPE,
                aid.MRC_AMOUNT_TO_POST,                    -- R12TU New column
                aid.MRC_BASE_AMOUNT_TO_POST,                --R12TU New column
                aid.MRC_CASH_JE_BATCH_ID,                  -- R12TU New column
                aid.MRC_JE_BATCH_ID,                        --R12TU New column
                aid.MRC_POSTED_AMOUNT,                     -- R12TU New column
                aid.MRC_POSTED_BASE_AMOUNT,                -- R12TU New column
                aid.MRC_RECEIPT_CONVERSION_RATE,
                aid.CREDIT_CARD_TRX_ID,
                aid.DIST_MATCH_TYPE,
                aid.RCV_TRANSACTION_ID,
                AP_INVOICE_DISTRIBUTIONS_S.NEXTVAL INVOICE_DISTRIBUTION_ID,
                NULL PARENT_REVERSAL_ID, --- Reversal, so populating parent's distr. id -- aid.PARENT_REVERSAL_ID,
                --aid.TAX_RECOVERY_RATE,--R12TU Obsolete Column
                --aid.TAX_RECOVERY_OVERRIDE_FLAG,--R12TU Obsolete Column
                aid.TAX_RECOVERABLE_FLAG,
                --aid.TAX_CODE_OVERRIDE_FLAG,--R12TU Obsolete Column
                --aid.TAX_CODE_ID,--R12TU Obsolete Column
                aid.PA_CC_AR_INVOICE_ID,
                aid.PA_CC_AR_INVOICE_LINE_NUM,
                aid.PA_CC_PROCESSED_CODE,
                aid.MERCHANT_DOCUMENT_NUMBER,
                aid.MERCHANT_NAME,
                aid.MERCHANT_REFERENCE,
                aid.MERCHANT_TAX_REG_NUMBER,
                aid.MERCHANT_TAXPAYER_ID,
                aid.COUNTRY_OF_SUPPLY,
                aid.MATCHED_UOM_LOOKUP_CODE,
                aid.GMS_BURDENABLE_RAW_COST,
                aid.ACCOUNTING_EVENT_ID,                          ---- ???????
                aid.PREPAY_DISTRIBUTION_ID,
                --aid.CREDIT_CARD_TRX_ID, -- R12TU Column reordered
                aid.UPGRADE_POSTED_AMT,
                aid.UPGRADE_BASE_POSTED_AMT,
                aid.INVENTORY_TRANSFER_STATUS,
                aid.COMPANY_PREPAID_INVOICE_ID,
                aid.CC_REVERSAL_FLAG,
                --aid.PREPAY_TAX_PARENT_ID,
                aid.AWT_WITHHELD_AMT,
                aid.INVOICE_INCLUDES_PREPAY_FLAG,
                aid.PRICE_CORRECT_INV_ID,
                aid.PRICE_CORRECT_QTY,
                aid.PA_CMT_XFACE_FLAG,
                aid.CANCELLATION_FLAG,
                --aid.FULLY_PAID_ACCTD_FLAG,-- R12TU Obsolete
                --aid.ROOT_DISTRIBUTION_ID,-- R12TU Obsolete
                --aid.XINV_PARENT_REVERSAL_ID,-- R12TU Obsolete
                aid.INVOICE_LINE_NUMBER,                   -- R12TU New Column
                aid.CORRECTED_INVOICE_DIST_ID,             -- R12TU New Column
                aid.ROUNDING_AMT,                          -- R12TU New Column
                aid.CHARGE_APPLICABLE_TO_DIST_ID,          -- R12TU New Column
                aid.CORRECTED_QUANTITY,                    -- R12TU New Column
                aid.RELATED_ID,                            -- R12TU New Column
                aid.ASSET_BOOK_TYPE_CODE,                  -- R12TU New Column
                aid.ASSET_CATEGORY_ID,                     -- R12TU New Column
                aid.DISTRIBUTION_CLASS,                    -- R12TU New Column
                aid.FINAL_PAYMENT_ROUNDING,                -- R12TU New Column
                aid.FINAL_APPLICATION_ROUNDING,            -- R12TU New Column
                aid.AMOUNT_AT_PREPAY_XRATE,                -- R12TU New Column
                aid.CASH_BASIS_FINAL_APP_ROUNDING,         -- R12TU New Column
                aid.AMOUNT_AT_PREPAY_PAY_XRATE,            -- R12TU New Column
                aid.INTENDED_USE,                          -- R12TU New Column
                aid.DETAIL_TAX_DIST_ID,                    -- R12TU New Column
                aid.REC_NREC_RATE,                         -- R12TU New Column
                aid.RECOVERY_RATE_ID,                      -- R12TU New Column
                aid.RECOVERY_RATE_NAME,                    -- R12TU New Column
                aid.RECOVERY_TYPE_CODE,                    -- R12TU New Column
                aid.RECOVERY_RATE_CODE,                    -- R12TU New Column
                aid.WITHHOLDING_TAX_CODE_ID,               -- R12TU New Column
                aid.TAX_ALREADY_DISTRIBUTED_FLAG,          -- R12TU New Column
                aid.SUMMARY_TAX_LINE_ID,                   -- R12TU New Column
                aid.TAXABLE_AMOUNT,                        -- R12TU New Column
                aid.TAXABLE_BASE_AMOUNT,                   -- R12TU New Column
                aid.EXTRA_PO_ERV,                          -- R12TU New Column
                aid.PREPAY_TAX_DIFF_AMOUNT,                -- R12TU New Column
                aid.TAX_CODE_ID,                           -- R12TU New Column
                aid.VAT_CODE,                              -- R12TU New Column
                aid.AMOUNT_INCLUDES_TAX_FLAG,              -- R12TU New Column
                aid.TAX_CALCULATED_FLAG,                   -- R12TU New Column
                aid.TAX_RECOVERY_RATE,                     -- R12TU New Column
                aid.TAX_RECOVERY_OVERRIDE_FLAG,            -- R12TU New Column
                aid.TAX_CODE_OVERRIDE_FLAG,                -- R12TU New Column
                aid.TOTAL_DIST_AMOUNT,                     -- R12TU New Column
                aid.TOTAL_DIST_BASE_AMOUNT,                -- R12TU New Column
                aid.PREPAY_TAX_PARENT_ID,                  -- R12TU New Column
                aid.CANCELLED_FLAG,                        -- R12TU New Column
                aid.OLD_DISTRIBUTION_ID,                   -- R12TU New Column
                aid.OLD_DIST_LINE_NUMBER,                  -- R12TU New Column
                aid.AMOUNT_VARIANCE,
                aid.BASE_AMOUNT_VARIANCE,
                aid.HISTORICAL_FLAG,                       -- R12TU New Column
                aid.RCV_CHARGE_ADDITION_FLAG,              -- R12TU New Column
                aid.AWT_RELATED_ID,                        -- R12TU New Column
                aid.RELATED_RETAINAGE_DIST_ID,             -- R12TU New Column
                aid.RETAINED_AMOUNT_REMAINING,             -- R12TU New Column
                aid.BC_EVENT_ID,                           -- R12TU New Column
                aid.RETAINED_INVOICE_DIST_ID,              -- R12TU New Column
                aid.FINAL_RELEASE_ROUNDING,                -- R12TU New Column
                aid.FULLY_PAID_ACCTD_FLAG,                 -- R12TU New Column
                aid.ROOT_DISTRIBUTION_ID,                  -- R12TU New Column
                aid.XINV_PARENT_REVERSAL_ID,               -- R12TU New Column
                aid.RECURRING_PAYMENT_ID,
                aid.RELEASE_INV_DIST_DERIVED_FROM,         -- R12TU New Column
                aid.PAY_AWT_GROUP_ID                       -- R12TU New Column
           FROM ap_invoices_all ai, ap_invoice_distributions_all aid
          WHERE     ai.invoice_id = aid.invoice_id
                AND ai.source IN
                       ('PCARD_PARTIAL', 'PCARD_FINAL', 'PCARD_CREDIT')
                AND NVL (aid.reversal_flag, 'N') = 'N'
                AND aid.amount > 0 -- This is to avoid reversal line getting picked up this sql
                AND EXISTS
                       (SELECT 'x'
                          FROM ap_holds_all ah2,
                               po_line_locations_all pll,
                               po_distributions_all pod
                         WHERE     ah2.invoice_id = ai.invoice_id
                               AND ah2.hold_lookup_code = 'FINAL MATCHING'
                               AND ah2.release_lookup_code IS NULL -------------------------------
                               AND ah2.line_location_id =
                                      pll.line_location_id
                               AND pod.line_location_id =
                                      pll.line_location_id
                               AND aid.po_distribution_id =
                                      pod.po_distribution_id
                               AND pll.closed_code = 'FINALLY CLOSED')
                --------------------------- and ai.invoice_id IN (423433)
                AND EXISTS
                       (SELECT 'x'
                          FROM nihap_pc_daily_txns_all ds
                         WHERE     ai.invoice_num = ds.invoice_num
                               AND ds.stmt_period = p_period
							   AND pcard_program_context_id = gpcardprogramcontextid);


      CURSOR c5
      IS
         SELECT pll.*
           FROM ap_holds_all ah, po_line_locations_all pll
          WHERE     ah.line_location_id = pll.line_location_id
                AND EXISTS
                       (SELECT 'x'
                          FROM ap_invoices_all ai
                         WHERE     ah.invoice_id = ai.invoice_id
                               AND ai.source IN
                                      ('PCARD_PARTIAL',
                                       'PCARD_FINAL',
                                       'PCARD_CREDIT')
                               AND EXISTS
                                      (SELECT 'x'
                                         FROM nihap_pc_daily_txns_all ds
                                        WHERE     ai.invoice_num =
                                                     ds.invoice_num
                                              AND ds.stmt_period = p_period
											  AND pcard_program_context_id = gpcardprogramcontextid))
                AND ah.hold_lookup_code = 'QTY ORD'
                AND NVL (ah.status_flag, 'S') <> 'R';

      vRecordCounter    NUMBER := 0;
      vDistLineNumber   NUMBER;
   BEGIN
      fnd_file.put_line (
         fnd_file.LOG,
         '---------------------------------------------------------------------');

      fnd_file.put_line (
         fnd_file.LOG,
         '                              Parameters                   ');

      fnd_file.put_line (fnd_file.LOG, 'p_period           :- ' || p_period);

      fnd_file.put_line (
         fnd_file.LOG,
         '-----------------------------------------------------------------------');

      IF (p_period IS NULL)
      THEN
         BEGIN
            fnd_file.put_line (
               fnd_file.LOG,
               'Fixing interface lines to get POETT from Po distributions and make PO# null in in apps.ap_invoice_lines_interface table');

            UPDATE apps.ap_invoice_lines_interface aili
               SET (project_id,
                    task_id,
                    expenditure_type,
                    expenditure_item_date,
                    expenditure_organization_id) =
                      (SELECT DISTINCT project_id,
                                       task_id,
                                       expenditure_type,
                                       expenditure_item_date,
                                       expenditure_organization_id
                         FROM apps.po_headers_all poh,
                              apps.po_distributions_all pod
                        WHERE     poh.segment1 = aili.po_number
                              AND poh.po_header_id = pod.po_header_id)
             WHERE     invoice_id IN
                          (SELECT invoice_id
                             FROM apps.ap_invoices_interface
                            WHERE     source IN
                                         ('PCARD_PARTIAL',
                                          'PCARD_FINAL',
                                          'PCARD_CREDIT')
                                  AND NVL (status, 'x') != 'PROCESSED')
                   AND EXISTS
                          (SELECT *
                             FROM apps.ap_interface_rejections
                            WHERE     parent_table =
                                         'AP_INVOICE_LINES_INTERFACE'
                                  AND parent_id IN
                                         (SELECT invoice_line_id
                                            FROM apps.ap_invoice_lines_interface
                                           WHERE invoice_id = aili.invoice_id)
                                  AND reject_lookup_code =
                                         'INVALID SHIPMENT TYPE')
                   AND po_number IS NOT NULL;           -- Added on 12/22/2011

            fnd_file.put_line (
               fnd_file.LOG,
                  SQL%ROWCOUNT
               || ' records updated in apps.ap_invoice_lines_interface to populate POETT from PO distributions');
            fnd_file.put_line (
               fnd_file.LOG,
               'Fixing interface lines to make PO# = NULL in apps.ap_invoice_lines_interface table');

            UPDATE apps.ap_invoice_lines_interface aili
               SET po_number = NULL
             WHERE     invoice_id IN
                          (SELECT invoice_id
                             FROM apps.ap_invoices_interface
                            WHERE     source IN
                                         ('PCARD_PARTIAL',
                                          'PCARD_FINAL',
                                          'PCARD_CREDIT')
                                  AND NVL (status, 'x') != 'PROCESSED')
                   AND project_id IS NOT NULL
                   AND EXISTS
                          (SELECT *
                             FROM apps.ap_interface_rejections
                            WHERE     parent_table =
                                         'AP_INVOICE_LINES_INTERFACE'
                                  AND parent_id IN
                                         (SELECT invoice_line_id
                                            FROM apps.ap_invoice_lines_interface
                                           WHERE invoice_id = aili.invoice_id)
                                  AND reject_lookup_code =
                                         'INVALID SHIPMENT TYPE');

            fnd_file.put_line (
               fnd_file.LOG,
                  SQL%ROWCOUNT
               || ' records updated in apps.ap_invoice_lines_interface to populate PO_number = NULL to make it unmatched line');

            COMMIT;

            fnd_file.put_line (
               fnd_file.LOG,
               'Updating pay alone flag to N for all P-card unprocessed invoices in the interface table');

            UPDATE apps.ap_invoices_interface aii
               SET exclusive_payment_flag = 'N'
             WHERE     source LIKE 'PCARD%'
                   AND exclusive_payment_flag = 'Y'
                   AND status != 'PROCESSED';

            fnd_file.put_line (
               fnd_file.LOG,
                  SQL%ROWCOUNT
               || ' records updated to set pay alone flag to N for all P-card unprocessed invoices in the interface table');

            UPDATE apps.ap_invoices_all ai
               SET exclusive_payment_flag = 'N'
             WHERE     source LIKE 'PCARD%'
                   AND exclusive_payment_flag = 'Y'
                   AND NVL (ai.amount_paid, 0) = 0;

            fnd_file.put_line (
               fnd_file.LOG,
                  SQL%ROWCOUNT
               || ' records updated to set pay alone flag to N for all P-card unpaid invoices');

            COMMIT;

            fnd_file.put_line (
               fnd_file.LOG,
               'Updating pay alone flag to N for all P-card unpaid invoices');

            UPDATE apps.ap_invoices_all ai
               SET exclusive_payment_flag = 'N'
             WHERE     source LIKE 'PCARD%'
                   AND exclusive_payment_flag = 'Y'
                   AND NVL (ai.amount_paid, 0) = 0;

            fnd_file.put_line (
               fnd_file.LOG,
                  SQL%ROWCOUNT
               || ' records updated to set pay alone flag to N for all P-card unpaid invoices');
         --COMMIT;
         END;
      ELSE
         BEGIN
            FOR c1_rec IN C1
            LOOP
               vRecordCounter := vRecordCounter + 1;

               SELECT MAX (distribution_line_number) + 1
                 INTO vDistLineNumber
                 FROM apps.ap_invoice_distributions_all
                WHERE invoice_id = c1_rec.invoice_id;

               -- QTY ORD Adjustment - Reversal Entries

               INSERT INTO ap_invoice_distributions_all
                    VALUES (c1_rec.ACCOUNTING_DATE,
                            c1_rec.ACCRUAL_POSTED_FLAG,
                            c1_rec.ASSETS_ADDITION_FLAG,
                            c1_rec.ASSETS_TRACKING_FLAG,
                            c1_rec.CASH_POSTED_FLAG,
                            vDistLineNumber, --------------------------- c1_rec.DISTRIBUTION_LINE_NUMBER,
                            c1_rec.DIST_CODE_COMBINATION_ID,
                            c1_rec.INVOICE_ID,
                            c1_rec.LAST_UPDATED_BY,
                            c1_rec.LAST_UPDATE_DATE,
                            c1_rec.LINE_TYPE_LOOKUP_CODE,
                            c1_rec.PERIOD_NAME,
                            c1_rec.SET_OF_BOOKS_ID,
                            c1_rec.ACCTS_PAY_CODE_COMBINATION_ID,
                            c1_rec.AMOUNT,
                            c1_rec.BASE_AMOUNT,
                            c1_rec.BASE_INVOICE_PRICE_VARIANCE,
                            c1_rec.BATCH_ID,
                            c1_rec.CREATED_BY,
                            c1_rec.CREATION_DATE,
                            c1_rec.DESCRIPTION,
                            c1_rec.EXCHANGE_RATE_VARIANCE,
                            c1_rec.FINAL_MATCH_FLAG,
                            c1_rec.INCOME_TAX_REGION,
                            c1_rec.INVOICE_PRICE_VARIANCE,
                            c1_rec.LAST_UPDATE_LOGIN,
                            c1_rec.MATCH_STATUS_FLAG,
                            c1_rec.POSTED_FLAG,
                            c1_rec.PO_DISTRIBUTION_ID,
                            c1_rec.PROGRAM_APPLICATION_ID,
                            c1_rec.PROGRAM_ID,
                            c1_rec.PROGRAM_UPDATE_DATE,
                            c1_rec.QUANTITY_INVOICED,
                            c1_rec.RATE_VAR_CODE_COMBINATION_ID,
                            c1_rec.REQUEST_ID,
                            c1_rec.REVERSAL_FLAG,
                            c1_rec.TYPE_1099,
                            c1_rec.UNIT_PRICE,
                            -- c1_rec.VAT_CODE,--R12TU
                            c1_rec.AMOUNT_ENCUMBERED,
                            c1_rec.BASE_AMOUNT_ENCUMBERED,
                            c1_rec.ENCUMBERED_FLAG,
                            c1_rec.EXCHANGE_DATE,
                            c1_rec.EXCHANGE_RATE,
                            c1_rec.EXCHANGE_RATE_TYPE,
                            c1_rec.PRICE_ADJUSTMENT_FLAG,
                            c1_rec.PRICE_VAR_CODE_COMBINATION_ID,
                            c1_rec.QUANTITY_UNENCUMBERED,
                            c1_rec.STAT_AMOUNT,
                            c1_rec.AMOUNT_TO_POST,
                            c1_rec.ATTRIBUTE1,
                            c1_rec.ATTRIBUTE10,
                            c1_rec.ATTRIBUTE11,
                            c1_rec.ATTRIBUTE12,
                            c1_rec.ATTRIBUTE13,
                            c1_rec.ATTRIBUTE14,
                            c1_rec.ATTRIBUTE15,
                            c1_rec.ATTRIBUTE2,
                            c1_rec.ATTRIBUTE3,
                            c1_rec.ATTRIBUTE4,
                            c1_rec.ATTRIBUTE5,
                            c1_rec.ATTRIBUTE6,
                            c1_rec.ATTRIBUTE7,
                            c1_rec.ATTRIBUTE8,
                            c1_rec.ATTRIBUTE9,
                            c1_rec.ATTRIBUTE_CATEGORY,
                            c1_rec.BASE_AMOUNT_TO_POST,
                            c1_rec.CASH_JE_BATCH_ID,
                            c1_rec.EXPENDITURE_ITEM_DATE,
                            c1_rec.EXPENDITURE_ORGANIZATION_ID,
                            c1_rec.EXPENDITURE_TYPE,
                            c1_rec.JE_BATCH_ID,
                            c1_rec.PARENT_INVOICE_ID,
                            c1_rec.PA_ADDITION_FLAG,
                            c1_rec.PA_QUANTITY,
                            c1_rec.POSTED_AMOUNT,
                            c1_rec.POSTED_BASE_AMOUNT,
                            c1_rec.PREPAY_AMOUNT_REMAINING,
                            c1_rec.PROJECT_ACCOUNTING_CONTEXT,
                            c1_rec.PROJECT_ID,
                            c1_rec.TASK_ID,
                            c1_rec.USSGL_TRANSACTION_CODE,
                            c1_rec.USSGL_TRX_CODE_CONTEXT,
                            c1_rec.EARLIEST_SETTLEMENT_DATE,
                            c1_rec.REQ_DISTRIBUTION_ID,
                            c1_rec.QUANTITY_VARIANCE,
                            c1_rec.BASE_QUANTITY_VARIANCE,
                            c1_rec.PACKET_ID,
                            c1_rec.AWT_FLAG,
                            c1_rec.AWT_GROUP_ID,
                            c1_rec.AWT_TAX_RATE_ID,
                            c1_rec.AWT_GROSS_AMOUNT,
                            c1_rec.AWT_INVOICE_ID,
                            c1_rec.AWT_ORIGIN_GROUP_ID,
                            c1_rec.REFERENCE_1,
                            c1_rec.REFERENCE_2,
                            c1_rec.ORG_ID,
                            c1_rec.OTHER_INVOICE_ID,
                            c1_rec.AWT_INVOICE_PAYMENT_ID,
                            c1_rec.GLOBAL_ATTRIBUTE_CATEGORY,
                            c1_rec.GLOBAL_ATTRIBUTE1,
                            c1_rec.GLOBAL_ATTRIBUTE2,
                            c1_rec.GLOBAL_ATTRIBUTE3,
                            c1_rec.GLOBAL_ATTRIBUTE4,
                            c1_rec.GLOBAL_ATTRIBUTE5,
                            c1_rec.GLOBAL_ATTRIBUTE6,
                            c1_rec.GLOBAL_ATTRIBUTE7,
                            c1_rec.GLOBAL_ATTRIBUTE8,
                            c1_rec.GLOBAL_ATTRIBUTE9,
                            c1_rec.GLOBAL_ATTRIBUTE10,
                            c1_rec.GLOBAL_ATTRIBUTE11,
                            c1_rec.GLOBAL_ATTRIBUTE12,
                            c1_rec.GLOBAL_ATTRIBUTE13,
                            c1_rec.GLOBAL_ATTRIBUTE14,
                            c1_rec.GLOBAL_ATTRIBUTE15,
                            c1_rec.GLOBAL_ATTRIBUTE16,
                            c1_rec.GLOBAL_ATTRIBUTE17,
                            c1_rec.GLOBAL_ATTRIBUTE18,
                            c1_rec.GLOBAL_ATTRIBUTE19,
                            c1_rec.GLOBAL_ATTRIBUTE20,
                            --c1_rec.AMOUNT_INCLUDES_TAX_FLAG,-- R12TU
                            --c1_rec.TAX_CALCULATED_FLAG,-- R12TU
                            c1_rec.LINE_GROUP_NUMBER,
                            c1_rec.RECEIPT_VERIFIED_FLAG,
                            c1_rec.RECEIPT_REQUIRED_FLAG,
                            c1_rec.RECEIPT_MISSING_FLAG,
                            c1_rec.JUSTIFICATION,
                            c1_rec.EXPENSE_GROUP,
                            c1_rec.START_EXPENSE_DATE,
                            c1_rec.END_EXPENSE_DATE,
                            c1_rec.RECEIPT_CURRENCY_CODE,
                            c1_rec.RECEIPT_CONVERSION_RATE,
                            c1_rec.RECEIPT_CURRENCY_AMOUNT,
                            c1_rec.DAILY_AMOUNT,
                            c1_rec.WEB_PARAMETER_ID,
                            c1_rec.ADJUSTMENT_REASON,
                            c1_rec.AWARD_ID,
                            c1_rec.MRC_ACCRUAL_POSTED_FLAG, --R12TU New columns
                            c1_rec.MRC_CASH_POSTED_FLAG,   --R12TU New columns
                            c1_rec.MRC_DIST_CODE_COMBINATION_ID,
                            c1_rec.MRC_AMOUNT,             -- R12TU New column
                            c1_rec.MRC_BASE_AMOUNT,
                            c1_rec.MRC_BASE_INV_PRICE_VARIANCE,
                            c1_rec.MRC_EXCHANGE_RATE_VARIANCE,
                            c1_rec.MRC_POSTED_FLAG,        --R12TU New columns
                            c1_rec.MRC_PROGRAM_APPLICATION_ID, -- R12TU New columns
                            c1_rec.MRC_PROGRAM_ID,         --R12TU New columns
                            c1_rec.MRC_PROGRAM_UPDATE_DATE, -- R12TU New columns
                            c1_rec.MRC_RATE_VAR_CCID,
                            c1_rec.MRC_REQUEST_ID,         -- R12TU New column
                            c1_rec.MRC_EXCHANGE_DATE,
                            c1_rec.MRC_EXCHANGE_RATE,
                            c1_rec.MRC_EXCHANGE_RATE_TYPE,
                            c1_rec.MRC_AMOUNT_TO_POST,     -- R12TU New column
                            c1_rec.MRC_BASE_AMOUNT_TO_POST, --R12TU New column
                            c1_rec.MRC_CASH_JE_BATCH_ID,   -- R12TU New column
                            c1_rec.MRC_JE_BATCH_ID,         --R12TU New column
                            c1_rec.MRC_POSTED_AMOUNT,      -- R12TU New column
                            c1_rec.MRC_POSTED_BASE_AMOUNT, -- R12TU New column
                            c1_rec.MRC_RECEIPT_CONVERSION_RATE,
                            c1_rec.CREDIT_CARD_TRX_ID,     -- R12TU New column
                            c1_rec.DIST_MATCH_TYPE,
                            c1_rec.RCV_TRANSACTION_ID,
                            c1_rec.INVOICE_DISTRIBUTION_ID,
                            c1_rec.PARENT_REVERSAL_ID,
                            --c1_rec.TAX_RECOVERY_RATE,--R12TU Obsolete Column
                            --c1_rec.TAX_RECOVERY_OVERRIDE_FLAG,--R12TU Obsolete Column
                            c1_rec.TAX_RECOVERABLE_FLAG,
                            --c1_rec.TAX_CODE_OVERRIDE_FLAG,--R12TU Obsolete Column
                            --c1_rec.TAX_CODE_ID,--R12TU Obsolete Column
                            c1_rec.PA_CC_AR_INVOICE_ID,
                            c1_rec.PA_CC_AR_INVOICE_LINE_NUM,
                            c1_rec.PA_CC_PROCESSED_CODE,
                            c1_rec.MERCHANT_DOCUMENT_NUMBER,
                            c1_rec.MERCHANT_NAME,
                            c1_rec.MERCHANT_REFERENCE,
                            c1_rec.MERCHANT_TAX_REG_NUMBER,
                            c1_rec.MERCHANT_TAXPAYER_ID,
                            c1_rec.COUNTRY_OF_SUPPLY,
                            c1_rec.MATCHED_UOM_LOOKUP_CODE,
                            c1_rec.GMS_BURDENABLE_RAW_COST,
                            c1_rec.ACCOUNTING_EVENT_ID,
                            c1_rec.PREPAY_DISTRIBUTION_ID,
                            --c1_rec.CREDIT_CARD_TRX_ID,-- R12TU Different position
                            c1_rec.UPGRADE_POSTED_AMT,
                            c1_rec.UPGRADE_BASE_POSTED_AMT,
                            c1_rec.INVENTORY_TRANSFER_STATUS,
                            c1_rec.COMPANY_PREPAID_INVOICE_ID,
                            c1_rec.CC_REVERSAL_FLAG,
                            --c1_rec.PREPAY_TAX_PARENT_ID,--R12TU Obsolete
                            c1_rec.AWT_WITHHELD_AMT,
                            c1_rec.INVOICE_INCLUDES_PREPAY_FLAG,
                            c1_rec.PRICE_CORRECT_INV_ID,
                            c1_rec.PRICE_CORRECT_QTY,
                            c1_rec.PA_CMT_XFACE_FLAG,
                            c1_rec.CANCELLATION_FLAG,
                            --c1_rec.FULLY_PAID_ACCTD_FLAG,-- R12TU Obsolete
                            --c1_rec.ROOT_DISTRIBUTION_ID,-- R12TU Obsolete
                            --c1_rec.XINV_PARENT_REVERSAL_ID,-- R12TU Obsolete
                            c1_rec.INVOICE_LINE_NUMBER,    -- R12TU New Column
                            c1_rec.CORRECTED_INVOICE_DIST_ID, -- R12TU New Column
                            c1_rec.ROUNDING_AMT,           -- R12TU New Column
                            c1_rec.CHARGE_APPLICABLE_TO_DIST_ID, -- R12TU New Column
                            c1_rec.CORRECTED_QUANTITY,     -- R12TU New Column
                            c1_rec.RELATED_ID,             -- R12TU New Column
                            c1_rec.ASSET_BOOK_TYPE_CODE,   -- R12TU New Column
                            c1_rec.ASSET_CATEGORY_ID,      -- R12TU New Column
                            c1_rec.DISTRIBUTION_CLASS,     -- R12TU New Column
                            c1_rec.FINAL_PAYMENT_ROUNDING, -- R12TU New Column
                            c1_rec.FINAL_APPLICATION_ROUNDING, -- R12TU New Column
                            c1_rec.AMOUNT_AT_PREPAY_XRATE, -- R12TU New Column
                            c1_rec.CASH_BASIS_FINAL_APP_ROUNDING, -- R12TU New Column
                            c1_rec.AMOUNT_AT_PREPAY_PAY_XRATE, -- R12TU New Column
                            c1_rec.INTENDED_USE,           -- R12TU New Column
                            c1_rec.DETAIL_TAX_DIST_ID,     -- R12TU New Column
                            c1_rec.REC_NREC_RATE,          -- R12TU New Column
                            c1_rec.RECOVERY_RATE_ID,       -- R12TU New Column
                            c1_rec.RECOVERY_RATE_NAME,     -- R12TU New Column
                            c1_rec.RECOVERY_TYPE_CODE,     -- R12TU New Column
                            c1_rec.RECOVERY_RATE_CODE,     -- R12TU New Column
                            c1_rec.WITHHOLDING_TAX_CODE_ID, -- R12TU New Column
                            c1_rec.TAX_ALREADY_DISTRIBUTED_FLAG, -- R12TU New Column
                            c1_rec.SUMMARY_TAX_LINE_ID,    -- R12TU New Column
                            c1_rec.TAXABLE_AMOUNT,         -- R12TU New Column
                            c1_rec.TAXABLE_BASE_AMOUNT,    -- R12TU New Column
                            c1_rec.EXTRA_PO_ERV,           -- R12TU New Column
                            c1_rec.PREPAY_TAX_DIFF_AMOUNT, -- R12TU New Column
                            c1_rec.TAX_CODE_ID,            -- R12TU New Column
                            c1_rec.VAT_CODE,               -- R12TU New Column
                            c1_rec.AMOUNT_INCLUDES_TAX_FLAG, -- R12TU New Column
                            c1_rec.TAX_CALCULATED_FLAG,    -- R12TU New Column
                            c1_rec.TAX_RECOVERY_RATE,      -- R12TU New Column
                            c1_rec.TAX_RECOVERY_OVERRIDE_FLAG, -- R12TU New Column
                            c1_rec.TAX_CODE_OVERRIDE_FLAG, -- R12TU New Column
                            c1_rec.TOTAL_DIST_AMOUNT,      -- R12TU New Column
                            c1_rec.TOTAL_DIST_BASE_AMOUNT, -- R12TU New Column
                            c1_rec.PREPAY_TAX_PARENT_ID,   -- R12TU New Column
                            c1_rec.CANCELLED_FLAG,         -- R12TU New Column
                            c1_rec.OLD_DISTRIBUTION_ID,    -- R12TU New Column
                            c1_rec.OLD_DIST_LINE_NUMBER,   -- R12TU New Column
                            c1_rec.AMOUNT_VARIANCE,
                            c1_rec.baSE_AMOUNT_VARIANCE,
                            c1_rec.HISTORICAL_FLAG,        -- R12TU New Column
                            c1_rec.RCV_CHARGE_ADDITION_FLAG, -- R12TU New Column
                            c1_rec.AWT_RELATED_ID,         -- R12TU New Column
                            c1_rec.RELATED_RETAINAGE_DIST_ID, -- R12TU New Column
                            c1_rec.RETAINED_AMOUNT_REMAINING, -- R12TU New Column
                            c1_rec.BC_EVENT_ID,            -- R12TU New Column
                            c1_rec.RETAINED_INVOICE_DIST_ID, -- R12TU New Column
                            c1_rec.FINAL_RELEASE_ROUNDING, -- R12TU New Column
                            c1_rec.FULLY_PAID_ACCTD_FLAG,  -- R12TU New Column
                            c1_rec.ROOT_DISTRIBUTION_ID,   -- R12TU New Column
                            c1_rec.XINV_PARENT_REVERSAL_ID, -- R12TU New Column
                            c1_rec.RECURRING_PAYMENT_ID,
                            c1_rec.RELEASE_INV_DIST_DERIVED_FROM, -- R12TU New Column
                            c1_rec.PAY_AWT_GROUP_ID        -- R12TU New Column
                                                   );
            END LOOP;

            fnd_file.put_line (
               fnd_file.LOG,
                  vRecordCounter
               || ' records inserted into AP_Invoice_Distributions_all to reverse existing matched lines');

            vRecordCounter := 0;

            FOR c2_rec IN C2
            LOOP
               vRecordCounter := vRecordCounter + 1;

               SELECT MAX (distribution_line_number) + 1
                 INTO vDistLineNumber
                 FROM ap_invoice_distributions_all
                WHERE invoice_id = c2_rec.invoice_id;

               -- QTY ORD Adjustment - Reversal Entries

               INSERT INTO ap_invoice_distributions_all
                    VALUES (c2_rec.ACCOUNTING_DATE,
                            c2_rec.ACCRUAL_POSTED_FLAG,
                            c2_rec.ASSETS_ADDITION_FLAG,
                            c2_rec.ASSETS_TRACKING_FLAG,
                            c2_rec.CASH_POSTED_FLAG,
                            vDistLineNumber, -------------- c2_rec.DISTRIBUTION_LINE_NUMBER,
                            c2_rec.DIST_CODE_COMBINATION_ID,
                            c2_rec.INVOICE_ID,
                            c2_rec.LAST_UPDATED_BY,
                            c2_rec.LAST_UPDATE_DATE,
                            c2_rec.LINE_TYPE_LOOKUP_CODE,
                            c2_rec.PERIOD_NAME,
                            c2_rec.SET_OF_BOOKS_ID,
                            c2_rec.ACCTS_PAY_CODE_COMBINATION_ID,
                            c2_rec.AMOUNT,
                            c2_rec.BASE_AMOUNT,
                            c2_rec.BASE_INVOICE_PRICE_VARIANCE,
                            c2_rec.BATCH_ID,
                            c2_rec.CREATED_BY,
                            c2_rec.CREATION_DATE,
                            c2_rec.DESCRIPTION,
                            c2_rec.EXCHANGE_RATE_VARIANCE,
                            c2_rec.FINAL_MATCH_FLAG,
                            c2_rec.INCOME_TAX_REGION,
                            c2_rec.INVOICE_PRICE_VARIANCE,
                            c2_rec.LAST_UPDATE_LOGIN,
                            c2_rec.MATCH_STATUS_FLAG,
                            c2_rec.POSTED_FLAG,
                            c2_rec.PO_DISTRIBUTION_ID,
                            c2_rec.PROGRAM_APPLICATION_ID,
                            c2_rec.PROGRAM_ID,
                            c2_rec.PROGRAM_UPDATE_DATE,
                            c2_rec.QUANTITY_INVOICED,
                            c2_rec.RATE_VAR_CODE_COMBINATION_ID,
                            c2_rec.REQUEST_ID,
                            c2_rec.REVERSAL_FLAG,
                            c2_rec.TYPE_1099,
                            c2_rec.UNIT_PRICE,
                            -- c2_rec.VAT_CODE, --R12TU
                            c2_rec.AMOUNT_ENCUMBERED,
                            c2_rec.BASE_AMOUNT_ENCUMBERED,
                            c2_rec.ENCUMBERED_FLAG,
                            c2_rec.EXCHANGE_DATE,
                            c2_rec.EXCHANGE_RATE,
                            c2_rec.EXCHANGE_RATE_TYPE,
                            c2_rec.PRICE_ADJUSTMENT_FLAG,
                            c2_rec.PRICE_VAR_CODE_COMBINATION_ID,
                            c2_rec.QUANTITY_UNENCUMBERED,
                            c2_rec.STAT_AMOUNT,
                            c2_rec.AMOUNT_TO_POST,
                            c2_rec.ATTRIBUTE1,
                            c2_rec.ATTRIBUTE10,
                            c2_rec.ATTRIBUTE11,
                            c2_rec.ATTRIBUTE12,
                            c2_rec.ATTRIBUTE13,
                            c2_rec.ATTRIBUTE14,
                            c2_rec.ATTRIBUTE15,
                            c2_rec.ATTRIBUTE2,
                            c2_rec.ATTRIBUTE3,
                            c2_rec.ATTRIBUTE4,
                            c2_rec.ATTRIBUTE5,
                            c2_rec.ATTRIBUTE6,
                            c2_rec.ATTRIBUTE7,
                            c2_rec.ATTRIBUTE8,
                            c2_rec.ATTRIBUTE9,
                            c2_rec.ATTRIBUTE_CATEGORY,
                            c2_rec.BASE_AMOUNT_TO_POST,
                            c2_rec.CASH_JE_BATCH_ID,
                            c2_rec.EXPENDITURE_ITEM_DATE,
                            c2_rec.EXPENDITURE_ORGANIZATION_ID,
                            c2_rec.EXPENDITURE_TYPE,
                            c2_rec.JE_BATCH_ID,
                            c2_rec.PARENT_INVOICE_ID,
                            c2_rec.PA_ADDITION_FLAG,
                            c2_rec.PA_QUANTITY,
                            c2_rec.POSTED_AMOUNT,
                            c2_rec.POSTED_BASE_AMOUNT,
                            c2_rec.PREPAY_AMOUNT_REMAINING,
                            c2_rec.PROJECT_ACCOUNTING_CONTEXT,
                            c2_rec.PROJECT_ID,
                            c2_rec.TASK_ID,
                            c2_rec.USSGL_TRANSACTION_CODE,
                            c2_rec.USSGL_TRX_CODE_CONTEXT,
                            c2_rec.EARLIEST_SETTLEMENT_DATE,
                            c2_rec.REQ_DISTRIBUTION_ID,
                            c2_rec.QUANTITY_VARIANCE,
                            c2_rec.BASE_QUANTITY_VARIANCE,
                            c2_rec.PACKET_ID,
                            c2_rec.AWT_FLAG,
                            c2_rec.AWT_GROUP_ID,
                            c2_rec.AWT_TAX_RATE_ID,
                            c2_rec.AWT_GROSS_AMOUNT,
                            c2_rec.AWT_INVOICE_ID,
                            c2_rec.AWT_ORIGIN_GROUP_ID,
                            c2_rec.REFERENCE_1,
                            c2_rec.REFERENCE_2,
                            c2_rec.ORG_ID,
                            c2_rec.OTHER_INVOICE_ID,
                            c2_rec.AWT_INVOICE_PAYMENT_ID,
                            c2_rec.GLOBAL_ATTRIBUTE_CATEGORY,
                            c2_rec.GLOBAL_ATTRIBUTE1,
                            c2_rec.GLOBAL_ATTRIBUTE2,
                            c2_rec.GLOBAL_ATTRIBUTE3,
                            c2_rec.GLOBAL_ATTRIBUTE4,
                            c2_rec.GLOBAL_ATTRIBUTE5,
                            c2_rec.GLOBAL_ATTRIBUTE6,
                            c2_rec.GLOBAL_ATTRIBUTE7,
                            c2_rec.GLOBAL_ATTRIBUTE8,
                            c2_rec.GLOBAL_ATTRIBUTE9,
                            c2_rec.GLOBAL_ATTRIBUTE10,
                            c2_rec.GLOBAL_ATTRIBUTE11,
                            c2_rec.GLOBAL_ATTRIBUTE12,
                            c2_rec.GLOBAL_ATTRIBUTE13,
                            c2_rec.GLOBAL_ATTRIBUTE14,
                            c2_rec.GLOBAL_ATTRIBUTE15,
                            c2_rec.GLOBAL_ATTRIBUTE16,
                            c2_rec.GLOBAL_ATTRIBUTE17,
                            c2_rec.GLOBAL_ATTRIBUTE18,
                            c2_rec.GLOBAL_ATTRIBUTE19,
                            c2_rec.GLOBAL_ATTRIBUTE20,
                            --c2_rec.AMOUNT_INCLUDES_TAX_FLAG,-- R12TU
                            --c2_rec.TAX_CALCULATED_FLAG,-- R12TU
                            c2_rec.LINE_GROUP_NUMBER,
                            c2_rec.RECEIPT_VERIFIED_FLAG,
                            c2_rec.RECEIPT_REQUIRED_FLAG,
                            c2_rec.RECEIPT_MISSING_FLAG,
                            c2_rec.JUSTIFICATION,
                            c2_rec.EXPENSE_GROUP,
                            c2_rec.START_EXPENSE_DATE,
                            c2_rec.END_EXPENSE_DATE,
                            c2_rec.RECEIPT_CURRENCY_CODE,
                            c2_rec.RECEIPT_CONVERSION_RATE,
                            c2_rec.RECEIPT_CURRENCY_AMOUNT,
                            c2_rec.DAILY_AMOUNT,
                            c2_rec.WEB_PARAMETER_ID,
                            c2_rec.ADJUSTMENT_REASON,
                            c2_rec.AWARD_ID,
                            c2_rec.MRC_ACCRUAL_POSTED_FLAG, --R12TU New columns
                            c2_rec.MRC_CASH_POSTED_FLAG,   --R12TU New columns
                            c2_rec.MRC_DIST_CODE_COMBINATION_ID,
                            c2_rec.MRC_AMOUNT,             -- R12TU New column
                            c2_rec.MRC_BASE_AMOUNT,
                            c2_rec.MRC_BASE_INV_PRICE_VARIANCE,
                            c2_rec.MRC_EXCHANGE_RATE_VARIANCE,
                            c2_rec.MRC_POSTED_FLAG,        --R12TU New columns
                            c2_rec.MRC_PROGRAM_APPLICATION_ID, -- R12TU New columns
                            c2_rec.MRC_PROGRAM_ID,         --R12TU New columns
                            c2_rec.MRC_PROGRAM_UPDATE_DATE, -- R12TU New columns
                            c2_rec.MRC_RATE_VAR_CCID,
                            c2_rec.MRC_REQUEST_ID,         -- R12TU New column
                            c2_rec.MRC_EXCHANGE_DATE,
                            c2_rec.MRC_EXCHANGE_RATE,
                            c2_rec.MRC_EXCHANGE_RATE_TYPE,
                            c2_rec.MRC_AMOUNT_TO_POST,     -- R12TU New column
                            c2_rec.MRC_BASE_AMOUNT_TO_POST, --R12TU New column
                            c2_rec.MRC_CASH_JE_BATCH_ID,   -- R12TU New column
                            c2_rec.MRC_JE_BATCH_ID,         --R12TU New column
                            c2_rec.MRC_POSTED_AMOUNT,      -- R12TU New column
                            c2_rec.MRC_POSTED_BASE_AMOUNT, -- R12TU New column
                            c2_rec.MRC_RECEIPT_CONVERSION_RATE,
                            c2_rec.CREDIT_CARD_TRX_ID,     -- R12TU New column
                            c2_rec.DIST_MATCH_TYPE,
                            c2_rec.RCV_TRANSACTION_ID,
                            c2_rec.INVOICE_DISTRIBUTION_ID,
                            c2_rec.PARENT_REVERSAL_ID,
                            --c2_rec.TAX_RECOVERY_RATE,--R12TU Obsolete Column
                            --c2_rec.TAX_RECOVERY_OVERRIDE_FLAG,--R12TU Obsolete Column
                            c2_rec.TAX_RECOVERABLE_FLAG,
                            --c2_rec.TAX_CODE_OVERRIDE_FLAG,--R12TU Obsolete Column
                            --c2_rec.TAX_CODE_ID,--R12TU Obsolete Column
                            c2_rec.PA_CC_AR_INVOICE_ID,
                            c2_rec.PA_CC_AR_INVOICE_LINE_NUM,
                            c2_rec.PA_CC_PROCESSED_CODE,
                            c2_rec.MERCHANT_DOCUMENT_NUMBER,
                            c2_rec.MERCHANT_NAME,
                            c2_rec.MERCHANT_REFERENCE,
                            c2_rec.MERCHANT_TAX_REG_NUMBER,
                            c2_rec.MERCHANT_TAXPAYER_ID,
                            c2_rec.COUNTRY_OF_SUPPLY,
                            c2_rec.MATCHED_UOM_LOOKUP_CODE,
                            c2_rec.GMS_BURDENABLE_RAW_COST,
                            c2_rec.ACCOUNTING_EVENT_ID,
                            c2_rec.PREPAY_DISTRIBUTION_ID,
                            --c2_rec.CREDIT_CARD_TRX_ID,-- R12TU Different position
                            c2_rec.UPGRADE_POSTED_AMT,
                            c2_rec.UPGRADE_BASE_POSTED_AMT,
                            c2_rec.INVENTORY_TRANSFER_STATUS,
                            c2_rec.COMPANY_PREPAID_INVOICE_ID,
                            c2_rec.CC_REVERSAL_FLAG,
                            --c2_rec.PREPAY_TAX_PARENT_ID,--R12TU Obsolete
                            c2_rec.AWT_WITHHELD_AMT,
                            c2_rec.INVOICE_INCLUDES_PREPAY_FLAG,
                            c2_rec.PRICE_CORRECT_INV_ID,
                            c2_rec.PRICE_CORRECT_QTY,
                            c2_rec.PA_CMT_XFACE_FLAG,
                            c2_rec.CANCELLATION_FLAG,
                            --c2_rec.FULLY_PAID_ACCTD_FLAG,-- R12TU Obsolete
                            --c2_rec.ROOT_DISTRIBUTION_ID,-- R12TU Obsolete
                            --c2_rec.XINV_PARENT_REVERSAL_ID,-- R12TU Obsolete
                            c2_rec.INVOICE_LINE_NUMBER,    -- R12TU New Column
                            c2_rec.CORRECTED_INVOICE_DIST_ID, -- R12TU New Column
                            c2_rec.ROUNDING_AMT,           -- R12TU New Column
                            c2_rec.CHARGE_APPLICABLE_TO_DIST_ID, -- R12TU New Column
                            c2_rec.CORRECTED_QUANTITY,     -- R12TU New Column
                            c2_rec.RELATED_ID,             -- R12TU New Column
                            c2_rec.ASSET_BOOK_TYPE_CODE,   -- R12TU New Column
                            c2_rec.ASSET_CATEGORY_ID,      -- R12TU New Column
                            c2_rec.DISTRIBUTION_CLASS,     -- R12TU New Column
                            c2_rec.FINAL_PAYMENT_ROUNDING, -- R12TU New Column
                            c2_rec.FINAL_APPLICATION_ROUNDING, -- R12TU New Column
                            c2_rec.AMOUNT_AT_PREPAY_XRATE, -- R12TU New Column
                            c2_rec.CASH_BASIS_FINAL_APP_ROUNDING, -- R12TU New Column
                            c2_rec.AMOUNT_AT_PREPAY_PAY_XRATE, -- R12TU New Column
                            c2_rec.INTENDED_USE,           -- R12TU New Column
                            c2_rec.DETAIL_TAX_DIST_ID,     -- R12TU New Column
                            c2_rec.REC_NREC_RATE,          -- R12TU New Column
                            c2_rec.RECOVERY_RATE_ID,       -- R12TU New Column
                            c2_rec.RECOVERY_RATE_NAME,     -- R12TU New Column
                            c2_rec.RECOVERY_TYPE_CODE,     -- R12TU New Column
                            c2_rec.RECOVERY_RATE_CODE,     -- R12TU New Column
                            c2_rec.WITHHOLDING_TAX_CODE_ID, -- R12TU New Column
                            c2_rec.TAX_ALREADY_DISTRIBUTED_FLAG, -- R12TU New Column
                            c2_rec.SUMMARY_TAX_LINE_ID,    -- R12TU New Column
                            c2_rec.TAXABLE_AMOUNT,         -- R12TU New Column
                            c2_rec.TAXABLE_BASE_AMOUNT,    -- R12TU New Column
                            c2_rec.EXTRA_PO_ERV,           -- R12TU New Column
                            c2_rec.PREPAY_TAX_DIFF_AMOUNT, -- R12TU New Column
                            c2_rec.TAX_CODE_ID,            -- R12TU New Column
                            c2_rec.VAT_CODE,               -- R12TU New Column
                            c2_rec.AMOUNT_INCLUDES_TAX_FLAG, -- R12TU New Column
                            c2_rec.TAX_CALCULATED_FLAG,    -- R12TU New Column
                            c2_rec.TAX_RECOVERY_RATE,      -- R12TU New Column
                            c2_rec.TAX_RECOVERY_OVERRIDE_FLAG, -- R12TU New Column
                            c2_rec.TAX_CODE_OVERRIDE_FLAG, -- R12TU New Column
                            c2_rec.TOTAL_DIST_AMOUNT,      -- R12TU New Column
                            c2_rec.TOTAL_DIST_BASE_AMOUNT, -- R12TU New Column
                            c2_rec.PREPAY_TAX_PARENT_ID,   -- R12TU New Column
                            c2_rec.CANCELLED_FLAG,         -- R12TU New Column
                            c2_rec.OLD_DISTRIBUTION_ID,    -- R12TU New Column
                            c2_rec.OLD_DIST_LINE_NUMBER,   -- R12TU New Column
                            c2_rec.AMOUNT_VARIANCE,
                            c2_rec.BASE_AMOUNT_VARIANCE,
                            c2_rec.HISTORICAL_FLAG,        -- R12TU New Column
                            c2_rec.RCV_CHARGE_ADDITION_FLAG, -- R12TU New Column
                            c2_rec.AWT_RELATED_ID,         -- R12TU New Column
                            c2_rec.RELATED_RETAINAGE_DIST_ID, -- R12TU New Column
                            c2_rec.RETAINED_AMOUNT_REMAINING, -- R12TU New Column
                            c2_rec.BC_EVENT_ID,            -- R12TU New Column
                            c2_rec.RETAINED_INVOICE_DIST_ID, -- R12TU New Column
                            c2_rec.FINAL_RELEASE_ROUNDING, -- R12TU New Column
                            c2_rec.FULLY_PAID_ACCTD_FLAG,  -- R12TU New Column
                            c2_rec.ROOT_DISTRIBUTION_ID,   -- R12TU New Column
                            c2_rec.XINV_PARENT_REVERSAL_ID, -- R12TU New Column
                            c2_rec.RECURRING_PAYMENT_ID,
                            c2_rec.RELEASE_INV_DIST_DERIVED_FROM, -- R12TU New Column
                            c2_rec.PAY_AWT_GROUP_ID        -- R12TU New Column
                                                   );
            END LOOP;


            fnd_file.put_line (
               fnd_file.LOG,
                  vRecordCounter
               || ' records inserted into AP_Invoice_Distributions_all to create unmatched lines');


            UPDATE ap_invoice_distributions_all aid
               SET reversal_flag = 'Y'
             WHERE     aid.amount > 0 -- This is to avoid reversal line getting picked up this sql
                   AND NVL (aid.reversal_flag, 'N') = 'N'
                   AND EXISTS
                          (SELECT 'x'
                             FROM ap_holds_all ah2,
                                  po_line_locations_all pll,
                                  po_distributions_all pod
                            WHERE     ah2.invoice_id = aid.invoice_id
                                  AND ah2.hold_lookup_code = 'FINAL MATCHING'
                                  AND ah2.release_lookup_code IS NULL -------------------------------
                                  AND ah2.line_location_id =
                                         pll.line_location_id
                                  AND pod.line_location_id =
                                         pll.line_location_id
                                  AND aid.po_distribution_id =
                                         pod.po_distribution_id
                                  AND pll.closed_code = 'FINALLY CLOSED')
                   --------------------------- and ai.invoice_id IN (423433)
                   AND EXISTS
                          (SELECT 'x'
                             FROM ap_invoices_all ai
                            WHERE     ai.invoice_id = aid.invoice_id
                                  AND ai.source IN
                                         ('PCARD_PARTIAL',
                                          'PCARD_FINAL',
                                          'PCARD_CREDIT')
                                  AND EXISTS
                                         (SELECT 'x'
                                            FROM nihap_pc_daily_txns_all ds
                                           WHERE     ai.invoice_num =
                                                        ds.invoice_num
                                                 AND ds.stmt_period =
                                                        p_period
															AND pcard_program_context_id = gpcardprogramcontextid));

            fnd_file.put_line (
               fnd_file.LOG,
                  SQL%ROWCOUNT
               || ' records updated in AP_Invoice_Distributions_all to set reversal flag for the original line to Y');

            COMMIT;



            --- Issue # 3

            -- Upward Adjustment entries
            INSERT INTO ap_invoice_distributions_all
               (SELECT SYSDATE ACCOUNTING_DATE,
                       aid.ACCRUAL_POSTED_FLAG,
                       aid.ASSETS_ADDITION_FLAG,
                       aid.ASSETS_TRACKING_FLAG,
                       aid.CASH_POSTED_FLAG,
                       99 DISTRIBUTION_LINE_NUMBER,
                       aid.DIST_CODE_COMBINATION_ID,
                       aid.INVOICE_ID,
                       -1 LAST_UPDATED_BY,
                       SYSDATE LAST_UPDATE_DATE,
                       aid.LINE_TYPE_LOOKUP_CODE,
                       aid.PERIOD_NAME,
                       aid.SET_OF_BOOKS_ID,
                       aid.ACCTS_PAY_CODE_COMBINATION_ID,
                       (  ai.invoice_amount
                        - (SELECT SUM (amount)
                             FROM ap_invoice_distributions_all aid3
                            WHERE aid3.invoice_id = ai.invoice_id))
                          AMOUNT,
                       NULL BASE_AMOUNT,
                       NULL BASE_INVOICE_PRICE_VARIANCE,
                       aid.BATCH_ID,
                       -1 CREATED_BY,
                       SYSDATE CREATION_DATE,
                       'P-Card Upward Adjustment' DESCRIPTION,
                       aid.EXCHANGE_RATE_VARIANCE,
                       aid.FINAL_MATCH_FLAG,
                       aid.INCOME_TAX_REGION,
                       aid.INVOICE_PRICE_VARIANCE,
                       aid.LAST_UPDATE_LOGIN,
                       aid.MATCH_STATUS_FLAG,
                       'N' POSTED_FLAG,
                       NULL,                       --- aid.PO_DISTRIBUTION_ID,
                       aid.PROGRAM_APPLICATION_ID,
                       aid.PROGRAM_ID,
                       SYSDATE PROGRAM_UPDATE_DATE,
                       NULL QUANTITY_INVOICED,
                       aid.RATE_VAR_CODE_COMBINATION_ID,
                       NULL REQUEST_ID,
                       aid.REVERSAL_FLAG,
                       aid.TYPE_1099,
                       NULL UNIT_PRICE,
                       -- aid.VAT_CODE, --R12TU
                       aid.AMOUNT_ENCUMBERED,
                       aid.BASE_AMOUNT_ENCUMBERED,
                       aid.ENCUMBERED_FLAG,
                       aid.EXCHANGE_DATE,
                       aid.EXCHANGE_RATE,
                       aid.EXCHANGE_RATE_TYPE,
                       aid.PRICE_ADJUSTMENT_FLAG,
                       aid.PRICE_VAR_CODE_COMBINATION_ID,
                       aid.QUANTITY_UNENCUMBERED,
                       aid.STAT_AMOUNT,
                       aid.AMOUNT_TO_POST,
                       aid.ATTRIBUTE1,
                       aid.ATTRIBUTE10,
                       aid.ATTRIBUTE11,
                       aid.ATTRIBUTE12,
                       aid.ATTRIBUTE13,
                       aid.ATTRIBUTE14,
                       aid.ATTRIBUTE15,
                       aid.ATTRIBUTE2,
                       aid.ATTRIBUTE3,
                       aid.ATTRIBUTE4,
                       aid.ATTRIBUTE5,
                       aid.ATTRIBUTE6,
                       aid.ATTRIBUTE7,
                       aid.ATTRIBUTE8,
                       aid.ATTRIBUTE9,
                       aid.ATTRIBUTE_CATEGORY,
                       aid.BASE_AMOUNT_TO_POST,
                       aid.CASH_JE_BATCH_ID,
                       aid.EXPENDITURE_ITEM_DATE,
                       aid.EXPENDITURE_ORGANIZATION_ID,
                       aid.EXPENDITURE_TYPE,
                       aid.JE_BATCH_ID,
                       aid.PARENT_INVOICE_ID,
                       aid.PA_ADDITION_FLAG,
                       NULL PA_QUANTITY,
                       aid.POSTED_AMOUNT,
                       aid.POSTED_BASE_AMOUNT,
                       aid.PREPAY_AMOUNT_REMAINING,
                       aid.PROJECT_ACCOUNTING_CONTEXT,
                       aid.PROJECT_ID,
                       aid.TASK_ID,
                       aid.USSGL_TRANSACTION_CODE,
                       aid.USSGL_TRX_CODE_CONTEXT,
                       aid.EARLIEST_SETTLEMENT_DATE,
                       aid.REQ_DISTRIBUTION_ID,
                       aid.QUANTITY_VARIANCE,
                       aid.BASE_QUANTITY_VARIANCE,
                       aid.PACKET_ID,
                       aid.AWT_FLAG,
                       aid.AWT_GROUP_ID,
                       aid.AWT_TAX_RATE_ID,
                       aid.AWT_GROSS_AMOUNT,
                       aid.AWT_INVOICE_ID,
                       aid.AWT_ORIGIN_GROUP_ID,
                       aid.REFERENCE_1,
                       aid.REFERENCE_2,
                       aid.ORG_ID,
                       aid.OTHER_INVOICE_ID,
                       aid.AWT_INVOICE_PAYMENT_ID,
                       aid.GLOBAL_ATTRIBUTE_CATEGORY,
                       aid.GLOBAL_ATTRIBUTE1,
                       aid.GLOBAL_ATTRIBUTE2,
                       aid.GLOBAL_ATTRIBUTE3,
                       aid.GLOBAL_ATTRIBUTE4,
                       aid.GLOBAL_ATTRIBUTE5,
                       aid.GLOBAL_ATTRIBUTE6,
                       aid.GLOBAL_ATTRIBUTE7,
                       aid.GLOBAL_ATTRIBUTE8,
                       aid.GLOBAL_ATTRIBUTE9,
                       aid.GLOBAL_ATTRIBUTE10,
                       aid.GLOBAL_ATTRIBUTE11,
                       aid.GLOBAL_ATTRIBUTE12,
                       aid.GLOBAL_ATTRIBUTE13,
                       aid.GLOBAL_ATTRIBUTE14,
                       aid.GLOBAL_ATTRIBUTE15,
                       aid.GLOBAL_ATTRIBUTE16,
                       aid.GLOBAL_ATTRIBUTE17,
                       aid.GLOBAL_ATTRIBUTE18,
                       aid.GLOBAL_ATTRIBUTE19,
                       aid.GLOBAL_ATTRIBUTE20,
                       --aid.AMOUNT_INCLUDES_TAX_FLAG,-- R12TU
                       --aid.TAX_CALCULATED_FLAG,-- R12TU
                       aid.LINE_GROUP_NUMBER,
                       aid.RECEIPT_VERIFIED_FLAG,
                       aid.RECEIPT_REQUIRED_FLAG,
                       aid.RECEIPT_MISSING_FLAG,
                       aid.JUSTIFICATION,
                       aid.EXPENSE_GROUP,
                       aid.START_EXPENSE_DATE,
                       aid.END_EXPENSE_DATE,
                       aid.RECEIPT_CURRENCY_CODE,
                       aid.RECEIPT_CONVERSION_RATE,
                       aid.RECEIPT_CURRENCY_AMOUNT,
                       aid.DAILY_AMOUNT,
                       aid.WEB_PARAMETER_ID,
                       aid.ADJUSTMENT_REASON,
                       aid.AWARD_ID,
                       aid.MRC_ACCRUAL_POSTED_FLAG,        --R12TU New columns
                       aid.MRC_CASH_POSTED_FLAG,           --R12TU New columns
                       aid.MRC_DIST_CODE_COMBINATION_ID,
                       aid.MRC_AMOUNT,                     -- R12TU New column
                       aid.MRC_BASE_AMOUNT,
                       aid.MRC_BASE_INV_PRICE_VARIANCE,
                       aid.MRC_EXCHANGE_RATE_VARIANCE,
                       aid.MRC_POSTED_FLAG,                --R12TU New columns
                       aid.MRC_PROGRAM_APPLICATION_ID,    -- R12TU New columns
                       aid.MRC_PROGRAM_ID,                 --R12TU New columns
                       aid.MRC_PROGRAM_UPDATE_DATE,       -- R12TU New columns
                       aid.MRC_RATE_VAR_CCID,
                       aid.MRC_REQUEST_ID,                 -- R12TU New column
                       aid.MRC_EXCHANGE_DATE,
                       aid.MRC_EXCHANGE_RATE,
                       aid.MRC_EXCHANGE_RATE_TYPE,
                       aid.MRC_AMOUNT_TO_POST,             -- R12TU New column
                       aid.MRC_BASE_AMOUNT_TO_POST,         --R12TU New column
                       aid.MRC_CASH_JE_BATCH_ID,           -- R12TU New column
                       aid.MRC_JE_BATCH_ID,                 --R12TU New column
                       aid.MRC_POSTED_AMOUNT,              -- R12TU New column
                       aid.MRC_POSTED_BASE_AMOUNT,         -- R12TU New column
                       aid.MRC_RECEIPT_CONVERSION_RATE,
                       aid.CREDIT_CARD_TRX_ID,             -- R12TU New column
                       aid.DIST_MATCH_TYPE,
                       aid.RCV_TRANSACTION_ID,
                       AP_INVOICE_DISTRIBUTIONS_S.NEXTVAL
                          INVOICE_DISTRIBUTION_ID,
                       aid.PARENT_REVERSAL_ID,
                       --aid.TAX_RECOVERY_RATE,--R12TU Obsolete Column
                       --aid.TAX_RECOVERY_OVERRIDE_FLAG,--R12TU Obsolete Column
                       aid.TAX_RECOVERABLE_FLAG,
                       --aid.TAX_CODE_OVERRIDE_FLAG,--R12TU Obsolete Column
                       --aid.TAX_CODE_ID,--R12TU Obsolete Column
                       aid.PA_CC_AR_INVOICE_ID,
                       aid.PA_CC_AR_INVOICE_LINE_NUM,
                       aid.PA_CC_PROCESSED_CODE,
                       aid.MERCHANT_DOCUMENT_NUMBER,
                       aid.MERCHANT_NAME,
                       aid.MERCHANT_REFERENCE,
                       aid.MERCHANT_TAX_REG_NUMBER,
                       aid.MERCHANT_TAXPAYER_ID,
                       aid.COUNTRY_OF_SUPPLY,
                       aid.MATCHED_UOM_LOOKUP_CODE,
                       aid.GMS_BURDENABLE_RAW_COST,
                       aid.ACCOUNTING_EVENT_ID,
                       aid.PREPAY_DISTRIBUTION_ID,
                       --aid.CREDIT_CARD_TRX_ID,-- R12TU Different position
                       aid.UPGRADE_POSTED_AMT,
                       aid.UPGRADE_BASE_POSTED_AMT,
                       aid.INVENTORY_TRANSFER_STATUS,
                       aid.COMPANY_PREPAID_INVOICE_ID,
                       aid.CC_REVERSAL_FLAG,
                       --aid.PREPAY_TAX_PARENT_ID,--R12TU Obsolete
                       aid.AWT_WITHHELD_AMT,
                       aid.INVOICE_INCLUDES_PREPAY_FLAG,
                       aid.PRICE_CORRECT_INV_ID,
                       aid.PRICE_CORRECT_QTY,
                       aid.PA_CMT_XFACE_FLAG,
                       aid.CANCELLATION_FLAG,
                       --aid.FULLY_PAID_ACCTD_FLAG,-- R12TU Obsolete
                       --aid.ROOT_DISTRIBUTION_ID,-- R12TU Obsolete
                       --aid.XINV_PARENT_REVERSAL_ID,-- R12TU Obsolete
                       aid.INVOICE_LINE_NUMBER,            -- R12TU New Column
                       aid.CORRECTED_INVOICE_DIST_ID,      -- R12TU New Column
                       aid.ROUNDING_AMT,                   -- R12TU New Column
                       aid.CHARGE_APPLICABLE_TO_DIST_ID,   -- R12TU New Column
                       aid.CORRECTED_QUANTITY,             -- R12TU New Column
                       aid.RELATED_ID,                     -- R12TU New Column
                       aid.ASSET_BOOK_TYPE_CODE,           -- R12TU New Column
                       aid.ASSET_CATEGORY_ID,              -- R12TU New Column
                       aid.DISTRIBUTION_CLASS,             -- R12TU New Column
                       aid.FINAL_PAYMENT_ROUNDING,         -- R12TU New Column
                       aid.FINAL_APPLICATION_ROUNDING,     -- R12TU New Column
                       aid.AMOUNT_AT_PREPAY_XRATE,         -- R12TU New Column
                       aid.CASH_BASIS_FINAL_APP_ROUNDING,  -- R12TU New Column
                       aid.AMOUNT_AT_PREPAY_PAY_XRATE,     -- R12TU New Column
                       aid.INTENDED_USE,                   -- R12TU New Column
                       aid.DETAIL_TAX_DIST_ID,             -- R12TU New Column
                       aid.REC_NREC_RATE,                  -- R12TU New Column
                       aid.RECOVERY_RATE_ID,               -- R12TU New Column
                       aid.RECOVERY_RATE_NAME,             -- R12TU New Column
                       aid.RECOVERY_TYPE_CODE,             -- R12TU New Column
                       aid.RECOVERY_RATE_CODE,             -- R12TU New Column
                       aid.WITHHOLDING_TAX_CODE_ID,        -- R12TU New Column
                       aid.TAX_ALREADY_DISTRIBUTED_FLAG,   -- R12TU New Column
                       aid.SUMMARY_TAX_LINE_ID,            -- R12TU New Column
                       aid.TAXABLE_AMOUNT,                 -- R12TU New Column
                       aid.TAXABLE_BASE_AMOUNT,            -- R12TU New Column
                       aid.EXTRA_PO_ERV,                   -- R12TU New Column
                       aid.PREPAY_TAX_DIFF_AMOUNT,         -- R12TU New Column
                       aid.TAX_CODE_ID,                    -- R12TU New Column
                       aid.VAT_CODE,                       -- R12TU New Column
                       aid.AMOUNT_INCLUDES_TAX_FLAG,       -- R12TU New Column
                       aid.TAX_CALCULATED_FLAG,            -- R12TU New Column
                       aid.TAX_RECOVERY_RATE,              -- R12TU New Column
                       aid.TAX_RECOVERY_OVERRIDE_FLAG,     -- R12TU New Column
                       aid.TAX_CODE_OVERRIDE_FLAG,         -- R12TU New Column
                       aid.TOTAL_DIST_AMOUNT,              -- R12TU New Column
                       aid.TOTAL_DIST_BASE_AMOUNT,         -- R12TU New Column
                       aid.PREPAY_TAX_PARENT_ID,           -- R12TU New Column
                       aid.CANCELLED_FLAG,                 -- R12TU New Column
                       aid.OLD_DISTRIBUTION_ID,            -- R12TU New Column
                       aid.OLD_DIST_LINE_NUMBER,           -- R12TU New Column
                       aid.AMOUNT_VARIANCE,
                       aid.BASE_AMOUNT_VARIANCE,
                       aid.HISTORICAL_FLAG,                -- R12TU New Column
                       aid.RCV_CHARGE_ADDITION_FLAG,       -- R12TU New Column
                       aid.AWT_RELATED_ID,                 -- R12TU New Column
                       aid.RELATED_RETAINAGE_DIST_ID,      -- R12TU New Column
                       aid.RETAINED_AMOUNT_REMAINING,      -- R12TU New Column
                       aid.BC_EVENT_ID,                    -- R12TU New Column
                       aid.RETAINED_INVOICE_DIST_ID,       -- R12TU New Column
                       aid.FINAL_RELEASE_ROUNDING,         -- R12TU New Column
                       aid.FULLY_PAID_ACCTD_FLAG,          -- R12TU New Column
                       aid.ROOT_DISTRIBUTION_ID,           -- R12TU New Column
                       aid.XINV_PARENT_REVERSAL_ID,        -- R12TU New Column
                       aid.RECURRING_PAYMENT_ID,
                       aid.RELEASE_INV_DIST_DERIVED_FROM,  -- R12TU New Column
                       aid.PAY_AWT_GROUP_ID                -- R12TU New Column
                  FROM ap_invoices_all ai,
                       ap_invoice_distributions_all aid,
                       ap_holds_all ah
                 WHERE     ai.invoice_id = aid.invoice_id
                       AND ai.invoice_id = ah.invoice_id
                       AND ai.source IN
                              ('PCARD_PARTIAL', 'PCARD_FINAL', 'PCARD_CREDIT')
                       AND ah.hold_lookup_code = 'DIST VARIANCE'
                       AND release_lookup_code IS NULL
                       AND ai.invoice_amount >
                              (SELECT NVL (SUM (aid2.amount), 2)
                                 FROM ap_invoice_distributions_all aid2
                                WHERE aid2.invoice_id = ai.invoice_id)
                       --  AND aid.amount = (SELECT MAX(amount) FROM ap_invoice_distributions_all aid2 WHERE aid2.invoice_id = ai.invoice_id)
                       AND aid.invoice_distribution_id =
                              (SELECT MAX (invoice_distribution_id)
                                 FROM ap_invoice_distributions_all aid3
                                WHERE     aid3.invoice_id = ai.invoice_id
                                      AND aid3.amount =
                                             (SELECT MAX (amount)
                                                FROM ap_invoice_distributions_all aid2
                                               WHERE aid2.invoice_id =
                                                        ai.invoice_id)));

            fnd_file.put_line (
               fnd_file.LOG,
                  SQL%ROWCOUNT
               || ' upward adjustment records inserted into AP_Invoice_Distributions_all to fix DIST VARIANCE hold');

            -- Downward Adjustment entries
            INSERT INTO ap_invoice_distributions_all
               (SELECT SYSDATE ACCOUNTING_DATE,
                       aid.ACCRUAL_POSTED_FLAG,
                       aid.ASSETS_ADDITION_FLAG,
                       aid.ASSETS_TRACKING_FLAG,
                       aid.CASH_POSTED_FLAG,
                       99 DISTRIBUTION_LINE_NUMBER,
                       aid.DIST_CODE_COMBINATION_ID,
                       aid.INVOICE_ID,
                       -1 LAST_UPDATED_BY,
                       SYSDATE LAST_UPDATE_DATE,
                       aid.LINE_TYPE_LOOKUP_CODE,
                       aid.PERIOD_NAME,
                       aid.SET_OF_BOOKS_ID,
                       aid.ACCTS_PAY_CODE_COMBINATION_ID,
                       (  ai.invoice_amount
                        - (SELECT SUM (amount)
                             FROM ap_invoice_distributions_all aid3
                            WHERE aid3.invoice_id = ai.invoice_id))
                          AMOUNT,
                       NULL BASE_AMOUNT,
                       NULL BASE_INVOICE_PRICE_VARIANCE,
                       aid.BATCH_ID,
                       -1 CREATED_BY,
                       SYSDATE CREATION_DATE,
                       'P-Card Downward Adjustment' DESCRIPTION,
                       aid.EXCHANGE_RATE_VARIANCE,
                       aid.FINAL_MATCH_FLAG,
                       aid.INCOME_TAX_REGION,
                       aid.INVOICE_PRICE_VARIANCE,
                       aid.LAST_UPDATE_LOGIN,
                       aid.MATCH_STATUS_FLAG,
                       'N' POSTED_FLAG,
                       NULL,                       --- aid.PO_DISTRIBUTION_ID,
                       aid.PROGRAM_APPLICATION_ID,
                       aid.PROGRAM_ID,
                       SYSDATE PROGRAM_UPDATE_DATE,
                       NULL QUANTITY_INVOICED,
                       aid.RATE_VAR_CODE_COMBINATION_ID,
                       NULL REQUEST_ID,
                       aid.REVERSAL_FLAG,
                       aid.TYPE_1099,
                       NULL UNIT_PRICE,
                       --aid.VAT_CODE,--R12TU
                       aid.AMOUNT_ENCUMBERED,
                       aid.BASE_AMOUNT_ENCUMBERED,
                       aid.ENCUMBERED_FLAG,
                       aid.EXCHANGE_DATE,
                       aid.EXCHANGE_RATE,
                       aid.EXCHANGE_RATE_TYPE,
                       aid.PRICE_ADJUSTMENT_FLAG,
                       aid.PRICE_VAR_CODE_COMBINATION_ID,
                       aid.QUANTITY_UNENCUMBERED,
                       aid.STAT_AMOUNT,
                       aid.AMOUNT_TO_POST,
                       aid.ATTRIBUTE1,
                       aid.ATTRIBUTE10,
                       aid.ATTRIBUTE11,
                       aid.ATTRIBUTE12,
                       aid.ATTRIBUTE13,
                       aid.ATTRIBUTE14,
                       aid.ATTRIBUTE15,
                       aid.ATTRIBUTE2,
                       aid.ATTRIBUTE3,
                       aid.ATTRIBUTE4,
                       aid.ATTRIBUTE5,
                       aid.ATTRIBUTE6,
                       aid.ATTRIBUTE7,
                       aid.ATTRIBUTE8,
                       aid.ATTRIBUTE9,
                       aid.ATTRIBUTE_CATEGORY,
                       aid.BASE_AMOUNT_TO_POST,
                       aid.CASH_JE_BATCH_ID,
                       aid.EXPENDITURE_ITEM_DATE,
                       aid.EXPENDITURE_ORGANIZATION_ID,
                       aid.EXPENDITURE_TYPE,
                       aid.JE_BATCH_ID,
                       aid.PARENT_INVOICE_ID,
                       aid.PA_ADDITION_FLAG,
                       NULL PA_QUANTITY,
                       aid.POSTED_AMOUNT,
                       aid.POSTED_BASE_AMOUNT,
                       aid.PREPAY_AMOUNT_REMAINING,
                       aid.PROJECT_ACCOUNTING_CONTEXT,
                       aid.PROJECT_ID,
                       aid.TASK_ID,
                       aid.USSGL_TRANSACTION_CODE,
                       aid.USSGL_TRX_CODE_CONTEXT,
                       aid.EARLIEST_SETTLEMENT_DATE,
                       aid.REQ_DISTRIBUTION_ID,
                       aid.QUANTITY_VARIANCE,
                       aid.BASE_QUANTITY_VARIANCE,
                       aid.PACKET_ID,
                       aid.AWT_FLAG,
                       aid.AWT_GROUP_ID,
                       aid.AWT_TAX_RATE_ID,
                       aid.AWT_GROSS_AMOUNT,
                       aid.AWT_INVOICE_ID,
                       aid.AWT_ORIGIN_GROUP_ID,
                       aid.REFERENCE_1,
                       aid.REFERENCE_2,
                       aid.ORG_ID,
                       aid.OTHER_INVOICE_ID,
                       aid.AWT_INVOICE_PAYMENT_ID,
                       aid.GLOBAL_ATTRIBUTE_CATEGORY,
                       aid.GLOBAL_ATTRIBUTE1,
                       aid.GLOBAL_ATTRIBUTE2,
                       aid.GLOBAL_ATTRIBUTE3,
                       aid.GLOBAL_ATTRIBUTE4,
                       aid.GLOBAL_ATTRIBUTE5,
                       aid.GLOBAL_ATTRIBUTE6,
                       aid.GLOBAL_ATTRIBUTE7,
                       aid.GLOBAL_ATTRIBUTE8,
                       aid.GLOBAL_ATTRIBUTE9,
                       aid.GLOBAL_ATTRIBUTE10,
                       aid.GLOBAL_ATTRIBUTE11,
                       aid.GLOBAL_ATTRIBUTE12,
                       aid.GLOBAL_ATTRIBUTE13,
                       aid.GLOBAL_ATTRIBUTE14,
                       aid.GLOBAL_ATTRIBUTE15,
                       aid.GLOBAL_ATTRIBUTE16,
                       aid.GLOBAL_ATTRIBUTE17,
                       aid.GLOBAL_ATTRIBUTE18,
                       aid.GLOBAL_ATTRIBUTE19,
                       aid.GLOBAL_ATTRIBUTE20,
                       --aid.AMOUNT_INCLUDES_TAX_FLAG,-- R12TU
                       --aid.TAX_CALCULATED_FLAG,-- R12TU
                       aid.LINE_GROUP_NUMBER,
                       aid.RECEIPT_VERIFIED_FLAG,
                       aid.RECEIPT_REQUIRED_FLAG,
                       aid.RECEIPT_MISSING_FLAG,
                       aid.JUSTIFICATION,
                       aid.EXPENSE_GROUP,
                       aid.START_EXPENSE_DATE,
                       aid.END_EXPENSE_DATE,
                       aid.RECEIPT_CURRENCY_CODE,
                       aid.RECEIPT_CONVERSION_RATE,
                       aid.RECEIPT_CURRENCY_AMOUNT,
                       aid.DAILY_AMOUNT,
                       aid.WEB_PARAMETER_ID,
                       aid.ADJUSTMENT_REASON,
                       aid.AWARD_ID,
                       aid.MRC_ACCRUAL_POSTED_FLAG,        --R12TU New columns
                       aid.MRC_CASH_POSTED_FLAG,           --R12TU New columns
                       aid.MRC_DIST_CODE_COMBINATION_ID,
                       aid.MRC_AMOUNT,                     -- R12TU New column
                       aid.MRC_BASE_AMOUNT,
                       aid.MRC_BASE_INV_PRICE_VARIANCE,
                       aid.MRC_EXCHANGE_RATE_VARIANCE,
                       aid.MRC_POSTED_FLAG,                --R12TU New columns
                       aid.MRC_PROGRAM_APPLICATION_ID,    -- R12TU New columns
                       aid.MRC_PROGRAM_ID,                 --R12TU New columns
                       aid.MRC_PROGRAM_UPDATE_DATE,       -- R12TU New columns
                       aid.MRC_RATE_VAR_CCID,
                       aid.MRC_REQUEST_ID,                 -- R12TU New column
                       aid.MRC_EXCHANGE_DATE,
                       aid.MRC_EXCHANGE_RATE,
                       aid.MRC_EXCHANGE_RATE_TYPE,
                       aid.MRC_AMOUNT_TO_POST,             -- R12TU New column
                       aid.MRC_BASE_AMOUNT_TO_POST,         --R12TU New column
                       aid.MRC_CASH_JE_BATCH_ID,           -- R12TU New column
                       aid.MRC_JE_BATCH_ID,                 --R12TU New column
                       aid.MRC_POSTED_AMOUNT,              -- R12TU New column
                       aid.MRC_POSTED_BASE_AMOUNT,         -- R12TU New column
                       aid.MRC_RECEIPT_CONVERSION_RATE,
                       aid.CREDIT_CARD_TRX_ID,             -- R12TU New column
                       aid.DIST_MATCH_TYPE,
                       aid.RCV_TRANSACTION_ID,
                       AP_INVOICE_DISTRIBUTIONS_S.NEXTVAL
                          INVOICE_DISTRIBUTION_ID,
                       aid.PARENT_REVERSAL_ID,
                       --aid.TAX_RECOVERY_RATE,--R12TU Obsolete Column
                       --aid.TAX_RECOVERY_OVERRIDE_FLAG,--R12TU Obsolete Column
                       aid.TAX_RECOVERABLE_FLAG,
                       --aid.TAX_CODE_OVERRIDE_FLAG,--R12TU Obsolete Column
                       --aid.TAX_CODE_ID,--R12TU Obsolete Column
                       aid.PA_CC_AR_INVOICE_ID,
                       aid.PA_CC_AR_INVOICE_LINE_NUM,
                       aid.PA_CC_PROCESSED_CODE,
                       aid.MERCHANT_DOCUMENT_NUMBER,
                       aid.MERCHANT_NAME,
                       aid.MERCHANT_REFERENCE,
                       aid.MERCHANT_TAX_REG_NUMBER,
                       aid.MERCHANT_TAXPAYER_ID,
                       aid.COUNTRY_OF_SUPPLY,
                       aid.MATCHED_UOM_LOOKUP_CODE,
                       aid.GMS_BURDENABLE_RAW_COST,
                       aid.ACCOUNTING_EVENT_ID,
                       aid.PREPAY_DISTRIBUTION_ID,
                       --aid.CREDIT_CARD_TRX_ID,-- R12TU Different position
                       aid.UPGRADE_POSTED_AMT,
                       aid.UPGRADE_BASE_POSTED_AMT,
                       aid.INVENTORY_TRANSFER_STATUS,
                       aid.COMPANY_PREPAID_INVOICE_ID,
                       aid.CC_REVERSAL_FLAG,
                       --aid.PREPAY_TAX_PARENT_ID,--R12TU Obsolete
                       aid.AWT_WITHHELD_AMT,
                       aid.INVOICE_INCLUDES_PREPAY_FLAG,
                       aid.PRICE_CORRECT_INV_ID,
                       aid.PRICE_CORRECT_QTY,
                       aid.PA_CMT_XFACE_FLAG,
                       aid.CANCELLATION_FLAG,
                       --aid.FULLY_PAID_ACCTD_FLAG,-- R12TU Obsolete
                       --aid.ROOT_DISTRIBUTION_ID,-- R12TU Obsolete
                       --aid.XINV_PARENT_REVERSAL_ID,-- R12TU Obsolete
                       aid.INVOICE_LINE_NUMBER,            -- R12TU New Column
                       aid.CORRECTED_INVOICE_DIST_ID,      -- R12TU New Column
                       aid.ROUNDING_AMT,                   -- R12TU New Column
                       aid.CHARGE_APPLICABLE_TO_DIST_ID,   -- R12TU New Column
                       aid.CORRECTED_QUANTITY,             -- R12TU New Column
                       aid.RELATED_ID,                     -- R12TU New Column
                       aid.ASSET_BOOK_TYPE_CODE,           -- R12TU New Column
                       aid.ASSET_CATEGORY_ID,              -- R12TU New Column
                       aid.DISTRIBUTION_CLASS,             -- R12TU New Column
                       aid.FINAL_PAYMENT_ROUNDING,         -- R12TU New Column
                       aid.FINAL_APPLICATION_ROUNDING,     -- R12TU New Column
                       aid.AMOUNT_AT_PREPAY_XRATE,         -- R12TU New Column
                       aid.CASH_BASIS_FINAL_APP_ROUNDING,  -- R12TU New Column
                       aid.AMOUNT_AT_PREPAY_PAY_XRATE,     -- R12TU New Column
                       aid.INTENDED_USE,                   -- R12TU New Column
                       aid.DETAIL_TAX_DIST_ID,             -- R12TU New Column
                       aid.REC_NREC_RATE,                  -- R12TU New Column
                       aid.RECOVERY_RATE_ID,               -- R12TU New Column
                       aid.RECOVERY_RATE_NAME,             -- R12TU New Column
                       aid.RECOVERY_TYPE_CODE,             -- R12TU New Column
                       aid.RECOVERY_RATE_CODE,             -- R12TU New Column
                       aid.WITHHOLDING_TAX_CODE_ID,        -- R12TU New Column
                       aid.TAX_ALREADY_DISTRIBUTED_FLAG,   -- R12TU New Column
                       aid.SUMMARY_TAX_LINE_ID,            -- R12TU New Column
                       aid.TAXABLE_AMOUNT,                 -- R12TU New Column
                       aid.TAXABLE_BASE_AMOUNT,            -- R12TU New Column
                       aid.EXTRA_PO_ERV,                   -- R12TU New Column
                       aid.PREPAY_TAX_DIFF_AMOUNT,         -- R12TU New Column
                       aid.TAX_CODE_ID,                    -- R12TU New Column
                       aid.VAT_CODE,                       -- R12TU New Column
                       aid.AMOUNT_INCLUDES_TAX_FLAG,       -- R12TU New Column
                       aid.TAX_CALCULATED_FLAG,            -- R12TU New Column
                       aid.TAX_RECOVERY_RATE,              -- R12TU New Column
                       aid.TAX_RECOVERY_OVERRIDE_FLAG,     -- R12TU New Column
                       aid.TAX_CODE_OVERRIDE_FLAG,         -- R12TU New Column
                       aid.TOTAL_DIST_AMOUNT,              -- R12TU New Column
                       aid.TOTAL_DIST_BASE_AMOUNT,         -- R12TU New Column
                       aid.PREPAY_TAX_PARENT_ID,           -- R12TU New Column
                       aid.CANCELLED_FLAG,                 -- R12TU New Column
                       aid.OLD_DISTRIBUTION_ID,            -- R12TU New Column
                       aid.OLD_DIST_LINE_NUMBER,           -- R12TU New Column
                       aid.AMOUNT_VARIANCE,
                       aid.BASE_AMOUNT_VARIANCE,
                       aid.HISTORICAL_FLAG,                -- R12TU New Column
                       aid.RCV_CHARGE_ADDITION_FLAG,       -- R12TU New Column
                       aid.AWT_RELATED_ID,                 -- R12TU New Column
                       aid.RELATED_RETAINAGE_DIST_ID,      -- R12TU New Column
                       aid.RETAINED_AMOUNT_REMAINING,      -- R12TU New Column
                       aid.BC_EVENT_ID,                    -- R12TU New Column
                       aid.RETAINED_INVOICE_DIST_ID,       -- R12TU New Column
                       aid.FINAL_RELEASE_ROUNDING,         -- R12TU New Column
                       aid.FULLY_PAID_ACCTD_FLAG,          -- R12TU New Column
                       aid.ROOT_DISTRIBUTION_ID,           -- R12TU New Column
                       aid.XINV_PARENT_REVERSAL_ID,        -- R12TU New Column
                       aid.RECURRING_PAYMENT_ID,
                       aid.RELEASE_INV_DIST_DERIVED_FROM,  -- R12TU New Column
                       aid.PAY_AWT_GROUP_ID                -- R12TU New Column
                  FROM ap_invoices_all ai,
                       ap_invoice_distributions_all aid,
                       ap_holds_all ah
                 WHERE     ai.invoice_id = aid.invoice_id
                       AND ai.invoice_id = ah.invoice_id
                       AND ai.source IN
                              ('PCARD_PARTIAL', 'PCARD_FINAL', 'PCARD_CREDIT')
                       AND ah.hold_lookup_code = 'DIST VARIANCE'
                       AND release_lookup_code IS NULL -------------------------------
                       AND ai.invoice_amount <
                              (SELECT NVL (SUM (aid2.amount), 2)
                                 FROM ap_invoice_distributions_all aid2
                                WHERE aid2.invoice_id = ai.invoice_id)
                       AND aid.amount =
                              (SELECT MAX (amount)
                                 FROM ap_invoice_distributions_all aid2
                                WHERE aid2.invoice_id = ai.invoice_id)
                       AND aid.invoice_distribution_id =
                              (SELECT MAX (invoice_distribution_id)
                                 FROM ap_invoice_distributions_all aid3
                                WHERE     aid3.invoice_id = ai.invoice_id
                                      AND aid3.amount =
                                             (SELECT MAX (amount)
                                                FROM ap_invoice_distributions_all aid2
                                               WHERE aid2.invoice_id =
                                                        ai.invoice_id)) --  AND NOT EXISTS
                                                                       --      (SELECT 'x' FROM ap_holds_all ah2
                                                                       --        WHERE ah2.invoice_id = ai.invoice_id
                                                                       --          AND ah2.hold_lookup_code = 'QTY ORD'
                                                                       --          AND ah2.release_lookup_code IS NULL
                                                                       --      )
               );

            fnd_file.put_line (
               fnd_file.LOG,
                  SQL%ROWCOUNT
               || ' downward adjustment records inserted into AP_Invoice_Distributions_all to fix DIST VARIANCE hold');

            COMMIT;


            /*
               FOR c3_rec IN C3
               LOOP
                  vRecordCounter := vRecordCounter + 1;


                  SELECT MAX (distribution_line_number) + 1
                    INTO vDistLineNumber
                    FROM ap_invoice_distributions_all
                   WHERE invoice_id = c3_rec.invoice_id;

                  -- QTY ORD Adjustment - Reversal Entries

                  INSERT INTO ap_invoice_distributions_all
                       VALUES (c3_rec.ACCOUNTING_DATE,
                               c3_rec.ACCRUAL_POSTED_FLAG,
                               c3_rec.ASSETS_ADDITION_FLAG,
                               c3_rec.ASSETS_TRACKING_FLAG,
                               c3_rec.CASH_POSTED_FLAG,
                               vDistLineNumber, --------------------------- c3_rec.DISTRIBUTION_LINE_NUMBER,
                               c3_rec.DIST_CODE_COMBINATION_ID,
                               c3_rec.INVOICE_ID,
                               c3_rec.LAST_UPDATED_BY,
                               c3_rec.LAST_UPDATE_DATE,
                               c3_rec.LINE_TYPE_LOOKUP_CODE,
                               c3_rec.PERIOD_NAME,
                               c3_rec.SET_OF_BOOKS_ID,
                               c3_rec.ACCTS_PAY_CODE_COMBINATION_ID,
                               c3_rec.AMOUNT,
                               c3_rec.BASE_AMOUNT,
                               c3_rec.BASE_INVOICE_PRICE_VARIANCE,
                               c3_rec.BATCH_ID,
                               c3_rec.CREATED_BY,
                               c3_rec.CREATION_DATE,
                               c3_rec.DESCRIPTION,
                               c3_rec.EXCHANGE_RATE_VARIANCE,
                               c3_rec.FINAL_MATCH_FLAG,
                               c3_rec.INCOME_TAX_REGION,
                               c3_rec.INVOICE_PRICE_VARIANCE,
                               c3_rec.LAST_UPDATE_LOGIN,
                               c3_rec.MATCH_STATUS_FLAG,
                               c3_rec.POSTED_FLAG,
                               c3_rec.PO_DISTRIBUTION_ID,
                               c3_rec.PROGRAM_APPLICATION_ID,
                               c3_rec.PROGRAM_ID,
                               c3_rec.PROGRAM_UPDATE_DATE,
                               c3_rec.QUANTITY_INVOICED,
                               c3_rec.RATE_VAR_CODE_COMBINATION_ID,
                               c3_rec.REQUEST_ID,
                               c3_rec.REVERSAL_FLAG,
                               c3_rec.TYPE_1099,
                               c3_rec.UNIT_PRICE,
                               -- c3_rec.VAT_CODE,--R12TU
                               c3_rec.AMOUNT_ENCUMBERED,
                               c3_rec.BASE_AMOUNT_ENCUMBERED,
                               c3_rec.ENCUMBERED_FLAG,
                               c3_rec.EXCHANGE_DATE,
                               c3_rec.EXCHANGE_RATE,
                               c3_rec.EXCHANGE_RATE_TYPE,
                               c3_rec.PRICE_ADJUSTMENT_FLAG,
                               c3_rec.PRICE_VAR_CODE_COMBINATION_ID,
                               c3_rec.QUANTITY_UNENCUMBERED,
                               c3_rec.STAT_AMOUNT,
                               c3_rec.AMOUNT_TO_POST,
                               c3_rec.ATTRIBUTE1,
                               c3_rec.ATTRIBUTE10,
                               c3_rec.ATTRIBUTE11,
                               c3_rec.ATTRIBUTE12,
                               c3_rec.ATTRIBUTE13,
                               c3_rec.ATTRIBUTE14,
                               c3_rec.ATTRIBUTE15,
                               c3_rec.ATTRIBUTE2,
                               c3_rec.ATTRIBUTE3,
                               c3_rec.ATTRIBUTE4,
                               c3_rec.ATTRIBUTE5,
                               c3_rec.ATTRIBUTE6,
                               c3_rec.ATTRIBUTE7,
                               c3_rec.ATTRIBUTE8,
                               c3_rec.ATTRIBUTE9,
                               c3_rec.ATTRIBUTE_CATEGORY,
                               c3_rec.BASE_AMOUNT_TO_POST,
                               c3_rec.CASH_JE_BATCH_ID,
                               c3_rec.EXPENDITURE_ITEM_DATE,
                               c3_rec.EXPENDITURE_ORGANIZATION_ID,
                               c3_rec.EXPENDITURE_TYPE,
                               c3_rec.JE_BATCH_ID,
                               c3_rec.PARENT_INVOICE_ID,
                               c3_rec.PA_ADDITION_FLAG,
                               c3_rec.PA_QUANTITY,
                               c3_rec.POSTED_AMOUNT,
                               c3_rec.POSTED_BASE_AMOUNT,
                               c3_rec.PREPAY_AMOUNT_REMAINING,
                               c3_rec.PROJECT_ACCOUNTING_CONTEXT,
                               c3_rec.PROJECT_ID,
                               c3_rec.TASK_ID,
                               c3_rec.USSGL_TRANSACTION_CODE,
                               c3_rec.USSGL_TRX_CODE_CONTEXT,
                               c3_rec.EARLIEST_SETTLEMENT_DATE,
                               c3_rec.REQ_DISTRIBUTION_ID,
                               c3_rec.QUANTITY_VARIANCE,
                               c3_rec.BASE_QUANTITY_VARIANCE,
                               c3_rec.PACKET_ID,
                               c3_rec.AWT_FLAG,
                               c3_rec.AWT_GROUP_ID,
                               c3_rec.AWT_TAX_RATE_ID,
                               c3_rec.AWT_GROSS_AMOUNT,
                               c3_rec.AWT_INVOICE_ID,
                               c3_rec.AWT_ORIGIN_GROUP_ID,
                               c3_rec.REFERENCE_1,
                               c3_rec.REFERENCE_2,
                               c3_rec.ORG_ID,
                               c3_rec.OTHER_INVOICE_ID,
                               c3_rec.AWT_INVOICE_PAYMENT_ID,
                               c3_rec.GLOBAL_ATTRIBUTE_CATEGORY,
                               c3_rec.GLOBAL_ATTRIBUTE1,
                               c3_rec.GLOBAL_ATTRIBUTE2,
                               c3_rec.GLOBAL_ATTRIBUTE3,
                               c3_rec.GLOBAL_ATTRIBUTE4,
                               c3_rec.GLOBAL_ATTRIBUTE5,
                               c3_rec.GLOBAL_ATTRIBUTE6,
                               c3_rec.GLOBAL_ATTRIBUTE7,
                               c3_rec.GLOBAL_ATTRIBUTE8,
                               c3_rec.GLOBAL_ATTRIBUTE9,
                               c3_rec.GLOBAL_ATTRIBUTE10,
                               c3_rec.GLOBAL_ATTRIBUTE11,
                               c3_rec.GLOBAL_ATTRIBUTE12,
                               c3_rec.GLOBAL_ATTRIBUTE13,
                               c3_rec.GLOBAL_ATTRIBUTE14,
                               c3_rec.GLOBAL_ATTRIBUTE15,
                               c3_rec.GLOBAL_ATTRIBUTE16,
                               c3_rec.GLOBAL_ATTRIBUTE17,
                               c3_rec.GLOBAL_ATTRIBUTE18,
                               c3_rec.GLOBAL_ATTRIBUTE19,
                               c3_rec.GLOBAL_ATTRIBUTE20,
                               --c3_rec.AMOUNT_INCLUDES_TAX_FLAG,-- R12TU
                               --c3_rec.TAX_CALCULATED_FLAG,-- R12TU
                               c3_rec.LINE_GROUP_NUMBER,
                               c3_rec.RECEIPT_VERIFIED_FLAG,
                               c3_rec.RECEIPT_REQUIRED_FLAG,
                               c3_rec.RECEIPT_MISSING_FLAG,
                               c3_rec.JUSTIFICATION,
                               c3_rec.EXPENSE_GROUP,
                               c3_rec.START_EXPENSE_DATE,
                               c3_rec.END_EXPENSE_DATE,
                               c3_rec.RECEIPT_CURRENCY_CODE,
                               c3_rec.RECEIPT_CONVERSION_RATE,
                               c3_rec.RECEIPT_CURRENCY_AMOUNT,
                               c3_rec.DAILY_AMOUNT,
                               c3_rec.WEB_PARAMETER_ID,
                               c3_rec.ADJUSTMENT_REASON,
                               c3_rec.AWARD_ID,
                               c3_rec.MRC_ACCRUAL_POSTED_FLAG,         --R12TU New columns
                               c3_rec.MRC_CASH_POSTED_FLAG,            --R12TU New columns
                               c3_rec.MRC_DIST_CODE_COMBINATION_ID,
                               c3_rec.MRC_AMOUNT,                      -- R12TU New column
                               c3_rec.MRC_BASE_AMOUNT,
                               c3_rec.MRC_BASE_INV_PRICE_VARIANCE,
                               c3_rec.MRC_EXCHANGE_RATE_VARIANCE,
                               c3_rec.MRC_POSTED_FLAG,                 --R12TU New columns
                               c3_rec.MRC_PROGRAM_APPLICATION_ID,     -- R12TU New columns
                               c3_rec.MRC_PROGRAM_ID,                  --R12TU New columns
                               c3_rec.MRC_PROGRAM_UPDATE_DATE,        -- R12TU New columns
                               c3_rec.MRC_RATE_VAR_CCID,
                               c3_rec.MRC_REQUEST_ID,                  -- R12TU New column
                               c3_rec.MRC_EXCHANGE_DATE,
                               c3_rec.MRC_EXCHANGE_RATE,
                               c3_rec.MRC_EXCHANGE_RATE_TYPE,
                               c3_rec.MRC_AMOUNT_TO_POST,              -- R12TU New column
                               c3_rec.MRC_BASE_AMOUNT_TO_POST,          --R12TU New column
                               c3_rec.MRC_CASH_JE_BATCH_ID,            -- R12TU New column
                               c3_rec.MRC_JE_BATCH_ID,                  --R12TU New column
                               c3_rec.MRC_POSTED_AMOUNT,               -- R12TU New column
                               c3_rec.MRC_POSTED_BASE_AMOUNT,          -- R12TU New column
                               c3_rec.MRC_RECEIPT_CONVERSION_RATE,
                               c3_rec.CREDIT_CARD_TRX_ID,              -- R12TU New column
                               c3_rec.DIST_MATCH_TYPE,
                               c3_rec.RCV_TRANSACTION_ID,
                               c3_rec.INVOICE_DISTRIBUTION_ID,
                               c3_rec.PARENT_REVERSAL_ID,
                               --c3_rec.TAX_RECOVERY_RATE,--R12TU Obsolete Column
                               --c3_rec.TAX_RECOVERY_OVERRIDE_FLAG,--R12TU Obsolete Column
                               c3_rec.TAX_RECOVERABLE_FLAG,
                               --c3_rec.TAX_CODE_OVERRIDE_FLAG,--R12TU Obsolete Column
                               --c3_rec.TAX_CODE_ID,--R12TU Obsolete Column
                               c3_rec.PA_CC_AR_INVOICE_ID,
                               c3_rec.PA_CC_AR_INVOICE_LINE_NUM,
                               c3_rec.PA_CC_PROCESSED_CODE,
                               c3_rec.MERCHANT_DOCUMENT_NUMBER,
                               c3_rec.MERCHANT_NAME,
                               c3_rec.MERCHANT_REFERENCE,
                               c3_rec.MERCHANT_TAX_REG_NUMBER,
                               c3_rec.MERCHANT_TAXPAYER_ID,
                               c3_rec.COUNTRY_OF_SUPPLY,
                               c3_rec.MATCHED_UOM_LOOKUP_CODE,
                               c3_rec.GMS_BURDENABLE_RAW_COST,
                               c3_rec.ACCOUNTING_EVENT_ID,
                               c3_rec.PREPAY_DISTRIBUTION_ID,
                               --c3_rec.CREDIT_CARD_TRX_ID,-- R12TU Different position
                               c3_rec.UPGRADE_POSTED_AMT,
                               c3_rec.UPGRADE_BASE_POSTED_AMT,
                               c3_rec.INVENTORY_TRANSFER_STATUS,
                               c3_rec.COMPANY_PREPAID_INVOICE_ID,
                               c3_rec.CC_REVERSAL_FLAG,
                               --c3_rec.PREPAY_TAX_PARENT_ID,--R12TU Obsolete
                               c3_rec.AWT_WITHHELD_AMT,
                               c3_rec.INVOICE_INCLUDES_PREPAY_FLAG,
                               c3_rec.PRICE_CORRECT_INV_ID,
                               c3_rec.PRICE_CORRECT_QTY,
                               c3_rec.PA_CMT_XFACE_FLAG,
                               c3_rec.CANCELLATION_FLAG,
                               --c3_rec.FULLY_PAID_ACCTD_FLAG,-- R12TU Obsolete
                               --c3_rec.ROOT_DISTRIBUTION_ID,-- R12TU Obsolete
                               --c3_rec.XINV_PARENT_REVERSAL_ID,-- R12TU Obsolete
                               c3_rec.INVOICE_LINE_NUMBER,             -- R12TU New Column
                               c3_rec.CORRECTED_INVOICE_DIST_ID,       -- R12TU New Column
                               c3_rec.ROUNDING_AMT,                    -- R12TU New Column
                               c3_rec.CHARGE_APPLICABLE_TO_DIST_ID,    -- R12TU New Column
                               c3_rec.CORRECTED_QUANTITY,              -- R12TU New Column
                               c3_rec.RELATED_ID,                      -- R12TU New Column
                               c3_rec.ASSET_BOOK_TYPE_CODE,            -- R12TU New Column
                               c3_rec.ASSET_CATEGORY_ID,               -- R12TU New Column
                               c3_rec.DISTRIBUTION_CLASS,              -- R12TU New Column
                               c3_rec.FINAL_PAYMENT_ROUNDING,          -- R12TU New Column
                               c3_rec.FINAL_APPLICATION_ROUNDING,      -- R12TU New Column
                               c3_rec.AMOUNT_AT_PREPAY_XRATE,          -- R12TU New Column
                               c3_rec.CASH_BASIS_FINAL_APP_ROUNDING,   -- R12TU New Column
                               c3_rec.AMOUNT_AT_PREPAY_PAY_XRATE,      -- R12TU New Column
                               c3_rec.INTENDED_USE,                    -- R12TU New Column
                               c3_rec.DETAIL_TAX_DIST_ID,              -- R12TU New Column
                               c3_rec.REC_NREC_RATE,                   -- R12TU New Column
                               c3_rec.RECOVERY_RATE_ID,                -- R12TU New Column
                               c3_rec.RECOVERY_RATE_NAME,              -- R12TU New Column
                               c3_rec.RECOVERY_TYPE_CODE,              -- R12TU New Column
                               c3_rec.RECOVERY_RATE_CODE,              -- R12TU New Column
                               c3_rec.WITHHOLDING_TAX_CODE_ID,         -- R12TU New Column
                               c3_rec.TAX_ALREADY_DISTRIBUTED_FLAG,    -- R12TU New Column
                               c3_rec.SUMMARY_TAX_LINE_ID,             -- R12TU New Column
                               c3_rec.TAXABLE_AMOUNT,                  -- R12TU New Column
                               c3_rec.TAXABLE_BASE_AMOUNT,             -- R12TU New Column
                               c3_rec.EXTRA_PO_ERV,                    -- R12TU New Column
                               c3_rec.PREPAY_TAX_DIFF_AMOUNT,          -- R12TU New Column
                               c3_rec.TAX_CODE_ID,                     -- R12TU New Column
                               c3_rec.VAT_CODE,                        -- R12TU New Column
                               c3_rec.AMOUNT_INCLUDES_TAX_FLAG,        -- R12TU New Column
                               c3_rec.TAX_CALCULATED_FLAG,             -- R12TU New Column
                               c3_rec.TAX_RECOVERY_RATE,               -- R12TU New Column
                               c3_rec.TAX_RECOVERY_OVERRIDE_FLAG,      -- R12TU New Column
                               c3_rec.TAX_CODE_OVERRIDE_FLAG,          -- R12TU New Column
                               c3_rec.TOTAL_DIST_AMOUNT,               -- R12TU New Column
                               c3_rec.TOTAL_DIST_BASE_AMOUNT,          -- R12TU New Column
                               c3_rec.PREPAY_TAX_PARENT_ID,            -- R12TU New Column
                               c3_rec.CANCELLED_FLAG,                  -- R12TU New Column
                               c3_rec.OLD_DISTRIBUTION_ID,             -- R12TU New Column
                               c3_rec.OLD_DIST_LINE_NUMBER,            -- R12TU New Column
                               c3_rec.AMOUNT_VARIANCE,
                               c3_rec.baSE_AMOUNT_VARIANCE,
                               c3_rec.HISTORICAL_FLAG,                 -- R12TU New Column
                               c3_rec.RCV_CHARGE_ADDITION_FLAG,        -- R12TU New Column
                               c3_rec.AWT_RELATED_ID,                  -- R12TU New Column
                               c3_rec.RELATED_RETAINAGE_DIST_ID,       -- R12TU New Column
                               c3_rec.RETAINED_AMOUNT_REMAINING,       -- R12TU New Column
                               c3_rec.BC_EVENT_ID,                     -- R12TU New Column
                               c3_rec.RETAINED_INVOICE_DIST_ID,        -- R12TU New Column
                               c3_rec.FINAL_RELEASE_ROUNDING,          -- R12TU New Column
                               c3_rec.FULLY_PAID_ACCTD_FLAG,           -- R12TU New Column
                               c3_rec.ROOT_DISTRIBUTION_ID,            -- R12TU New Column
                               c3_rec.XINV_PARENT_REVERSAL_ID,         -- R12TU New Column
                               c3_rec.RECURRING_PAYMENT_ID,
                               c3_rec.RELEASE_INV_DIST_DERIVED_FROM,   -- R12TU New Column
                               c3_rec.PAY_AWT_GROUP_ID                 -- R12TU New Column
                                                      );
               END LOOP;

               fnd_file.put_line (fnd_file.LOG,
                     vRecordCounter
                  || ' records inserted into AP_Invoice_Distributions_all to reverse existing matched lines');

               vRecordCounter := 0;

               FOR c4_rec IN C4
               LOOP
                  vRecordCounter := vRecordCounter + 1;

                  SELECT MAX (distribution_line_number) + 1
                    INTO vDistLineNumber
                    FROM ap_invoice_distributions_all
                   WHERE invoice_id = c4_rec.invoice_id;

                  -- QTY ORD Adjustment - Reversal Entries

                  INSERT INTO ap_invoice_distributions_all
                       VALUES (c4_rec.ACCOUNTING_DATE,
                               c4_rec.ACCRUAL_POSTED_FLAG,
                               c4_rec.ASSETS_ADDITION_FLAG,
                               c4_rec.ASSETS_TRACKING_FLAG,
                               c4_rec.CASH_POSTED_FLAG,
                               vDistLineNumber, -------------- c4_rec.DISTRIBUTION_LINE_NUMBER,
                               c4_rec.DIST_CODE_COMBINATION_ID,
                               c4_rec.INVOICE_ID,
                               c4_rec.LAST_UPDATED_BY,
                               c4_rec.LAST_UPDATE_DATE,
                               c4_rec.LINE_TYPE_LOOKUP_CODE,
                               c4_rec.PERIOD_NAME,
                               c4_rec.SET_OF_BOOKS_ID,
                               c4_rec.ACCTS_PAY_CODE_COMBINATION_ID,
                               c4_rec.AMOUNT,
                               c4_rec.BASE_AMOUNT,
                               c4_rec.BASE_INVOICE_PRICE_VARIANCE,
                               c4_rec.BATCH_ID,
                               c4_rec.CREATED_BY,
                               c4_rec.CREATION_DATE,
                               c4_rec.DESCRIPTION,
                               c4_rec.EXCHANGE_RATE_VARIANCE,
                               c4_rec.FINAL_MATCH_FLAG,
                               c4_rec.INCOME_TAX_REGION,
                               c4_rec.INVOICE_PRICE_VARIANCE,
                               c4_rec.LAST_UPDATE_LOGIN,
                               c4_rec.MATCH_STATUS_FLAG,
                               c4_rec.POSTED_FLAG,
                               c4_rec.PO_DISTRIBUTION_ID,
                               c4_rec.PROGRAM_APPLICATION_ID,
                               c4_rec.PROGRAM_ID,
                               c4_rec.PROGRAM_UPDATE_DATE,
                               c4_rec.QUANTITY_INVOICED,
                               c4_rec.RATE_VAR_CODE_COMBINATION_ID,
                               c4_rec.REQUEST_ID,
                               c4_rec.REVERSAL_FLAG,
                               c4_rec.TYPE_1099,
                               c4_rec.UNIT_PRICE,
                               --c4_rec.VAT_CODE,--R12TU
                               c4_rec.AMOUNT_ENCUMBERED,
                               c4_rec.BASE_AMOUNT_ENCUMBERED,
                               c4_rec.ENCUMBERED_FLAG,
                               c4_rec.EXCHANGE_DATE,
                               c4_rec.EXCHANGE_RATE,
                               c4_rec.EXCHANGE_RATE_TYPE,
                               c4_rec.PRICE_ADJUSTMENT_FLAG,
                               c4_rec.PRICE_VAR_CODE_COMBINATION_ID,
                               c4_rec.QUANTITY_UNENCUMBERED,
                               c4_rec.STAT_AMOUNT,
                               c4_rec.AMOUNT_TO_POST,
                               c4_rec.ATTRIBUTE1,
                               c4_rec.ATTRIBUTE10,
                               c4_rec.ATTRIBUTE11,
                               c4_rec.ATTRIBUTE12,
                               c4_rec.ATTRIBUTE13,
                               c4_rec.ATTRIBUTE14,
                               c4_rec.ATTRIBUTE15,
                               c4_rec.ATTRIBUTE2,
                               c4_rec.ATTRIBUTE3,
                               c4_rec.ATTRIBUTE4,
                               c4_rec.ATTRIBUTE5,
                               c4_rec.ATTRIBUTE6,
                               c4_rec.ATTRIBUTE7,
                               c4_rec.ATTRIBUTE8,
                               c4_rec.ATTRIBUTE9,
                               c4_rec.ATTRIBUTE_CATEGORY,
                               c4_rec.BASE_AMOUNT_TO_POST,
                               c4_rec.CASH_JE_BATCH_ID,
                               c4_rec.EXPENDITURE_ITEM_DATE,
                               c4_rec.EXPENDITURE_ORGANIZATION_ID,
                               c4_rec.EXPENDITURE_TYPE,
                               c4_rec.JE_BATCH_ID,
                               c4_rec.PARENT_INVOICE_ID,
                               c4_rec.PA_ADDITION_FLAG,
                               c4_rec.PA_QUANTITY,
                               c4_rec.POSTED_AMOUNT,
                               c4_rec.POSTED_BASE_AMOUNT,
                               c4_rec.PREPAY_AMOUNT_REMAINING,
                               c4_rec.PROJECT_ACCOUNTING_CONTEXT,
                               c4_rec.PROJECT_ID,
                               c4_rec.TASK_ID,
                               c4_rec.USSGL_TRANSACTION_CODE,
                               c4_rec.USSGL_TRX_CODE_CONTEXT,
                               c4_rec.EARLIEST_SETTLEMENT_DATE,
                               c4_rec.REQ_DISTRIBUTION_ID,
                               c4_rec.QUANTITY_VARIANCE,
                               c4_rec.BASE_QUANTITY_VARIANCE,
                               c4_rec.PACKET_ID,
                               c4_rec.AWT_FLAG,
                               c4_rec.AWT_GROUP_ID,
                               c4_rec.AWT_TAX_RATE_ID,
                               c4_rec.AWT_GROSS_AMOUNT,
                               c4_rec.AWT_INVOICE_ID,
                               c4_rec.AWT_ORIGIN_GROUP_ID,
                               c4_rec.REFERENCE_1,
                               c4_rec.REFERENCE_2,
                               c4_rec.ORG_ID,
                               c4_rec.OTHER_INVOICE_ID,
                               c4_rec.AWT_INVOICE_PAYMENT_ID,
                               c4_rec.GLOBAL_ATTRIBUTE_CATEGORY,
                               c4_rec.GLOBAL_ATTRIBUTE1,
                               c4_rec.GLOBAL_ATTRIBUTE2,
                               c4_rec.GLOBAL_ATTRIBUTE3,
                               c4_rec.GLOBAL_ATTRIBUTE4,
                               c4_rec.GLOBAL_ATTRIBUTE5,
                               c4_rec.GLOBAL_ATTRIBUTE6,
                               c4_rec.GLOBAL_ATTRIBUTE7,
                               c4_rec.GLOBAL_ATTRIBUTE8,
                               c4_rec.GLOBAL_ATTRIBUTE9,
                               c4_rec.GLOBAL_ATTRIBUTE10,
                               c4_rec.GLOBAL_ATTRIBUTE11,
                               c4_rec.GLOBAL_ATTRIBUTE12,
                               c4_rec.GLOBAL_ATTRIBUTE13,
                               c4_rec.GLOBAL_ATTRIBUTE14,
                               c4_rec.GLOBAL_ATTRIBUTE15,
                               c4_rec.GLOBAL_ATTRIBUTE16,
                               c4_rec.GLOBAL_ATTRIBUTE17,
                               c4_rec.GLOBAL_ATTRIBUTE18,
                               c4_rec.GLOBAL_ATTRIBUTE19,
                               c4_rec.GLOBAL_ATTRIBUTE20,
                               --c4_rec.AMOUNT_INCLUDES_TAX_FLAG,-- R12TU
                               --c4_rec.TAX_CALCULATED_FLAG,-- R12TU
                               c4_rec.LINE_GROUP_NUMBER,
                               c4_rec.RECEIPT_VERIFIED_FLAG,
                               c4_rec.RECEIPT_REQUIRED_FLAG,
                               c4_rec.RECEIPT_MISSING_FLAG,
                               c4_rec.JUSTIFICATION,
                               c4_rec.EXPENSE_GROUP,
                               c4_rec.START_EXPENSE_DATE,
                               c4_rec.END_EXPENSE_DATE,
                               c4_rec.RECEIPT_CURRENCY_CODE,
                               c4_rec.RECEIPT_CONVERSION_RATE,
                               c4_rec.RECEIPT_CURRENCY_AMOUNT,
                               c4_rec.DAILY_AMOUNT,
                               c4_rec.WEB_PARAMETER_ID,
                               c4_rec.ADJUSTMENT_REASON,
                               c4_rec.AWARD_ID,
                               c4_rec.MRC_ACCRUAL_POSTED_FLAG,         --R12TU New columns
                               c4_rec.MRC_CASH_POSTED_FLAG,            --R12TU New columns
                               c4_rec.MRC_DIST_CODE_COMBINATION_ID,
                               c4_rec.MRC_AMOUNT,                      -- R12TU New column
                               c4_rec.MRC_BASE_AMOUNT,
                               c4_rec.MRC_BASE_INV_PRICE_VARIANCE,
                               c4_rec.MRC_EXCHANGE_RATE_VARIANCE,
                               c4_rec.MRC_POSTED_FLAG,                 --R12TU New columns
                               c4_rec.MRC_PROGRAM_APPLICATION_ID,     -- R12TU New columns
                               c4_rec.MRC_PROGRAM_ID,                  --R12TU New columns
                               c4_rec.MRC_PROGRAM_UPDATE_DATE,        -- R12TU New columns
                               c4_rec.MRC_RATE_VAR_CCID,
                               c4_rec.MRC_REQUEST_ID,                  -- R12TU New column
                               c4_rec.MRC_EXCHANGE_DATE,
                               c4_rec.MRC_EXCHANGE_RATE,
                               c4_rec.MRC_EXCHANGE_RATE_TYPE,
                               c4_rec.MRC_AMOUNT_TO_POST,              -- R12TU New column
                               c4_rec.MRC_BASE_AMOUNT_TO_POST,          --R12TU New column
                               c4_rec.MRC_CASH_JE_BATCH_ID,            -- R12TU New column
                               c4_rec.MRC_JE_BATCH_ID,                  --R12TU New column
                               c4_rec.MRC_POSTED_AMOUNT,               -- R12TU New column
                               c4_rec.MRC_POSTED_BASE_AMOUNT,          -- R12TU New column
                               c4_rec.MRC_RECEIPT_CONVERSION_RATE,
                               c4_rec.CREDIT_CARD_TRX_ID,              -- R12TU New column
                               c4_rec.DIST_MATCH_TYPE,
                               c4_rec.RCV_TRANSACTION_ID,
                               c4_rec.INVOICE_DISTRIBUTION_ID,
                               c4_rec.PARENT_REVERSAL_ID,
                               --c4_rec.TAX_RECOVERY_RATE,--R12TU Obsolete Column
                               --c4_rec.TAX_RECOVERY_OVERRIDE_FLAG,--R12TU Obsolete Column
                               c4_rec.TAX_RECOVERABLE_FLAG,
                               --c4_rec.TAX_CODE_OVERRIDE_FLAG,--R12TU Obsolete Column
                               --c4_rec.TAX_CODE_ID,--R12TU Obsolete Column
                               c4_rec.PA_CC_AR_INVOICE_ID,
                               c4_rec.PA_CC_AR_INVOICE_LINE_NUM,
                               c4_rec.PA_CC_PROCESSED_CODE,
                               c4_rec.MERCHANT_DOCUMENT_NUMBER,
                               c4_rec.MERCHANT_NAME,
                               c4_rec.MERCHANT_REFERENCE,
                               c4_rec.MERCHANT_TAX_REG_NUMBER,
                               c4_rec.MERCHANT_TAXPAYER_ID,
                               c4_rec.COUNTRY_OF_SUPPLY,
                               c4_rec.MATCHED_UOM_LOOKUP_CODE,
                               c4_rec.GMS_BURDENABLE_RAW_COST,
                               c4_rec.ACCOUNTING_EVENT_ID,
                               c4_rec.PREPAY_DISTRIBUTION_ID,
                               --c4_rec.CREDIT_CARD_TRX_ID,-- R12TU Different position
                               c4_rec.UPGRADE_POSTED_AMT,
                               c4_rec.UPGRADE_BASE_POSTED_AMT,
                               c4_rec.INVENTORY_TRANSFER_STATUS,
                               c4_rec.COMPANY_PREPAID_INVOICE_ID,
                               c4_rec.CC_REVERSAL_FLAG,
                               --c4_rec.PREPAY_TAX_PARENT_ID,--R12TU Obsolete
                               c4_rec.AWT_WITHHELD_AMT,
                               c4_rec.INVOICE_INCLUDES_PREPAY_FLAG,
                               c4_rec.PRICE_CORRECT_INV_ID,
                               c4_rec.PRICE_CORRECT_QTY,
                               c4_rec.PA_CMT_XFACE_FLAG,
                               c4_rec.CANCELLATION_FLAG,
                               --c4_rec.FULLY_PAID_ACCTD_FLAG,-- R12TU Obsolete
                               --c4_rec.ROOT_DISTRIBUTION_ID,-- R12TU Obsolete
                               --c4_rec.XINV_PARENT_REVERSAL_ID,-- R12TU Obsolete
                               c4_rec.INVOICE_LINE_NUMBER,             -- R12TU New Column
                               c4_rec.CORRECTED_INVOICE_DIST_ID,       -- R12TU New Column
                               c4_rec.ROUNDING_AMT,                    -- R12TU New Column
                               c4_rec.CHARGE_APPLICABLE_TO_DIST_ID,    -- R12TU New Column
                               c4_rec.CORRECTED_QUANTITY,              -- R12TU New Column
                               c4_rec.RELATED_ID,                      -- R12TU New Column
                               c4_rec.ASSET_BOOK_TYPE_CODE,            -- R12TU New Column
                               c4_rec.ASSET_CATEGORY_ID,               -- R12TU New Column
                               c4_rec.DISTRIBUTION_CLASS,              -- R12TU New Column
                               c4_rec.FINAL_PAYMENT_ROUNDING,          -- R12TU New Column
                               c4_rec.FINAL_APPLICATION_ROUNDING,      -- R12TU New Column
                               c4_rec.AMOUNT_AT_PREPAY_XRATE,          -- R12TU New Column
                               c4_rec.CASH_BASIS_FINAL_APP_ROUNDING,   -- R12TU New Column
                               c4_rec.AMOUNT_AT_PREPAY_PAY_XRATE,      -- R12TU New Column
                               c4_rec.INTENDED_USE,                    -- R12TU New Column
                               c4_rec.DETAIL_TAX_DIST_ID,              -- R12TU New Column
                               c4_rec.REC_NREC_RATE,                   -- R12TU New Column
                               c4_rec.RECOVERY_RATE_ID,                -- R12TU New Column
                               c4_rec.RECOVERY_RATE_NAME,              -- R12TU New Column
                               c4_rec.RECOVERY_TYPE_CODE,              -- R12TU New Column
                               c4_rec.RECOVERY_RATE_CODE,              -- R12TU New Column
                               c4_rec.WITHHOLDING_TAX_CODE_ID,         -- R12TU New Column
                               c4_rec.TAX_ALREADY_DISTRIBUTED_FLAG,    -- R12TU New Column
                               c4_rec.SUMMARY_TAX_LINE_ID,             -- R12TU New Column
                               c4_rec.TAXABLE_AMOUNT,                  -- R12TU New Column
                               c4_rec.TAXABLE_BASE_AMOUNT,             -- R12TU New Column
                               c4_rec.EXTRA_PO_ERV,                    -- R12TU New Column
                               c4_rec.PREPAY_TAX_DIFF_AMOUNT,          -- R12TU New Column
                               c4_rec.TAX_CODE_ID,                     -- R12TU New Column
                               c4_rec.VAT_CODE,                        -- R12TU New Column
                               c4_rec.AMOUNT_INCLUDES_TAX_FLAG,        -- R12TU New Column
                               c4_rec.TAX_CALCULATED_FLAG,             -- R12TU New Column
                               c4_rec.TAX_RECOVERY_RATE,               -- R12TU New Column
                               c4_rec.TAX_RECOVERY_OVERRIDE_FLAG,      -- R12TU New Column
                               c4_rec.TAX_CODE_OVERRIDE_FLAG,          -- R12TU New Column
                               c4_rec.TOTAL_DIST_AMOUNT,               -- R12TU New Column
                               c4_rec.TOTAL_DIST_BASE_AMOUNT,          -- R12TU New Column
                               c4_rec.PREPAY_TAX_PARENT_ID,            -- R12TU New Column
                               c4_rec.CANCELLED_FLAG,                  -- R12TU New Column
                               c4_rec.OLD_DISTRIBUTION_ID,             -- R12TU New Column
                               c4_rec.OLD_DIST_LINE_NUMBER,            -- R12TU New Column
                               c4_rec.AMOUNT_VARIANCE,
                               c4_rec.BASE_AMOUNT_VARIANCE,
                               c4_rec.HISTORICAL_FLAG,                 -- R12TU New Column
                               c4_rec.RCV_CHARGE_ADDITION_FLAG,        -- R12TU New Column
                               c4_rec.AWT_RELATED_ID,                  -- R12TU New Column
                               c4_rec.RELATED_RETAINAGE_DIST_ID,       -- R12TU New Column
                               c4_rec.RETAINED_AMOUNT_REMAINING,       -- R12TU New Column
                               c4_rec.BC_EVENT_ID,                     -- R12TU New Column
                               c4_rec.RETAINED_INVOICE_DIST_ID,        -- R12TU New Column
                               c4_rec.FINAL_RELEASE_ROUNDING,          -- R12TU New Column
                               c4_rec.FULLY_PAID_ACCTD_FLAG,           -- R12TU New Column
                               c4_rec.ROOT_DISTRIBUTION_ID,            -- R12TU New Column
                               c4_rec.XINV_PARENT_REVERSAL_ID,         -- R12TU New Column
                               c4_rec.RECURRING_PAYMENT_ID,
                               c4_rec.RELEASE_INV_DIST_DERIVED_FROM,   -- R12TU New Column
                               c4_rec.PAY_AWT_GROUP_ID                 -- R12TU New Column
                                                      );
               END LOOP;
                 fnd_file.put_line (fnd_file.LOG,
                     vRecordCounter
                  || ' records inserted into AP_Invoice_Distributions_all to create unmatched lines');

            */
            -- R12TU now do the update to match the Ordered and billed quantity
            -- Hold is if quantity_billed exeeds the quantity ordered.

            FOR c5_rec IN C5
            LOOP
               vRecordCounter := vRecordCounter + 1;


               IF c5_rec.quantity_billed > c5_rec.quantity
               THEN
                  IF ROUND (c5_rec.quantity_billed) = c5_rec.quantity
                  THEN
                     UPDATE PO_LINE_LOCATIONS_ALL
                        SET quantity_billed = ROUND (quantity_billed)
                      WHERE line_location_id = c5_rec.line_location_id;
                  ELSE
                     UPDATE PO_LINE_LOCATIONS_ALL
                        SET quantity_billed = quantity
                      WHERE line_location_id = c5_rec.line_location_id;
                  END IF;
               END IF;
            END LOOP;


            fnd_file.put_line (
               fnd_file.LOG,
                  vRecordCounter
               || ' Records updated in ap_line_locations_all to release ORD QTY Hold.');

            /*
               UPDATE ap_invoice_distributions_all aid
                  SET reversal_flag = 'Y'
                WHERE     aid.amount > 0 -- This is to avoid reversal line getting picked up this sql
                      AND NVL (aid.reversal_flag, 'N') = 'N'
                      AND EXISTS
                             (SELECT 'x'
                                FROM ap_holds_all ah2,
                                     po_line_locations_all pll,
                                     po_distributions_all pod
                               WHERE     ah2.invoice_id = aid.invoice_id
                                     AND ah2.hold_lookup_code = 'QTY ORD'
                                     AND ah2.release_lookup_code IS NULL -------------------------------
                                     AND ah2.line_location_id = pll.line_location_id
                                     AND pod.line_location_id = pll.line_location_id
                                     AND aid.po_distribution_id = pod.po_distribution_id)
                      AND EXISTS
                             (SELECT 'x'
                                FROM ap_invoices_all ai
                               WHERE     ai.invoice_id = aid.invoice_id
                                     AND ai.source IN
                                            ('PCARD_PARTIAL',
                                             'PCARD_FINAL',
                                             'PCARD_CREDIT')
                                     AND EXISTS
                                            (SELECT 'x'
                                               FROM nihap_pc_daily_txns_all ds
                                              WHERE     ai.invoice_num = ds.invoice_num
                                                    AND ds.stmt_period = p_period));

               fnd_file.put_line (fnd_file.LOG,
                     SQL%ROWCOUNT
                  || ' records updated in AP_Invoice_Distributions_all to set reversal flag for the original line to Y');

               COMMIT;
            */
            fnd_file.put_line (
               fnd_file.LOG,
               'Updating pay alone flag to N for all P-card unpaid invoices');

            UPDATE ap_invoices_all ai
               SET exclusive_payment_flag = 'N'
             WHERE     source LIKE 'PCARD%'
                   AND exclusive_payment_flag = 'Y'
                   AND NVL (ai.amount_paid, 0) = 0;

            fnd_file.put_line (
               fnd_file.LOG,
                  SQL%ROWCOUNT
               || ' records updated to set pay alone flag to N for all P-card unpaid invoices');
         END;
      END IF;

      COMMIT;
   EXCEPTION
      WHEN OTHERS
      THEN
         fnd_file.put_line (fnd_file.LOG, 'PCARD_ADJUSTMENT:- ' || SQLERRM);
   END Pcard_adjustment;

END NIHAP_PCARD_PFT_WAIVER;
/
