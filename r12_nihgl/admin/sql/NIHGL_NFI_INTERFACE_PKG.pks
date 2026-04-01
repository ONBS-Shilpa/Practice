CREATE OR REPLACE PACKAGE APPS.nihgl_NFI_interface_pkg AUTHID DEFINER
IS
   /* $Header: NIHGL_NBP_INTERFACE_PKG.pks 1.2 2013/05/17 14:22:30  ship $ */

/* ------------------------------------------------------------------------------------------------------
     || Package      : nih_nbp_interface_pkg.pks
     ||
     || Author       : Rbiz
     ||
     || Created      : 06/14/2024
     ||
     || Description  : This package created for importing NBP data from different
     ||                feder system into custom Interface tables and then validate
     ||                the file for any issues and process the records and finally
     ||                archived the successfull/error records in Archive table
   --  Ver      Date         Author                  Description
   --  -------  -----------  ------------------------------------
   --  1.0     6/14/24        Remedybiz             Rewrote from OPE as per NPB architecture
   --  1.1     01/10/25       Sergei Polikaov       CR NBSCH0002499
   --  1.4     10/28/2025     Satya Revu 			  NBSCH0002993 - PRB0028375 OPE events getting generated from errored transactions pre-2023	
   -------------------------------------------------------------------------------------------

   --------------------------------------------------------------------------------------------------  */
   /*
   || Declare global variables.
   */
   g_user_id              NUMBER (15) := fnd_global.user_id;--'23130';--fnd_global.user_id;
   g_request_id           NUMBER (15) := fnd_global.conc_request_id;
   g_login_id             NUMBER (15) := fnd_global.login_id;
   g_resp_id              NUMBER := fnd_global.resp_id;--50447;--fnd_global.resp_id;
   g_accounting_date      DATE;
   g_accounting_period    VARCHAR2 (30);
   g_sysdate              DATE := SYSDATE;
   g_accrual_flag         VARCHAR2 (1);


   g_resp_name            fnd_responsibility_tl.responsibility_name%TYPE;

   TYPE g_err_message_type_tbl IS TABLE OF VARCHAR2 (2000)
      INDEX BY VARCHAR2 (30 BYTE);

   g_err_message_tbl      g_err_message_type_tbl;

   TYPE g_txn_flow_flags_rec IS RECORD
   (
      batch_number       VARCHAR2 (30 BYTE),
      message_name       VARCHAR2 (30 BYTE),
      file_reject        VARCHAR2 (1 BYTE),
      hold_for_ope_eh    VARCHAR2 (1 BYTE),
      reject_error_txn   VARCHAR2 (1 BYTE)
   );

   TYPE g_txn_flow_flags_type_tbl IS TABLE OF g_txn_flow_flags_rec
      INDEX BY VARCHAR2 (200 BYTE);

   g_txn_flow_flags_tbl   g_txn_flow_flags_type_tbl;

    TYPE g_txn_hold_type_tbl IS TABLE OF NUMBER
      INDEX BY VARCHAR2 (50 BYTE);

    g_txn_hold_tbl         g_txn_hold_type_tbl;

   TYPE g_txn_reject_type_tbl IS TABLE OF NUMBER
      INDEX BY VARCHAR2 (50 BYTE);

   g_txn_reject_tbl       g_txn_reject_type_tbl;


   FUNCTION nih_billing_file_notify
         (p_batch_number IN VARCHAR2
       , p_call_type IN VARCHAR2
   )
     RETURN VARCHAR2;

   FUNCTION derive_single_year (p_fy IN VARCHAR2, p_parm_date DATE)
      RETURN VARCHAR2;


 Procedure send_mail (p_to        IN VARCHAR2,
                       p_from      IN VARCHAR2,
                       p_subject   IN VARCHAR2,
                       p_text_msg  IN VARCHAR2 DEFAULT NULL,
                       p_html_msg  IN VARCHAR2 DEFAULT NULL);


  PROCEDURE record_btch_hdr_status (p_record_status    VARCHAR2,
                                    p_file_name        VARCHAR2,
                                   p_batch_number     VARCHAR2,
                                   p_error_message    VARCHAR2,
                                    p_error_code       VARCHAR2,
                                   p_ope_batch_id     NUMBER);




   PROCEDURE reject_document (errbuf              OUT VARCHAR2,
                              retcode             OUT VARCHAR2,
                              p_file_name      IN     VARCHAR2,
                              p_batch_number   IN     VARCHAR2,
                              p_doc_ref        IN     VARCHAR2,
                              p_doc_number     IN     VARCHAR2,
                              p_can_fy         IN     NUMBER);




 Procedure   main( errbuf               out VARCHAR2,
                   retcode              out number,
                   totalrec             out number,
                   processed            out number,
                   errored              out number,
                   rejected             out number,
                   p_upload_id           in Number,
                   p_sob_id              in NUMBER,
                   p_batch_number        in VARCHAR2,
                   p_file_name           in VARCHAR2,
                   p_called_from_form    in VARCHAR2,
                   p_batch_desc          in VARCHAR2,   -- CR NBSCH0002499
                   p_JOURNAL_CATEGORY    in VARCHAR2    -- CR NBSCH0002499
                   );

   FUNCTION get_batch_category (p_batch_number VARCHAR2)
      RETURN VARCHAR2;

   FUNCTION get_tcode (p_legacy_tcode   IN VARCHAR2,
                       p_fund           IN VARCHAR2,
                       p_object_class   IN VARCHAR2,
                       p_ier            IN VARCHAR2,
                       p_batch_number   IN VARCHAR2)
      RETURN VARCHAR2;
	  
	  
	     PROCEDURE NFE_BILLING_MAIN (errbuf               OUT VARCHAR2,
                   retcode              OUT NUMBER,
                   p_sob_id                 NUMBER,
				   --Start of change 1 by Yash
                   p_batch_number           VARCHAR2,
                   p_file_name              VARCHAR2,
                   p_called_from_form       VARCHAR2
				   );
				   
END nihgl_NFI_interface_pkg;

