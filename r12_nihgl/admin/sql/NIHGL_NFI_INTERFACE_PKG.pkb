CREATE OR REPLACE PACKAGE BODY APPS.NIHGL_NFI_INTERFACE_PKG
IS
 /* $Header: NIHGL_NPB_INTERFACE_PKG.pkb 1.0 2023/02/23 15:11:28 ship $ */
 /* ****************************************************************************************************
   --  NAME:          NIHGL_NFI_INTERFACE_PKG.pkb
   --  Description  : This package created for importing NBP data from different
   --                 feeder systems into custom Interface tables and then validate
   --                 the file (for any issues and process the records) and finally
   --                 archived the successfull/error records in Archive table
   --  REVISIONS
   --
   --  Ver      Date         Author                  Description
   --  -------  -----------  ------------------------------------
   --  1.0     6/14/24        Remedybiz               Rewrote from OPE as per NPB architecture
   --  1.1     01/10/25       Sergei Polikaov      CR NBSCH0002499
   --  1.2     07/21/25       Sergei Polikaov      CR NBSCH0002920
   --  1.3     10/17/25		  Sergei Polikaov      CR NBSCH0003180
   --  1.4     10/28/2025     Satya Revu 		   CR NBSCH0002993 - PRB0028375 OPE events getting generated from errored transactions pre-2023
   --  1.5     12/29/2025	  Sergei Polikaov      CR NBSCH0003234
   --  1.6     03/02/2026	  Sergei Polikaov      CR NBSCH0003319
   -------------------------------------------------------------------------------------------
*/

   /* lobal Variable and Constant  */
   g_fund_value_set_name      VARCHAR2 (25);
   g_instance_name            VARCHAR2 (30);
   g_account                  VARCHAR2 (30);
   g_cfy                      NUMBER;
   g_ope_je_source            gl_je_sources.user_je_source_name%TYPE;
   g_fund_type_tcode_lookup   VARCHAR2 (30);
   g_ope_je_category          VARCHAR2 (50);
   g_total_segments           NUMBER;
   lc_errbuf                  VARCHAR2 (1000);
   g_chart_of_accounts_id     NUMBER;
   g_path                     VARCHAR2 (200);
   g_module                   varchar2(100) := 'main';
   g_batch_number             varchar2(100);    --CR NBSCH0002499
   g_batch_desc               varchar2(100);    --CR NBSCH0002499
   g_JOURNAL_CATEGORY         varchar2(50);    --CR NBSCH0002499

   -- SQL Loader load the data based on 1 as header
   -- 4 footer record
   g_hdr             CONSTANT NUMBER := 1;
   g_ftr             CONSTANT NUMBER := 4;

   --Start of change 1 by Yash
   --Global variable for OPE error handling change
   g_file_reject_flag         BOOLEAN := FALSE;
   g_index                    VARCHAR2 (50 BYTE);
   g_retcode                  VARCHAR2 (1);
   g_errbuf                   VARCHAR2 (2000);
   g_file_name                DBMS_SQL.VARCHAR2_TABLE;

   --End of change 1

   TYPE req_status_typ IS RECORD
   (
      request_id   NUMBER (15),
      dev_phase    VARCHAR2 (255),
      dev_status   VARCHAR2 (255),
      MESSAGE      VARCHAR2 (2000),
      phase        VARCHAR2 (255),
      status       VARCHAR2 (255)
   );

   /*
   || Record type defined to send information to email host program
   */
   TYPE email_filename_t IS RECORD
   (
      filename       VARCHAR2 (240),
      act_filename   VARCHAR2 (240),
      batchnumber    VARCHAR2 (10)
   );

   /*
   || Type defined to capture account segment values
   */
   TYPE segmentarray IS TABLE OF VARCHAR2 (30)
      INDEX BY BINARY_INTEGER;

   /*
   || Record type defined to flag debit or credit
   */
   -- Yash
   -- added the below segments1 to 14 for SGL derivation
   -- in TYPE accountrec
   TYPE accountrec IS RECORD
   (
      account_flag       VARCHAR2 (1),
      account_value      VARCHAR2 (30),
      transaction_code   VARCHAR2 (30),
      segment1           gl_code_combinations.segment1%TYPE,
      segment2           gl_code_combinations.segment2%TYPE,
      segment3           gl_code_combinations.segment3%TYPE,
      segment4           gl_code_combinations.segment4%TYPE,
      segment5           gl_code_combinations.segment5%TYPE,
      segment6           gl_code_combinations.segment6%TYPE,
      segment7           gl_code_combinations.segment7%TYPE,
      segment8           gl_code_combinations.segment8%TYPE,
      segment9           gl_code_combinations.segment9%TYPE,
      segment10          gl_code_combinations.segment10%TYPE,
      segment12          gl_code_combinations.segment12%TYPE,
      segment13          gl_code_combinations.segment13%TYPE,
      segment14          gl_code_combinations.segment14%TYPE
   );

   /*
   || Array type defined for AccountRec to have two rows;
   || one for debit and other for credit
   */
   TYPE accountarray IS TABLE OF accountrec
      INDEX BY BINARY_INTEGER;

  /*----------------------------------------------------------------------------------------------
   -- The logf procedure to write given string in request log file.
   -- Create a procedure to accept string as a parameter and
   -- insert that into log file of current concurrent program.
   -- -----------------------------------------------------------------------------------------------*/
   PROCEDURE logf (p_string VARCHAR2,p_module in varchar2)
   IS
   BEGIN
        --IF FND_LOG.LEVEL_STATEMENT >= FND_LOG.G_CURRENT_RUNTIME_LEVEL THEN
            NIHFND_APEX_APPLICATIONS_PKG.debug
                (
                    110,
                    1,
                    nvl(fnd_global.user_id,-100),
                    p_module,
                    'NIHGL_NFI_INTERFACE' ,
                    p_string);
        --END IF;
   -- p_reference1 IN VARCHAR2 DEFAULT NULL,
   -- p_reference2 IN VARCHAR2 DEFAULT NULL,

   END logf;

   -----------------------------------------------------------------------------------------------

--/*=======================================
   -- The load_report_data procedure used to create record in nihgl_ope_report_data table.
   --
   -- This procedure called in validate_hdr_data.
   -- =======================================*/
   PROCEDURE load_report_data (p_request_id     IN NUMBER,
                               p_file_name      IN VARCHAR2,
                               p_batch_number   IN VARCHAR2,
                               p_user_id        IN NUMBER,
                               p_login_id       IN NUMBER)
   IS
      PRAGMA AUTONOMOUS_TRANSACTION;
     p_module varchar2(100) := 'load_report_data';
   BEGIN
      logf ('g_request_id : ' || g_request_id , p_module);
      logf ('Loading data into report table ', p_module);
      logf (
            p_request_id
         || '         '
         || p_batch_number
         || '         '
         || p_file_name , p_module);

      INSERT INTO apps.nihgl_ope_report_data (OPE_REQUEST_ID,
                                              FILE_NAME,
                                              BATCH_NUMBER,
                                              SEND_REPORT,
                                              CREATED_BY,
                                              CREATION_DATE,
                                              LAST_UPDATED_BY,
                                              LAST_UPDATE_DATE,
                                              LAST_UPDATE_LOGIN)
           VALUES (p_request_id,
                   p_file_name,
                   p_batch_number,
                   'Y',
                   p_user_id,
                   SYSDATE,
                   p_user_id,
                   SYSDATE,
                   p_login_id);

      COMMIT;
   EXCEPTION
      WHEN OTHERS
      THEN
         logf ('Error in LOAD_REPORT_DATA :-' || SQLERRM, p_module);
   END load_report_data;


   -----------------------------------------------------------

   PROCEDURE email_list (pTo       IN     VARCHAR2,
                         pToList      OUT DBMS_SQL.VARCHAR2_TABLE)
   IS
      v_emails             VARCHAR2 (2500);
      vSemicolonLocation   NUMBER;
      vCounter             NUMBER := 1;
   BEGIN
      v_emails := pTo;

     /* LOOP
         vSemicolonLocation := INSTR (v_emails, ';');

         IF vSemicolonLocation = 0
         THEN
            pToList (vCounter) := v_emails;
            EXIT;
         ELSE
            pToList (vCounter) :=
               SUBSTR (v_emails, 1, (vSemicolonLocation - 1));
            v_emails := SUBSTR (v_emails, (vSemicolonLocation + 1));
            vCounter := vCounter + 1;
         END IF;
      END LOOP;*/
        v_emails := 'kannan.srinivasan@nih.gov';

   END email_list;

   -- ----------------------------------------------------------------------------------------------
   -- Funds check procedure used for NBP interface trasaction funds checking purpose.
   -- It creates records in nihgrt_funds_check_int to perform funds check by calling
   -- NIH custom nihgl_fsi_funds_check_pkg.main procedure.
   --
   -- The funds_check procedure called from validate_billing_data procedure.
  -------------------------------------------------------------------------------------------
   PROCEDURE funds_check (p_batch_number      IN     VARCHAR2,
                          p_document_number   IN     VARCHAR2,
                          p_file_name         IN     VARCHAR2,
                          p_result_out           OUT NUMBER,
                          p_errnum               OUT NUMBER,
                          p_errtext              OUT VARCHAR2)
   IS PRAGMA AUTONOMOUS_TRANSACTION;
      x_date                 VARCHAR2 (15) := TO_CHAR (SYSDATE);
      x_error_code           NUMBER (5);
      x_error_message        VARCHAR2 (2000);
      v_record_id            NUMBER;
      v_transaction_id       NUMBER;
      v_transaction_status   VARCHAR2 (10);
      v_tcode                VARCHAR2 (20);
      p_module varchar2(100) := 'funds_check ';

      CURSOR c_opefundck
      IS
         SELECT *
           FROM nihgl_ope_acct_dtls_int_tbl
          WHERE     batch_number = p_batch_number
                AND TRIM (document_ref || document_number) =
                       p_document_number
                AND record_status IN ('N')
                AND file_name = p_file_name;

   --
   BEGIN
      logf (
         'Funds Check start ' || 'p_document_number : ' || p_document_number , p_module);
      p_result_out := 0;
      p_errnum := 0;
      p_errtext := NULL;

      -- Funds Check Passed '||'v_transaction_id : '|| to_char(v_transaction_id)|| '   v_transaction_status : '|| v_transaction_status||'  p_ccid '||to_char(p_ccid)||'  p_doc_amount  '||to_char(p_doc_amount);
      -- RETURN;
      --
      FOR r_opefundck IN c_opefundck
      LOOP
         v_tcode := NULL;
         v_record_id := NULL;
         v_transaction_id := NULL;
         --
         logf (
               'Funds Check Doc ' || 'p_document_number : '
            || p_document_number  || '  r_opefundck.tcode  '
            || r_opefundck.tcode , p_module);

         --
         BEGIN
            SELECT LOOKUP_CODE
              INTO v_tcode
              FROM apps.FND_LOOKUP_VALUES_VL
             WHERE     lookup_type = 'NIH_OPE_FUNDCHK_TCODE'
                   AND LOOKUP_TYPE = 'NIH_OPE_FUNDCHK_TCODE'
                   AND lookup_code = r_opefundck.tcode;
         EXCEPTION
            WHEN OTHERS
            THEN
               v_tcode := NULL;
         END;

         --
         IF v_tcode IS NOT NULL
         THEN
            --
            -- Added following if condition for NBSCH0001185 on 7/17/2023
            -- Skip funds check if REVERSE_CODE is 2.
            IF NVL (r_opefundck.REVERSE_CODE, '1') = '2'
            THEN
               p_result_out := 0;
               p_errnum := 0;
               p_errtext :=
                     'Funds Check not validated for reversal. TCODE '
                  || r_opefundck.tcode;
               logf (p_errtext, p_module);
            ELSE                        -- Added for NBSCH0001185 on 7/17/2023
               --
               SELECT apps.nihgrt_funds_check_int_s.NEXTVAL,
                      apps.nihgrt_transaction_id_s.NEXTVAL
                 INTO v_record_id, v_transaction_id
                 FROM DUAL;

               --
               logf (
                     'Funds Check '
                  || 'v_record_id : '
                  || v_record_id
                  || '  v_transaction_id  '
                  || v_transaction_id,p_module);

               --
               INSERT
                 INTO apps.nihgrt_funds_check_int (record_id,
                                                   transaction_id,
                                                   line_number,
                                                   record_status,
                                                   increase_or_decrease,
                                                   award_amount,
                                                   fy,
                                                   can,
                                                   object_class,
                                                   user_je_source_name,
                                                   user_je_category_name,
                                                   accounting_date,
                                                   transaction_type_code,
                                                   transaction_code,
                                                   code_combination_id,
                                                   expenditure_org_id,
                                                   period_name,
                                                   PROJECT_ID,
                                                   TASK_ID,
                                                   EXPENDITURE_TYPE,
                                                   CREATION_DATE,
                                                   LAST_UPDATE_DATE,
                                                   LAST_UPDATED_BY,
                                                   CREATED_BY)
               VALUES (v_record_id,
                       v_transaction_id,
                       NVL (r_opefundck.line_number, 1),
                       'N',
                       1,
                       r_opefundck.amount,
                       r_opefundck.can_fy,
                       r_opefundck.can,
                       r_opefundck.object_class,
                       'Purchasing',
                       'Purchases',
                       r_opefundck.accounting_date,
                       'USSGL',
                       'B204',
                       NULL,
                       r_opefundck.exp_org_id,
                       r_opefundck.period_name,
                       r_opefundck.project_id,
                       r_opefundck.task_id,
                       r_opefundck.project_exp_type,
                       SYSDATE,
                       SYSDATE,
                       -1,
                       -1);

               logf ('Funds Check after insert'  , p_module);

               COMMIT;

               nihgl_fsi_funds_check_pkg.main (
                  p_transaction_id       => v_transaction_id,
                  p_transaction_status   => v_transaction_status);

               logf ('Funds Check after fund check call', p_module);

               IF v_transaction_status = 'PASS'
               THEN
                  logf ('Funds Check after fund check PASS',p_module);
                  p_result_out := 0;
                  p_errnum := 0;
                  p_errtext :=
                        'Funds Check Passed '
                     || 'v_transaction_id : '
                     || TO_CHAR (v_transaction_id)
                     || '   v_transaction_status : '
                     || v_transaction_status
                     || '  r_opefundck.amount  '
                     || TO_CHAR (r_opefundck.amount);

                  logf (p_errtext, p_module);
               ELSE
                  logf ('Funds Check after fund check FAIL' , p_module);
                  p_result_out := 9;
                  p_errnum := 9;
                  p_errtext :=
                        'Failed Funds Check'
                     || ' Document_Number '
                     || p_document_number
                     || ' TCODE'
                     || r_opefundck.tcode
                     || ' Transaction_status : '
                     || v_transaction_status
                     || '  Amount  '
                     || TO_CHAR (r_opefundck.amount);

                  logf (p_errtext , p_module);
                  RETURN;
               END IF;        --/* end of if v_transaction_status condition */

               COMMIT;
            END IF;                   --/* end of if reverse_code condition */
         ELSE
            p_result_out := 0;
            p_errnum := 0;
            p_errtext :=
                  'Funds Check TCODE not defined, Fund check not validated '
               || r_opefundck.tcode;
            logf (
                  'Funds Check TCODE not defined, Fund check not validated '
               || r_opefundck.tcode, p_module);
         END IF;                            --/* end of if v_tcode condition*/
      END LOOP;
   EXCEPTION
      WHEN OTHERS
      THEN
         p_result_out := 1;
         p_errnum := 1;
         p_errtext :=
            SUBSTR ('Unable to Check Funds (' || SQLERRM, 1, 78) || ')';
         x_error_code := SQLCODE;
         x_error_message := SQLERRM;
         COMMIT;
   END funds_check;

 -- --------------------------------------------------------------------------------------------------
   -- procedure send_email
 --------------------------------------------------------------------------

  Procedure send_mail (p_to        IN VARCHAR2,
                       p_from      IN VARCHAR2,
                       p_subject   IN VARCHAR2,
                       p_text_msg  IN VARCHAR2 DEFAULT NULL,
                       p_html_msg  IN VARCHAR2 DEFAULT NULL)
AS

  l_mail_conn   UTL_SMTP.connection;
  l_boundary    VARCHAR2(50) := '----=abc1234321cba=';
  l_smtp_host   VARCHAR2(100);
  l_smtp_port  NUMBER := 25;
  l_module varchar2(100) := 'send_mail';

  PROCEDURE process_recipients(p_mail_conn IN OUT UTL_SMTP.connection,
                               p_list      IN     VARCHAR2)
  is

  BEGIN

       logf('to list ' || p_list,l_module );
    IF TRIM(p_list) IS NOT NULL THEN
      FOR email_str IN (SELECT REGEXP_SUBSTR (p_list,
                                         '[^,]+',
                                         1,
                                         LEVEL) text
                     FROM DUAL
               CONNECT BY REGEXP_SUBSTR (p_list,
                                         '[^,]+',
                                         1,
                                         LEVEL)
                             IS NOT NULL)
        LOOP
          dbms_output.put_line(trim(email_str.text));
          UTL_SMTP.rcpt(p_mail_conn, trim(email_str.text));
          logf( 'added email '|| trim(email_str.text), l_module);

      END LOOP;
    END IF;
  END;


 BEGIN
   logf ('sending e-mail' , l_module);
   --return;

  Fnd_Profile.get ('NIHMM_SMTP_SERVER', l_smtp_host);
  l_mail_conn := UTL_SMTP.open_connection(l_smtp_host, l_smtp_port);

  UTL_SMTP.helo(l_mail_conn, l_smtp_host);
  UTL_SMTP.mail(l_mail_conn, p_from);
  process_recipients(l_mail_conn, p_to);
 --UTL_SMTP.rcpt(l_mail_conn, p_to);
  UTL_SMTP.open_data(l_mail_conn);

  UTL_SMTP.write_data(l_mail_conn, 'Date: ' || TO_CHAR(SYSDATE, 'DD-MON-YYYY HH24:MI:SS') || UTL_TCP.crlf);
  UTL_SMTP.write_data(l_mail_conn, 'To: ' || p_to || UTL_TCP.crlf);
  UTL_SMTP.write_data(l_mail_conn, 'From: ' || p_from || UTL_TCP.crlf);
  UTL_SMTP.write_data(l_mail_conn, 'Subject: ' || p_subject || UTL_TCP.crlf);
  UTL_SMTP.write_data(l_mail_conn, 'Reply-To: ' || p_from || UTL_TCP.crlf);
  UTL_SMTP.write_data(l_mail_conn, 'MIME-Version: 1.0' || UTL_TCP.crlf);
  UTL_SMTP.write_data(l_mail_conn, 'Content-Type: multipart/alternative; boundary="' || l_boundary || '"' || UTL_TCP.crlf || UTL_TCP.crlf);

  IF p_text_msg IS NOT NULL THEN
    UTL_SMTP.write_data(l_mail_conn, '--' || l_boundary || UTL_TCP.crlf);
    UTL_SMTP.write_data(l_mail_conn, 'Content-Type: text/plain; charset="iso-8859-1"' || UTL_TCP.crlf || UTL_TCP.crlf);

    UTL_SMTP.write_data(l_mail_conn, p_text_msg);
    UTL_SMTP.write_data(l_mail_conn, UTL_TCP.crlf || UTL_TCP.crlf);
  END IF;

  IF p_html_msg IS NOT NULL THEN
    UTL_SMTP.write_data(l_mail_conn, '--' || l_boundary || UTL_TCP.crlf);
    UTL_SMTP.write_data(l_mail_conn, 'Content-Type: text/html; charset="iso-8859-1"' || UTL_TCP.crlf || UTL_TCP.crlf);

    UTL_SMTP.write_data(l_mail_conn, p_html_msg);
    UTL_SMTP.write_data(l_mail_conn, UTL_TCP.crlf || UTL_TCP.crlf);
  END IF;

  UTL_SMTP.write_data(l_mail_conn, '--' || l_boundary || '--' || UTL_TCP.crlf);
  UTL_SMTP.close_data(l_mail_conn);

  UTL_SMTP.quit(l_mail_conn);

  exception
  when others then
    logf(  'sending e-mail error ' || sqlerrm, l_module);
END;


  --------------------------------------------------------------------------------
 procedure send_hdr_rejection_mail (p_file_name in varchar2,
                                   p_batch_name in varchar2,
                                   p_error_mesg in varchar2)

  is

    l_html varchar2 (32627);
    l_module  varchar2(100) :=  'send_hdr_rejection_mail';


   begin

   logf('Begin' , l_Module);

   l_html := '<html> <head>
           <style>
        table, th, td {
          border: 1px solid black;
          border-collapse: collapse;
                  text-align: center;
        }
        th {
          background-color: #96D4D4;
        }
        </style>
        </head> ';
       l_html := l_html || '<body>  <br>  Hello, <br> <br>  NBP has processed the following file      <br>
                 File : ' || p_file_name   || '&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp Batch  :' || p_batch_name ||
             '<br>  <br> <br>This file has the following file validation  <br> <br> ' ||
            '<span style=''color:red''>   Issue  : ' ||  p_error_mesg || '<br> <br> '||
            '<span style=''color:green''> Action : Please fix the issue(s) and re upload the file for reprocessing  <br>   <br>
             <br> <br>  Thanks <br> NBS Team  <br>  </body> </html>';

  send_mail(p_to        =>'kannan.srinivasan@nih.gov',
            p_from      =>'kannan.srinivasan@nih.gov',
            p_subject   =>   'Warning ' || g_instance_name || ':  NBP FILE Upload ' || p_file_name,
            p_text_msg  =>   g_instance_name || ':  NBP FILE Upload',
            p_html_msg  =>   l_html
            );

   EXCEPTION
      WHEN OTHERS
      THEN
         logf ('Error while Recording Error: ' || SQLERRM,l_module);
         g_retcode := 2;
         g_errbuf :=
            'Error while Recording Error: ' || SUBSTR (SQLERRM, 1, 250);
   END;


 --------------------------------------------------------------------------------
 procedure send_status_mail (p_file_name in varchar2,
                            p_batch_name in varchar2,
                            p_error_mesg out  varchar2)

  is

    l_html varchar2 (32627);
    l_module  varchar2(100) :=  'send_status_mail';
    l_total_cnt number(18);
    l_process_cnt number(18);
    l_error_cnt number(18);
    l_rejected_cnt number(18);

   begin

   logf('Begin' , l_Module);
   logf('request_id ' || g_request_id || '  file Name ' || p_file_name , l_Module);

    select count(*)  ,
             sum(decode(record_status , 'E', 1, 0)) l_error_cnt ,
             sum(decode(record_status , 'P', 1, 0)) l_PROCESS_cnt ,
             sum(decode(record_status , 'R', 1, 0)) l_rejected_cnt
        into l_total_cnt,l_error_cnt, l_process_cnt, l_rejected_cnt
        from nihgl_ope_acct_dtls_int_tbl
        where file_name = p_file_name
        and   request_id  = g_request_id;

   l_html := '<html> <head>
           <style>
        table, th, td {
          border: 1px solid black;
          border-collapse: collapse;
                  text-align: center;
        }
        th {
          background-color: #96D4D4;
        }
        </style>
        </head> ';
       l_html := l_html || '<body> ' ||  '<br>  hello, <br> <br>  NBP has processed the following file and please find the status    <br>
                 File : ' || p_file_name   || '&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp Batch  :' || p_batch_name || '  <br>  <br>
      <table>
        <th> Number of Records </th>
        <th> Processed Records </th>
        <th> Rejected Records </th>
        <th> Errored Records </th>
         <tr> ' ||
        '<td> ' || l_total_cnt || ' </td> ' ||
        '<td> ' || l_process_cnt || ' </td> ' ||
        '<td> ' || l_rejected_cnt || ' </td> ' ||
        '<td> ' || l_error_cnt || ' </td> ' ||
         ' </tr> </table> <br> <br>
         <p> Please review the file in the application  <br> <br>  Thanks <br> NBS Team  <br>  </body> </html>';

  send_mail(p_to        =>'kannan.srinivasan@nih.gov',
            p_from      =>'kannan.srinivasan@nih.gov',
            p_subject   =>   g_instance_name || ':  NBP FILE Upload ' || p_file_name,
            p_text_msg  =>   g_instance_name || ':  NBP FILE Upload',
            p_html_msg  =>   l_html
            );

 EXCEPTION
      WHEN OTHERS
      THEN
         logf ('Error while Recording Error: ' || SQLERRM,l_module);
         g_retcode := 2;
         g_errbuf :=
            'Error while Recording Error: ' || SUBSTR (SQLERRM, 1, 250);
   END;

   ----------------------------------------------------------------------------------------------
   -- Record_error procedure used to update OPE interaface
   -- detail table nihgl_ope_acct_dtls_int_tbl of a given p_record_id.
   -- CR 32752 for Status Email to the NBS Users Functionality
   -- Update the detail interface records based on ope_detail_id and ope_batch_id
   -- if any of the column values validation failed
   --
   -- ---------------------------------------------------------------------------------------------------
   PROCEDURE record_error (p_error_message    VARCHAR2,
                           p_ope_batch_id     NUMBER,
                           p_ope_detail_id    NUMBER,
                           p_record_id        NUMBER)
   AS

     l_module varchar2(100) := 'record_error';
   BEGIN
      logf('begin ' , l_module);

      UPDATE nihgl_ope_acct_dtls_int_tbl
         SET error_message =
                   error_message
                || DECODE (error_message,
                           NULL, p_error_message,
                           '; ' || p_error_message),
             ERROR_CODE = 'ERROR',
             last_updated_by = fnd_global.user_id,
             last_update_date = SYSDATE,
             last_update_login = fnd_global.login_id,
             record_status = 'E'
       WHERE     DOCUMENT_NUMBER = (SELECT DOCUMENT_NUMBER
                                      FROM nihgl_ope_acct_dtls_int_tbl
                                     WHERE record_id = p_record_id)
             AND ope_batch_id = p_ope_batch_id
             AND ope_detail_id = NVL (p_ope_detail_id, ope_detail_id);

      COMMIT;
   EXCEPTION
      WHEN OTHERS
      THEN
         logf ('Error while Recording Error: ' || SQLERRM,l_module);
         g_retcode := 2;
         g_errbuf :=
            'Error while Recording Error: ' || SUBSTR (SQLERRM, 1, 250);
   END record_error;

   -- -----------------------------------------------------------------------------------------------
   -- procedure update_err_msg
   --Start of change 1 by Yash
   --- Update the error message to NULL before validating the record

   -----------------------------------------------------------------------------------------------------
   PROCEDURE update_err_msg (p_error_message          VARCHAR2,
                             p_funds_check_message    VARCHAR2,
                             p_funds_check_code       VARCHAR2,
                             p_doc_ref                VARCHAR2,
                             p_doc_number             VARCHAR2,
                             p_can_fy                 VARCHAR2,
                             p_file_name              VARCHAR2,
                             p_batch_number           VARCHAR2,
                             p_record_status          VARCHAR2,
                             p_error_code             VARCHAR2)
   AS
    l_module varchar2(100) := 'update_err_msg';

   BEGIN
     logf('begin ' , l_module);

      IF     p_doc_ref IS NOT NULL
         AND p_doc_number IS NOT NULL
         AND p_can_fy IS NOT NULL
         AND p_file_name IS NOT NULL
         AND p_batch_number IS NOT NULL
      THEN
         UPDATE nihgl_ope_acct_dtls_int_tbl
            SET error_message = p_error_message,
                funds_check_message = p_funds_check_message,
                funds_check_code = p_funds_check_code,
                last_updated_by = fnd_global.user_id,
                last_update_date = SYSDATE,
                last_update_login = fnd_global.login_id,
                record_status = NVL (p_record_status, record_status)
          WHERE     document_ref = p_doc_ref
                AND document_number = p_doc_number
                AND can_fy = p_can_fy
                AND file_name = p_file_name
                AND batch_number = p_batch_number;
      ELSIF     p_file_name IS NOT NULL
            AND p_batch_number IS NOT NULL
            AND p_record_status IS NOT NULL
            AND p_doc_ref IS NULL
            AND p_doc_number IS NULL
            AND p_can_fy IS NULL
      THEN
         UPDATE nihgl_ope_acct_dtls_int_tbl
            SET error_message = p_error_message,
                funds_check_message = p_funds_check_message,
                funds_check_code = p_funds_check_code,
                last_updated_by = fnd_global.user_id,
                last_update_date = SYSDATE,
                last_update_login = fnd_global.login_id,
                ERROR_CODE = p_error_code,
                record_status = p_record_status
          WHERE     file_name = p_file_name
                AND batch_number = p_batch_number
                AND record_status <> 'R';
      ELSIF     p_record_status IS NOT NULL
            AND p_file_name IS NULL
            AND p_batch_number IS NULL
            AND p_doc_ref IS NULL
            AND p_doc_number IS NULL
            AND p_can_fy IS NULL
      THEN
         UPDATE nihgl_ope_acct_dtls_int_tbl
            SET error_message = p_error_message,
                funds_check_message = p_funds_check_message,
                funds_check_code = p_funds_check_code,
                last_updated_by = fnd_global.user_id,
                last_update_date = SYSDATE,
                last_update_login = fnd_global.login_id,
                ERROR_CODE = p_error_code,
                record_status = p_record_status
          WHERE record_status = 'E';
      ELSE
         logf ('Invalid argument call to sub routine UPDATE_ERR_MSG' , l_module);
      END IF;

      IF SQL%ROWCOUNT > 0
      THEN
         COMMIT;
      END IF;
   EXCEPTION
      WHEN OTHERS
      THEN
         logf (
               'Error while Updating the error message to NULL in UPDATE_ERR_MSG'
            || SQLERRM,  l_module);
         g_retcode := 2;
         g_errbuf :=
               'Error while Updating the error message to NULL in UPDATE_ERR_MSG: '
            || SUBSTR (SQLERRM, 1, 250);
   END update_err_msg;

  /* ----------------------------------------------------------------------------------------------------------------
   --Added by Srinivas Rayankula on 10/30/2023 -- Start
   -- Function get_can_fy can used to derive the can_fy for Royalty CAN
   -- Usage: This function called from insert_into_intf_tbl procedure
  ------------------------------------------------------------------------------------*/
   FUNCTION check_royalty_can (p_can IN VARCHAR2, p_task_num IN VARCHAR2)
      RETURN BOOLEAN
   IS
      l_can_type   NUMBER;
      l_can_fy     NUMBER;
      l_return     BOOLEAN;
   BEGIN
      l_can_type := NULL;
      l_can_fy := NULL;
      l_return := FALSE;

      SELECT SUBSTR (pap.ATTRIBUTE2, 11, 1), SUBSTR (pap.ATTRIBUTE2, 7, 4)
        INTO l_can_type, l_can_fy
        FROM pa_projects_all pap, pa_tasks pat, pa_project_statuses b
       WHERE     pap.NAME = p_can
             AND pap.project_status_code = b.project_status_code
             AND b.project_system_status_code NOT IN ('REJECTED', 'CLOSED')
             AND pat.task_number = p_task_num                            --'1'
             AND (    NVL (pap.start_date, SYSDATE) <= SYSDATE
                  AND (   pap.completion_date >= SYSDATE
                       OR pap.completion_date IS NULL))
             AND NVL (pap.enabled_flag, 'Y') = 'Y'
             AND pap.template_flag = 'N'
             AND pap.project_id = pat.project_id; --   FETCH FIRST ROW ONLY;

      IF  NVL(l_can_type,0)>1 AND l_can_fy <= 2019 THEN
         l_return := TRUE;
      ELSE
      l_return := FALSE;
   END IF;

      RETURN (l_return);
   EXCEPTION
      WHEN OTHERS
      THEN
         RETURN (l_return);
   END check_royalty_can;

/* ------------------------------------------------------------------------------------------------------------


-- Procedure record_btch_hdr_status can used to update  the batch header
-- record status based on the file name,batch number, record status,
-- error message and error code provided for OPE error handling form.
-- Usage:
-- 1. This procedure called from MAIN procedure that is when P_CALLED_FROM_FORM parameter is 'Y'
-- then update header record to 'N' for regular processing.
--
-- 2. Also called from validate_txn_data procedure when some of the detail lines have errored out
-- for the batch and are held in the interface table for error processing if so mark the header
-- as error with appropriate error message.
-- ------------------------------------------------------------------------------------------------------------*/
   PROCEDURE record_btch_hdr_status (p_record_status    VARCHAR2,
                                     p_file_name        VARCHAR2,
                                     p_batch_number     VARCHAR2,
                                     p_error_message    VARCHAR2,
                                     p_error_code       VARCHAR2,
                                     p_ope_batch_id     NUMBER )
   AS
    l_module varchar2(100) :=  'record_btch_hdr_status';

   BEGIN
     logf('begin' , l_Module);

      IF p_file_name IS NOT NULL AND p_batch_number IS NOT NULL
      THEN
         UPDATE nihgl_ope_acct_btchs_int_tbl
            SET error_message =
                   DECODE (
                      p_record_status,
                      'N', NULL,
                         error_message
                      || DECODE (error_message,
                                 NULL, p_error_message,
                                 '; ' || p_error_message)),
                ERROR_CODE = DECODE (p_record_status, 'N', NULL, p_error_code),
                last_updated_by = fnd_global.user_id,
                last_update_date = SYSDATE,
                last_update_login = fnd_global.login_id,
                record_status = p_record_status
          WHERE file_name = p_file_name
            AND batch_number = p_batch_number
            AND ope_batch_id = NVL (p_ope_batch_id, ope_batch_id)
            AND record_status <> p_record_status;
      ELSIF p_file_name IS NULL AND p_batch_number IS NULL
      THEN
         UPDATE nihgl_ope_acct_btchs_int_tbl
            SET error_message =
                   DECODE (
                      p_record_status,
                      'N', NULL,
                         error_message
                      || DECODE (error_message,
                                 NULL, p_error_message,
                                 '; ' || p_error_message)),
                ERROR_CODE = DECODE (p_record_status, 'N', NULL, p_error_code),
                last_updated_by = fnd_global.user_id,
                last_update_date = SYSDATE,
                last_update_login = fnd_global.login_id,
                record_status = p_record_status
          WHERE ope_batch_id = NVL (p_ope_batch_id, ope_batch_id)
            AND record_status <> p_record_status;
      END IF;

      IF SQL%ROWCOUNT > 0
      THEN
         COMMIT;
      END IF;
   EXCEPTION
      WHEN OTHERS
      THEN
         logf ('Error while Recording batch header status' || SQLERRM , l_module);
         g_retcode := 2;
         g_errbuf  := 'Error while Recording batch header status: ' || SUBSTR (SQLERRM, 1, 250);
   END record_btch_hdr_status;

/* --------------------------------------------------------------------------------
     -- procedure update_err_doc_number

        Update the detail interface records based on
       if any of the document number block of records have error

-- -------------------------------------------------------------------------------- */
   PROCEDURE update_err_doc_number (p_batch_number    VARCHAR2,
                                    p_file_name       VARCHAR2)
   IS
      CURSOR cur_err_doc_number
      IS
         SELECT UNIQUE
                TRIM (document_ref || document_number) document_number,
                file_name,
                can_fy,
                tcode
           FROM nihgl_ope_acct_dtls_int_tbl a
          WHERE     record_status = 'E'
                AND batch_number = p_batch_number
                AND file_name = p_file_name;

    l_module varchar2(100) :=  'update_err_doc_number';

   BEGIN
     logf('begin' , l_Module);
      FOR cur_err_doc_number_rec IN cur_err_doc_number
      LOOP
           logf('Updating document for document ref/num and FY for ' || cur_err_doc_number_rec.document_number , l_Module);

         --One of the other lines error out for the combination
         --of Document Ref/Number and Fiscal Year

         UPDATE nihgl_ope_acct_dtls_int_tbl
            SET record_status = 'E',
                ERROR_CODE = NULL,
                error_message = g_err_message_tbl ('NIHOPETXN018')
          WHERE     TRIM (document_ref || document_number) =
                       cur_err_doc_number_rec.document_number
                AND record_status IN ('N', 'P', 'V')
                AND batch_number = p_batch_number
                AND can_fy = cur_err_doc_number_rec.can_fy
                AND file_name = cur_err_doc_number_rec.file_name;



         COMMIT;
      END LOOP;
   EXCEPTION
      WHEN OTHERS
      THEN
         logf (' Error in update_err_doc_number ' || SUBSTR (SQLERRM, 1, 250),l_module);
         g_retcode := 2;
         g_errbuf :=
            'Error in update_err_doc_number: ' || SUBSTR (SQLERRM, 1, 250);
   END update_err_doc_number;

   /*  ----------------------------------------------------------------------
       -- function update_lcnt

      || Create a function to return the count based
      || on file name and record status value.

  ------------------------------------------------------------------------     */
   FUNCTION update_lcnt (p_file_name       IN VARCHAR2,
                         p_record_status   IN VARCHAR2 ,
                         p_doc_ref         IN VARCHAR2 DEFAULT NULL,
                         p_doc_number      IN VARCHAR2 DEFAULT NULL,
                         p_can_fy          IN VARCHAR2 DEFAULT NULL
                                                                   )
      RETURN NUMBER
   IS
      l_cnt   NUMBER := 0;
      l_module varchar2(100) := 'update_lcnt';
   BEGIN

      IF     p_doc_ref IS NOT NULL
         AND p_doc_number IS NOT NULL
         AND p_can_fy IS NOT NULL
      THEN
         SELECT COUNT (*)
           INTO l_cnt
           FROM nihgl_ope_acct_dtls_int_tbl a
          WHERE     record_status = p_record_status
                AND file_name = p_file_name
               AND document_ref = p_doc_ref
               AND document_number = p_doc_number
              AND can_fy = p_can_fy;
      ELSE
         SELECT COUNT (*)
           INTO l_cnt
           FROM nihgl_ope_acct_dtls_int_tbl a
          WHERE record_status = p_record_status AND file_name = p_file_name;
      END IF;

      RETURN (l_cnt);
   EXCEPTION
      WHEN OTHERS
      THEN
         l_cnt := 0;
         g_retcode := 2;
         g_errbuf := 'Error in update_lcnt: ' || SUBSTR (SQLERRM, 1, 250);
         RETURN (l_cnt);
   END update_lcnt;

  /* ------------------------------------------------------------------------------------------
      procedure derive_accounting_date
      Get the accounting preiod and date
      Get only the first Open period for same accounting date or further

    -------------------------------------------------------------------------------------------  */
   PROCEDURE derive_accounting_date (p_sob_id         IN NUMBER,
                                     p_ope_batch_id   IN NUMBER)
   IS
      l_accounting_period   VARCHAR2 (30);
      l_accounting_date     DATE;
   BEGIN
      SELECT NVL (MAX (accounting_date), TRUNC (SYSDATE))
        INTO g_accounting_date
        FROM nihgl_ope_acct_dtls_int_tbl
       WHERE ope_batch_id = p_ope_batch_id;

      /*
      || Get only the first Open perid for same accounting date or further
      */
      BEGIN
         FOR c1rec
            IN (  SELECT CASE
                            WHEN SYSDATE >= start_date THEN TRUNC (SYSDATE)
             --changed Accouting Date Logic to derive current system date when the fiel is run
                   ELSE start_date                         -- Krishna
                    END
                start_date,  -- PRB0000666 Derive the Current date
                         period_name
                    FROM gl_period_statuses_v
                   WHERE     start_date >= TRUNC (g_accounting_date, 'MM')
                         AND adjustment_period_flag = 'N'
                         AND application_id =
                                (SELECT application_id
                                   FROM fnd_application
                                  WHERE application_short_name = 'SQLGL')
                         AND closing_status = 'O'
                ORDER BY start_date)
         LOOP
            l_accounting_period := c1rec.period_name;
            l_accounting_date := c1rec.start_date;
            EXIT;
         END LOOP;
      EXCEPTION
         WHEN OTHERS
         THEN
            l_accounting_period := NULL;
            l_accounting_date := NULL;
      END;

      g_accounting_period := l_accounting_period;


      IF l_accounting_date IS NOT NULL
      THEN
         g_accounting_date := l_accounting_date;
      END IF;

   EXCEPTION
      WHEN OTHERS
      THEN
         g_accounting_date := NULL;
         g_accounting_period := NULL;
   END derive_accounting_date;
-- ---------------------------------------------------------------------------
-- Function derive_ccid Derive the ccid
-- ---------------------------------------------------------------------------------------
   FUNCTION derive_ccid (p_attribute2          VARCHAR2,
                         p_fy                  NUMBER,
                         p_object_class        VARCHAR2,
                         p_sgl                 VARCHAR2,
                         p_ccid_flex       OUT VARCHAR2,
                         p_error_code      OUT VARCHAR2,
                         p_error_message   OUT VARCHAR2)
      RETURN NUMBER
   IS
      v_segments        fnd_flex_ext.segmentarray;
      v_segment_num     NUMBER;
      v_nih_fund        VARCHAR2 (25);
      v_account_code    VARCHAR2 (240);
      l_ccid            NUMBER;
      l_ccid_flex       VARCHAR2 (240);
      l_error_code      VARCHAR2 (30);
      l_error_message   VARCHAR2 (2000);
      l_coa             NUMBER;
   BEGIN
      v_segment_num :=
         fnd_flex_ext.breakup_segments (p_attribute2, '.', v_segments);

      IF TO_NUMBER (SUBSTR (v_segments (1), 11, 1)) > 1
      THEN
         v_nih_fund := v_segments (1);
      ELSE
         v_nih_fund :=
            LTRIM (
               RTRIM (
                     SUBSTR (v_segments (1), 1, 6)
                  || p_fy
                  || SUBSTR (v_segments (1), 11)));
      END IF;

      v_segments (1) := v_nih_fund;
      v_segments (2) := p_fy;
      v_segments (10) := p_object_class;
      v_segments (11) := p_sgl;
      l_ccid_flex :=
         fnd_flex_ext.concatenate_segments (n_segments   => g_total_segments,
                                            segments     => v_segments,
                                            delimiter    => '.');

      BEGIN
         SELECT CHART_OF_ACCOUNTS_ID
           INTO l_coa
           FROM apps.gl_ledgers
          WHERE ledger_id = fnd_profile.VALUE ('GL_SET_OF_BKS_ID');
      END;

      l_ccid :=
         fnd_flex_ext.get_ccid ('SQLGL',
                                'GL#',
                                l_coa,                        --101,FCI Change
                                TO_CHAR (SYSDATE, 'DD-MON-YYYY'),
                                l_ccid_flex);

      IF NVL (l_ccid, 0) = 0
      THEN
         l_error_code := 'E';
         l_error_message := fnd_flex_ext.GET_MESSAGE;
      END IF;

      p_error_message := l_error_message;
      p_error_code := l_error_code;
      p_ccid_flex := l_ccid_flex;
      RETURN (l_ccid);
   EXCEPTION
      WHEN OTHERS
      THEN
         l_error_code := 'E';
         l_error_message := SQLERRM;
         p_error_message := l_error_message;
         p_error_code := l_error_code;
         l_ccid := 0;
         p_ccid_flex := NULL;
         RETURN (l_ccid);
   END derive_ccid;
 /* -- --------------------------------------------------------------------
    -- procedure nbp_archive

   --Added the below overloaded procedure for OPE billing interface
   --error handling form
     Archive the Interface data, error or process
-- ============================================================  */
   PROCEDURE nbp_archive (p_file_name        IN VARCHAR2,
                          errbuf    out varchar2,
                          retcode   out number)

   IS    PRAGMA AUTONOMOUS_TRANSACTION;
      lc_errbuf             VARCHAR2 (2000);
      lb_error_exist        BOOLEAN := TRUE;
      l_posting_status      VARCHAR2 (1);

      --Start of change 1 by Yash
      lb_chk_hdr_arc_flag   BOOLEAN := FALSE;
      ln_cnt                NUMBER := 0;
       l_count number(18);
      l_err_cnt  number(18);
     l_proc_cnt number(18);
     l_module varchar2(100) := 'nbp_archive ';


   BEGIN
      logf('begin ' , l_module);


    logf('Inserting nihgl_ope_acct_btchs_arc_tbl ' , l_module);
               INSERT INTO nihgl_ope_acct_btchs_arc_tbl
                  SELECT *
                    FROM nihgl_ope_acct_btchs_int_tbl
                   WHERE  file_name = p_file_name
                         and request_id = g_request_id;


     logf('Inserting nihgl_ope_acct_dtls_arc_tbl ' , l_module);
               INSERT INTO nihgl_ope_acct_dtls_arc_tbl
                  SELECT *
                    FROM nihgl_ope_acct_dtls_int_tbl
                      WHERE  file_name = p_file_name
                          and request_id = g_request_id;
        logf('Deleting nihgl_ope_acct_btchs_arc_tbl ' , l_module);
                        DELETE FROM nihgl_ope_acct_btchs_int_tbl
                         WHERE file_name = p_file_name
                          and request_id = g_request_id;
         logf('Inserting ihgl_ope_acct_dtls_arc_tbl ' , l_module);
                         DELETE FROM nihgl_ope_acct_dtls_int_tbl
                          WHERE file_name = p_file_name
                          and request_id = g_request_id;


             -- Final status
   select count(*),
         sum(decode(record_status , 'E', 1, 0)) l_err_cnt ,
     sum(decode(record_status , 'P', 1, 0)) l_proc_cnt
        into
         l_count ,l_err_cnt, l_proc_cnt
   from nihgl_ope_acct_dtls_arc_tbl
   where request_id = g_request_id;


     logf('Total records   ' || l_count , l_module);
     logf('Processed Count ' || l_proc_cnt , l_module);
     logf('Error records   ' || l_err_cnt , l_module);


    if l_count =  l_proc_cnt and l_proc_cnt > 0 then
     errbuf := 'Transactions successfully transferred';
     retcode := 0;
    elsif l_err_cnt > 0 and l_proc_cnt > 0 then
     errbuf := 'Partial Transactions got transfered';
     retcode := 1;
    elsif l_count = l_err_cnt  and l_err_cnt > 0 then
     errbuf := 'No Transaction got transfered';
     retcode := 2;
    else
     errbuf := 'No Transaction got transfered';
     retcode := 2;
    End if;



    update nihgl_ope_acct_btchs_arc_tbl
        set record_status = decode(retcode , 0, 'P', 'E')
        where request_id = g_request_id;

      COMMIT;


   EXCEPTION
      WHEN OTHERS    THEN

         g_retcode := 2;
         g_errbuf := 'Error in inserting record into Archive Tables: ' || SUBSTR (SQLERRM, 1, 250);
         logf (g_errbuf,l_module);
   END nbp_archive;


/* --------------------------------------------------------------------------
   Function derive_single_year

      || Get the fy for single digit year

-- ----------------------------------------------------------------------------  */
   FUNCTION derive_single_year (p_fy IN VARCHAR2, p_parm_date DATE)
      RETURN VARCHAR2
   IS
      x_fy   VARCHAR2 (25);
      l_module varchar2(100) := 'dervier_single_year';

   BEGIN
      logf('begin ' , L_module);
      /*
      ||If P_FY is in this year, the year should be taken as this year
      || If P_FY is in the next 4 years, the year should be taken as one of the next 4 years
      || Else, the year should be taken as one the last (previous) 5 years.
      */
      --  E.g.
      -- Scenario 1
      -- If parm date is 10/15/2009 (means FY10),
      --           If FY= 0, then BFY = 2010
      --           If FY= 1, then BFY = 2011
      --           If FY= 2, then BFY = 2012
      --           If FY= 3, then BFY = 2013
      --           If FY= 4, then BFY = 2014
      --           If FY= 5, then BFY = 2005  -- *** note the change **
      --           If FY= 6, then BFY = 2006
      --           If FY= 7, then BFY = 2007
      --           If FY= 8, then BFY = 2008
      --           If FY= 9, then BFY = 2009
      --
      -- Scenario 2
      -- If parm date is 10/15/2011 (means FY12),
      --           If FY= 0, then BFY = 2010
      --           If FY= 1, then BFY = 2011
      --           If FY= 2, then BFY = 2012
      --           If FY= 3, then BFY = 2013
      --           If FY= 4, then BFY = 2014
      --           If FY= 5, then BFY = 2015
      --           If FY= 6, then BFY = 2016
      --           If FY= 7, then BFY = 2007  -- *** note the change ***
      --           If FY= 8, then BFY = 2008
      --           If FY= 9, then BFY = 2009
      --
      --
      --   Scenario 1: (Note that parm date FY is 2010)
      --             FY   Parm Date              BFY

      --             0    20-OCT-2009            2010 *
      --             1    20-OCT-2009            2011
      --             2    20-OCT-2009            2012
      --             3    20-OCT-2009            2013
      --             4    20-OCT-2009            2014
      --             5    20-OCT-2009            2005  -- Note the change
      --             6    20-OCT-2009            2006
      --             7    20-OCT-2009            2007
      --             8    20-OCT-2009            2008
      --             9    20-OCT-2009            2009

      --   Scenario 2: (Note that parm date FY is 2011)
      --             FY   Parm Date              BFY

      --             0    20-OCT-2010            2010
      --             1    20-OCT-2010            2011 *
      --             2    20-OCT-2010            2012
      --             3    20-OCT-2010            2013
      --             4    20-OCT-2010            2014
      --             5    20-OCT-2010            2015
      --             6    20-OCT-2010            2006 -- Note the change
      --             7    20-OCT-2010            2007
      --             8    20-OCT-2010            2008
      --             9    20-OCT-2010            2009

      --   Scenario 3: (Note that parm date FY is 2012)
      --             FY   Parm Date              BFY

      --             0    20-OCT-2011            2010
      --             1    20-OCT-2011            2011
      --             2    20-OCT-2011            2012 *
      --             3    20-OCT-2011            2013
      --             4    20-OCT-2011            2014
      --             5    20-OCT-2011            2015
      --             6    20-OCT-2011            2016
      --             7    20-OCT-2011            2007 -- Note the change
      --             8    20-OCT-2011            2008
      --             9    20-OCT-2011            2009

      --   Scenario 3: (Note that parm date FY is 2017)
      --             FY   Parm Date              BFY

      --             0    20-OCT-2016            2020
      --             1    20-OCT-2016            2021
      --             2    20-OCT-2016            2012 -- Note the change
      --             3    20-OCT-2016            2013
      --             4    20-OCT-2016            2014
      --             5    20-OCT-2016            2015
      --             6    20-OCT-2016            2016
      --             7    20-OCT-2016            2017  *
      --             8    20-OCT-2016            2018
      --             9    20-OCT-2016            2019
      SELECT CASE p_fy
                WHEN TO_CHAR (ADD_MONTHS (p_parm_date, 3), 'Y')
                THEN
                   TO_CHAR (ADD_MONTHS (p_parm_date, 3), 'YYYY')
                WHEN TO_CHAR (ADD_MONTHS (p_parm_date, 3 + 12 * 1), 'Y')
                THEN
                   TO_CHAR (ADD_MONTHS (p_parm_date, 3 + 12 * 1), 'YYYY')
                WHEN TO_CHAR (ADD_MONTHS (p_parm_date, 3 + 12 * 2), 'Y')
                THEN
                   TO_CHAR (ADD_MONTHS (p_parm_date, 3 + 12 * 2), 'YYYY')
                WHEN TO_CHAR (ADD_MONTHS (p_parm_date, 3 + 12 * 3), 'Y')
                THEN
                   TO_CHAR (ADD_MONTHS (p_parm_date, 3 + 12 * 3), 'YYYY')
                WHEN TO_CHAR (ADD_MONTHS (p_parm_date, 3 + 12 * 4), 'Y')
                THEN
                   TO_CHAR (ADD_MONTHS (p_parm_date, 3 + 12 * 4), 'YYYY')
                WHEN TO_CHAR (ADD_MONTHS (p_parm_date, 3 + 12 * 5), 'Y')
                THEN
                   TO_CHAR (ADD_MONTHS (p_parm_date, 3 - 12 * 5), 'YYYY')
                WHEN TO_CHAR (ADD_MONTHS (p_parm_date, 3 + 12 * 6), 'Y')
                THEN
                   TO_CHAR (ADD_MONTHS (p_parm_date, 3 - 12 * 4), 'YYYY')
                WHEN TO_CHAR (ADD_MONTHS (p_parm_date, 3 + 12 * 7), 'Y')
                THEN
                   TO_CHAR (ADD_MONTHS (p_parm_date, 3 - 12 * 3), 'YYYY')
                WHEN TO_CHAR (ADD_MONTHS (p_parm_date, 3 + 12 * 8), 'Y')
                THEN
                   TO_CHAR (ADD_MONTHS (p_parm_date, 3 - 12 * 2), 'YYYY')
                WHEN TO_CHAR (ADD_MONTHS (p_parm_date, 3 + 12 * 9), 'Y')
                THEN
                   TO_CHAR (ADD_MONTHS (p_parm_date, 3 - 12 * 1), 'YYYY')
             END
                YEAR1
        INTO x_fy
        FROM DUAL;

      RETURN (x_fy);
   EXCEPTION
      WHEN OTHERS
      THEN
         logf (' Error in derive_single_year Function: ' || SQLERRM,l_module);
         RETURN ('9999');
   END derive_single_year;
-- =======================================
-- procedure DELETE_RECORD
 --  Delete the records from staging Master/Detail tables

-- =======================================
   PROCEDURE DELETE_RECORD (p_file_name VARCHAR2)
   IS    PRAGMA AUTONOMOUS_TRANSACTION;
      lc_errbuf   VARCHAR2 (1000);
      l_module varchar2(100) := 'DELETE_RECORD';
   BEGIN
      EXECUTE IMMEDIATE
            'DELETE FROM nihgl_ope_acct_btchs_stg_tbl '
         || ' WHERE file_name = '
         || ''''
         || p_file_name
         || '''';

      EXECUTE IMMEDIATE
            'DELETE FROM nihgl_ope_acct_dtls_stg_tbl '
         || ' WHERE file_name = '
         || ''''
         || p_file_name
         || '''';

      COMMIT;
   EXCEPTION
      WHEN OTHERS
      THEN
         lc_errbuf :=
               'Error in deleting record for worng data in both batch '
            || 'table and detail table :'
            || SQLERRM;
         logf (lc_errbuf, l_module);
         g_retcode := 2;
         g_errbuf :=
               'Error in deleting record for worng data in both batch table and detail table: '
            || SUBSTR (SQLERRM, 1, 250);
   END DELETE_RECORD;
-- =======================================

   FUNCTION get_tcode (p_legacy_tcode   IN VARCHAR2,
                       p_fund           IN VARCHAR2,
                       p_object_class   IN VARCHAR2,
                       p_ier            IN VARCHAR2,
                       p_batch_number   IN VARCHAR2)
      RETURN VARCHAR2
   IS
      CURSOR cDemon_Code (
         p_fund IN VARCHAR2)
      IS
         SELECT ffvv.attribute2 demon_code
           FROM apps.fnd_flex_value_sets ffvs, fnd_flex_values_vl ffvv
          WHERE     ffvs.flex_value_set_name = g_fund_value_set_name
                AND ffvv.flex_value_set_id = ffvs.flex_value_set_id
                AND flex_value = p_fund;

      CURSOR cTcode (
         p_legacy_code    VARCHAR2,
         p_demon_code     VARCHAR2)
      IS
         SELECT attribute1 tcode,
                attribute4 oc,
                attribute5 ier,
                attribute6 batch_number
           FROM fnd_lookup_values_vl
          WHERE     lookup_type = 'NIHGL_OPE_TCODE_MAPPING'
                AND attribute2 = p_legacy_tcode
                AND attribute3 = p_demon_code
                AND (    NVL (enabled_flag, 'Y') = 'Y'
                     AND (    NVL (start_date_active, SYSDATE) <= SYSDATE
                          AND (   end_date_active >= SYSDATE
                               OR end_date_active IS NULL)));

      rec                     cTcode%ROWTYPE;
      l_demon_code            VARCHAR2 (240);
      l_tcode                 VARCHAR2 (250);
      l_bat_tcode             VARCHAR2 (250);
      l_ier_match             BOOLEAN := FALSE;
      l_batchno_match         BOOLEAN := FALSE;
      l_no_oc_batch_tcode     VARCHAR2 (250);
      --Start of change 3 by Yash
      --Added parameters below
      l_oc2_batch_ier_tcode   VARCHAR2 (250);
      l_oc4_ier_tcode         VARCHAR2 (250);
      l_oc2_ier_tcode         VARCHAR2 (250);
      l_batch_ier_tcode       VARCHAR2 (250);
      l_ier_tcode             VARCHAR2 (250);
      l_batch_ier_match       BOOLEAN := FALSE;
      --Commented the original lines below
      --l_oc_batch_tcode     VARCHAR2 (250);
      --l_oc_tcode           VARCHAR2 (250);
      --l_oc_match           BOOLEAN := FALSE;
      --l_oc_batch_match     BOOLEAN := FALSE;
      --l_oc_ier_match       BOOLEAN := FALSE;
      --Modified the above parameters for 4/2 character object class search
      l_oc4_batch_tcode       VARCHAR2 (250);
      l_oc2_batch_tcode       VARCHAR2 (250);
      l_oc4_tcode             VARCHAR2 (250);
      l_oc2_tcode             VARCHAR2 (250);
      l_oc4_match             BOOLEAN := FALSE;
      l_oc2_match             BOOLEAN := FALSE;
      l_oc4_batch_match       BOOLEAN := FALSE;
      l_oc2_batch_match       BOOLEAN := FALSE;
      l_oc4_ier_match         BOOLEAN := FALSE;
      l_oc2_ier_match         BOOLEAN := FALSE;
      l_module varchar2(100) := 'get_tcode';
   --End of change 3
   BEGIN

        logf('begin ' , l_module);
      --Fetch the demon code based on the input fund
      OPEN cDemon_Code (p_fund);

      FETCH cDemon_Code INTO l_demon_code;

      CLOSE cDemon_code;

      --Fetch the lookup records based on legacy tcode and demon code
      OPEN cTcode (p_legacy_tcode, l_demon_code);

      LOOP
         FETCH cTcode INTO rec;


         EXIT WHEN cTcode%NOTFOUND;

         --Start of change 3 by Yash
         l_oc4_match := FALSE;
         l_oc2_match := FALSE;
         l_ier_match := FALSE;
         l_batchno_match := FALSE;

         --End of change 3

         --Check if the OC from the lookup record is same as input value
         --4 character OC is checked first and if no match found then
         --2 character OC is checked
         IF rec.oc IS NOT NULL
         THEN
            IF p_object_class = rec.oc
            THEN
               --Start of change 3 by Yash
               --Commented the original code below
               --l_oc_match := TRUE;
               --Added the new logic to enable 4 and 2 character OC search
               --4 Character search takes higher precedence over 2 character
               l_oc4_match := TRUE;
            ELSIF SUBSTR (p_object_class, 1, 2) = rec.oc
            THEN
               l_oc2_match := TRUE;
            --End of change 3
            END IF;
         END IF;

         --Check if IER is matched from the lookup record to the
         --input value
         IF rec.ier IS NOT NULL
         THEN
            IF p_ier = rec.ier
            THEN
               l_ier_match := TRUE;
            END IF;
         END IF;

         --Check if batch number from the lookup record is matching the
         --input value batch number
         IF rec.batch_number IS NOT NULL
         THEN
            IF p_batch_number = rec.batch_number
            THEN
               l_batchno_match := TRUE;
            END IF;
         END IF;

         --Start of change 3 by Yash
         --Commented the original code below
         /*IF l_oc_match AND l_ier_match AND l_batchno_match
         THEN
            l_tcode := rec.tcode;
            EXIT;
         END IF;*/
         --Modified the original code to match 4 and 2 character OC
         --Match 4 character OC and if a match is found then
         --the 4 character OC takes highest precedence and hence
         --consider that tcode and exit the loop
         --If a 4 character OC match is not found then search for 2 character OC
         --match and if found then it takes the second highest precedence
         IF l_oc4_match AND l_ier_match AND l_batchno_match
         THEN
            l_tcode := rec.tcode;
            EXIT;
         ELSIF l_oc2_match AND l_ier_match AND l_batchno_match
         THEN
            l_oc2_batch_ier_tcode := rec.tcode;
         END IF;

         --Check if OC (4/2 char) and Batch number match
         IF l_oc4_match AND l_batchno_match AND rec.ier IS NULL
         THEN
            l_oc4_batch_tcode := rec.tcode;
         ELSIF l_oc2_match AND l_batchno_match AND rec.ier IS NULL
         THEN
            l_oc2_batch_tcode := rec.tcode;
         END IF;

         --Check if OC (4/2 char) and IER match
         IF l_oc4_match AND l_ier_match AND rec.batch_number IS NULL
         THEN
            l_oc4_ier_tcode := rec.tcode;
         ELSIF l_oc2_match AND l_ier_match AND rec.batch_number IS NULL
         THEN
            l_oc2_ier_tcode := rec.tcode;
         END IF;

         --Check if batch and IER match
         IF l_batchno_match AND l_ier_match AND rec.oc IS NULL
         THEN
            l_batch_ier_tcode := rec.tcode;
         END IF;

         --Check if OC (4/2 char) match
         IF l_oc4_match AND rec.batch_number IS NULL AND rec.ier IS NULL
         THEN
            l_oc4_tcode := rec.tcode;
         ELSIF l_oc2_match AND rec.batch_number IS NULL AND rec.ier IS NULL
         THEN
            l_oc2_tcode := rec.tcode;
         END IF;

         --Check if batch match
         IF l_batchno_match AND rec.oc IS NULL AND rec.ier IS NULL
         THEN
            l_bat_tcode := rec.tcode;
         END IF;

         --Check if IER match
         IF l_ier_match AND rec.oc IS NULL AND rec.batch_number IS NULL
         THEN
            l_ier_tcode := rec.tcode;
         END IF;

         --End of change 3

         --If no OC (4/2 char), batch or IER match found
         --then use the default one
         IF rec.oc IS NULL AND rec.batch_number IS NULL AND rec.ier IS NULL
         THEN
            l_no_oc_batch_tcode := rec.tcode;
         END IF;
      END LOOP;

      CLOSE cTcode;

      IF l_tcode IS NULL
      THEN
         --Start of change 3 by Yash
         --Commented the original code below
         /*IF l_oc_match AND l_batchno_match
         THEN
            l_tcode := l_oc_batch_tcode;
         ELSE
            IF l_batchno_match
            THEN
               l_tcode := l_bat_tcode;
            ELSIF l_oc_match
            THEN
               l_tcode := l_oc_tcode;
            ELSE
               l_tcode := l_no_oc_batch_tcode;
            END IF;
         END IF;*/
         --Modified the original code to include 4/2 character OC match
         --and IER batch combinations
         --The below conditional statement follows the precedence listed above the function
         --declaration. If 4 character OC, batch and IER match is not found the l_tcode
         --will be blank which will enforce the execution of this conditional statement
         IF l_oc2_batch_ier_tcode IS NOT NULL
         THEN
            l_tcode := l_oc2_batch_ier_tcode;
         ELSIF l_oc4_batch_tcode IS NOT NULL
         THEN
            l_tcode := l_oc4_batch_tcode;
         ELSIF l_oc2_batch_tcode IS NOT NULL
         THEN
            l_tcode := l_oc2_batch_tcode;
         ELSIF l_oc4_ier_tcode IS NOT NULL
         THEN
            l_tcode := l_oc4_ier_tcode;
         ELSIF l_oc2_ier_tcode IS NOT NULL
         THEN
            l_tcode := l_oc2_ier_tcode;
         ELSIF l_batch_ier_tcode IS NOT NULL
         THEN
            l_tcode := l_batch_ier_tcode;
         ELSE
            IF l_oc4_tcode IS NOT NULL
            THEN
               l_tcode := l_oc4_tcode;
            ELSIF l_oc2_tcode IS NOT NULL
            THEN
               l_tcode := l_oc2_tcode;
            ELSIF l_bat_tcode IS NOT NULL
            THEN
               l_tcode := l_bat_tcode;
            ELSIF l_ier_tcode IS NOT NULL
            THEN
               l_tcode := l_ier_tcode;
            ELSE
               l_tcode := l_no_oc_batch_tcode;
            END IF;
         END IF;
      --End of change 3
      END IF;

      RETURN l_tcode;
   EXCEPTION
      WHEN OTHERS
      THEN
         l_tcode := NULL;
         RETURN l_tcode;
   END get_tcode;
-- =======================================
-- Function get_batch_category
-- =======================================
   FUNCTION get_batch_category (p_batch_number VARCHAR2)
      RETURN VARCHAR2
   IS
      l_je_category        VARCHAR2 (150);
      l_je_category_desc   VARCHAR2 (1500);
   BEGIN
   /*CR NBSCH0002499
      SELECT attribute2
        INTO l_je_category
        FROM fnd_lookup_values_vl
       WHERE     lookup_type = 'NIHGL_OPE_BILLING_FILE_EMAIL'
             AND lookup_code = p_batch_number
             AND (    NVL (enabled_flag, 'Y') = 'Y'
                  AND (    NVL (start_date_active, SYSDATE) <= SYSDATE
                       AND (   end_date_active >= SYSDATE
                            OR end_date_active IS NULL)))
             AND attribute2 IS NOT NULL;
    */
      SELECT user_je_category_name
        INTO l_je_category_desc
        FROM GL_JE_CATEGORIES_VL
   --    WHERE je_category_name = l_je_category; --CR NBSCH0002499
       WHERE je_category_name = g_JOURNAL_CATEGORY;

      RETURN l_je_category_desc;
   EXCEPTION
      WHEN OTHERS
      THEN
         l_je_category := NULL;
         RETURN l_je_category;
   END get_batch_category;
-- ------------------------------------------------------------------------
-- FUNCTION batch_exists Validate batch existence using lookup
-- -------------------------------------------------------------------------------
   FUNCTION batch_exists (p_batch_number       VARCHAR2,
                          p_batch_desc     OUT VARCHAR2,
                          p_je_category    OUT VARCHAR2)
      RETURN BOOLEAN
   IS
      l_batch_desc    VARCHAR2 (150);
      l_je_category   VARCHAR2 (150);
      l_module varchar2(100) := 'batch_exists';

   BEGIN

      logf('begin' , l_module);
      SELECT attribute1, attribute2
        INTO l_batch_desc, l_je_category
        FROM fnd_lookup_values_vl
       WHERE     lookup_type = 'NIHGL_OPE_BILLING_FILE_EMAIL'
             AND lookup_code = p_batch_number
             AND (    NVL (enabled_flag, 'Y') = 'Y'
                  AND (    NVL (start_date_active, SYSDATE) <= SYSDATE
                       AND (   end_date_active >= SYSDATE
                            OR end_date_active IS NULL)))
             AND attribute2 IS NOT NULL;

      p_batch_desc := l_batch_desc;
      p_je_category := l_je_category;

      RETURN TRUE;
   EXCEPTION
      WHEN OTHERS
      THEN
         p_batch_desc := NULL;
         p_je_category := NULL;
         logf('No Batch Description found for batch :'||p_batch_number,l_module);
         RETURN FALSE;
   END batch_exists;

/* --------------------------------------------------------------------------------
-- Procedure insert_into_intf_tbl insert data into
-- ope interface tables insert_into_intf_tbl,
-- nihgl_ope_acct_dtls_int_tbl and deletes the OPE staging
-- records once it moved to OPE Interface tables.
-- This procedure called in validate_hdr_data procedure.
-- ------------------------------------------------------------------------------------*/

   PROCEDURE insert_into_intf_tbl (p_file_name VARCHAR2)
   IS    PRAGMA AUTONOMOUS_TRANSACTION;
      CURSOR ope_inst_batches
      IS
         SELECT m.*
           FROM nihgl_ope_acct_btchs_stg_tbl m
          WHERE m.record_status = 'N'
            AND m.file_name = p_file_name
            AND m.hdr_ftr = g_ftr;


      CURSOR ope_inst_detail
      IS
         SELECT d.ROWID, ROW_NUMBER() OVER (ORDER BY d.record_id) as line_number, d.*
           FROM nihgl_ope_acct_dtls_stg_tbl d
          WHERE d.file_name = p_file_name AND d.record_status = 'N'
          order by d.record_id;

      ln_header_id            NUMBER;
      ln_batch_id             NUMBER;
      ln_detail_id            NUMBER;
      v_user_id               NUMBER;
      v_request_id            NUMBER;
      v_login                 NUMBER;
      v_source                VARCHAR2 (50);
      lc_record_status        VARCHAR2 (1);
      l_agency_code           nihgl_ope_acct_btchs_stg_tbl.agency_code%TYPE;
      l_accounting_point      nihgl_ope_acct_btchs_stg_tbl.accounting_point%TYPE;
      l_effective_date_from   nihgl_ope_acct_btchs_stg_tbl.effective_date_from%TYPE;
      l_effective_date_to     nihgl_ope_acct_btchs_stg_tbl.effective_date_to%TYPE;
   ln_can_fy            NUMBER;
   --ln_success           BOOLEAN;

     l_module varchar2(100) := 'insert_into_intf_tbl';
   BEGIN
     logf('begin ' , l_module);
      v_user_id := fnd_global.user_id;
      v_request_id := g_request_id; -- fnd_global.conc_request_id;
      v_login := fnd_global.login_id;

      FOR ope_inst_batches_rec IN ope_inst_batches
      LOOP
         SELECT nihgl_ope_acct_btchs_int_s.NEXTVAL INTO ln_batch_id FROM DUAL;

         l_agency_code := NULL;
         l_accounting_point := NULL;
         logf (
               ' Started loading staging data into Interface Table for Batch Number :'
            || ope_inst_batches_rec.batch_number, l_module);

         BEGIN
            SELECT agency_code,
                   accounting_point,
                   effective_date_from,
                   effective_date_to
              INTO l_agency_code,
                   l_accounting_point,
                   l_effective_date_from,
                   l_effective_date_to
              FROM nihgl_ope_acct_btchs_stg_tbl
             WHERE record_status = 'N'
               AND file_name = p_file_name
               AND lines_total IS NULL
               AND lines_total_amount IS NULL;

            logf('effective_date_from :'||l_effective_date_from,l_module);
            logf('effective_date_to :'||l_effective_date_to,l_module);

         EXCEPTION
            WHEN OTHERS THEN
               l_agency_code := NULL;
               l_accounting_point := NULL;
         END;

    logf('inserting  nihgl_ope_acct_btchs_int_tbl', l_module);
         INSERT INTO nihgl_ope_acct_btchs_int_tbl (ope_batch_id,
                                                   header_source,
                                                   accounting_date,
                                                   agency_code,
                                                   accounting_point,
                                                   batch_number,
                                                   lines_total,
                                                   lines_total_amount,
                                                   effective_date_from,
                                                   effective_date_to,
                                                   file_name,
                                                   record_id,
                                                   org_id,
                                                   set_of_books_id,
                                                   record_status,
                                                   request_id,
                                                   created_by,
                                                   creation_date,
                                                   last_updated_by,
                                                   last_update_date,
                                                   last_update_login)
              VALUES (
                        ln_batch_id,
                        v_source,
                        TO_DATE (
                           TO_CHAR (
                              TO_DATE (ope_inst_batches_rec.accounting_date,
                                       'MMDDYY'),
                              'MMDDYYYY'),
                           'MM/DD/YYYY'),
                        l_agency_code,
                        l_accounting_point,
                        TRIM (ope_inst_batches_rec.batch_number),
                        ope_inst_batches_rec.lines_total,
                        ope_inst_batches_rec.lines_total_amount,
                        l_effective_date_from,
                        l_effective_date_to,
                        ope_inst_batches_rec.file_name,
                        ope_inst_batches_rec.record_id,
                        ope_inst_batches_rec.org_id,
                        ope_inst_batches_rec.set_of_books_id,
                        'N',
                        --Mark as New in Interface Table
                        v_request_id,
                        v_user_id,                               -- CREATED_BY
                        SYSDATE,                              -- CREATION_DATE
                        v_user_id,
                        -- LAST_UPDATED_BY
                        SYSDATE,                           -- LAST_UPDATE_DATE
                        v_login);

         /*
         ||Update the both mnaster record to (P)rocess
         */
         UPDATE nihgl_ope_acct_btchs_stg_tbl
            SET record_status = 'P', request_id = v_request_id
          WHERE file_name = ope_inst_batches_rec.file_name;

    logf('inserting  nihgl_ope_acct_dtls_int_tbl' , l_module );
         FOR ope_inst_detail_rec IN ope_inst_detail
         LOOP
            SELECT nihgl_ope_acct_dtls_int_s.NEXTVAL
              INTO ln_detail_id
              FROM DUAL;

    logf('inserted ' || ln_detail_id , l_module );
    logf('record_id: ' || ope_inst_detail_rec.record_id , l_module );
    logf('line_number: ' || ope_inst_detail_rec.line_number , l_module );

   --Added by Srinivas Rayankula on 10/30/2023    -- Start
     ln_can_fy := NULL;

   logf('TRIM (ope_inst_detail_rec.reverse_code): ' || TRIM (ope_inst_detail_rec.reverse_code) , l_module );
   IF  TRIM (ope_inst_detail_rec.reverse_code) = '2' THEN

     logf('reverse_code - 2' , l_module );
     logf('TRIM (ope_inst_detail_rec.can): ' || TRIM (ope_inst_detail_rec.can) , l_module );
     logf('TRIM (ope_inst_detail_rec.task_number): ' || TRIM (ope_inst_detail_rec.task_number) , l_module );
     IF check_royalty_can(TRIM (ope_inst_detail_rec.can),TRIM (ope_inst_detail_rec.task_number)) THEN

        logf('check_royalty_can - true' , l_module );
        ln_can_fy := TO_NUMBER(ope_inst_detail_rec.ORIG_FY)+2010;
     ELSE
        logf('check_royalty_can - false' , l_module );
        logf('ope_inst_detail_rec.task_number: ' || ope_inst_detail_rec.task_number , l_module );

        BEGIN
                ln_can_fy := TO_NUMBER(SUBSTR(ope_inst_detail_rec.task_number, 1, 4));
        EXCEPTION
            WHEN OTHERS THEN
                ln_can_fy := NULL;
        END;

        logf('ln_can_fy: ' || ln_can_fy , l_module );
     END IF;

   ELSE
     logf('reverse_code - else' , l_module );
        BEGIN
                ln_can_fy := TO_NUMBER(SUBSTR(ope_inst_detail_rec.task_number, 1, 4));
        EXCEPTION
            WHEN OTHERS THEN
                ln_can_fy := NULL;
        END;

   END IF;
            --Added by Srinivas Rayankula on 10/30/2023    -- End

   logf('ln_can_fy: ' || ln_can_fy , l_module );
            INSERT INTO nihgl_ope_acct_dtls_int_tbl (ope_batch_id,
                                                     ope_detail_id,
                                                     accounting_date,
                                                     batch_number,
                                                     tcode,
                                                     reverse_code,
                                                     modifier_code,
                                                     document_ref,
                                                     document_number,
                                                     oth_document_ref,
                                                     oth_document_number,
                                                     geo_code,
                                                     can_fy,
                                                     can,
                                                     object_class,
                                                     amount,
                                                     primary_ein,
                                                     secondary_ein,
                                                     schedule_number,
                                                     gov_nongov,
                                                     CATEGORY,
                                                     case_11_code,
                                                     balance_of_payment,
                                                     gl_code,
                                                     type_of_service,
                                                     reserved,
                                                     grant_number,
                                                     grant_begin_date,
                                                     grant_end_date,
                                                     parm_date,
                                                     useriii,
                                                     clerkkid,
                                                     adbis,
                                                     attribute6,
                                                     source_code,
                                                     file_name,
                                                     record_id,
                                                     org_id,
                                                     set_of_books_id,
                                                     record_status,
                                                     request_id,
                                                     created_by,
                                                     creation_date,
                                                     last_updated_by,
                                                     last_update_date,
                                                     last_update_login,
                                                     name,
                                                     attribute1,
                                                     attribute2,
                                                     attribute3,
                                                     attribute4,
                                                     attribute5,
                                                     attribute7,
                                                     attribute8,
                                                     attribute9,
                                                     attribute10,
                                                     attribute11,
                                                     attribute12,
                                                     attribute13,
                                                     attribute14,
                                                     attribute15,
                                                     task_number, -- Added task_number,expenditure_item_date change2
                                                     expenditure_item_date,
                                                     line_number) --CR NBSCH0002499

                 VALUES (
                           ln_batch_id,                         --ope_batch_id
                           ln_detail_id,                       --ope_detail_id
                           TO_DATE (
                              TO_CHAR (
                                 TO_DATE (
                                    ope_inst_detail_rec.accounting_date,
                                    'MMDDYY'),
                                 'MMDDYYYY'),
                              'MM/DD/YYYY'),
                           TRIM (ope_inst_batches_rec.batch_number),
                           TRIM (ope_inst_detail_rec.tcode),
                           TRIM (ope_inst_detail_rec.reverse_code),
                           TRIM (ope_inst_detail_rec.modifier_code),
                           TRIM (ope_inst_detail_rec.document_ref),
                           TRIM (ope_inst_detail_rec.document_number),
                           TRIM (ope_inst_detail_rec.oth_document_ref),
                           TRIM (ope_inst_detail_rec.oth_document_number),
                           TRIM (ope_inst_detail_rec.geo_code),
                           /*TRIM (
                              TO_NUMBER (
                                 SUBSTR (ope_inst_detail_rec.task_number,
                                         1,
                                         4))),*/---TO_NUMBER (ope_inst_detail_rec.fy), -- Commented by Srinivas Rayankula on 10/30/2023
                           ln_can_fy,  --Added by Srinivas Rayankula on 10/30/2023
                           TRIM (ope_inst_detail_rec.can),
                           TRIM (ope_inst_detail_rec.object_class),
                           TO_NUMBER (ope_inst_detail_rec.amount),
                           TRIM (ope_inst_detail_rec.primary_ein),
                           TRIM (ope_inst_detail_rec.secondary_ein),
                           TRIM (ope_inst_detail_rec.schedule_number),
                           TRIM (ope_inst_detail_rec.gov_nongov),
                           TRIM (ope_inst_detail_rec.CATEGORY),
                           TRIM (ope_inst_detail_rec.case_11_code),
                           TRIM (ope_inst_detail_rec.balance_of_payment),
                           TRIM (ope_inst_detail_rec.gl_code),
                           TRIM (ope_inst_detail_rec.type_of_service),
                           TRIM (ope_inst_detail_rec.reserved),
                           TRIM (ope_inst_detail_rec.grant_number),
                           TO_DATE (
                              TO_CHAR (
                                 TO_DATE (
                                    ope_inst_detail_rec.grant_begin_date,
                                    'MMDDYY'),
                                 'MMDDYYYY'),
                              'MM/DD/YYYY'),
                           TO_DATE (
                              TO_CHAR (
                                 TO_DATE (ope_inst_detail_rec.grant_end_date,
                                          'MMDDYY'),
                                 'MMDDYYYY'),
                              'MM/DD/YYYY'),
                           TO_DATE (
                              TO_CHAR (
                                 TO_DATE (ope_inst_detail_rec.parm_date,
                                          'MMDDYY'),
                                 'MMDDYYYY'),
                              'MM/DD/YYYY'),
                           TRIM (ope_inst_detail_rec.useriii),
                           TRIM (ope_inst_detail_rec.clerkkid),
                           TRIM (ope_inst_detail_rec.adbis),
                           TRIM (ope_inst_detail_rec.suspense_code),
                           TRIM (ope_inst_detail_rec.source_code),
                           ope_inst_detail_rec.file_name,
                           ope_inst_detail_rec.record_id,
                           ope_inst_detail_rec.org_id,
                           ope_inst_detail_rec.set_of_books_id,
                           'N',
                           --Mark as new in Interface table--Record Status
                           v_request_id,
                           v_user_id,                            -- CREATED_BY
                           SYSDATE,                           -- CREATION_DATE
                           v_user_id,                       -- LAST_UPDATED_BY
                           SYSDATE,                        -- LAST_UPDATE_DATE
                           v_login,
                           TRIM (ope_inst_detail_rec.name),
                           TRIM (ope_inst_detail_rec.attribute1),
                           TRIM (ope_inst_detail_rec.attribute2),
                           TRIM (ope_inst_detail_rec.attribute3),
                           TRIM (ope_inst_detail_rec.attribute4),
                           TRIM (ope_inst_detail_rec.attribute5),
                           --   TRIM(ope_inst_detail_rec.attribute6),
                           TRIM (ope_inst_detail_rec.attribute7),
                           TRIM (ope_inst_detail_rec.attribute8),
                           TRIM (ope_inst_detail_rec.attribute9),
                           TRIM (ope_inst_detail_rec.attribute10),
                           TRIM (ope_inst_detail_rec.attribute11),
                           TRIM (ope_inst_detail_rec.attribute12),
                           TRIM (ope_inst_detail_rec.attribute13),
                           TRIM (ope_inst_detail_rec.attribute14),
                           TRIM (ope_inst_detail_rec.attribute15),
                           TRIM (ope_inst_detail_rec.task_number),
                           TO_DATE (
                              TO_CHAR (
                                 TO_DATE (
                                    ope_inst_detail_rec.expenditure_item_date,
                                    'MMDDYY'),
                                 'MMDDYYYY'),
                              'MM/DD/YYYY'), --  TRIM (ope_inst_detail_rec.expenditure_item_date) -- Added task_number change3
                           ope_inst_detail_rec.line_number); --CR NBSCH0002499

           logf('inserted: ' || SQL%ROWCOUNT , l_module );
           UPDATE nihgl_ope_acct_dtls_stg_tbl
               SET record_status = 'P', request_id = v_request_id
             WHERE     file_name = ope_inst_detail_rec.file_name
                   AND ROWID = ope_inst_detail_rec.ROWID
                   AND set_of_books_id = ope_inst_detail_rec.set_of_books_id;
                logf('prior commmiting' , l_Module);

           logf('updated: ' || SQL%ROWCOUNT , l_module );
                    COMMIT;

         END LOOP;
      END LOOP;


      /*----------------------------------------------------------------------------------------------
        Deleting the staging records once it moved to Interface table
      -------------------------------------------------------------------------------------------*/
      BEGIN
         DELETE FROM nihgl_ope_acct_btchs_stg_tbl
               WHERE record_status = 'P' AND request_id = v_request_id;

         DELETE FROM nihgl_ope_acct_dtls_stg_tbl
               WHERE record_status = 'P' AND request_id = v_request_id;

           COMMIT;
      EXCEPTION
         WHEN OTHERS THEN
            lc_errbuf :=
                  ' Error While Purging record from  Staging Tables for P :'
               || SUBSTR (SQLERRM, 1, 250);
            logf (lc_errbuf, L_MODULE);
      END;


   EXCEPTION
      WHEN OTHERS
      THEN
         lc_errbuf := NULL;
         lc_errbuf :=
               ' Error in Insert_into_intf_tbl and  Error is :'
            || SUBSTR (SQLERRM, 1, 250);
         logf (lc_errbuf,l_module);
         g_retcode := 2;
         g_errbuf :=
               'Error in Insert_into_intf_tbl and  Error is: '
            || SUBSTR (SQLERRM, 1, 250);
   END insert_into_intf_tbl;

------------------------------------------------------------------------------------------
-- Procedure inst_arch_file_vald_error
--
-- Insert into ope archive table if file level validation failed
--
-- -------------------------------------------------------------------------------------------
   PROCEDURE inst_arch_file_vald_error (p_file_name    VARCHAR2,
                                        p_error_msg    VARCHAR2)
   IS     PRAGMA AUTONOMOUS_TRANSACTION;
      CURSOR ope_inst_batches
      IS
         SELECT m.*
           FROM nihgl_ope_acct_btchs_stg_tbl m
          WHERE m.record_status = 'N'
            AND m.file_name = p_file_name
            AND m.hdr_ftr = g_ftr
            AND ROWNUM < 2;

      CURSOR ope_inst_detail
      IS
         SELECT d.ROWID, d.*
           FROM nihgl_ope_acct_dtls_stg_tbl d
          WHERE d.file_name = p_file_name AND d.record_status = 'N';

      ln_header_id            NUMBER;
      ln_batch_id             NUMBER;
      ln_batch_number         VARCHAR2 (3);
      ln_detail_id            NUMBER;
      v_user_id               NUMBER;
      v_request_id            NUMBER;
      v_login                 NUMBER;
      v_source                VARCHAR2 (50);
      lc_record_status        VARCHAR2 (1);
      l_agency_code           nihgl_ope_acct_btchs_stg_tbl.agency_code%TYPE;
      l_accounting_point      nihgl_ope_acct_btchs_stg_tbl.accounting_point%TYPE;
      --l_agency_code          nihgl_ope_acct_btchs_stg_tbl.agency_code%TYPE;
      l_effective_date_from   nihgl_ope_acct_btchs_stg_tbl.effective_date_from%TYPE;
      l_effective_date_to     nihgl_ope_acct_btchs_stg_tbl.effective_date_to%TYPE;
      l_module varchar2(100) := 'nst_arch_file_vald_error';
   --ln_success           BOOLEAN;
   BEGIN
      v_user_id := fnd_global.user_id;
      v_request_id := g_request_id; -- fnd_global.conc_request_id;
      v_login := fnd_global.login_id;


      FOR ope_inst_batches_rec IN ope_inst_batches
      LOOP
         SELECT nihgl_ope_acct_btchs_int_s.NEXTVAL INTO ln_batch_id FROM DUAL;

         BEGIN
            SELECT agency_code,
                   accounting_point,
                   effective_date_from,
                   effective_date_to
              INTO l_agency_code,
                   l_accounting_point,
                   l_effective_date_from,
                   l_effective_date_to
              FROM nihgl_ope_acct_btchs_stg_tbl
             WHERE record_status = 'N'
               AND file_name = p_file_name
               AND lines_total IS NULL
               AND lines_total_amount IS NULL;
         EXCEPTION
            WHEN OTHERS THEN
               l_agency_code := NULL;
               l_accounting_point := NULL;
         END;

         ln_batch_number := NULL;
         ln_batch_number := TRIM (ope_inst_batches_rec.batch_number);

         BEGIN
            INSERT INTO nihgl_ope_acct_btchs_arc_tbl (ope_batch_id,
                                                      header_source,
                                                      accounting_date,
                                                      agency_code,
                                                      accounting_point,
                                                      batch_number,
                                                      lines_total,
                                                      lines_total_amount,
                                                      effective_date_from,
                                                      effective_date_to,
                                                      file_name,
                                                      record_id,
                                                      org_id,
                                                      set_of_books_id,
                                                      record_status,
                                                      request_id,
                                                      created_by,
                                                      creation_date,
                                                      last_updated_by,
                                                      last_update_date,
                                                      last_update_login)
                 VALUES (
                           ln_batch_id,
                           v_source,
                           TO_DATE (
                              TO_CHAR (
                                 TO_DATE (
                                    ope_inst_batches_rec.accounting_date,
                                    'MMDDYY'),
                                 'MMDDYYYY'),
                              'MM/DD/YYYY'),
                           l_agency_code,
                           l_accounting_point,
                           ln_batch_number,
                           ope_inst_batches_rec.lines_total,
                           ope_inst_batches_rec.lines_total_amount,
                           l_effective_date_from,
                           l_effective_date_to,
                           ope_inst_batches_rec.file_name,
                           ope_inst_batches_rec.record_id,
                           ope_inst_batches_rec.org_id,
                           ope_inst_batches_rec.set_of_books_id,
                           'E',
                           --Mark as New in Interface Table
                           v_request_id,
                           v_user_id,                            -- CREATED_BY
                           SYSDATE,                           -- CREATION_DATE
                           v_user_id,                       -- LAST_UPDATED_BY
                           SYSDATE,                        -- LAST_UPDATE_DATE
                           v_login);
         EXCEPTION
            WHEN OTHERS THEN
               lc_errbuf := NULL;
               lc_errbuf :=
                     ' Error in Master (inst_arch_file_vald_error) and  Error is :'
                  || SUBSTR (SQLERRM, 1, 250);
               logf (lc_errbuf,l_module);
         END;
      END LOOP;

      FOR ope_inst_detail_rec IN ope_inst_detail
      LOOP
         SELECT nihgl_ope_acct_dtls_int_s.NEXTVAL INTO ln_detail_id FROM DUAL;

         BEGIN
            INSERT INTO nihgl_ope_acct_dtls_arc_tbl (ope_batch_id,
                                                     ope_detail_id,
                                                     accounting_date,
                                                     batch_number,
                                                     tcode,
                                                     reverse_code,
                                                     modifier_code,
                                                     document_ref,
                                                     document_number,
                                                     oth_document_ref,
                                                     oth_document_number,
                                                     geo_code,
                                                     can_fy,
                                                     can,
                                                     object_class,
                                                     amount,
                                                     primary_ein,
                                                     secondary_ein,
                                                     schedule_number,
                                                     gov_nongov,
                                                     CATEGORY,
                                                     case_11_code,
                                                     balance_of_payment,
                                                     gl_code,
                                                     type_of_service,
                                                     reserved,
                                                     grant_number,
                                                     grant_begin_date,
                                                     grant_end_date,
                                                     parm_date,
                                                     useriii,
                                                     clerkkid,
                                                     adbis,
                                                     source_code,
                                                     file_name,
                                                     record_id,
                                                     org_id,
                                                     set_of_books_id,
                                                     record_status,
                                                     ERROR_CODE,
                                                     error_message,
                                                     request_id,
                                                     created_by,
                                                     creation_date,
                                                     last_updated_by,
                                                     last_update_date,
                                                     last_update_login,
                                                     name,
                                                     attribute1,
                                                     attribute2,
                                                     attribute3,
                                                     attribute4,
                                                     attribute5,
                                                     attribute6,
                                                     attribute7,
                                                     attribute8,
                                                     attribute9,
                                                     attribute10,
                                                     task_number,
                                                     expenditure_item_date) -- Change4
                 VALUES (
                           ln_batch_id,                         --ope_batch_id
                           ln_detail_id,                       --ope_detail_id
                           TO_DATE (
                              TO_CHAR (
                                 TO_DATE (
                                    ope_inst_detail_rec.accounting_date,
                                    'MMDDYY'),
                                 'MMDDYYYY'),
                              'MM/DD/YYYY'),
                           ln_batch_number,
                           TRIM (ope_inst_detail_rec.tcode),
                           TRIM (ope_inst_detail_rec.reverse_code),
                           TRIM (ope_inst_detail_rec.modifier_code),
                           TRIM (ope_inst_detail_rec.document_ref),
                           TRIM (ope_inst_detail_rec.document_number),
                           TRIM (ope_inst_detail_rec.oth_document_ref),
                           TRIM (ope_inst_detail_rec.oth_document_number),
                           TRIM (ope_inst_detail_rec.geo_code),
                           TO_NUMBER (ope_inst_detail_rec.fy),
                           TRIM (ope_inst_detail_rec.can),
                           TRIM (ope_inst_detail_rec.object_class),
                           TO_NUMBER (ope_inst_detail_rec.amount),
                           TRIM (ope_inst_detail_rec.primary_ein),
                           TRIM (ope_inst_detail_rec.secondary_ein),
                           TRIM (ope_inst_detail_rec.schedule_number),
                           TRIM (ope_inst_detail_rec.gov_nongov),
                           TRIM (ope_inst_detail_rec.CATEGORY),
                           TRIM (ope_inst_detail_rec.case_11_code),
                           TRIM (ope_inst_detail_rec.balance_of_payment),
                           TRIM (ope_inst_detail_rec.gl_code),
                           TRIM (ope_inst_detail_rec.type_of_service),
                           TRIM (ope_inst_detail_rec.reserved),
                           TRIM (ope_inst_detail_rec.grant_number),
                           TO_DATE (
                              TO_CHAR (
                                 TO_DATE (
                                    ope_inst_detail_rec.grant_begin_date,
                                    'MMDDYY'),
                                 'MMDDYYYY'),
                              'MM/DD/YYYY'),
                           TO_DATE (
                              TO_CHAR (
                                 TO_DATE (ope_inst_detail_rec.grant_end_date,
                                          'MMDDYY'),
                                 'MMDDYYYY'),
                              'MM/DD/YYYY'),
                           TO_DATE (
                              TO_CHAR (
                                 TO_DATE (ope_inst_detail_rec.parm_date,
                                          'MMDDYY'),
                                 'MMDDYYYY'),
                              'MM/DD/YYYY'),
                           TRIM (ope_inst_detail_rec.useriii),
                           TRIM (ope_inst_detail_rec.clerkkid),
                           TRIM (ope_inst_detail_rec.adbis),
                           TRIM (ope_inst_detail_rec.source_code),
                           ope_inst_detail_rec.file_name,
                           ope_inst_detail_rec.record_id,
                           ope_inst_detail_rec.org_id,
                           ope_inst_detail_rec.set_of_books_id,
                           'E',
                           'ERROR',
                           p_error_msg,
                           v_request_id,
                           v_user_id,                            -- CREATED_BY
                           SYSDATE,                           -- CREATION_DATE
                           v_user_id,                       -- LAST_UPDATED_BY
                           SYSDATE,                        -- LAST_UPDATE_DATE
                           v_login,
                           TRIM (ope_inst_detail_rec.name),
                           TRIM (ope_inst_detail_rec.attribute1),
                           TRIM (ope_inst_detail_rec.attribute2),
                           TRIM (ope_inst_detail_rec.attribute3),
                           TRIM (ope_inst_detail_rec.attribute4),
                           TRIM (ope_inst_detail_rec.attribute5),
                           TRIM (ope_inst_detail_rec.attribute6),
                           TRIM (ope_inst_detail_rec.attribute7),
                           TRIM (ope_inst_detail_rec.attribute8),
                           TRIM (ope_inst_detail_rec.attribute9),
                           TRIM (ope_inst_detail_rec.attribute10),
                           TRIM (ope_inst_detail_rec.task_number),
                           TO_DATE (
                              TO_CHAR (
                                 TO_DATE (
                                    ope_inst_detail_rec.expenditure_item_date,
                                    'MMDDYY'),
                                 'MMDDYYYY'),
                              'MM/DD/YYYY') --TRIM (ope_inst_detail_rec.expenditure_item_date
                                           );                       -- Change5
         EXCEPTION
            WHEN OTHERS THEN
               lc_errbuf := NULL;
               lc_errbuf :=
                     ' Error in detail (inst_arch_file_vald_error) and  Error is :'
                  || SUBSTR (SQLERRM, 1, 250);
               logf (lc_errbuf,l_Module);
         END;
      END LOOP;

      -- END LOOP;
       COMMIT;
   EXCEPTION
      WHEN OTHERS
      THEN
         lc_errbuf := NULL;
         lc_errbuf :=
               ' Error in inst_arch_file_vald_error and  Error is :'
            || SUBSTR (SQLERRM, 1, 250);
         logf (lc_errbuf,l_module);
         g_retcode := 2;
         g_errbuf :=
               'Error in inst_arch_file_vald_error and  Error is: '
            || SUBSTR (SQLERRM, 1, 250);
   END inst_arch_file_vald_error;

/*---------------------------------------------------------------------------------------------------
-- The function nih_billing_file_notify is called from Unix host program for sending email
-- to the feder system for error records.
--
-- --------------------------------------------------------------------------------------------------*/
   FUNCTION nih_billing_file_notify (p_batch_number IN VARCHAR2
                                   , p_call_type IN VARCHAR2
                                    )
   RETURN VARCHAR2
   IS
      l_email_list   VARCHAR2 (240);
      l_module varchar2(100) := 'nih_billing_file_notify ';


     CURSOR email_cur
      IS
    select 1
       from
           nihfnd_sod_person_data_v
        where 1=0;

   BEGIN
      FOR c1rec IN email_cur
      LOOP
         --l_email_list := l_email_list ||e_email_id || ', ';
              null;


      END LOOP;

      IF l_email_list IS NOT NULL
      THEN
         l_email_list := LTRIM (RTRIM (l_email_list));
         l_email_list := SUBSTR (l_email_list, 1, LENGTH (l_email_list) - 1);
      END IF;

      RETURN (l_email_list);
   EXCEPTION
      WHEN OTHERS
      THEN
         l_email_list := NULL;
         logf (' Email Address Not Found Error :-> ' || SQLERRM,l_module);
         RETURN (l_email_list);
   END nih_billing_file_notify;
-----------------------------------------------------------------------------------------------------------
-- The function tcode_exists used for tcode.
-- Validate tcode exists in lookup_type 'NIHGL_OPE_TCODES' and return boolean TRUE
-- for enabled given lookup code otherwise return boolean FALSE.
-- -------------------------------------------------------------------------------------------------
   FUNCTION tcode_exists (p_tcode IN VARCHAR2, p_yesno OUT VARCHAR2)
      RETURN BOOLEAN
   IS
      l_yesno   VARCHAR2 (1) := NULL;
   BEGIN
      SELECT attribute1
        INTO l_yesno
        FROM fnd_lookup_values_vl
       WHERE     lookup_type = 'NIHGL_OPE_TCODES'
             AND lookup_code = p_tcode
             AND (    NVL (enabled_flag, 'Y') = 'Y'
                  AND (    NVL (start_date_active, SYSDATE) <= SYSDATE
                       AND (   end_date_active >= SYSDATE
                            OR end_date_active IS NULL)));

      p_yesno := l_yesno;
      RETURN TRUE;
   EXCEPTION
      WHEN OTHERS THEN
         l_yesno := 'N';
         p_yesno := l_yesno;
         RETURN FALSE;
   END tcode_exists;
-- ------------------------------------------------------------------------------------------------------------


/* ---------------------------------------------------------------------------------------------------------

 Function Balance_check_required Check if the balance check is required or NOT

--------------------------------------------------------------------------------------------------------- */

   FUNCTION Balance_check_required (p_tcode VARCHAR2)
      RETURN BOOLEAN
   IS
      l_yes_no   VARCHAR (150);
   BEGIN
      SELECT attribute1
        INTO l_yes_no
        FROM fnd_lookup_values_vl
       WHERE     lookup_type = 'NIHGL_OPE_TCODES'
             AND lookup_code = p_tcode
             AND (    NVL (enabled_flag, 'Y') = 'Y'
                  AND (    NVL (start_date_active, SYSDATE) <= SYSDATE
                       AND (   end_date_active >= SYSDATE
                            OR end_date_active IS NULL)));

      IF l_yes_no = 'Y'
      THEN
         RETURN TRUE;
      ELSE
         RETURN FALSE;
      END IF;
   EXCEPTION
      WHEN OTHERS
      THEN
         RETURN FALSE;
   END balance_check_required;
/* ----------------------------------------------------------------------------------------
        Function chk_amnt_bal_exist

       check if there exists enough balanceto reverse
        If exists then allow reverse otherwise fail the transaction.

---------------------------------------------------------------------------------------------  */

   FUNCTION chk_amnt_bal_exist (p_doc_num          VARCHAR2,
                                p_fiscal_year      NUMBER,
                                p_batch_number     VARCHAR2,
                                p_tcode            VARCHAR2,
                                p_can              VARCHAR2,	--CR NBSCH0003180
                                p_object_class     VARCHAR2,	--CR NBSCH0003180
                                p_ope_detail_id    NUMBER)
      RETURN NUMBER
   IS
      l_amount         NUMBER := 0;
      l_bal_amount     NUMBER := 0;
      l_tot_amount     NUMBER := 0;
      l_batch_number   VARCHAR2 (2);
      l_module varchar2(100) := 'chk_amnt_bal_exist';
   BEGIN

       logf('begin' , l_module);
      IF p_batch_number = 'Q2'
      THEN
         l_batch_number := 'Q1';
      ELSE
         l_batch_number := p_batch_number;
      END IF;


      SELECT NVL (SUM (DECODE (reverse_code, 2, -1 * amount, amount)), 0)
        INTO l_amount
        FROM nihgl_ope_acct_dtls_all_v
       WHERE     record_status IN ('N', 'V', 'P')
             AND TRIM (document_ref || document_number) = p_doc_num
             AND batch_number = l_batch_number --31966 Animesh Make Sure Q2 is same as Q1
             AND tcode IN
                    (SELECT attribute5
                       FROM fnd_lookup_values_vl
                      WHERE     lookup_type = 'NIHGL_OPE_TCODES'
                            AND lookup_code = p_tcode
                            AND (    NVL (enabled_flag, 'Y') = 'Y'
                                 AND (    NVL (start_date_active, SYSDATE) <=
                                             SYSDATE
                                      AND (   end_date_active >= SYSDATE
                                           OR end_date_active IS NULL))))
             AND can_fy = p_fiscal_year
             and can = p_can						--CR NBSCH0003180
             and object_class = p_object_class		--CR NBSCH0003180
             AND ope_detail_id <> p_ope_detail_id;

      SELECT NVL (SUM (DECODE (reverse_code, 2, -1 * amount, amount)), 0)
        INTO l_bal_amount
        FROM nihgl_ope_acct_dtls_all_v
       WHERE     record_status IN ('N', 'V', 'P')
             AND TRIM (document_ref || document_number) = p_doc_num
             AND batch_number = p_batch_number
             AND tcode = p_tcode
             AND can_fy = p_fiscal_year
             and can = p_can						--CR NBSCH0003180
             and object_class = p_object_class;		--CR NBSCH0003180

      l_tot_amount := l_amount - l_bal_amount;

      logf ('l_amount : ' || l_amount , l_module);
      logf ('l_bal_amount : ' || l_bal_amount, l_Module);
      logf ('l_tot_amount : ' || l_tot_amount,l_module);

      RETURN (l_tot_amount);
   EXCEPTION
      WHEN OTHERS
      THEN
         logf (
            ' Error in  Function chk_rev_amnt_bal_exist ' || ': ' || SQLERRM,l_module);
         RETURN 0;
   END chk_amnt_bal_exist;
-- ----------------------------------------------------------------------------------------------
--CR NBSCH0003180		Start
/* ----------------------------------------------------------------------------------------
        Function chk_amnt_bal_rev_exist

---------------------------------------------------------------------------------------------  */
/* OLD NBSCH0003234
   FUNCTION chk_amnt_bal_rev_exist (p_doc_num          VARCHAR2,
                                p_fiscal_year      NUMBER,
                                p_batch_number     VARCHAR2,
                                p_tcode            VARCHAR2,
                                p_can              VARCHAR2,
                                p_object_class     VARCHAR2,
                                p_amount           NUMBER,
                                p_ope_detail_id    NUMBER)

      RETURN NUMBER
   IS
      l_amount         NUMBER := 0;
      l_bal_amount     NUMBER := 0;
      l_tot_amount     NUMBER := 0;
      l_batch_number_050    VARCHAR2 (2);
      l_batch_number_181191    VARCHAR2 (2);
      l_module varchar2(100) := 'chk_amnt_bal_rev_exist';
   BEGIN

       logf('begin' , l_module);
      IF p_batch_number in ('Q1','Q2')
      THEN
         --050 (Obligations) and 310 (Budgets)
         l_batch_number_050 := 'Q1';
         --181 (Disbursements) and 221 (Revenue)
         l_batch_number_181191 := 'Q2';
      ELSE
         l_batch_number_050 := p_batch_number;
         l_batch_number_181191 := p_batch_number;
      END IF;
      logf ('p_batch_number : ' || p_batch_number , l_module);
      logf ('l_batch_number_050 : ' || l_batch_number_050 , l_module);
      logf ('l_batch_number_181_191 : ' || l_batch_number_181191 , l_module);


	if p_batch_number = 'Q2' then
      SELECT NVL (SUM (DECODE (reverse_code, 2, -1 * amount, amount)), 0)
        INTO l_amount
        FROM nihgl_ope_acct_dtls_all_v
       WHERE     record_status IN ('N', 'V', 'P')
             AND TRIM (document_ref || document_number) = p_doc_num
             AND batch_number = l_batch_number_181191
             AND tcode = p_tcode
             AND can_fy = p_fiscal_year
             and can = p_can
             and object_class = p_object_class
             AND ope_detail_id <> p_ope_detail_id;
	elsif p_batch_number = 'Q1' then
      SELECT NVL (SUM (DECODE (reverse_code, 2, -1 * amount, amount)), 0)
        INTO l_amount
        FROM nihgl_ope_acct_dtls_all_v
       WHERE     record_status IN ('N', 'V', 'P')
             AND TRIM (document_ref || document_number) = p_doc_num
             AND batch_number = l_batch_number_050 --31966 Animesh Make Sure Q2 is same as Q1
             AND tcode = '050'
             AND can_fy = p_fiscal_year
             and can = p_can
             and object_class = p_object_class
             AND ope_detail_id <> p_ope_detail_id;

      SELECT NVL (SUM (DECODE (reverse_code, 2, -1 * amount, amount)), 0)
        INTO l_bal_amount
        FROM nihgl_ope_acct_dtls_all_v
       WHERE     record_status IN ('N', 'V', 'P')
             AND TRIM (document_ref || document_number) = p_doc_num
             AND batch_number = l_batch_number_181191
             AND tcode in ('181', '191')
             AND can_fy = p_fiscal_year
             and can = p_can
             and object_class <> p_object_class;
    end if;

      l_tot_amount := l_amount - l_bal_amount - p_amount;

      logf ('l_amount : ' || l_amount , l_module);
      logf ('l_bal_amount : ' || l_bal_amount, l_Module);
      logf ('p_amount : ' || p_amount , l_module);
      logf ('l_tot_amount : ' || l_tot_amount,l_module);

      RETURN (l_tot_amount);
   EXCEPTION
      WHEN OTHERS
      THEN
         logf (
            ' Error in  Function chk_amnt_bal_rev_exist ' || ': ' || SQLERRM,l_module);
         RETURN 0;
   END chk_amnt_bal_rev_exist;

OLD NBSCH0003234 */

-- NBSCH0003234 Begin
    FUNCTION chk_amnt_bal_rev_exist (
        p_doc_num       VARCHAR2,
        p_fiscal_year   NUMBER,
        p_batch_number  VARCHAR2,
        p_tcode         VARCHAR2,
        p_can           VARCHAR2,
        p_object_class  VARCHAR2,
        p_amount        NUMBER,
        p_ope_detail_id NUMBER
    ) RETURN NUMBER
    IS
        l_amount         NUMBER := 0;
        l_bal_amount     NUMBER := 0;
        l_tot_amount     NUMBER := 0;

        l_batch_main     VARCHAR2(100);  -- batch corresponding to p_tcode
        l_batch_pair     VARCHAR2(100);  -- opposite batch (paired tcode side)

        l_module         VARCHAR2(100) := 'chk_amnt_bal_rev_exist';
    BEGIN
        logf('begin chk_amnt_bal_rev_exist', l_module);
        logf('p_doc_num       = ' || p_doc_num,       l_module);
        logf('p_fiscal_year   = ' || p_fiscal_year,   l_module);
        logf('p_batch_number  = ' || p_batch_number,  l_module);
        logf('p_tcode         = ' || p_tcode,         l_module);
        logf('p_can           = ' || p_can,           l_module);
        logf('p_object_class  = ' || p_object_class,  l_module);
        logf('p_amount        = ' || p_amount,        l_module);
        logf('p_ope_detail_id = ' || p_ope_detail_id, l_module);

        --------------------------------------------------------------------
        -- Determine correct batch (Q1/Q2) based on tcode mapping:
        -- 050,310 - Q1 (Obligation / Budget)
        -- 181,191,221 - Q2 (Disbursement / Revenue)
        --
        -- All other tcodes fallback to p_batch_number for backward compatibility.
        --------------------------------------------------------------------
        IF p_batch_number IN ('Q1', 'Q2') and p_tcode IN ('050', '310') THEN
            l_batch_main := 'Q1';
            l_batch_pair := 'Q2';
        ELSIF p_batch_number IN ('Q1', 'Q2') and p_tcode IN ('181', '191', '221') THEN
            l_batch_main := 'Q2';
            l_batch_pair := 'Q1';
        ELSE
            l_batch_main := p_batch_number;
            l_batch_pair := p_batch_number;
        END IF;

        logf('Batch main : ' || l_batch_main, l_module);
        logf('Batch pair : ' || l_batch_pair, l_module);

        --------------------------------------------------------------------
        -- 1) Main amount calculation for the current tcode (p_tcode)
        --------------------------------------------------------------------
        SELECT NVL(SUM(DECODE(reverse_code, 2, -1 * amount, amount)), 0)
          INTO l_amount
          FROM nihgl_ope_acct_dtls_all_v
         WHERE record_status IN ('N', 'V', 'P')
           AND TRIM(document_ref || document_number) = p_doc_num
           AND can_fy        = p_fiscal_year
           AND can           = p_can
           AND object_class  = p_object_class
           AND ope_detail_id <> p_ope_detail_id
           AND batch_number  = l_batch_main
           AND tcode         = p_tcode;

        --------------------------------------------------------------------
        -- 2) Paired-side amount (balancing side) - only for Q1 tcodes.
        --
        -- Logic:
        -- 050 (Q1) - paired with 181,191 (Q2), using object_class <> p_object_class
        -- 310 (Q1) - paired with 221 (Q2), using object_class = p_object_class
        --
        -- For Q2 tcodes (181,191,221) balancing amount is not calculated
        -- (same as old implementation).
        --------------------------------------------------------------------
        IF p_tcode = '050' THEN
            -- 050 (Obligations, Q1) - paired with 181/191 (Disbursements, Q2)
            SELECT NVL(SUM(DECODE(reverse_code, 2, -1 * amount, amount)), 0)
              INTO l_bal_amount
              FROM nihgl_ope_acct_dtls_all_v
             WHERE record_status IN ('N', 'V', 'P')
               AND TRIM(document_ref || document_number) = p_doc_num
               AND can_fy        = p_fiscal_year
               AND can           = p_can
               AND object_class <> p_object_class
               AND ope_detail_id <> p_ope_detail_id
               AND batch_number  = l_batch_pair
               AND tcode IN ('181', '191');

        ELSIF p_tcode = '310' THEN
            -- 310 (Budget, Q1) - paired with 221 (Revenue, Q2)
            SELECT NVL(SUM(DECODE(reverse_code, 2, -1 * amount, amount)), 0)
              INTO l_bal_amount
              FROM nihgl_ope_acct_dtls_all_v
             WHERE record_status IN ('N', 'V', 'P')
               AND TRIM(document_ref || document_number) = p_doc_num
               AND can_fy        = p_fiscal_year
               AND can           = p_can
               AND object_class  = p_object_class
               AND ope_detail_id <> p_ope_detail_id
               AND batch_number  = l_batch_pair
               AND tcode         = '221';

        ELSE
            -- For Q2 tcodes (181,191,221): no balancing query (old logic)
            l_bal_amount := 0;
        END IF;

        --------------------------------------------------------------------
        -- 3) Final result
        --------------------------------------------------------------------
        l_tot_amount := l_amount - l_bal_amount - p_amount;

        logf('l_amount : ' || l_amount, l_module);
        logf('l_bal_amount : ' || l_bal_amount, l_module);
        logf('p_amount : ' || p_amount, l_module);
        logf('l_tot_amount : ' || l_tot_amount, l_module);

        RETURN l_tot_amount;

    EXCEPTION
        WHEN OTHERS THEN
            logf('Error in chk_amnt_bal_rev_exist : ' || SQLERRM, l_module);
            RETURN 0;
    END chk_amnt_bal_rev_exist;
-- NBSCH0003234 End

-- ----------------------------------------------------------------------------------------------
-- CR NBSCH0003180		 end
-- Function chk_rev_txn_exist

-- NBSCH0003319 Begin
    FUNCTION chk_sum_simple (
        p_doc_num       VARCHAR2,
        p_fiscal_year   NUMBER,
        p_batch_number  VARCHAR2,
        p_tcode         VARCHAR2,
        p_can           VARCHAR2,
        p_object_class  VARCHAR2,
        p_amount        NUMBER,
        p_ope_detail_id NUMBER
    ) RETURN NUMBER
    IS
        l_bal_amount NUMBER := 0;
        l_module     VARCHAR2(100) := 'chk_cd_susp_sum_simple';
    BEGIN
        logf('begin '||l_module, l_module);
        logf('p_doc_num       = ' || p_doc_num,       l_module);
        logf('p_fiscal_year   = ' || p_fiscal_year,   l_module);
        logf('p_batch_number  = ' || p_batch_number,  l_module);
        logf('p_tcode         = ' || p_tcode,         l_module);
        logf('p_can           = ' || p_can,           l_module);
        logf('p_object_class  = ' || p_object_class,  l_module);
        logf('p_amount        = ' || p_amount,        l_module);
        logf('p_ope_detail_id = ' || p_ope_detail_id, l_module);
        
        IF NVL(p_amount, 0) <= 0 THEN
            logf(l_module || ': p_amount <= 0 => error', l_module);
            RETURN -1;
        END IF;

        SELECT NVL(SUM(DECODE(TRIM(reverse_code), '2', -1 * NVL(amount,0), NVL(amount,0))), 0)
          INTO l_bal_amount
          FROM nihgl_ope_acct_dtls_all_v
         WHERE record_status IN ('N', 'V', 'P')
           AND TRIM(document_ref || document_number) = p_doc_num
           AND can_fy       = p_fiscal_year
           AND can          = p_can
           AND object_class = p_object_class
           AND batch_number = p_batch_number
           AND tcode        = p_tcode;

        logf(l_module || ': l_bal_amount = ' || l_bal_amount, l_module);

        RETURN l_bal_amount;

    EXCEPTION
        WHEN OTHERS THEN
            logf(l_module || ' ERROR: ' || SQLERRM, l_module);
            RETURN -1;
    END chk_sum_simple;
-- NBSCH0003319 End

------------------------------------------------------------------------------------------------
   FUNCTION chk_rev_txn_exist (p_doc_num          VARCHAR2,
                               p_fiscal_year      NUMBER,
                               p_batch_number     VARCHAR2,
                               p_tcode            VARCHAR2,
                               p_can              VARCHAR2,		--CR NBSCH0003180
                               p_object_class     VARCHAR2,		--CR NBSCH0003180
                               p_amount           NUMBER,
                               p_ope_detail_id    NUMBER)
      RETURN NUMBER
   IS
      l_to_rev_amnt_bal   NUMBER := 0;
      l_rev_amnt_bal      NUMBER := 0;
      l_amount            NUMBER := 0;
      l_batch_number      VARCHAR2 (2);
      l_module  varchar2(100) := 'chk_rev_txn_exist';

   BEGIN
/*
      IF p_batch_number = 'Q2'
      THEN
         l_batch_number := 'Q1';
      ELSE
         l_batch_number := p_batch_number;
      END IF;
*/
      SELECT NVL (SUM (DECODE (reverse_code, 2, -1 * amount, amount)), 0)
        INTO l_amount
        FROM nihgl_ope_acct_dtls_all_v
       WHERE     record_status IN ('N', 'V', 'P')
             AND TRIM (document_ref || document_number) = p_doc_num
             AND batch_number = p_batch_number
             AND tcode = p_tcode
             AND can_fy = p_fiscal_year
             and can = p_can					--CR NBSCH0003180
             and object_class = p_object_class	--CR NBSCH0003180
             AND ope_detail_id <> p_ope_detail_id;

      l_rev_amnt_bal := l_amount - p_amount;
      logf ('p_amount : ' || p_amount,l_module);
      logf ('l_amount : ' || l_amount,l_module);
      RETURN l_rev_amnt_bal;
   EXCEPTION
      WHEN OTHERS
      THEN
         logf (' Error in  Function chk_rev_txn_exist ' || ': ' || SQLERRM,l_module);
         RETURN 0;
   END chk_rev_txn_exist;



-- ---------------------------------------------------------------------------------------------
-- Procedure update_cc_segments
   --Start of change 2 by Yash
----------------------------------------------------------------------------------------------
   PROCEDURE update_cc_segments (p_segment1    IN     VARCHAR2,
                                 p_segment2    IN OUT VARCHAR2,
                                 p_segment3    IN OUT VARCHAR2,
                                 p_segment4    IN OUT VARCHAR2,
                                 p_segment5    IN OUT VARCHAR2,
                                 p_segment6    IN OUT VARCHAR2,
                                 p_segment7    IN OUT VARCHAR2,
                                 p_segment8    IN OUT VARCHAR2,
                                 p_segment9    IN OUT VARCHAR2,
                                 p_segment10   IN OUT VARCHAR2,
                                 p_segment11   IN     VARCHAR2,
                                 p_segment12   IN OUT VARCHAR2,
                                 p_segment13   IN OUT VARCHAR2,
                                 p_segment14   IN OUT VARCHAR2)
   IS
    l_module varchar2(100) := 'update_cc_segments';
   BEGIN
      SELECT DECODE (fbdd.segment2_type, 'N', segment2, p_segment2),
             DECODE (fbdd.segment3_type, 'N', segment3, p_segment3),
             DECODE (fbdd.segment4_type, 'N', segment4, p_segment4),
             DECODE (fbdd.segment5_type, 'N', segment5, p_segment5),
             DECODE (fbdd.segment6_type, 'N', segment6, p_segment6),
             DECODE (fbdd.segment7_type, 'N', segment7, p_segment7),
             DECODE (fbdd.segment8_type, 'N', segment8, p_segment8),
             DECODE (fbdd.segment9_type, 'N', segment9, p_segment9),
             DECODE (fbdd.segment10_type, 'N', segment10, p_segment10),
             DECODE (fbdd.segment12_type, 'N', segment12, p_segment12),
             DECODE (fbdd.segment13_type, 'N', segment13, p_segment13),
             DECODE (fbdd.segment14_type, 'N', segment14, p_segment14)
        INTO p_segment2,
             p_segment3,
             p_segment4,
             p_segment5,
             p_segment6,
             p_segment7,
             p_segment8,
             p_segment9,
             p_segment10,
             p_segment12,
             p_segment13,
             p_segment14
        FROM fv_budget_distribution_hdr fbdh,
             fv_budget_distribution_dtl fbdd,
             fv_budget_levels fbl,
             fnd_lookup_values flv
       WHERE     fbdh.distribution_id = fbdd.distribution_id
             AND fbdd.budget_level_id = fbl.budget_level_id
             AND flv.lookup_type = 'NIHGL_OPE_SGL_BUD_DIST_LVL_MAP'
             AND flv.meaning = p_segment11
             AND flv.description = fbl.description
             AND fbdh.fund_value = p_segment1
             AND flv.enabled_flag = 'Y'
             AND SYSDATE BETWEEN flv.start_date_active
                             AND NVL (flv.end_date_active, '31-DEC-4712');
   EXCEPTION
      WHEN NO_DATA_FOUND
      THEN
         logf (
               'Checking UPDATE_CC_SEGMENTS for FUND: '
            || p_segment1
            || ' and SGL: '
            || p_segment11
            || ' - Using default segment values' , l_Module);
      WHEN TOO_MANY_ROWS
      THEN
         logf (
               'Too many rows found in UPDATE_CC_SEGMENTS for FUND: '
            || p_segment1
            || ' and SGL: '
            || p_segment11,L_MODULE);
      WHEN OTHERS
      THEN
         logf (
               'Fatal Error Occured in - UPDATE_CC_SEGMENTS : '
            || SUBSTR (SQLERRM, 1, 250),l_module);
         g_retcode := 2;
         g_errbuf :=
               'Fatal Error Occured in - UPDATE_CC_SEGMENTS: '
            || SUBSTR (SQLERRM, 1, 250);
   END update_cc_segments;
   --End of change 2
------------------------------------------------------------------------------------------------------------
-- Procedure process_SLA
-- ---------------------------------------------------------------------------------------------------------
   PROCEDURE process_SLA (p_batch_number    VARCHAR2,
                         p_file_name       VARCHAR2,
                         p_type            VARCHAR2)
   IS

         -------  Select headers with GL process flag as 'N'

      CURSOR c_btch_cur
      IS
           SELECT UNIQUE ope_batch_id,
                         batch_number,
                         file_name,
                         set_of_books_id,
                         event_type_code
             FROM nihgl_ope_acct_dtls_int_tbl
            WHERE     record_status = 'V'
                  AND batch_number = p_batch_number
                  AND file_name = p_file_name
         ORDER BY 1;

      ---  Select details for validation

      CURSOR c_dtls_cur (
         p_ope_batch_id    NUMBER,
         p_batch_number    VARCHAR2,
         p_file_name       VARCHAR2,
         p_sob_id          NUMBER)
      IS
           SELECT *
             FROM nihgl_ope_acct_dtls_int_tbl
            WHERE     ope_batch_id = p_ope_batch_id
                  AND batch_number = p_batch_number
                  AND file_name = p_file_name
                  AND set_of_books_id = p_sob_id
                  AND record_status = 'V'
         ORDER BY 2;

         --- Select details for validation

      CURSOR c_dtls_doc_cur (
         p_ope_batch_id    NUMBER,
         p_batch_number    VARCHAR2,
         p_file_name       VARCHAR2,
         p_sob_id          NUMBER)
      IS
           SELECT UNIQUE
                  TRIM (document_ref || document_number) document_number,
                  document_ref doc_ref,
                  document_number doc_number,
                  TO_DATE (attribute5, 'DD-MON-RRRR') accounting_date,
                  batch_number,
                  event_id
             FROM nihgl_ope_acct_dtls_int_tbl
            WHERE     ope_batch_id = p_ope_batch_id
                  AND batch_number = p_batch_number
                  AND file_name = p_file_name
                  AND set_of_books_id = p_sob_id
                  AND record_status = 'V'
         ORDER BY 1;

          ---  Funds Check Cursor

      CURSOR cur_bc_packet (
         p_batch_number    VARCHAR2,
         p_file_name       VARCHAR2)
      IS
         SELECT DISTINCT dtl.ope_detail_id,
                         gp.result_code result_code,
                         gl.description,
                         dtl.document_ref,
                         dtl.document_number,
                         dtl.can_fy,
                         gp.packet_id
           FROM gl_lookups gl,
                gl_bc_packets gp
                , nihgl_ope_acct_dtls_int_tbl dtl
          WHERE     gp.result_code = gl.lookup_code
                AND gl.lookup_type = 'FUNDS_CHECK_RESULT_CODE'
                AND gp.packet_id = dtl.packet_id
                AND gp.event_id = dtl.event_id
                AND gp.source_distribution_id_num_1 = dtl.line_number
                AND gp.result_code LIKE 'F%'
                AND gp.result_code <> 'F06'
                AND gp.template_id IS NULL
                AND gp.actual_flag <> 'E'
                AND gp.entered_dr <> 0
                AND dtl.batch_number = p_batch_number
                AND dtl.file_name = p_file_name;

      x_period_name                 VARCHAR2 (15);
      v_packet_id                   NUMBER;
      v_funds_check_code            VARCHAR2 (20);
      v_funds_check_message         VARCHAR2 (2000);
      v_funds_check_error_code      VARCHAR2 (30);
      v_funds_check_error_message   VARCHAR2 (2000);
      v_period_year                 NUMBER;
      v_period_num                  NUMBER;
      v_quarter_num                 NUMBER;
      v_je_source                   VARCHAR2 (30);
      v_je_category                 gl_je_categories.je_category_name%TYPE;
      v_user_je_category_name       gl_je_categories.user_je_category_name%TYPE;
      l_error_message               VARCHAR2 (2000);
      v_amount                      NUMBER;
      l_inst_bcp_flag               BOOLEAN := TRUE;
      l_fund_chk_flag               BOOLEAN := FALSE;
      l_cnt                         NUMBER;
      --FAH
      v_legal_entity_id             NUMBER;
      v_ledger_id                   NUMBER;
      v_event_id                    NUMBER;
      v_event_date                  DATE;
      v_array_event_info            xla_events_pub_pkg.t_array_event_info;
      v_event_source_info           xla_events_pub_pkg.t_event_source_info;
      v_event_type_code             xla_event_types_tl.event_type_code%TYPE;
      v_budgetary_flag              VARCHAR2 (1) := 'N';
      v_appl_id                     NUMBER;
      v_error_code                  VARCHAR2 (50);
      v_bc_mode                     VARCHAR2 (1) := 'R';
      v_override_flag               VARCHAR2 (1) := 'N';


       l_module varchar2(100) := 'process_SLA';
   BEGIN
     -- g_request_id := fnd_global.conc_request_id;
      g_user_id := fnd_global.user_id;
      g_login_id := fnd_global.login_id;
      v_bc_mode := p_type;

      logf ('begin ' , l_module);

      update_err_doc_number (p_batch_number, p_file_name);          -- TESTING

      FOR c_btch_rec IN c_btch_cur
      LOOP

               /*
        || Getting je_source_name, je_category_name based on
        || g_ope_je_source, g_ope_je_category_name
        */
         BEGIN
            SELECT je_source_name
              INTO v_je_source
              FROM gl_je_sources
             WHERE user_je_source_name = g_ope_je_source;
         EXCEPTION
            WHEN OTHERS
            THEN
               logf (' je_source_name is not setup',l_Module);
         END;

         v_user_je_category_name := get_batch_category (p_batch_number);

         logf ('g_JOURNAL_CATEGORY_01 ' || g_JOURNAL_CATEGORY , l_Module);

         BEGIN
            SELECT je_category_name
              INTO v_je_category
              FROM gl_je_categories
             WHERE user_je_category_name = v_user_je_category_name;--CR NBSCH0002499
         EXCEPTION
            WHEN OTHERS
            THEN
               logf (' je_category_name is not setup',l_Module);
         END;

         FOR c_dtl_doc_rec
            IN c_dtls_doc_cur (p_ope_batch_id   => c_btch_rec.ope_batch_id,
                               p_batch_number   => c_btch_rec.batch_number,
                               p_file_name      => c_btch_rec.file_name,
                               p_sob_id         => c_btch_rec.set_of_books_id)
         LOOP

         logf (' c_btch_rec.ope_batch_id :'||c_btch_rec.ope_batch_id||' c_btch_rec.batch_number: '||c_btch_rec.batch_number
              ||' c_btch_rec.file_name:'||c_btch_rec.file_name,l_module);

            logf ('calling   nihgl_fah_utilities_pkg.load_bc_event' , l_module);
             nihgl_fah_utilities_pkg.load_bc_event (
               p_event_id         => c_dtl_doc_rec.event_id,
               p_je_source_name   => g_ope_je_source,
               p_code             => v_error_code,
               p_message          => l_error_message);

             --nihgl_fah_utilities_pkg.delete_bc_event(c_dtl_doc_rec.event_id);


            logf ('BC Event created  '   || c_dtl_doc_rec.event_id , l_module);
            logf ('BC event creation  v_error_code: '  || v_error_code  || '  l_error message ' ||  l_error_message , l_module);

         IF v_error_code = '0'
         THEN

            logf ('Calling  nihgl_fah_utilities_pkg.process_event' , l_module);


        logf ('g_ope_je_source: ' || g_ope_je_source , l_Module);
        logf ('v_user_je_category_name: ' || v_user_je_category_name , l_Module);
        logf ('c_btch_rec.event_type_code: ' || c_btch_rec.event_type_code , l_Module);
        logf ('v_bc_mode: ' || v_bc_mode , l_Module);
        logf ('v_override_flag: ' || v_override_flag , l_Module);
        logf ('g_user_id: ' || g_user_id , l_Module);
        logf ('g_resp_id: ' || g_resp_id , l_Module);
        logf ('g_ope_je_source: ' || g_ope_je_source , l_Module);

       nihgl_fah_utilities_pkg.process_event (
               p_je_source_name     => g_ope_je_source,
               p_je_category_name   => v_user_je_category_name,
               P_event_type_code    => c_btch_rec.event_type_code,
               p_bc_mode            => v_bc_mode,
               p_override_flag      => v_override_flag,
               p_user_id            => g_user_id,
               p_resp_id            => g_resp_id,
               p_code               => v_error_code,
               p_message            => l_error_message,
               p_packet_id          => v_packet_id);

            logf ('Process Event v_error_code: ' || v_error_code , l_Module);
            logf ('Process Event error msg: ' || l_error_message , l_Module);
            logf ('Process Event packet id: ' || v_packet_id,l_module);

            FOR i IN (SELECT * FROM psa_bc_xla_events_gt)
            LOOP
               l_error_message := NULL;

            logf ('Process Event result_code: ' || i.result_code , l_module);
            logf ('Process Event   event_id : ' || i.event_id , l_module);

               IF i.result_code = 'XLA_ERROR'
               THEN
			   --CR NBSCH0003180
                l_error_message := 'Obligation line does not exist for the combination of Document Ref/Number, Fiscal Year, Batch Number, Object Class Code and T-code';
               /*
                  l_error_message :=
                        'Internal error - Unable to derive the accounting from FAH'
                     || '- Status Code: '
                     || i.result_code
                     || ', Please check the FAH Setup';*/

               ELSIF i.result_code <> 'SUCCESS'
               THEN
                  l_error_message := 'OPE funds check error';
               END IF;

         logf ('Updating nihgl_ope_acct_dtls_int_tbl with error codoe and record status'   , l_module);
               UPDATE nihgl_ope_acct_dtls_int_tbl
                  SET packet_id = v_packet_id,
                      error_message =
                         DECODE (error_message,
                                 NULL, l_error_message,
                                 error_message || '; ' || l_error_message),
                      ERROR_CODE =
                         DECODE (l_error_message, NULL, ERROR_CODE, 'ERROR'),
                      record_status =
                         DECODE (l_error_message, NULL, record_status, 'E')
                WHERE     ope_batch_id = c_btch_rec.ope_batch_id
                      AND batch_number = c_btch_rec.batch_number
                      AND file_name = c_btch_rec.file_name
                      AND event_id = i.event_id;

               nihgl_fah_utilities_pkg.delete_bc_event (i.event_id);
            END LOOP;

         ELSE
            FOR i IN (SELECT * FROM psa_bc_xla_events_gt)
            LOOP
               l_error_message := NULL;

            logf ('Process Event result_code 2: ' || i.result_code , l_module);
            logf ('Process Event i.event_id 2: ' || i.event_id , l_module);

               IF i.result_code = 'XLA_ERROR'
               THEN
                  l_error_message :=
                        'Internal error - Unable to derive the accounting from FAH'
                     || '- Status Code: '
                     || i.result_code
                     || ', Please check the FAH Setup';

               ELSIF i.result_code <> 'SUCCESS'
               THEN
                  l_error_message := 'OPE funds check error';
               END IF;


               UPDATE nihgl_ope_acct_dtls_int_tbl
                  SET packet_id = v_packet_id,
                      error_message =
                         DECODE (error_message,
                                 NULL, l_error_message,
                                 error_message || '; ' || l_error_message),
                      ERROR_CODE =
                         DECODE (l_error_message, NULL, ERROR_CODE, 'ERROR'),
                      record_status =
                         DECODE (l_error_message, NULL, record_status, 'E')
                WHERE     ope_batch_id = c_btch_rec.ope_batch_id
                      AND batch_number = c_btch_rec.batch_number
                      AND file_name = c_btch_rec.file_name
                      AND event_id = i.event_id;

               nihgl_fah_utilities_pkg.delete_bc_event (i.event_id);

            END LOOP;

         END IF;

            logf('Going for next BC events process ' , l_Module);
    END LOOP;

          logf ('p_type: ' || p_type , l_module);
         IF p_type = 'R'
         THEN
            --Transfer to GL
            logf ('before  nihgl_fah_utilities_pkg.transfer_to_gl' , l_module);

            logf ('g_ope_je_source: ' || g_ope_je_source , l_module);
            logf ('c_btch_rec.set_of_books_id: ' || c_btch_rec.set_of_books_id , l_module);
            logf ('g_ope_je_source: ' || g_ope_je_source , l_module);
            logf ('v_user_je_category_name: ' || v_user_je_category_name , l_module);
            logf ('v_user_je_category_name: ' || c_btch_rec.batch_number , l_module);
            nihgl_fah_utilities_pkg.transfer_to_gl (
               errbuf             => l_error_message,
               retcode            => v_error_code,
               p_je_source_name   => g_ope_je_source,
               p_ledger_id        => c_btch_rec.set_of_books_id,
               p_batch_name       =>    g_ope_je_source
                                     || '-'
                                     || v_user_je_category_name
                                     || '-'
                                     || c_btch_rec.batch_number);
            logf ('after  nihgl_fah_utilities_pkg.transfer_to_gl' , l_module);


            logf ('l_error_message: ' || l_error_message , l_module);
            logf ('v_error_code: ' || v_error_code , l_module);
            UPDATE nihgl_ope_acct_dtls_int_tbl
               SET error_message =
                      DECODE (error_message,
                              NULL, l_error_message,
                              error_message || '; ' || l_error_message),
                   ERROR_CODE =
                      DECODE (l_error_message, NULL, ERROR_CODE, 'ERROR'),
                   record_status =
                      DECODE (l_error_message, NULL, record_status, 'E')
             WHERE  ope_batch_id = c_btch_rec.ope_batch_id
                   AND batch_number = c_btch_rec.batch_number
                   AND file_name = c_btch_rec.file_name;


            UPDATE nihgl_ope_acct_dtls_int_tbl
               SET record_status = 'P', ERROR_CODE = 'PROCESSED'
             WHERE     ope_batch_id = c_btch_rec.ope_batch_id
                   AND batch_number = c_btch_rec.batch_number
                   AND file_name = c_btch_rec.file_name
                   AND record_status = 'V';
         END IF;
      END LOOP;

      FOR rec_bc_packet IN cur_bc_packet (p_batch_number, p_file_name)
      LOOP

         v_funds_check_message := rec_bc_packet.description;
         v_funds_check_code := rec_bc_packet.result_code;
         logf ('v_funds_check_message: ' || v_funds_check_message , l_module);
         logf ('v_funds_check_code: ' || v_funds_check_code , l_Module);
         BEGIN
            l_cnt := l_cnt + 1;

            --Start of change 1 by Yash
            --OPE Funds Check Error
            l_error_message := g_err_message_tbl ('NIHOPETXN029');
            logf ('l_error_message: ' || l_error_message,l_module);
            --Check if responsibility name, batch number and error message is setup in the setup from
            --else check if batch number and error message is setup in the setup form
            --else use default (DT) batch and error message

          IF g_txn_flow_flags_tbl.EXISTS (
                  g_resp_name || ':' || p_batch_number || ':NIHOPETXN029')
            THEN
            logf (' in 1 ' , l_Module);
               --The responsibility name, batch number and error message exist in the setup form
               --hence using the setup
               --Check for file reject for the following error
               IF NVL (
                     g_txn_flow_flags_tbl (
                           g_resp_name
                        || ':'
                        || p_batch_number
                        || ':NIHOPETXN029').file_reject,
                     'N') = 'Y'
               THEN
               logf (' in 2 ' , l_Module);
                  g_file_reject_flag := TRUE;
               END IF;


               --Check the reject error transaction flag for the following error
               IF NVL (
                     g_txn_flow_flags_tbl (
                           g_resp_name
                        || ':'
                        || p_batch_number
                        || ':NIHOPETXN029').reject_error_txn,
                     'N') = 'Y'
               THEN
               logf (' in 4 ' , l_Module);
                  g_index :=
                        TRIM (rec_bc_packet.document_ref)
                     || ':'
                     || TRIM (rec_bc_packet.document_number)
                     || ':'
                     || TRIM (rec_bc_packet.can_fy);

                  --If the index does not exist then include the index
                  IF NOT g_txn_reject_tbl.EXISTS (g_index)
                  THEN
                     g_txn_reject_tbl (g_index) := 1;
                  END IF;
               END IF;
            ELSIF g_txn_flow_flags_tbl.EXISTS (
                     p_batch_number || ':NIHOPETXN029')
            THEN
            logf (' in 5 ' , l_Module);
               --The batch number and error message exist in the setup form
               --hence using the setup
               --Check for file reject for the following error
               IF NVL (
                     g_txn_flow_flags_tbl (p_batch_number || ':NIHOPETXN029').file_reject,
                     'N') = 'Y'
               THEN
               logf (' in 6 ' , l_Module);
                  g_file_reject_flag := TRUE;
               END IF;



               --Check the reject error transaction flag for the following error
               IF NVL (
                     g_txn_flow_flags_tbl (p_batch_number || ':NIHOPETXN029').reject_error_txn,
                     'N') = 'Y'
               THEN
               logf (' in 9 ' , l_Module);
                  g_index :=
                        TRIM (rec_bc_packet.document_ref)
                     || ':'
                     || TRIM (rec_bc_packet.document_number)
                     || ':'
                     || TRIM (rec_bc_packet.can_fy);

                  --If the index does not exist then include the index
                  IF NOT g_txn_reject_tbl.EXISTS (g_index)
                  THEN
                  logf (' in 10 ' , l_Module);
                     g_txn_reject_tbl (g_index) := 1;
                  END IF;
               END IF;

            ELSE
               --The batch number and error message does not exist in the setup form
               --hence using the default DT setup
               --Check for file reject for the following error
               IF NVL (g_txn_flow_flags_tbl ('DT:NIHOPETXN029').file_reject,
                       'N') = 'Y'
               THEN
               logf (' in 11 ' , l_Module);
                  g_file_reject_flag := TRUE;
               END IF;

               --Check the hold for ope error handling flag for the following error
               IF NVL (
                     g_txn_flow_flags_tbl ('DT:NIHOPETXN029').hold_for_ope_eh,
                     'N') = 'Y'
               THEN
               logf (' in 12 ' , l_Module);
                  g_index :=
                        TRIM (rec_bc_packet.document_ref)
                     || ':'
                     || TRIM (rec_bc_packet.document_number)
                     || ':'
                     || TRIM (rec_bc_packet.can_fy);

                  --If the index does not exist then include the index
                  IF NOT g_txn_hold_tbl.EXISTS (g_index)
                  THEN
                  logf (' in 13 ',l_Module);
                     g_txn_hold_tbl (g_index) := 1;
                  END IF;
               END IF;

               --Check the reject error transaction flag for the following error
               IF NVL (
                     g_txn_flow_flags_tbl ('DT:NIHOPETXN029').reject_error_txn,
                     'N') = 'Y'
               THEN
               logf (' in 14 ' , l_module);
                  g_index :=
                        TRIM (rec_bc_packet.document_ref)
                     || ':'
                     || TRIM (rec_bc_packet.document_number)
                     || ':'
                     || TRIM (rec_bc_packet.can_fy);

                  --If the index does not exist then include the index
                  IF NOT g_txn_reject_tbl.EXISTS (g_index)
                  THEN
                  logf (' in 15 ' , l_Module);
                     g_txn_reject_tbl (g_index) := 1;
                  END IF;
               END IF;
            END IF;

            --End of change 1

            UPDATE nihgl_ope_acct_dtls_int_tbl
               SET funds_check_message =
                      funds_check_message || '; ' || v_funds_check_message,
                   funds_check_code =
                      funds_check_code || '; ' || v_funds_check_code,
                   error_message =
                      DECODE (error_message,
                              NULL, l_error_message,
                              error_message || '; ' || l_error_message),
                   --End of change 1
                   ERROR_CODE =
                      DECODE (v_funds_check_message,
                              NULL, ERROR_CODE,
                              'ERROR'),
                   record_status =
                      DECODE (v_funds_check_message,
                              NULL, record_status,
                              'E')
             WHERE     ope_detail_id = rec_bc_packet.ope_detail_id
                   AND packet_id = rec_bc_packet.packet_id;
         EXCEPTION
            WHEN OTHERS
            THEN
               l_error_message :=
                     ' Issue while dtls table update for packet id: '
                  || v_packet_id
                  || '-'
                  || SUBSTR (SQLERRM, 1, 250);
               logf (l_error_message , L_mODULE);
         END;
      END LOOP;
   EXCEPTION
      WHEN OTHERS
      THEN
         logf (
               ' Error in Procedure process_events - '
            || SUBSTR (SQLERRM, 1, 250), L_mODULE);
         g_retcode := 2;
         g_errbuf :=
            'Error in Procedure process_events: ' || SUBSTR (SQLERRM, 1, 250);
   END process_SLA;

/* -- -------------------------------------------------------------------------------------
-- Procedure process_events
    Using FAH
-- -------------------------------------------------------------------------------------------*/
   PROCEDURE create_sla_events (p_batch_number   IN     VARCHAR2,
                             p_file_name      IN     VARCHAR2,
                             retcode             OUT NUMBER,
                             errbuf              OUT VARCHAR2)
   IS

      CURSOR c_btch_cur
      IS
           SELECT UNIQUE ope_batch_id,
                         batch_number,
                         file_name,
                         set_of_books_id
             FROM nihgl_ope_acct_dtls_int_tbl
            WHERE     record_status = 'V'
                  AND batch_number = p_batch_number
                  AND file_name = p_file_name
         ORDER BY 1;


      --  Select details for validation

      CURSOR c_dtls_cur (
         p_ope_batch_id    NUMBER,
         p_batch_number    VARCHAR2,
         p_file_name       VARCHAR2,
         p_sob_id          NUMBER)
      IS
           SELECT *
             FROM nihgl_ope_acct_dtls_int_tbl
            WHERE     ope_batch_id = p_ope_batch_id
                  AND batch_number = p_batch_number
                  AND file_name = p_file_name
                  AND set_of_books_id = p_sob_id
                  AND record_status = 'V'
         ORDER BY 2;


        ---------  Select details for validation

      CURSOR c_dtls_doc_cur (
         p_ope_batch_id    NUMBER,
         p_batch_number    VARCHAR2,
         p_file_name       VARCHAR2,
         p_sob_id          NUMBER)
      IS
           SELECT UNIQUE
                  TRIM (document_ref || document_number) document_number,
                  document_ref doc_ref,
                  document_number doc_number,
                  TO_DATE (attribute5, 'DD-MON-RRRR') accounting_date,
                  batch_number                                       --,can_fy
             FROM nihgl_ope_acct_dtls_int_tbl
            WHERE     ope_batch_id = p_ope_batch_id
                  AND batch_number = p_batch_number
                  AND file_name = p_file_name
                  AND set_of_books_id = p_sob_id
                  AND record_status = 'V'
         ORDER BY 1;


       ---------  Funds Check Cursor

      CURSOR cur_bc_packet (
         p_packet_id NUMBER)
      IS
         SELECT gp.reference4 ope_detail_id,
                gp.result_code result_code,
                gl.description                     --Start of change 1 by Yash
                              --Added the below columns
                ,
                dtl.document_ref,
                dtl.document_number,
                dtl.can_fy
           --End of change 1
           FROM gl_lookups gl, gl_bc_packets gp    --Start of change 1 by Yash
                                               --Added the below table
                , nihgl_ope_acct_dtls_int_tbl dtl
          --End of change 1
          WHERE     gp.result_code = gl.lookup_code
                AND gl.lookup_type = 'FUNDS_CHECK_RESULT_CODE'
                AND gp.packet_id = p_packet_id
                --Start of change 1 by Yash
                --Added the below join condition
                AND gp.reference4 = dtl.ope_detail_id
                --End of change 1
                AND gp.result_code LIKE 'F%'
                AND gp.result_code <> 'F06'
                AND gp.template_id IS NULL
                AND gp.actual_flag <> 'E'
                AND gp.entered_dr <> 0;

      x_period_name                 VARCHAR2 (15);
      v_packet_id                   NUMBER;
      v_funds_check_code            VARCHAR2 (20);
      v_funds_check_message         VARCHAR2 (2000);
      v_funds_check_error_code      VARCHAR2 (30);
      v_funds_check_error_message   VARCHAR2 (2000);
      v_period_year                 NUMBER;
      v_period_num                  NUMBER;
      v_quarter_num                 NUMBER;
      v_je_source                   VARCHAR2 (30);
      v_je_category                 gl_je_categories.je_category_name%TYPE;
      v_user_je_category_name       gl_je_categories.user_je_category_name%TYPE;
      l_error_message               VARCHAR2 (2000);
      v_amount                      NUMBER;
      l_inst_bcp_flag               BOOLEAN := TRUE;
      l_fund_chk_flag               BOOLEAN := FALSE;
      l_cnt                         NUMBER;
      --FAH
      v_legal_entity_id             NUMBER;
      v_ledger_id                   NUMBER;
      v_event_id                    NUMBER;
      v_event_date                  DATE;
      v_array_event_info            xla_events_pub_pkg.t_array_event_info;
      v_event_source_info           xla_events_pub_pkg.t_event_source_info;
      v_event_type_code             xla_event_types_tl.event_type_code%TYPE;
      v_budgetary_flag              VARCHAR2 (1) := 'N';
      v_appl_id                     NUMBER;
      v_error_code                  VARCHAR2 (20);
      v_line_num                    NUMBER := 0;
      v_module                      varchar2(100) := 'create_sla_events';

   BEGIN
      --g_request_id :=  fnd_global.conc_request_id;
      g_user_id := fnd_global.user_id;
      g_login_id := fnd_global.login_id;

       logf ('Process Event Start ...',  v_module);

      FOR c_btch_rec IN c_btch_cur
      LOOP
               /*
        || Getting je_source_name, je_category_name based on
        || g_ope_je_source, g_ope_je_category_name
        */
         BEGIN
            SELECT je_source_name
              INTO v_je_source
              FROM gl_je_sources
             WHERE user_je_source_name = g_ope_je_source;
         EXCEPTION
            WHEN OTHERS
            THEN
               logf (' je_source_name is not setup',v_module);
         END;

         v_user_je_category_name := get_batch_category (p_batch_number);

         logf ('g_JOURNAL_CATEGORY_02 ' || g_JOURNAL_CATEGORY , v_module);

         BEGIN
            SELECT je_category_name
              INTO v_je_category
              FROM gl_je_categories
             WHERE user_je_category_name = v_user_je_category_name;
         EXCEPTION
            WHEN OTHERS
            THEN
               logf (' je_category_name is not setup',v_module);
         END;

         FOR c_dtl_doc_rec
            IN c_dtls_doc_cur (p_ope_batch_id   => c_btch_rec.ope_batch_id,
                               p_batch_number   => c_btch_rec.batch_number,
                               p_file_name      => c_btch_rec.file_name,
                               p_sob_id         => c_btch_rec.set_of_books_id)
         LOOP
            l_error_message := NULL;
            v_error_code := NULL;
            v_event_source_info.legal_entity_id := v_legal_entity_id;
            v_event_source_info.ledger_id := 1;
            v_event_source_info.source_id_char_1 :=
               c_dtl_doc_rec.document_number;
            v_event_source_info.source_id_char_2 := c_dtl_doc_rec.batch_number;
            v_event_source_info.transaction_number :=
               c_dtl_doc_rec.document_number;

            logf (' v_user_je_category_name: '
                        || v_user_je_category_name, v_module);
            logf (' c_dtl_doc_rec.batch_number: '
                        || c_dtl_doc_rec.batch_number, v_module);
            -- Added by Vidya Uppalapati for the OPE related changes
            IF v_user_je_category_name = 'NIH CS FFS'
            THEN
               BEGIN
                  SELECT description
                    INTO v_event_type_code
                    FROM fnd_lookup_values
                   WHERE     lookup_type = v_user_je_category_name
                         AND lookup_code = c_dtl_doc_rec.batch_number;
               EXCEPTION
                  WHEN OTHERS
                  THEN
                     logf (
                           ' The Event type mapping is missing for the Batch Number: '
                        || c_dtl_doc_rec.batch_number, v_module);
               END;
            ELSE
               v_event_type_code := NULL;
            END IF;

            nihgl_fah_utilities_pkg.create_event (
               p_event_source_info       => v_event_source_info,
               p_event_date              => c_dtl_doc_rec.accounting_date,
               p_je_category_name        => v_je_category,
               p_je_source_name          => v_je_source,
               p_delete_unprocess_flag   => 'Y',
               p_event_id                => v_event_id,
               p_event_type_code         => v_event_type_code,
               p_application_id          => v_appl_id,
               p_message                 => l_error_message,
               p_code                    => v_error_code);
--CR NBSCH0002499
/*
            v_line_num := 0;

            FOR i
               IN (SELECT *
                     FROM nihgl_ope_acct_dtls_int_tbl
                    WHERE     ope_batch_id = c_btch_rec.ope_batch_id
                          AND batch_number = c_btch_rec.batch_number
                          AND file_name = c_btch_rec.file_name
                          AND set_of_books_id = c_btch_rec.set_of_books_id
                          AND document_ref || document_number =
                                 c_dtl_doc_rec.document_number)
            LOOP
               v_line_num := v_line_num + 1;

               UPDATE nihgl_ope_acct_dtls_int_tbl
                  SET line_number = v_line_num
                WHERE ope_detail_id = i.ope_detail_id;
            END LOOP;
*/

            logf (' c_btch_rec.ope_batch_id: '
                        || c_btch_rec.ope_batch_id
                ||' c_btch_rec.batch_number: '
                        || c_btch_rec.batch_number
                ||' c_btch_rec.file_name: '
                        || c_btch_rec.file_name
                ||' c_btch_rec.set_of_books_id: '
                        || c_btch_rec.set_of_books_id
                ||' c_dtl_doc_rec.document_number: '
                        || c_dtl_doc_rec.document_number, v_module);
            logf (' c_dtl_doc_rec.accounting_date: '
                        || c_dtl_doc_rec.accounting_date, v_module);
            logf (' v_je_category: '
                        || v_je_category, v_module);
            logf (' v_je_source: '
                        || v_je_source, v_module);
            logf (' v_event_id: '
                        || v_event_id, v_module);
            logf (' v_event_type_code: '
                        || v_event_type_code, v_module);
            logf (' v_appl_id: '
                        || v_appl_id, v_module);
            logf (' l_error_message: '
                        || l_error_message, v_module);
            logf (' v_error_code: '
                        || v_error_code, v_module);

            UPDATE nihgl_ope_acct_dtls_int_tbl
               SET event_id = v_event_id,
                   application_id = v_appl_id,
                   --line_number = 1,
                   ledger_id = c_btch_rec.set_of_books_id,
                   event_type_code = v_event_type_code,
                   error_message =
                      DECODE (error_message,
                              NULL, l_error_message,
                              error_message || '; ' || l_error_message),
                   ERROR_CODE =
                      DECODE (l_error_message, NULL, ERROR_CODE, 'ERROR'),
                   record_status =
                      DECODE (l_error_message, NULL, record_status, 'E')
             WHERE     ope_batch_id = c_btch_rec.ope_batch_id
                   AND batch_number = c_btch_rec.batch_number
                   AND file_name = c_btch_rec.file_name
                   AND set_of_books_id = c_btch_rec.set_of_books_id
                   AND document_ref || document_number =
                          c_dtl_doc_rec.document_number;
         END LOOP;
      END LOOP;
   EXCEPTION
      WHEN OTHERS
      THEN
         logf (
               ' Error in Procedure create_sla_events - '
            || SUBSTR (SQLERRM, 1, 250), v_module);
         g_retcode := 2;
         g_errbuf :=
            'Error in Procedure create_sla_events: ' || SUBSTR (SQLERRM, 1, 250);
   END create_sla_events;
 /* ----------------------------------------------------------------------------------------------------
     -- Procedure validate_can

        Validate the can and get the values of task_id, project_id

-- ------------------------------------------------------------------------------------------------------*/
   FUNCTION validate_can (
      p_can                            IN     VARCHAR2,
      p_can_fyr                        IN     VARCHAR2,          -- FCI Change
      p_exp_item_dt                    IN     DATE,              -- FCI Change
      p_project_id                        OUT NUMBER,
      -- p_task_id                           OUT NUMBER,
      p_carrying_out_organization_id      OUT NUMBER,
      p_attribute2                        OUT pa_projects_all.attribute2%TYPE)
      RETURN BOOLEAN
   IS
      l_project_id       pa_projects_all.project_id%TYPE;
      l_task_id          pa_tasks.task_id%TYPE;
      l_exporg_id        pa_projects_all.carrying_out_organization_id%TYPE;
      l_attribute2       pa_projects_all.attribute2%TYPE;
      l_project_id1      pa_projects_all.project_id%TYPE;
      l_attribute2_1     pa_projects_all.attribute2%TYPE;
      l_can_exist_flag   BOOLEAN := TRUE;
      l_module varchar2(100)  :=  'validate_can';
   BEGIN

         logf('starts ' , L_module);
      --FCI CHANGE START  CR CR39674

      SELECT pap.project_id, pap.attribute2, pap.carrying_out_organization_id
        INTO l_project_id1, l_attribute2_1, l_exporg_id
        FROM apps.pa_projects_all pap, apps.pa_project_statuses b
       WHERE     pap.NAME = p_can
             AND pap.project_status_code = b.project_status_code
             and b.project_system_status_code IN ('APPROVED') --CR NBSCH0002499
             --AND b.project_system_status_code NOT IN ('REJECTED', 'CLOSED') --CR NBSCH0002499
             AND (    NVL (pap.start_date, SYSDATE) <= SYSDATE
                  AND (   pap.completion_date >= SYSDATE
                       OR pap.completion_date IS NULL))
             AND NVL (pap.enabled_flag, 'Y') = 'Y'
             AND pap.template_flag = 'N';

      --FCI CHANGE END  CR CR39674

      p_project_id := l_project_id;
      --  p_task_id := l_task_id;
      p_attribute2 := l_attribute2;
      p_carrying_out_organization_id := l_exporg_id;

         logf('end good ' , L_module);
      RETURN (l_can_exist_flag);
   EXCEPTION
      WHEN OTHERS
      THEN

         logf('end bad ' , L_module);
         p_project_id := 0;
         p_attribute2 := NULL;
         --   p_task_id := 0;
         p_carrying_out_organization_id := 0;
         l_can_exist_flag := FALSE;
         RETURN (l_can_exist_flag);
   END validate_can;
-- =======================================
-- Procedure validate_task_number
--  Validate Task Number

-- =======================================
   FUNCTION validate_task_number (
      p_can                            IN     VARCHAR2,
      p_can_fyr                        IN     VARCHAR2,          -- FCI Change
      p_task_num                       IN     VARCHAR2,          -- FCI Change
      p_exp_item_dt                    IN     DATE,              -- FCI Change
      p_project_id                        OUT NUMBER,
      p_task_id                           OUT NUMBER,
      p_carrying_out_organization_id      OUT NUMBER,
      p_attribute2                        OUT pa_projects_all.attribute2%TYPE)
      RETURN BOOLEAN
   IS
      l_project_id        pa_projects_all.project_id%TYPE;
      l_task_id           pa_tasks.task_id%TYPE;
      l_exporg_id         pa_projects_all.carrying_out_organization_id%TYPE;
      l_attribute2        pa_projects_all.attribute2%TYPE;
      l_task_exist_flag   BOOLEAN := TRUE;
      l_module varchar2(100)  :=  'validate_task_number';
   BEGIN

 logf ('begin ' , l_module);

      SELECT pap.project_id,
             pap.attribute2,
             pat.task_id,
             pap.carrying_out_organization_id
        INTO l_project_id,
             l_attribute2,
             l_task_id,
             l_exporg_id
        FROM pa_projects_all pap, pa_tasks pat, pa_project_statuses b
       WHERE     pap.NAME = p_can
             AND pap.project_status_code = b.project_status_code
             and b.project_system_status_code IN ('APPROVED') --CR NBSCH0002499
             --AND b.project_system_status_code NOT IN ('REJECTED', 'CLOSED') --CR NBSCH0002499
             AND pat.task_number = p_task_num                            --'1'
             -- AND pat.task_number = p_can_fyr||'.999' -- FCI Change
             AND (    NVL (pap.start_date, SYSDATE) <= SYSDATE
                  AND (   pap.completion_date >= SYSDATE
                       OR pap.completion_date IS NULL))
             AND NVL (pap.enabled_flag, 'Y') = 'Y'
             AND pap.template_flag = 'N'
             AND pap.project_id = pat.project_id
             AND (    NVL (pat.start_date, SYSDATE) <= TRUNC(p_exp_item_dt)--CR NBSCH0002499
                  AND (   pat.completion_date >= TRUNC(p_exp_item_dt)--CR NBSCH0002499
                       OR pat.completion_date IS NULL));--CR NBSCH0002499

      p_project_id := l_project_id;
      p_task_id := l_task_id;
      p_attribute2 := l_attribute2;
      p_carrying_out_organization_id := l_exporg_id;

      logf ('end good' , l_module);

      RETURN (l_task_exist_flag);
   EXCEPTION
      WHEN OTHERS
      THEN
         logf ('end bad' , l_module);
         p_project_id := 0;
         p_attribute2 := NULL;
         p_task_id := 0;
         p_carrying_out_organization_id := 0;
         l_task_exist_flag := FALSE;
         RETURN (l_task_exist_flag);
   END validate_task_number;

--CR NBSCH0002499 begin
-- =======================================
-- Procedure validate_exp_it_dt_task_number
--  Validate expenditure_item_date in task_number
-- It is used only when validate_task_number returns a negative value, for checking issues related to expenditure_item_date.
-- It is almost the same as validate_task_number, but without OUT variables, and it returns TRUE only if the issue is related to expenditure_item_date.

-- =======================================
   FUNCTION validate_exp_it_dt_task_number (
      p_can                            IN     VARCHAR2,
      p_can_fyr                        IN     VARCHAR2,          -- FCI Change
      p_task_num                       IN     VARCHAR2,          -- FCI Change
      p_exp_item_dt                    IN     DATE)
      RETURN BOOLEAN
   IS
      l_project_id        pa_projects_all.project_id%TYPE;
      l_task_id           pa_tasks.task_id%TYPE;
      l_exporg_id         pa_projects_all.carrying_out_organization_id%TYPE;
      l_attribute2        pa_projects_all.attribute2%TYPE;
      l_task_exist_flag   BOOLEAN := TRUE;
      l_module varchar2(100)  :=  'validate_task_number';
   BEGIN

 logf ('begin ' , l_module);

      SELECT pap.project_id,
             pap.attribute2,
             pat.task_id,
             pap.carrying_out_organization_id
        INTO l_project_id,
             l_attribute2,
             l_task_id,
             l_exporg_id
        FROM pa_projects_all pap, pa_tasks pat, pa_project_statuses b
       WHERE     pap.NAME = p_can
             AND pap.project_status_code = b.project_status_code
             and b.project_system_status_code IN ('APPROVED') --CR NBSCH0002499
             --AND b.project_system_status_code NOT IN ('REJECTED', 'CLOSED') --CR NBSCH0002499
             AND pat.task_number = p_task_num                            --'1'
             -- AND pat.task_number = p_can_fyr||'.999' -- FCI Change
             AND (    NVL (pap.start_date, SYSDATE) <= SYSDATE
                  AND (   pap.completion_date >= SYSDATE
                       OR pap.completion_date IS NULL))
             AND NVL (pap.enabled_flag, 'Y') = 'Y'
             AND pap.template_flag = 'N'
             AND pap.project_id = pat.project_id;
             --AND (    NVL (pat.start_date, SYSDATE) <= TRUNC(p_exp_item_dt)
             --     AND (   pat.completion_date >= TRUNC(p_exp_item_dt)
             --          OR pat.completion_date IS NULL));


      logf ('end good' , l_module);

      RETURN (l_task_exist_flag);
   EXCEPTION
      WHEN OTHERS
      THEN
         logf ('end bad' , l_module);
         l_task_exist_flag := FALSE;
         RETURN (l_task_exist_flag);
   END validate_exp_it_dt_task_number;
-- CR NBSCH0002499 end
-- =======================================
-- Function validate_object_class
-- Validate the Object Class
-- =======================================
   FUNCTION validate_object_class (p_object_class       IN     VARCHAR2,
                                   p_expenditure_type      OUT VARCHAR2)
      RETURN BOOLEAN
   IS
      l_expenditure_type   pa_expenditure_types.expenditure_type%TYPE;
      l_oc_exist_flag      BOOLEAN := TRUE;
      l_module varchar2(100) := 'validate_task_number';
   BEGIN
     logf('begin' , l_module);

      SELECT UNIQUE expenditure_type
        INTO l_expenditure_type
        FROM pa_expenditure_types
       WHERE     SUBSTR (expenditure_type, 1, 5) = p_object_class -- FCI Change object class from 4 to 5 char
             AND (    NVL (start_date_active, SYSDATE) <= SYSDATE
                  AND (end_date_active >= SYSDATE OR end_date_active IS NULL));

      p_expenditure_type := l_expenditure_type;
      RETURN (l_oc_exist_flag);
   EXCEPTION
      WHEN OTHERS
      THEN
         p_expenditure_type := NULL;
         l_oc_exist_flag := FALSE;
         RETURN (l_oc_exist_flag);
   END validate_object_class;

-- =======================================
-- Function get_can_status
-- =======================================
   FUNCTION get_can_status (p_can IN VARCHAR2, p_fy IN VARCHAR2)
      RETURN VARCHAR2
   IS
      l_status   VARCHAR2 (4000);
         l_module varchar2(100) := 'get_can_status';

      CURSOR cr_can_status
      IS
         SELECT CASE
                   WHEN NVL (
                           TS.CANCELLATION_DATE,
                              '30-SEP'
                           || TO_CHAR (ADD_MONTHS (SYSDATE, 3), 'YYYY')) >=
                              '30-SEP'
                           || TO_CHAR (ADD_MONTHS (SYSDATE, 3), 'YYYY')
                   THEN
                      'ACTIVE'
                   ELSE
                      'CANCELED'
                END
                   CAN_STATUS
           FROM apps.pa_projects_all ppa,
                fv_fund_parameters fp,
                apps.fv_treasury_symbols ts
          WHERE     1 = 1
                AND ppa.name = p_can
                AND (CASE
                        WHEN     SUBSTR (ppa.attribute2, 11, 1) IN ('1', '0')
                             AND    SUBSTR (ppa.attribute2, 1, 6)
                                 || p_fy
                                 || SUBSTR (ppa.attribute2, 11, 3) =
                                    SUBSTR (fp.fund_value, 1, 13)
                        THEN
                           1
                        WHEN     SUBSTR (ppa.attribute2, 11, 1) > '1'
                             AND SUBSTR (ppa.attribute2, 1, 13) =
                                    SUBSTR (fp.fund_value, 1, 13)
                        THEN
                           1
                        ELSE
                           0
                     END) = 1
                AND FP.TREASURY_SYMBOL_ID = TS.TREASURY_SYMBOL_ID;

   BEGIN

       logf('begin ' , l_module);
       logf('p_can: '||p_can||' p_fy: '||p_fy , l_module);
      OPEN cr_can_status;

      FETCH cr_can_status INTO l_status;

      CLOSE cr_can_status;


       logf('end good ' , l_module);
      RETURN l_status;
   EXCEPTION
      WHEN OTHERS
      THEN
         logf('end bad ' , l_module);
         l_status := NULL;
         RETURN l_status;
   END get_can_status;
-- --------------------------------------------------------------------------------------
-- Function validate_fy
-- Validating for the Financial Year

-- -----------------------------------------------------------------------------------
   FUNCTION validate_fy (p_sob_id          IN     NUMBER,
                         p_can             IN     VARCHAR2,
                         p_fy              IN     NUMBER,
                         p_doc_ref         IN     VARCHAR2,
                         p_doc_number      IN     VARCHAR2,
                         p_batch_number    IN     VARCHAR2,
                         p_error_message      OUT VARCHAR2)
      RETURN BOOLEAN
   IS
      l_error_message    VARCHAR2 (200) := NULL;
      l_financial_year   nihgl_ope_acct_dtls_int_tbl.can_fy%TYPE;
      l_fy_flag          BOOLEAN := TRUE;
      l_module varchar2(100) := 'validate_fy';
   BEGIN

      logf('begin ' , l_Module);

      SELECT DISTINCT gp.period_year
        INTO l_financial_year
        FROM gl_periods gp, gl_period_statuses gps, fnd_application fa
       WHERE     gp.period_set_name = 'NIH_CALENDAR'
             AND gp.period_name = gps.period_name
             AND fa.application_id = gps.application_id
             AND gps.closing_status IN ('O', 'F', 'C')
             AND gp.adjustment_period_flag = 'N'
             AND gps.set_of_books_id = p_sob_id
             AND fa.application_short_name = 'SQLGL'
             AND TRIM (SYSDATE) BETWEEN gp.start_date AND gp.end_date;


       --------- Validate whether it is in the Expired Year or Not

      --
      --CR 42504  Valdiate CAN and FY based on fund Expiration and Caceltaion Year not FY
      --
      IF (get_can_status (p_can, p_fy) = 'CANCELED') --     (p_fy < l_financial_year - 5)
      THEN
         --Start of change 1 by Yash
         --CAN is in expired Fiscal Year
         l_error_message := g_err_message_tbl ('NIHOPETXN003');

         --Check if responsibility name, batch number and error message is setup in the setup from
         --else check if batch number and error message is setup in the setup form
         --else use default (DT) batch and error message
         IF g_txn_flow_flags_tbl.EXISTS (
               g_resp_name || ':' || p_batch_number || ':NIHOPETXN003')
         THEN
            --The responsibility name, batch number and error message exist in the setup form
            --hence using the setup
            --Check for file reject for the following error
            IF NVL (
                  g_txn_flow_flags_tbl (
                     g_resp_name || ':' || p_batch_number || ':NIHOPETXN003').file_reject,
                  'N') = 'Y'
            THEN
               g_file_reject_flag := TRUE;
            END IF;


            --Check the reject error transaction flag for the following error
            IF NVL (
                  g_txn_flow_flags_tbl (
                     g_resp_name || ':' || p_batch_number || ':NIHOPETXN003').reject_error_txn,
                  'N') = 'Y'
            THEN
               g_index :=
                     TRIM (p_doc_ref)
                  || ':'
                  || TRIM (p_doc_number)
                  || ':'
                  || TRIM (p_fy);

               --If the index does not exist then include the index
               IF NOT g_txn_reject_tbl.EXISTS (g_index)
               THEN
                  g_txn_reject_tbl (g_index) := 1;
               END IF;
            END IF;
         ELSIF g_txn_flow_flags_tbl.EXISTS (
                  p_batch_number || ':NIHOPETXN003')
         THEN
            --The batch number and error message exist in the setup form
            --hence using the setup
            --Check for file reject for the following error
            IF NVL (
                  g_txn_flow_flags_tbl (p_batch_number || ':NIHOPETXN003').file_reject,
                  'N') = 'Y'
            THEN
               g_file_reject_flag := TRUE;
            END IF;



            --Check the reject error transaction flag for the following error
            IF NVL (
                  g_txn_flow_flags_tbl (p_batch_number || ':NIHOPETXN003').reject_error_txn,
                  'N') = 'Y'
            THEN
               g_index :=
                     TRIM (p_doc_ref)
                  || ':'
                  || TRIM (p_doc_number)
                  || ':'
                  || TRIM (p_fy);

               --If the index does not exist then include the index
               IF NOT g_txn_reject_tbl.EXISTS (g_index)
               THEN
                  g_txn_reject_tbl (g_index) := 1;
               END IF;
            END IF;

         ELSE
            --The batch number and error message does not exist in the setup form
            --hence using the default DT setup
            --Check for file reject for the following error
            IF NVL (g_txn_flow_flags_tbl ('DT:NIHOPETXN003').file_reject,
                    'N') = 'Y'
            THEN
               g_file_reject_flag := TRUE;
            END IF;



            --Check the reject error transaction flag for the following error
            IF NVL (
                  g_txn_flow_flags_tbl ('DT:NIHOPETXN003').reject_error_txn,
                  'N') = 'Y'
            THEN
               g_index :=
                     TRIM (p_doc_ref)
                  || ':'
                  || TRIM (p_doc_number)
                  || ':'
                  || TRIM (p_fy);

               --If the index does not exist then include the index
               IF NOT g_txn_reject_tbl.EXISTS (g_index)
               THEN
                  g_txn_reject_tbl (g_index) := 1;
               END IF;
            END IF;
         END IF;

         --End of change 1
         p_error_message := l_error_message;
         l_fy_flag := FALSE;

      ELSIF (p_fy > l_financial_year)
      THEN
         --Start of change 1 by Yash
         --CAN is in future Fiscal Year
         l_error_message := g_err_message_tbl ('NIHOPETXN004');

         --Check if responsibility name, batch number and error message is setup in the setup from
         --else check if batch number and error message is setup in the setup form
         --else use default (DT) batch and error message
         IF g_txn_flow_flags_tbl.EXISTS (
               g_resp_name || ':' || p_batch_number || ':NIHOPETXN004')
         THEN
            --The responsibility name, batch number and error message exist in the setup form
            --hence using the setup
            --Check for file reject for the following error
            IF NVL (
                  g_txn_flow_flags_tbl (
                     g_resp_name || ':' || p_batch_number || ':NIHOPETXN004').file_reject,
                  'N') = 'Y'
            THEN
               g_file_reject_flag := TRUE;
            END IF;



            --Check the reject error transaction flag for the following error
            IF NVL (
                  g_txn_flow_flags_tbl (
                     g_resp_name || ':' || p_batch_number || ':NIHOPETXN004').reject_error_txn,
                  'N') = 'Y'
            THEN
               g_index :=
                     TRIM (p_doc_ref)
                  || ':'
                  || TRIM (p_doc_number)
                  || ':'
                  || TRIM (p_fy);

               --If the index does not exist then include the index
               IF NOT g_txn_reject_tbl.EXISTS (g_index)
               THEN
                  g_txn_reject_tbl (g_index) := 1;
               END IF;
            END IF;

         ELSIF g_txn_flow_flags_tbl.EXISTS (
                  p_batch_number || ':NIHOPETXN004')
         THEN
            --The batch number and error message exist in the setup form
            --hence using the setup
            --Check for file reject for the following error
            IF NVL (
                  g_txn_flow_flags_tbl (p_batch_number || ':NIHOPETXN004').file_reject,
                  'N') = 'Y'
            THEN
               g_file_reject_flag := TRUE;
            END IF;



            --Check the reject error transaction flag for the following error
            IF NVL (
                  g_txn_flow_flags_tbl (p_batch_number || ':NIHOPETXN004').reject_error_txn,
                  'N') = 'Y'
            THEN
               g_index :=
                     TRIM (p_doc_ref)
                  || ':'
                  || TRIM (p_doc_number)
                  || ':'
                  || TRIM (p_fy);

               --If the index does not exist then include the index
               IF NOT g_txn_reject_tbl.EXISTS (g_index)
               THEN
                  g_txn_reject_tbl (g_index) := 1;
               END IF;
            END IF;

         ELSE
            --The batch number and error message does not exist in the setup form
            --hence using the default DT setup
            --Check for file reject for the following error
            IF NVL (g_txn_flow_flags_tbl ('DT:NIHOPETXN004').file_reject,
                    'N') = 'Y'
            THEN
               g_file_reject_flag := TRUE;
            END IF;



            --Check the reject error transaction flag for the following error
            IF NVL (
                  g_txn_flow_flags_tbl ('DT:NIHOPETXN004').reject_error_txn,
                  'N') = 'Y'
            THEN
               g_index :=
                     TRIM (p_doc_ref)
                  || ':'
                  || TRIM (p_doc_number)
                  || ':'
                  || TRIM (p_fy);

               --If the index does not exist then include the index
               IF NOT g_txn_reject_tbl.EXISTS (g_index)
               THEN
                  g_txn_reject_tbl (g_index) := 1;
               END IF;
            END IF;
         END IF;

         --End of change 1
         p_error_message := l_error_message;
         l_fy_flag := FALSE;
      END IF;

      logf('end good l_fy_flag' , l_Module);
      RETURN (l_fy_flag);
   EXCEPTION
      WHEN NO_DATA_FOUND
      THEN
         --Start of change 1 by Yash
         --Accounting Date is not in open/future/closed GL period
         l_error_message := g_err_message_tbl ('NIHOPETXN005');

         --Check if responsibility name, batch number and error message is setup in the setup from
         --else check if batch number and error message is setup in the setup form
         --else use default (DT) batch and error message
         IF g_txn_flow_flags_tbl.EXISTS (
               g_resp_name || ':' || p_batch_number || ':NIHOPETXN005')
         THEN
            --The responsibility name, batch number and error message exist in the setup form
            --hence using the setup
            --Check for file reject for the following error
            IF NVL (
                  g_txn_flow_flags_tbl (
                     g_resp_name || ':' || p_batch_number || ':NIHOPETXN005').file_reject,
                  'N') = 'Y'
            THEN
               g_file_reject_flag := TRUE;
            END IF;



            --Check the reject error transaction flag for the following error
            IF NVL (
                  g_txn_flow_flags_tbl (
                     g_resp_name || ':' || p_batch_number || ':NIHOPETXN005').reject_error_txn,
                  'N') = 'Y'
            THEN
               g_index :=
                     TRIM (p_doc_ref)
                  || ':'
                  || TRIM (p_doc_number)
                  || ':'
                  || TRIM (p_fy);

               --If the index does not exist then include the index
               IF NOT g_txn_reject_tbl.EXISTS (g_index)
               THEN
                  g_txn_reject_tbl (g_index) := 1;
               END IF;
            END IF;
         ELSIF g_txn_flow_flags_tbl.EXISTS (
                  p_batch_number || ':NIHOPETXN005')
         THEN
            --The batch number and error message exist in the setup form
            --hence using the setup
            --Check for file reject for the following error
            IF NVL (
                  g_txn_flow_flags_tbl (p_batch_number || ':NIHOPETXN005').file_reject,
                  'N') = 'Y'
            THEN
               g_file_reject_flag := TRUE;
            END IF;



            --Check the reject error transaction flag for the following error
            IF NVL (
                  g_txn_flow_flags_tbl (p_batch_number || ':NIHOPETXN005').reject_error_txn,
                  'N') = 'Y'
            THEN
               g_index :=
                     TRIM (p_doc_ref)
                  || ':'
                  || TRIM (p_doc_number)
                  || ':'
                  || TRIM (p_fy);

               --If the index does not exist then include the index
               IF NOT g_txn_reject_tbl.EXISTS (g_index)
               THEN
                  g_txn_reject_tbl (g_index) := 1;
               END IF;
            END IF;
         ELSE
            --The batch number and error message does not exist in the setup form
            --hence using the default DT setup
            --Check for file reject for the following error
            IF NVL (g_txn_flow_flags_tbl ('DT:NIHOPETXN005').file_reject,
                    'N') = 'Y'
            THEN
               g_file_reject_flag := TRUE;
            END IF;



            --Check the reject error transaction flag for the following error
            IF NVL (
                  g_txn_flow_flags_tbl ('DT:NIHOPETXN005').reject_error_txn,
                  'N') = 'Y'
            THEN
               g_index :=
                     TRIM (p_doc_ref)
                  || ':'
                  || TRIM (p_doc_number)
                  || ':'
                  || TRIM (p_fy);

               --If the index does not exist then include the index
               IF NOT g_txn_reject_tbl.EXISTS (g_index)
               THEN
                  g_txn_reject_tbl (g_index) := 1;
               END IF;
            END IF;
         END IF;

         --End of change 1
         p_error_message := l_error_message;
         l_fy_flag := FALSE;

         logf('end bad no_data_found' , l_Module);
         RETURN (l_fy_flag);
      WHEN OTHERS
      THEN
         logf('end bad OTHERS' , l_Module);
         l_error_message :=
            ' Exception-Period Year -' || SUBSTR (SQLERRM, 1, 100);
         p_error_message := l_error_message;
         l_fy_flag := FALSE;
         RETURN (l_fy_flag);
   END validate_fy;
/* --  -----------------------------------------------------------------------------------------
-- Function chk_obligation_exist

       chk_obligation_exist is to check the existence of Obligation
 ---------------------------------------------------------------------------------------------- */

   FUNCTION chk_obligation_exist (p_doc_num         VARCHAR2,
                                  p_fiscal_year     NUMBER,
                                  p_batch_number    VARCHAR2,
                                  p_can              VARCHAR2,	--CR NBSCH0003180
                                  p_object_class     VARCHAR2)	--CR NBSCH0003180
      RETURN NUMBER
   IS
      l_cnt            NUMBER := 0;
      l_batch_number   VARCHAR2 (2);
    l_module varchar2(100) := 'chk_obligation_exist';
   BEGIN
      IF p_batch_number = 'Q2'
      THEN
         l_batch_number := 'Q1';
      ELSE
         l_batch_number := p_batch_number;
      END IF;

      SELECT COUNT (*)
        INTO l_cnt
        FROM nihgl_ope_acct_dtls_all_v
       WHERE     record_status IN ('N', 'V', 'P')
             AND TRIM (document_ref || document_number) = p_doc_num
             AND batch_number = l_batch_number --DECODE (p_batch_number, 'Q2', 'Q1', p_batch_number)
             AND tcode = '050'
             AND can_fy = p_fiscal_year
             and can = p_can					--CR NBSCH0003180
             and object_class = p_object_class;	--CR NBSCH0003180

      RETURN l_cnt;
   EXCEPTION
      WHEN OTHERS
      THEN
         logf (
            ' Error in  Function chk_obligation_exist ' || ': ' || SQLERRM , l_module);
         RETURN 0;
   END chk_obligation_exist;
-- --------------------------------------------------------------------------------------------
-- Function chk_obligation_amnt_bal_exist
-- chk_obligation_amnt_bal_exist is to check the balance of obligation amount
-- ---------------------------------------------------------------------------------------------
   FUNCTION chk_obligation_amnt_bal_exist (p_doc_num          VARCHAR2,
                                           p_fiscal_year      NUMBER,
                                           p_batch_number     VARCHAR2,
                                           p_can              VARCHAR2,	--CR NBSCH0003180
                                           p_object_class     VARCHAR2,	--CR NBSCH0003180
                                           p_ope_detail_id    NUMBER)
      RETURN NUMBER
   IS
      l_050_amount      NUMBER := 0;
      l_181191_amount   NUMBER := 0;
      l_tot_amount      NUMBER := 0;
      l_batch_number_050    VARCHAR2 (2);		--CR NBSCH0003180
      l_batch_number_181191    VARCHAR2 (2);	--CR NBSCH0003180

      l_module varchar2(100) := 'chk_obligation_amnt_bal_exist';

   BEGIN

      logf('begin ' , l_Module);

      IF p_batch_number in ('Q1','Q2')				--CR NBSCH0003180
      THEN
         l_batch_number_050 := 'Q1';				--CR NBSCH0003180
         l_batch_number_181191 := 'Q2';				--CR NBSCH0003180
      ELSE
         l_batch_number_050 := p_batch_number;		--CR NBSCH0003180
         l_batch_number_181191 := p_batch_number;	--CR NBSCH0003180
      END IF;
      logf ('p_batch_number : ' || p_batch_number , l_module);					--CR NBSCH0003180
      logf ('l_batch_number_050 : ' || l_batch_number_050 , l_module);			--CR NBSCH0003180
      logf ('l_batch_number_181_191 : ' || l_batch_number_181191 , l_module);	--CR NBSCH0003180
      SELECT NVL (SUM (DECODE (reverse_code, 2, -1 * amount, amount)), 0)
        INTO l_050_amount
        FROM nihgl_ope_acct_dtls_all_v
       WHERE     record_status IN ('N', 'V', 'P')
             AND TRIM (document_ref || document_number) = p_doc_num
             AND batch_number = l_batch_number_050 --CR NBSCH0003180
             AND tcode = '050'
             AND can_fy = p_fiscal_year
             and can = p_can					--CR NBSCH0003180
             and object_class = p_object_class	--CR NBSCH0003180
             AND ope_detail_id <> p_ope_detail_id;

      SELECT NVL (SUM (DECODE (reverse_code, 2, -1 * amount, amount)), 0)
        INTO l_181191_amount
        FROM nihgl_ope_acct_dtls_all_v
       WHERE     record_status IN ('N', 'V', 'P')
             AND TRIM (document_ref || document_number) = p_doc_num
             AND batch_number = l_batch_number_181191	--CR NBSCH0003180
             AND tcode IN ('181', '191')
             AND can_fy = p_fiscal_year
             and can = p_can					--CR NBSCH0003180
             and object_class = p_object_class;	--CR NBSCH0003180

      l_tot_amount := l_050_amount - l_181191_amount;
      logf ('l_050_amount : ' || l_050_amount, l_module);
      logf ('l_181191_amount : ' || l_181191_amount , l_module);
      logf ('l_tot_amount : ' || l_tot_amount , l_module);
      RETURN (l_tot_amount);
   EXCEPTION
      WHEN OTHERS
      THEN
         logf (
               ' Error in  Function chk_obligation_amnt_bal_exist '
            || ': '
            || SQLERRM , l_module);
         RETURN 0;
   END chk_obligation_amnt_bal_exist;
-- =======================================
-- Function chk_deobligation_exist
  ------ chk_deobligation_exist is to validate the Obligation VS De-Obligation
-- =======================================
   FUNCTION chk_deobligation_exist (p_doc_num          VARCHAR2,
                                    p_fiscal_year      NUMBER,
                                    p_batch_number     VARCHAR2,
                                    p_can              VARCHAR2,		--CR NBSCH0003180
                                    p_object_class     VARCHAR2,		--CR NBSCH0003180
                                    p_amount           NUMBER,
                                    p_ope_detail_id    NUMBER)
      RETURN NUMBER
   IS
      l_obl_amnt_bal     NUMBER := 0;
      l_deobl_amnt_bal   NUMBER := 0;
      l_module varchar2(100) := 'chk_deobligation_exists';
   BEGIN
    logf('begin ' , l_module);

      l_obl_amnt_bal :=
         chk_obligation_amnt_bal_exist (p_doc_num,
                                        p_fiscal_year,
                                        p_batch_number,
                                        p_can,				--CR NBSCH0003180
                                        p_object_class,		--CR NBSCH0003180
                                        p_ope_detail_id);
      l_deobl_amnt_bal := l_obl_amnt_bal - p_amount;
      logf ('l_obl_amnt_bal : ' || l_obl_amnt_bal,l_module);
      logf ('p_amount : ' || p_amount, l_module);
      logf ('l_deobl_amnt_bal : ' || l_deobl_amnt_bal , l_Module);
      RETURN l_deobl_amnt_bal;
   EXCEPTION
      WHEN OTHERS
      THEN
         logf (
            ' Error in  Function chk_deobligation_exist ' || ': ' || SQLERRM , l_module);
         RETURN 0;
   END chk_deobligation_exist;
/* -- =======================================
-- Function validate_amnt  Validate_amnt is to validate the amount of all tcodes comming in the
    document number block
-- =============================================== ================== */
   FUNCTION validate_amnt (p_docnum          VARCHAR2, --P_FY           NUMBER,
                           p_file_name       VARCHAR2,
                           p_batch_number    VARCHAR2)
      RETURN BOOLEAN
   IS
      l_avg_amnt     NUMBER;
      l_tcode_amnt   NUMBER;
      l_bol_flag     BOOLEAN := TRUE;
      l_module varchar2(100) := 'validate_amnt';

   BEGIN
   --  logf('begin ' , l_module);

      SELECT NVL (amnt / cnt, 0)
        INTO l_avg_amnt
        FROM (SELECT (SELECT SUM (amnt)
                        FROM (  SELECT SUM (
                                          DECODE (reverse_code,
                                                  1, NVL (amount, 0),
                                                  2, -1 * NVL (amount, 0)))
                                          amnt,
                                       tcode
                                  FROM nihgl_ope_acct_dtls_int_tbl a
                                 WHERE     TRIM (
                                              document_ref || document_number) =
                                              p_docnum --AND can_fy          = P_FY
                                       AND file_name = p_file_name
                                       AND batch_number = p_batch_number
                              GROUP BY TRIM (document_ref || document_number),
                                       tcode))
                        amnt,
                     (SELECT COUNT (*)
                        FROM (  SELECT SUM (
                                          DECODE (reverse_code,
                                                  1, NVL (amount, 0),
                                                  2, -1 * NVL (amount, 0)))
                                          amnt,
                                       tcode
                                  FROM nihgl_ope_acct_dtls_int_tbl a
                                 WHERE     TRIM (
                                              document_ref || document_number) =
                                              p_docnum --AND can_fy          = P_FY
                                       AND file_name = p_file_name
                                       AND batch_number = p_batch_number
                              GROUP BY TRIM (document_ref || document_number),
                                       tcode))
                        cnt
                FROM DUAL);

      FOR c1rec
         IN (  SELECT SUM (
                         DECODE (reverse_code,
                                 1, NVL (amount, 0),
                                 2, -1 * NVL (amount, 0)))
                         amnt,
                      tcode
                 FROM nihgl_ope_acct_dtls_int_tbl a
                WHERE     TRIM (document_ref || document_number) = p_docnum --AND can_fy          = P_FY
                      AND file_name = p_file_name
                      AND batch_number = p_batch_number
             GROUP BY TRIM (document_ref || document_number), tcode)
      LOOP
         l_tcode_amnt := c1rec.amnt;
         EXIT;
      END LOOP;

      IF l_avg_amnt <> l_tcode_amnt
      THEN
         l_bol_flag := FALSE;
      END IF;

      RETURN (l_bol_flag);
   EXCEPTION
      WHEN OTHERS
      THEN
         logf (' Error in  Function validate_amnt ' || ': ' || SQLERRM,l_module);
         l_bol_flag := FALSE;
         RETURN (l_bol_flag);
   END validate_amnt;
-----------------------------------------------------------------------------------------------------------
 ---- Validate the Billing data from Feder System
-- ------------------------------------------------------------------------------------------------------
   PROCEDURE validate_billing_data (
      p_sob_id              NUMBER,
      p_batch_number        VARCHAR2,
      p_file_name           VARCHAR2,

      p_doc_ref             VARCHAR2 DEFAULT NULL,
      p_doc_number          VARCHAR2 DEFAULT NULL,
      p_can_fy              NUMBER DEFAULT NULL,
      p_called_from_form    VARCHAR2 DEFAULT NULL)

   IS      PRAGMA AUTONOMOUS_TRANSACTION;

    l_module varchar2(100) := 'validate_billing_data';

      CURSOR c_dtls1_cur
      IS
           SELECT UNIQUE ope_batch_id, batch_number
             FROM nihgl_ope_acct_btchs_int_tbl
            WHERE   record_status =
                         DECODE (p_called_from_form, 'Y', 'E', 'N')
                  AND batch_number = p_batch_number
                  AND file_name = p_file_name
         ORDER BY batch_number, file_name;

      CURSOR c_dtls2_cur (
         p_ope_batch_id    NUMBER,
         p_batch_number    VARCHAR2)
      IS

           SELECT a.ROWID,
                  a.*,
                  (SELECT NVL (MAX (period_year), a.can_fy)
                     FROM apps.gl_periods gp
                    WHERE a.expenditure_item_date BETWEEN gp.start_date
                                                      AND gp.end_date)
                     new_fy
             FROM nihgl_ope_acct_dtls_int_tbl a
            WHERE     ope_batch_id = p_ope_batch_id
                  AND batch_number = p_batch_number
                  AND file_name = p_file_name
                  AND NVL (document_ref, 'NULL') =
                         NVL (p_doc_ref, NVL (document_ref, 'NULL'))
                  AND NVL (document_number, 'NULL') =
                         NVL (p_doc_number, NVL (document_number, 'NULL'))
                  AND NVL (can_fy, 0) = NVL (p_can_fy, NVL (can_fy, 0))
                  AND record_status = 'N'
                ORDER BY batch_number, line_number;

      CURSOR c_docmas_cur
      IS
           SELECT UNIQUE
                  batch_number,
                  TRIM (document_ref || document_number) document_number,

                  document_ref doc_ref,
                  document_number doc_number,
                  can_fy,
                  file_name
             FROM nihgl_ope_acct_dtls_int_tbl
            WHERE     record_status = 'N'
                  AND batch_number = p_batch_number
                  AND file_name = p_file_name
                  AND NVL (document_ref, 'NULL') =
                         NVL (p_doc_ref, NVL (document_ref, 'NULL'))
                  AND NVL (document_number, 'NULL') =
                         NVL (p_doc_number, NVL (document_number, 'NULL'))
                  AND NVL (can_fy, 0) = NVL (p_can_fy, NVL (can_fy, 0))
         ORDER BY document_number;

      CURSOR c_docdtl_cur (
         p_batch_number       VARCHAR2,
         p_document_number    VARCHAR2,
         p_file_name          VARCHAR2)
      IS
           SELECT UNIQUE                                             --can_fy,
                  batch_number,
                  TRIM (document_ref || document_number) document_number,
                  tcode,
                  file_name
             FROM nihgl_ope_acct_dtls_int_tbl a
            WHERE                                          --can_fy = p_fy AND
                 batch_number = p_batch_number
                  AND TRIM (document_ref || document_number) =
                         p_document_number
                  AND record_status IN ('N', 'V')
                  AND file_name = p_file_name
         ORDER BY document_number, tcode;

      CURSOR c_org_cur
      IS
         SELECT hou.set_of_books_id set_of_books_id,
                hou.organization_id org_id,
                gsob.chart_of_accounts_id,
                fifs.concatenated_segment_delimiter segment_delimiter
           FROM fnd_id_flex_structures fifs,
                fnd_application fa,
                hr_operating_units hou,
                gl_sets_of_books gsob
          WHERE     gsob.set_of_books_id = hou.set_of_books_id
                AND fa.application_short_name = 'SQLGL'
                AND fifs.application_id = fa.application_id
                AND fifs.id_flex_code = 'GL#'
                AND fifs.id_flex_num = gsob.chart_of_accounts_id
                AND gsob.set_of_books_id = p_sob_id;

      l_status                   VARCHAR2 (1);
      l_project_id               pa_projects_all.project_id%TYPE;
      lb_success                 BOOLEAN;
      l_error_message            VARCHAR2 (2000);
      l_task_id                  NUMBER;
      l_ope_batch_id             nihgl_ope_acct_dtls_int_tbl.ope_batch_id%TYPE;
      l_ope_detail_id            nihgl_ope_acct_dtls_int_tbl.ope_detail_id%TYPE;
      l_record_id                nihgl_ope_acct_dtls_int_tbl.record_id%TYPE;
      l_exporg_id                pa_projects_all.carrying_out_organization_id%TYPE;
      l_attribute2               pa_projects_all.attribute2%TYPE;
      l_expenditure_type         pa_expenditure_types.expenditure_type%TYPE;
      l_financial_year           nihgl_ope_acct_dtls_int_tbl.can_fy%TYPE;
      l_fund_type                VARCHAR2 (30);
      l_transaction_code         VARCHAR2 (5);
      l_cnt                      NUMBER;
      l_obg_exist                NUMBER;
      l_obg_amnt_bal_exist       NUMBER;
      l_amnt_bal_exist           NUMBER;
      l_rev_amnt_exist           NUMBER;
      l_deoblg_amnt_exist        NUMBER;
      l_rev_line_chk_flag        VARCHAR2 (1);
      l_yesno                    VARCHAR2 (1);
      v_org_flag                 VARCHAR2 (1) := 'Y';
      x_set_of_books_id          NUMBER;
      x_org_id                   NUMBER;
      x_segment_delimiter        VARCHAR2 (10);
      --v_account                     VARCHAr2(50);
      l_ccid                     NUMBER;
      l_ccid_flex                VARCHAR2 (240);
      l_error_code               VARCHAR2 (20);
      c_org_rec                  c_org_cur%ROWTYPE;
      v_gl_rec_exist             BOOLEAN := FALSE;
      l_line_segments            fnd_flex_ext.segmentarray;
      l_proess_flag              BOOLEAN := FALSE;
      --Start of change 1 by Yash
      --Added the below variable
      l_tcode_mapping_cnt        NUMBER;
      -- Animesh
      l_balance_check_required   BOOLEAN := FALSE;
      l_batch_desc               VARCHAR2 (150) := NULL;
      l_je_category              VARCHAR2 (150) := NULL;
      l_flex_num                 NUMBER;
      v_period_fy                NUMBER;
      v_result_out               NUMBER;
      v_errnum                   NUMBER;
      v_errtext                  VARCHAR2 (240);
   l_can_fy            NUMBER;

   l_fp_fund_count NUMBER;
   l_imn VARCHAR2 (240);

   BEGIN


   logf ('starts .......' , l_module);
      OPEN c_org_cur;

      FETCH c_org_cur INTO c_org_rec;

      IF c_org_cur%NOTFOUND
      THEN
         v_org_flag := 'N';
      END IF;

      CLOSE c_org_cur;

      x_set_of_books_id := p_sob_id;
      x_org_id := c_org_rec.org_id;
      g_chart_of_accounts_id := c_org_rec.chart_of_accounts_id;
      x_segment_delimiter := c_org_rec.segment_delimiter;

      SELECT CHART_OF_ACCOUNTS_ID
        INTO l_flex_num
        FROM apps.gl_ledgers
       WHERE ledger_id = fnd_profile.VALUE ('GL_SET_OF_BKS_ID');

      SELECT COUNT (1)
        INTO g_total_segments
        FROM fnd_id_flex_segments
       WHERE id_flex_code = 'GL#' AND id_flex_num = l_flex_num;



      BEGIN

         l_batch_desc := NULL;
         /*CR NBSCH0002499
         IF batch_exists (p_batch_number   => p_batch_number,
                          p_batch_desc     => l_batch_desc,
                          p_je_category    => l_je_category)
         */
         IF g_batch_desc is not null                --CR NBSCH0002499
         THEN

            --l_batch_desc := TRIM (l_batch_desc);  --CR NBSCH0002499
            l_batch_desc := TRIM (g_batch_desc);    --CR NBSCH0002499
            logf('Batch Description :'||l_batch_desc,l_module);
         ELSE
            /*CR NBSCH0002499
            logf (
                  ' Batch Description is not Setup in Lookup'
               || ' NIHGL_OPE_BILLING_FILE_EMAIL for: '
               || p_batch_number,l_module);
            */
            logf (                                          --CR NBSCH0002499
                  ' Batch Description is not Setup in '     --CR NBSCH0002499
               || ' NFI.NIH_NFI_INTERFACES  for: '          --CR NBSCH0002499
               || p_batch_number,l_module);                 --CR NBSCH0002499
         END IF;
      /*CR NBSCH0002499
      EXCEPTION
         WHEN OTHERS
         THEN
            l_error_message :=
                  ' Batch Description is not Setup in Lookup'
               || ' NIHGL_OPE_BILLING_FILE_EMAIL for: '
               || p_batch_number
               || '-'
               || SUBSTR (SQLERRM, 1, 250);
            logf (l_error_message,l_module);
      */
      END;



      FOR c_dtls1_cur_rec IN c_dtls1_cur                       --Master Cursor
      LOOP
         v_gl_rec_exist := TRUE;
         derive_accounting_date (p_sob_id, c_dtls1_cur_rec.ope_batch_id);

         IF     g_accounting_date IS NOT NULL
            AND g_accounting_period IS NOT NULL
            AND g_account IS NOT NULL
            AND g_ope_je_source IS NOT NULL
            AND g_ope_je_category IS NOT NULL
            AND g_fund_type_tcode_lookup IS NOT NULL
         THEN
            FOR c_dtls2_cur_rec
               IN c_dtls2_cur (c_dtls1_cur_rec.ope_batch_id,
                               c_dtls1_cur_rec.batch_number)
            LOOP
               BEGIN
                  lc_errbuf := NULL;
                  lb_success := TRUE;
                  l_error_message := NULL;
                  l_record_id := c_dtls2_cur_rec.record_id;
                  l_exporg_id := NULL;
                  l_project_id := NULL;
                  l_task_id := NULL;
                  l_financial_year := NULL;
                  l_fund_type := NULL;
                  l_transaction_code := NULL;
                  l_expenditure_type := NULL;
                  l_yesno := NULL;
                  l_ope_batch_id := c_dtls2_cur_rec.ope_batch_id;
                  l_ope_detail_id := c_dtls2_cur_rec.ope_detail_id;

                  ---FCI Change starts

                  --------------------------validate -expenditure_item_date--------------------------------------
                  IF TRIM (c_dtls2_cur_rec.expenditure_item_date) IS NULL
                  THEN
                     --Start of change 1 by Yash
                     --Document Number value is Blank
                     l_error_message := g_err_message_tbl ('NIHOPETXN035');

                     --Check if responsibility name, batch number and error message is setup in the setup from
                     --else check if batch number and error message is setup in the setup form
                     --else use default (DT) batch and error message
                     IF g_txn_flow_flags_tbl.EXISTS (
                              g_resp_name
                           || ':'
                           || p_batch_number
                           || ':NIHOPETXN035')
                     THEN
                        --The responsibility name, batch number and error message exist in the setup form
                        --hence using the setup
                        --Check for file reject for the following error
                        IF NVL (
                              g_txn_flow_flags_tbl (
                                    g_resp_name
                                 || ':'
                                 || p_batch_number
                                 || ':NIHOPETXN035').file_reject,
                              'N') = 'Y'
                        THEN
                           g_file_reject_flag := TRUE;
                        END IF;



                        --Check the reject error transaction flag for the following error
                        IF NVL (
                              g_txn_flow_flags_tbl (
                                    g_resp_name
                                 || ':'
                                 || p_batch_number
                                 || ':NIHOPETXN035').reject_error_txn,
                              'N') = 'Y'
                        THEN
                           g_index :=
                                 TRIM (c_dtls2_cur_rec.document_ref)
                              || ':'
                              || TRIM (c_dtls2_cur_rec.document_number)
                              || ':'
                              || TRIM (c_dtls2_cur_rec.can_fy);

                           --If the index does not exist then include the index
                           IF NOT g_txn_reject_tbl.EXISTS (g_index)
                           THEN
                              g_txn_reject_tbl (g_index) := 1;
                           END IF;
                        END IF;
                     ELSIF g_txn_flow_flags_tbl.EXISTS (
                              p_batch_number || ':NIHOPETXN035')
                     THEN
                        --The batch number and error message exist in the setup form
                        --hence using the setup
                        --Check for file reject for the following error
                        IF NVL (
                              g_txn_flow_flags_tbl (
                                 p_batch_number || ':NIHOPETXN035').file_reject,
                              'N') = 'Y'
                        THEN
                           g_file_reject_flag := TRUE;
                        END IF;


                        --Check the reject error transaction flag for the following error
                        IF NVL (
                              g_txn_flow_flags_tbl (
                                 p_batch_number || ':NIHOPETXN035').reject_error_txn,
                              'N') = 'Y'
                        THEN
                           g_index :=
                                 TRIM (c_dtls2_cur_rec.document_ref)
                              || ':'
                              || TRIM (c_dtls2_cur_rec.document_number)
                              || ':'
                              || TRIM (c_dtls2_cur_rec.can_fy);

                           --If the index does not exist then include the index
                           IF NOT g_txn_reject_tbl.EXISTS (g_index)
                           THEN
                              g_txn_reject_tbl (g_index) := 1;
                           END IF;
                        END IF;

                     ELSE
                        --The batch number and error message does not exist in the setup form
                        --hence using the default DT setup
                        --Check for file reject for the following error
                        IF NVL (
                              g_txn_flow_flags_tbl ('DT:NIHOPETXN035').file_reject,
                              'N') = 'Y'
                        THEN
                           g_file_reject_flag := TRUE;
                        END IF;



                        --Check the reject error transaction flag for the following error
                        IF NVL (
                              g_txn_flow_flags_tbl ('DT:NIHOPETXN035').reject_error_txn,
                              'N') = 'Y'
                        THEN
                           g_index :=
                                 TRIM (c_dtls2_cur_rec.document_ref)
                              || ':'
                              || TRIM (c_dtls2_cur_rec.document_number)
                              || ':'
                              || TRIM (c_dtls2_cur_rec.can_fy);

                           --If the index does not exist then include the index
                           IF NOT g_txn_reject_tbl.EXISTS (g_index)
                           THEN
                              g_txn_reject_tbl (g_index) := 1;
                           END IF;
                        END IF;
                     END IF;

                     --End of change 1
                     record_error (l_error_message,
                                   l_ope_batch_id,
                                   l_ope_detail_id,
                                   l_record_id);
                     lb_success := FALSE;
                  END IF;

       --Added by Srinivas Rayankula on 10/30/2023    -- Start
      l_can_fy := NULL;

        IF  TRIM (c_dtls2_cur_rec.reverse_code) = '2' THEN
           IF check_royalty_can(c_dtls2_cur_rec.can,c_dtls2_cur_rec.task_number) THEN
             l_can_fy := c_dtls2_cur_rec.can_fy;
          ELSE
           l_can_fy := TRIM (
              TO_NUMBER (
                SUBSTR (c_dtls2_cur_rec.task_number,
                1,
                4)));
         END IF;

      ELSE
         l_can_fy := TRIM (
              TO_NUMBER (
                SUBSTR (c_dtls2_cur_rec.task_number,
            1,
            4)));

      END IF;


            --Added by Srinivas Rayankula on 10/30/2023    -- End



                  /*Updating can_fy based on expenditure_item_date in table nihgl_ope_acct_dtls_int_tbl  */

                  /* Select Fiscal Year */

                  BEGIN
                     SELECT period_year
                       INTO v_period_fy
                       FROM gl_period_statuses
                      WHERE     application_id = 201
                            AND ADD_MONTHS (
                                   TRUNC (
                                      c_dtls2_cur_rec.expenditure_item_date),
                                   0) BETWEEN start_date
                                          AND end_date
                            AND adjustment_period_flag = 'N';

                     UPDATE nihgl_ope_acct_dtls_int_tbl
                        SET can_fy = l_can_fy
                 -- Modified by Srinivas Rayankula on 10/30/2023
                      WHERE ope_detail_id = l_ope_detail_id;

                     COMMIT;
                  END;

                  -- FCI Change ends


         -------------------------------Validate Document Number is not null----------------------------------------

                  IF TRIM (c_dtls2_cur_rec.document_number) IS NULL
                  THEN
                     --Start of change 1 by Yash
                     --Document Number value is Blank
                     l_error_message := g_err_message_tbl ('NIHOPETXN001');

                     --Check if responsibility name, batch number and error message is setup in the setup from
                     --else check if batch number and error message is setup in the setup form
                     --else use default (DT) batch and error message
                     IF g_txn_flow_flags_tbl.EXISTS (
                              g_resp_name
                           || ':'
                           || p_batch_number
                           || ':NIHOPETXN001')
                     THEN
                        --The responsibility name, batch number and error message exist in the setup form
                        --hence using the setup
                        --Check for file reject for the following error
                        IF NVL (
                              g_txn_flow_flags_tbl (
                                    g_resp_name
                                 || ':'
                                 || p_batch_number
                                 || ':NIHOPETXN001').file_reject,
                              'N') = 'Y'
                        THEN
                           g_file_reject_flag := TRUE;
                        END IF;



                        --Check the reject error transaction flag for the following error
                        IF NVL (
                              g_txn_flow_flags_tbl (
                                    g_resp_name
                                 || ':'
                                 || p_batch_number
                                 || ':NIHOPETXN001').reject_error_txn,
                              'N') = 'Y'
                        THEN
                           g_index :=
                                 TRIM (c_dtls2_cur_rec.document_ref)
                              || ':'
                              || TRIM (c_dtls2_cur_rec.document_number)
                              || ':'
                              || TRIM (c_dtls2_cur_rec.can_fy);

                           --If the index does not exist then include the index
                           IF NOT g_txn_reject_tbl.EXISTS (g_index)
                           THEN
                              g_txn_reject_tbl (g_index) := 1;
                           END IF;
                        END IF;

                     ELSIF g_txn_flow_flags_tbl.EXISTS (
                              p_batch_number || ':NIHOPETXN001')
                     THEN
                        --The batch number and error message exist in the setup form
                        --hence using the setup
                        --Check for file reject for the following error
                        IF NVL (
                              g_txn_flow_flags_tbl (
                                 p_batch_number || ':NIHOPETXN001').file_reject,
                              'N') = 'Y'
                        THEN
                           g_file_reject_flag := TRUE;
                        END IF;

                        --Check the reject error transaction flag for the following error
                        IF NVL (
                              g_txn_flow_flags_tbl (
                                 p_batch_number || ':NIHOPETXN001').reject_error_txn,
                              'N') = 'Y'
                        THEN
                           g_index :=
                                 TRIM (c_dtls2_cur_rec.document_ref)
                              || ':'
                              || TRIM (c_dtls2_cur_rec.document_number)
                              || ':'
                              || TRIM (c_dtls2_cur_rec.can_fy);

                           --If the index does not exist then include the index
                           IF NOT g_txn_reject_tbl.EXISTS (g_index)
                           THEN
                              g_txn_reject_tbl (g_index) := 1;
                           END IF;
                        END IF;

                     ELSE
                        --The batch number and error message does not exist in the setup form
                        --hence using the default DT setup
                        --Check for file reject for the following error
                        IF NVL (
                              g_txn_flow_flags_tbl ('DT:NIHOPETXN001').file_reject,
                              'N') = 'Y'
                        THEN
                           g_file_reject_flag := TRUE;
                        END IF;



                        --Check the reject error transaction flag for the following error
                        IF NVL (
                              g_txn_flow_flags_tbl ('DT:NIHOPETXN001').reject_error_txn,
                              'N') = 'Y'
                        THEN
                           g_index :=
                                 TRIM (c_dtls2_cur_rec.document_ref)
                              || ':'
                              || TRIM (c_dtls2_cur_rec.document_number)
                              || ':'
                              || TRIM (c_dtls2_cur_rec.can_fy);

                           --If the index does not exist then include the index
                           IF NOT g_txn_reject_tbl.EXISTS (g_index)
                           THEN
                              g_txn_reject_tbl (g_index) := 1;
                           END IF;
                        END IF;
                     END IF;

                     --End of change 1
                     record_error (l_error_message,
                                   l_ope_batch_id,
                                   l_ope_detail_id,
                                   l_record_id);
                     lb_success := FALSE;
                  END IF;


           ------------------------------Validate Financial Year is not null ---------------------------------------------

                  IF TRIM (c_dtls2_cur_rec.can_fy) IS NULL
                  THEN
                     --Start of change 1 by Yash
                     --Fiscal Year value is Blank
                     l_error_message := g_err_message_tbl ('NIHOPETXN002');

                     --Check if responsibility name, batch number and error message is setup in the setup from
                     --else check if batch number and error message is setup in the setup form
                     --else use default (DT) batch and error message
                     IF g_txn_flow_flags_tbl.EXISTS (
                              g_resp_name
                           || ':'
                           || p_batch_number
                           || ':NIHOPETXN002')
                     THEN
                        --The responsibility name, batch number and error message exist in the setup form
                        --hence using the setup
                        --Check for file reject for the following error
                        IF NVL (
                              g_txn_flow_flags_tbl (
                                    g_resp_name
                                 || ':'
                                 || p_batch_number
                                 || ':NIHOPETXN002').file_reject,
                              'N') = 'Y'
                        THEN
                           g_file_reject_flag := TRUE;
                        END IF;



                        --Check the reject error transaction flag for the following error
                        IF NVL (
                              g_txn_flow_flags_tbl (
                                    g_resp_name
                                 || ':'
                                 || p_batch_number
                                 || ':NIHOPETXN002').reject_error_txn,
                              'N') = 'Y'
                        THEN
                           g_index :=
                                 TRIM (c_dtls2_cur_rec.document_ref)
                              || ':'
                              || TRIM (c_dtls2_cur_rec.document_number)
                              || ':'
                              || TRIM (c_dtls2_cur_rec.can_fy);

                           --If the index does not exist then include the index
                           IF NOT g_txn_reject_tbl.EXISTS (g_index)
                           THEN
                              g_txn_reject_tbl (g_index) := 1;
                           END IF;
                        END IF;

                     ELSIF g_txn_flow_flags_tbl.EXISTS (
                              p_batch_number || ':NIHOPETXN002')
                     THEN
                        --The batch number and error message exist in the setup form
                        --hence using the setup
                        --Check for file reject for the following error
                        IF NVL (
                              g_txn_flow_flags_tbl (
                                 p_batch_number || ':NIHOPETXN002').file_reject,
                              'N') = 'Y'
                        THEN
                           g_file_reject_flag := TRUE;
                        END IF;


                        --Check the reject error transaction flag for the following error
                        IF NVL (
                              g_txn_flow_flags_tbl (
                                 p_batch_number || ':NIHOPETXN002').reject_error_txn,
                              'N') = 'Y'
                        THEN
                           g_index :=
                                 TRIM (c_dtls2_cur_rec.document_ref)
                              || ':'
                              || TRIM (c_dtls2_cur_rec.document_number)
                              || ':'
                              || TRIM (c_dtls2_cur_rec.can_fy);

                           --If the index does not exist then include the index
                           IF NOT g_txn_reject_tbl.EXISTS (g_index)
                           THEN
                              g_txn_reject_tbl (g_index) := 1;
                           END IF;
                        END IF;

                     ELSE
                        --The batch number and error message does not exist in the setup form
                        --hence using the default DT setup
                        --Check for file reject for the following error
                        IF NVL (
                              g_txn_flow_flags_tbl ('DT:NIHOPETXN002').file_reject,
                              'N') = 'Y'
                        THEN
                           g_file_reject_flag := TRUE;
                        END IF;



                        --Check the reject error transaction flag for the following error
                        IF NVL (
                              g_txn_flow_flags_tbl ('DT:NIHOPETXN002').reject_error_txn,
                              'N') = 'Y'
                        THEN
                           g_index :=
                                 TRIM (c_dtls2_cur_rec.document_ref)
                              || ':'
                              || TRIM (c_dtls2_cur_rec.document_number)
                              || ':'
                              || TRIM (c_dtls2_cur_rec.can_fy);

                           --If the index does not exist then include the index
                           IF NOT g_txn_reject_tbl.EXISTS (g_index)
                           THEN
                              g_txn_reject_tbl (g_index) := 1;
                           END IF;
                        END IF;
                     END IF;

                     --End of change 1
                     record_error (l_error_message,
                                   l_ope_batch_id,
                                   l_ope_detail_id,
                                   l_record_id);
                     lb_success := FALSE;
                  ELSIF NOT validate_fy (
                               p_sob_id          => c_dtls2_cur_rec.set_of_books_id,
                               p_can             => c_dtls2_cur_rec.can, --CR 42504
                               p_fy              => c_dtls2_cur_rec.can_fy,
                               p_doc_ref         => c_dtls2_cur_rec.document_ref,
                               p_doc_number      => c_dtls2_cur_rec.document_number,
                               p_batch_number    => c_dtls2_cur_rec.batch_number,
                               p_error_message   => l_error_message)
                  THEN
                     record_error (l_error_message,
                                   l_ope_batch_id,
                                   l_ope_detail_id,
                                   l_record_id);
                     lb_success := FALSE;
                  END IF;


           ---------------------------alidate Object Class is not null ----------------------

                  IF TRIM (c_dtls2_cur_rec.object_class) IS NULL
                  THEN
                     --Start of change 1 by Yash
                     --Object Class value is blank
                     l_error_message := g_err_message_tbl ('NIHOPETXN006');

                     --Check if responsibility name, batch number and error message is setup in the setup from
                     --else check if batch number and error message is setup in the setup from
                     --else use default (DT) batch and error message
                     IF g_txn_flow_flags_tbl.EXISTS (
                              g_resp_name
                           || ':'
                           || p_batch_number
                           || ':NIHOPETXN006')
                     THEN
                        --The responsibility name, batch number and error message exist in the setup form
                        --hence using the setup
                        --Check for file reject for the following error
                        IF NVL (
                              g_txn_flow_flags_tbl (
                                    g_resp_name
                                 || ':'
                                 || p_batch_number
                                 || ':NIHOPETXN006').file_reject,
                              'N') = 'Y'
                        THEN
                           g_file_reject_flag := TRUE;
                        END IF;



                        --Check the reject error transaction flag for the following error
                        IF NVL (
                              g_txn_flow_flags_tbl (
                                    g_resp_name
                                 || ':'
                                 || p_batch_number
                                 || ':NIHOPETXN006').reject_error_txn,
                              'N') = 'Y'
                        THEN
                           g_index :=
                                 TRIM (c_dtls2_cur_rec.document_ref)
                              || ':'
                              || TRIM (c_dtls2_cur_rec.document_number)
                              || ':'
                              || TRIM (c_dtls2_cur_rec.can_fy);

                           --If the index does not exist then include the index
                           IF NOT g_txn_reject_tbl.EXISTS (g_index)
                           THEN
                              g_txn_reject_tbl (g_index) := 1;
                           END IF;
                        END IF;

                     ELSIF g_txn_flow_flags_tbl.EXISTS (
                              p_batch_number || ':NIHOPETXN006')
                     THEN
                        --The batch number and error message exist in the setup form
                        --hence using the setup
                        --Check for file reject for the following error
                        IF NVL (
                              g_txn_flow_flags_tbl (
                                 p_batch_number || ':NIHOPETXN006').file_reject,
                              'N') = 'Y'
                        THEN
                           g_file_reject_flag := TRUE;
                        END IF;


                        --Check the reject error transaction flag for the following error
                        IF NVL (
                              g_txn_flow_flags_tbl (
                                 p_batch_number || ':NIHOPETXN006').reject_error_txn,
                              'N') = 'Y'
                        THEN
                           g_index :=
                                 TRIM (c_dtls2_cur_rec.document_ref)
                              || ':'
                              || TRIM (c_dtls2_cur_rec.document_number)
                              || ':'
                              || TRIM (c_dtls2_cur_rec.can_fy);

                           --If the index does not exist then include the index
                           IF NOT g_txn_reject_tbl.EXISTS (g_index)
                           THEN
                              g_txn_reject_tbl (g_index) := 1;
                           END IF;
                        END IF;

                     ELSE
                        --The batch number and error message does not exist in the setup form
                        --hence using the default DT setup
                        --Check for file reject for the following error
                        IF NVL (
                              g_txn_flow_flags_tbl ('DT:NIHOPETXN006').file_reject,
                              'N') = 'Y'
                        THEN
                           g_file_reject_flag := TRUE;
                        END IF;



                        --Check the reject error transaction flag for the following error
                        IF NVL (
                              g_txn_flow_flags_tbl ('DT:NIHOPETXN006').reject_error_txn,
                              'N') = 'Y'
                        THEN
                           g_index :=
                                 TRIM (c_dtls2_cur_rec.document_ref)
                              || ':'
                              || TRIM (c_dtls2_cur_rec.document_number)
                              || ':'
                              || TRIM (c_dtls2_cur_rec.can_fy);

                           --If the index does not exist then include the index
                           IF NOT g_txn_reject_tbl.EXISTS (g_index)
                           THEN
                              g_txn_reject_tbl (g_index) := 1;
                           END IF;
                        END IF;
                     END IF;

                     --End of change 1
                     record_error (l_error_message,
                                   l_ope_batch_id,
                                   l_ope_detail_id,
                                   l_record_id);
                     lb_success := FALSE;
                  ELSIF NOT validate_object_class (
                               p_object_class       => c_dtls2_cur_rec.object_class,
                               p_expenditure_type   => l_expenditure_type)
                  THEN
                     --Start of change 1 by Yash
                     --Object Class is Invalid
                     l_error_message := g_err_message_tbl ('NIHOPETXN007');

                     --Check if responsibility name, batch number and error message is setup in the setup from
                     --else check if batch number and error message is setup in the setup form
                     --else use default (DT) batch and error message
                     IF g_txn_flow_flags_tbl.EXISTS (
                              g_resp_name
                           || ':'
                           || p_batch_number
                           || ':NIHOPETXN007')
                     THEN
                        --The responsibility name, batch number and error message exist in the setup form
                        --hence using the setup
                        --Check for file reject for the following error
                        IF NVL (
                              g_txn_flow_flags_tbl (
                                    g_resp_name
                                 || ':'
                                 || p_batch_number
                                 || ':NIHOPETXN007').file_reject,
                              'N') = 'Y'
                        THEN
                           g_file_reject_flag := TRUE;
                        END IF;

                        --Check the reject error transaction flag for the following error
                        IF NVL (
                              g_txn_flow_flags_tbl (
                                    g_resp_name
                                 || ':'
                                 || p_batch_number
                                 || ':NIHOPETXN007').reject_error_txn,
                              'N') = 'Y'
                        THEN
                           g_index :=
                                 TRIM (c_dtls2_cur_rec.document_ref)
                              || ':'
                              || TRIM (c_dtls2_cur_rec.document_number)
                              || ':'
                              || TRIM (c_dtls2_cur_rec.can_fy);

                           --If the index does not exist then include the index
                           IF NOT g_txn_reject_tbl.EXISTS (g_index)
                           THEN
                              g_txn_reject_tbl (g_index) := 1;
                           END IF;
                        END IF;
                     ELSIF g_txn_flow_flags_tbl.EXISTS (
                              p_batch_number || ':NIHOPETXN007')
                     THEN
                        --The batch number and error message exist in the setup form
                        --hence using the setup
                        --Check for file reject for the following error
                        IF NVL (
                              g_txn_flow_flags_tbl (
                                 p_batch_number || ':NIHOPETXN007').file_reject,
                              'N') = 'Y'
                        THEN
                           g_file_reject_flag := TRUE;
                        END IF;

                        --Check the reject error transaction flag for the following error
                        IF NVL (
                              g_txn_flow_flags_tbl (
                                 p_batch_number || ':NIHOPETXN007').reject_error_txn,
                              'N') = 'Y'
                        THEN
                           g_index :=
                                 TRIM (c_dtls2_cur_rec.document_ref)
                              || ':'
                              || TRIM (c_dtls2_cur_rec.document_number)
                              || ':'
                              || TRIM (c_dtls2_cur_rec.can_fy);

                           --If the index does not exist then include the index
                           IF NOT g_txn_reject_tbl.EXISTS (g_index)
                           THEN
                              g_txn_reject_tbl (g_index) := 1;
                           END IF;
                        END IF;
                     ELSE
                        --The batch number and error message does not exist in the setup form
                        --hence using the default DT setup
                        --Check for file reject for the following error
                        IF NVL (
                              g_txn_flow_flags_tbl ('DT:NIHOPETXN007').file_reject,
                              'N') = 'Y'
                        THEN
                           g_file_reject_flag := TRUE;
                        END IF;


                        --Check the reject error transaction flag for the following error
                        IF NVL (
                              g_txn_flow_flags_tbl ('DT:NIHOPETXN007').reject_error_txn,
                              'N') = 'Y'
                        THEN
                           g_index :=
                                 TRIM (c_dtls2_cur_rec.document_ref)
                              || ':'
                              || TRIM (c_dtls2_cur_rec.document_number)
                              || ':'
                              || TRIM (c_dtls2_cur_rec.can_fy);

                           --If the index does not exist then include the index
                           IF NOT g_txn_reject_tbl.EXISTS (g_index)
                           THEN
                              g_txn_reject_tbl (g_index) := 1;
                           END IF;
                        END IF;
                     END IF;

                     --End of change 1
                     record_error (l_error_message,
                                   l_ope_batch_id,
                                   l_ope_detail_id,
                                   l_record_id);
                     lb_success := FALSE;
                  END IF;


         ---------------------- Validate CAN is not null and Valid------------

                  IF TRIM (c_dtls2_cur_rec.can) IS NULL
                  THEN
                     --Start of change 1 by Yash
                     --CAN value is blank
                     l_error_message := g_err_message_tbl ('NIHOPETXN008');

                     --Check if responsibility name, batch number and error message is setup in the setup from
                     --else check if batch number and error message is setup in the setup form
                     --else use default (DT) batch and error message
                     IF g_txn_flow_flags_tbl.EXISTS (
                              g_resp_name
                           || ':'
                           || p_batch_number
                           || ':NIHOPETXN008')
                     THEN
                        --The responsibility name, batch number and error message exist in the setup form
                        --hence using the setup
                        --Check for file reject for the following error
                        IF NVL (
                              g_txn_flow_flags_tbl (
                                    g_resp_name
                                 || ':'
                                 || p_batch_number
                                 || ':NIHOPETXN008').file_reject,
                              'N') = 'Y'
                        THEN
                           g_file_reject_flag := TRUE;
                        END IF;



                        --Check the reject error transaction flag for the following error
                        IF NVL (
                              g_txn_flow_flags_tbl (
                                    g_resp_name
                                 || ':'
                                 || p_batch_number
                                 || ':NIHOPETXN008').reject_error_txn,
                              'N') = 'Y'
                        THEN
                           g_index :=
                                 TRIM (c_dtls2_cur_rec.document_ref)
                              || ':'
                              || TRIM (c_dtls2_cur_rec.document_number)
                              || ':'
                              || TRIM (c_dtls2_cur_rec.can_fy);

                           --If the index does not exist then include the index
                           IF NOT g_txn_reject_tbl.EXISTS (g_index)
                           THEN
                              g_txn_reject_tbl (g_index) := 1;
                           END IF;
                        END IF;

                     ELSIF g_txn_flow_flags_tbl.EXISTS (
                              p_batch_number || ':NIHOPETXN008')
                     THEN
                        --The batch number and error message exist in the setup form
                        --hence using the setup
                        --Check for file reject for the following error
                        IF NVL (
                              g_txn_flow_flags_tbl (
                                 p_batch_number || ':NIHOPETXN008').file_reject,
                              'N') = 'Y'
                        THEN
                           g_file_reject_flag := TRUE;
                        END IF;



                        --Check the reject error transaction flag for the following error
                        IF NVL (
                              g_txn_flow_flags_tbl (
                                 p_batch_number || ':NIHOPETXN008').reject_error_txn,
                              'N') = 'Y'
                        THEN
                           g_index :=
                                 TRIM (c_dtls2_cur_rec.document_ref)
                              || ':'
                              || TRIM (c_dtls2_cur_rec.document_number)
                              || ':'
                              || TRIM (c_dtls2_cur_rec.can_fy);

                           --If the index does not exist then include the index
                           IF NOT g_txn_reject_tbl.EXISTS (g_index)
                           THEN
                              g_txn_reject_tbl (g_index) := 1;
                           END IF;
                        END IF;

                     ELSE
                        --The batch number and error message does not exist in the setup form
                        --hence using the default DT setup
                        --Check for file reject for the following error
                        IF NVL (
                              g_txn_flow_flags_tbl ('DT:NIHOPETXN008').file_reject,
                              'N') = 'Y'
                        THEN
                           g_file_reject_flag := TRUE;
                        END IF;



                        --Check the reject error transaction flag for the following error
                        IF NVL (
                              g_txn_flow_flags_tbl ('DT:NIHOPETXN008').reject_error_txn,
                              'N') = 'Y'
                        THEN
                           g_index :=
                                 TRIM (c_dtls2_cur_rec.document_ref)
                              || ':'
                              || TRIM (c_dtls2_cur_rec.document_number)
                              || ':'
                              || TRIM (c_dtls2_cur_rec.can_fy);

                           --If the index does not exist then include the index
                           IF NOT g_txn_reject_tbl.EXISTS (g_index)
                           THEN
                              g_txn_reject_tbl (g_index) := 1;
                           END IF;
                        END IF;
                     END IF;

                     --End of change 1
                     record_error (l_error_message,
                                   l_ope_batch_id,
                                   l_ope_detail_id,
                                   l_record_id);
                     lb_success := FALSE;
                  ELSIF NOT validate_can (
                               p_can                            => c_dtls2_cur_rec.can,
                               p_can_fyr                        => c_dtls2_cur_rec.can_fy, -- FCI Change
                               p_exp_item_dt                    => c_dtls2_cur_rec.expenditure_item_date, -- FCI Change 07/31/19
                               p_project_id                     => l_project_id,
                               --   p_task_id                        => l_task_id,
                               p_carrying_out_organization_id   => l_exporg_id,
                               p_attribute2                     => l_attribute2)
                  THEN
                     --Start of change 1 by Yash
                     --CAN is Invalid
                     logf ('chech NIHOPETXN009 1' , l_module);
                     logf ('chech NIHOPETXN009 l_error_message: '|| l_error_message , l_module);
                     l_error_message := g_err_message_tbl ('NIHOPETXN009');
                     logf ('chech NIHOPETXN009 2' , l_module);
                     logf ('chech NIHOPETXN009 l_error_message: '|| l_error_message , l_module);

                     --Check if responsibility name, batch number and error message is setup in the setup from
                     --else check if batch number and error message is setup in the setup form
                     --else use default (DT) batch and error message
                     IF g_txn_flow_flags_tbl.EXISTS (
                              g_resp_name
                           || ':'
                           || p_batch_number
                           || ':NIHOPETXN009')
                     THEN

                     logf ('chech NIHOPETXN009 3' , l_module);
                        --The responsibility name, batch number and error message exist in the setup form
                        --hence using the setup
                        --Check for file reject for the following error
                        IF NVL (
                              g_txn_flow_flags_tbl (
                                    g_resp_name
                                 || ':'
                                 || p_batch_number
                                 || ':NIHOPETXN009').file_reject,
                              'N') = 'Y'
                        THEN
                            logf ('chech NIHOPETXN009 4' , l_module);
                           g_file_reject_flag := TRUE;
                        END IF;


                        --Check the reject error transaction flag for the following error
                        IF NVL (
                              g_txn_flow_flags_tbl (
                                    g_resp_name
                                 || ':'
                                 || p_batch_number
                                 || ':NIHOPETXN009').reject_error_txn,
                              'N') = 'Y'
                        THEN
                        logf ('chech NIHOPETXN009 5' , l_module);
                           g_index :=
                                 TRIM (c_dtls2_cur_rec.document_ref)
                              || ':'
                              || TRIM (c_dtls2_cur_rec.document_number)
                              || ':'
                              || TRIM (c_dtls2_cur_rec.can_fy);

                           --If the index does not exist then include the index
                           IF NOT g_txn_reject_tbl.EXISTS (g_index)
                           THEN
                           logf ('chech NIHOPETXN009 6' , l_module);
                              g_txn_reject_tbl (g_index) := 1;
                           END IF;
                        END IF;

                     ELSIF g_txn_flow_flags_tbl.EXISTS (
                              p_batch_number || ':NIHOPETXN009')
                     THEN
                        --The batch number and error message exist in the setup form
                        --hence using the setup
                        --Check for file reject for the following error
                        logf ('chech NIHOPETXN009 7' , l_module);
                        IF NVL (
                              g_txn_flow_flags_tbl (
                                 p_batch_number || ':NIHOPETXN009').file_reject,
                              'N') = 'Y'
                        THEN
                        logf ('chech NIHOPETXN009 8' , l_module);
                           g_file_reject_flag := TRUE;
                        END IF;



                        --Check the reject error transaction flag for the following error
                        IF NVL (
                              g_txn_flow_flags_tbl (
                                 p_batch_number || ':NIHOPETXN009').reject_error_txn,
                              'N') = 'Y'
                        THEN
                        logf ('chech NIHOPETXN009 9' , l_module);
                           g_index :=
                                 TRIM (c_dtls2_cur_rec.document_ref)
                              || ':'
                              || TRIM (c_dtls2_cur_rec.document_number)
                              || ':'
                              || TRIM (c_dtls2_cur_rec.can_fy);

                           --If the index does not exist then include the index
                           IF NOT g_txn_reject_tbl.EXISTS (g_index)
                           THEN
                           logf ('chech NIHOPETXN009 10' , l_module);
                              g_txn_reject_tbl (g_index) := 1;
                           END IF;
                        END IF;

                     ELSE
                        --The batch number and error message does not exist in the setup form
                        --hence using the default DT setup
                        --Check for file reject for the following error
                        logf ('chech NIHOPETXN009 11' , l_module);
                        IF NVL (
                              g_txn_flow_flags_tbl ('DT:NIHOPETXN009').file_reject,
                              'N') = 'Y'
                        THEN
                        logf ('chech NIHOPETXN009 12' , l_module);
                           g_file_reject_flag := TRUE;
                        END IF;



                        --Check the reject error transaction flag for the following error
                        IF NVL (
                              g_txn_flow_flags_tbl ('DT:NIHOPETXN009').reject_error_txn,
                              'N') = 'Y'
                        THEN
                        logf ('chech NIHOPETXN009 13' , l_module);
                           g_index :=
                                 TRIM (c_dtls2_cur_rec.document_ref)
                              || ':'
                              || TRIM (c_dtls2_cur_rec.document_number)
                              || ':'
                              || TRIM (c_dtls2_cur_rec.can_fy);

                           --If the index does not exist then include the index
                           IF NOT g_txn_reject_tbl.EXISTS (g_index)
                           THEN
                           logf ('chech NIHOPETXN009 14' , l_module);
                              g_txn_reject_tbl (g_index) := 1;
                           END IF;
                        END IF;
                     END IF;

                     logf ('chech NIHOPETXN009 15 l_error_message: '|| l_error_message , l_module);
                     --End of change 1
                     record_error (l_error_message,
                                   l_ope_batch_id,
                                   l_ope_detail_id,
                                   l_record_id);
                     lb_success := FALSE;
                  END IF;

									-- Start NBSCH0002910--
    ----------------------------------------validate Fund in CAN---------------------------------------------------------------------

		IF c_dtls2_cur_rec.can IS NOT null
		then
			  select
					substr(ppa.attribute2, 1, 6)
			  into
					l_imn
			  from
					apps.pa_projects_all ppa
			  where
					name = c_dtls2_cur_rec.can;

	     IF l_imn = '080566' then
            select
                  count(*)
                into
                    l_fp_fund_count
                from
                    apps.pa_projects_all ppa,
                    apps.FV_FUND_PARAMETERS fp,
                    apps.FV_TREASURY_SYMBOLS fts
                where
                        fts.TREASURY_SYMBOL_ID = fp.TREASURY_SYMBOL_ID
                    and fp.fund_value like substr(ppa.attribute2, 1, 6) || substr(c_dtls2_cur_rec.task_number, 1, 4) || '_' ||substr(ppa.attribute2, 12, 2) || '%'
                    and ppa.name = c_dtls2_cur_rec.can;
		ELSE
               select
                    count(*)
                into
                    l_fp_fund_count
                from
                    apps.pa_projects_all ppa,
                    apps.FV_FUND_PARAMETERS fp,
                    apps.FV_TREASURY_SYMBOLS fts
                where
                        fts.TREASURY_SYMBOL_ID = fp.TREASURY_SYMBOL_ID
                    and fp.fund_value like substr(ppa.attribute2, 1, 6) || substr(c_dtls2_cur_rec.task_number, 1, 4) || substr(ppa.attribute2, 11, 3) || '%'
                    and ppa.name = c_dtls2_cur_rec.can;
         END IF;


			IF (l_fp_fund_count = 0 )
            THEN
                l_error_message :=
                    g_err_message_tbl ('NIHOPETXN038');

                logf (l_error_message,l_module);
                record_error (l_error_message,
                              l_ope_batch_id,
                              l_ope_detail_id,
                              l_record_id);
                lb_success := FALSE;
             END IF;

		END IF;

		-- End NBSCH0002910--

    ---------------------------------------Validate TASK_NUMBER is not null and Valid--------------------------------------------------

                  IF TRIM (c_dtls2_cur_rec.task_number) IS NULL
                  THEN
                     --Start of change 1 by Yash
                     --TASK_NUMBER value is blank

                     logf ('chech NIHOPETXN033 1 l_error_message: '|| l_error_message , l_module);
                     l_error_message := g_err_message_tbl ('NIHOPETXN033');

                     --Check if responsibility name, batch number and error message is setup in the setup from
                     --else check if batch number and error message is setup in the setup form
                     --else use default (DT) batch and error message
                     IF g_txn_flow_flags_tbl.EXISTS (
                              g_resp_name
                           || ':'
                           || p_batch_number
                           || ':NIHOPETXN033')
                     THEN
                        --The responsibility name, batch number and error message exist in the setup form
                        --hence using the setup
                        --Check for file reject for the following error
                        IF NVL (
                              g_txn_flow_flags_tbl (
                                    g_resp_name
                                 || ':'
                                 || p_batch_number
                                 || ':NIHOPETXN033').file_reject,
                              'N') = 'Y'
                        THEN
                           g_file_reject_flag := TRUE;
                        END IF;



                        --Check the reject error transaction flag for the following error
                        IF NVL (
                              g_txn_flow_flags_tbl (
                                    g_resp_name
                                 || ':'
                                 || p_batch_number
                                 || ':NIHOPETXN033').reject_error_txn,
                              'N') = 'Y'
                        THEN
                           g_index :=
                                 TRIM (c_dtls2_cur_rec.document_ref)
                              || ':'
                              || TRIM (c_dtls2_cur_rec.document_number)
                              || ':'
                              || TRIM (c_dtls2_cur_rec.can_fy);

                           --If the index does not exist then include the index
                           IF NOT g_txn_reject_tbl.EXISTS (g_index)
                           THEN
                              g_txn_reject_tbl (g_index) := 1;
                           END IF;
                        END IF;

                     ELSIF g_txn_flow_flags_tbl.EXISTS (
                              p_batch_number || ':NIHOPETXN033')
                     THEN
                        --The batch number and error message exist in the setup form
                        --hence using the setup
                        --Check for file reject for the following error
                        IF NVL (
                              g_txn_flow_flags_tbl (
                                 p_batch_number || ':NIHOPETXN033').file_reject,
                              'N') = 'Y'
                        THEN
                           g_file_reject_flag := TRUE;
                        END IF;



                        --Check the reject error transaction flag for the following error
                        IF NVL (
                              g_txn_flow_flags_tbl (
                                 p_batch_number || ':NIHOPETXN033').reject_error_txn,
                              'N') = 'Y'
                        THEN
                           g_index :=
                                 TRIM (c_dtls2_cur_rec.document_ref)
                              || ':'
                              || TRIM (c_dtls2_cur_rec.document_number)
                              || ':'
                              || TRIM (c_dtls2_cur_rec.can_fy);

                           --If the index does not exist then include the index
                           IF NOT g_txn_reject_tbl.EXISTS (g_index)
                           THEN
                              g_txn_reject_tbl (g_index) := 1;
                           END IF;
                        END IF;

                     ELSE
                        --The batch number and error message does not exist in the setup form
                        --hence using the default DT setup
                        --Check for file reject for the following error
                        IF NVL (
                              g_txn_flow_flags_tbl ('DT:NIHOPETXN033').file_reject,
                              'N') = 'Y'
                        THEN
                           g_file_reject_flag := TRUE;
                        END IF;



                        --Check the reject error transaction flag for the following error
                        IF NVL (
                              g_txn_flow_flags_tbl ('DT:NIHOPETXN033').reject_error_txn,
                              'N') = 'Y'
                        THEN
                           g_index :=
                                 TRIM (c_dtls2_cur_rec.document_ref)
                              || ':'
                              || TRIM (c_dtls2_cur_rec.document_number)
                              || ':'
                              || TRIM (c_dtls2_cur_rec.can_fy);

                           --If the index does not exist then include the index
                           IF NOT g_txn_reject_tbl.EXISTS (g_index)
                           THEN
                              g_txn_reject_tbl (g_index) := 1;
                           END IF;
                        END IF;
                     END IF;

                     --End of change 1
                     record_error (l_error_message,
                                   l_ope_batch_id,
                                   l_ope_detail_id,
                                   l_record_id);
                     lb_success := FALSE;
                  ELSIF NOT validate_task_number (
                               p_can                            => c_dtls2_cur_rec.can,
                               p_can_fyr                        => c_dtls2_cur_rec.can_fy, -- FCI Change
                               p_task_num                       => c_dtls2_cur_rec.task_number, -- FCI Change
                               p_exp_item_dt                    => c_dtls2_cur_rec.expenditure_item_date, -- FCI Change 07/31/19
                               p_project_id                     => l_project_id,
                               p_task_id                        => l_task_id,
                               p_carrying_out_organization_id   => l_exporg_id,
                               p_attribute2                     => l_attribute2)
                  THEN
                   --CR NBSCH0002499 begin
                   if validate_exp_it_dt_task_number(
                               p_can                            => c_dtls2_cur_rec.can,
                               p_can_fyr                        => c_dtls2_cur_rec.can_fy,
                               p_task_num                       => c_dtls2_cur_rec.task_number,
                               p_exp_item_dt                    => c_dtls2_cur_rec.expenditure_item_date) then
                     logf ('Under ELSIF NOT validate_task_number - if validate_exp_it_dt_task_number (0)' , l_module);
                     l_error_message := g_err_message_tbl ('NIHOPETXN037');
                   else
                   --CR NBSCH0002499 end

                     logf ('Under ELSIF NOT validate_task_number (1)' , l_module);
                     --Start of change 1 by Yash
                     --CAN is Invalid

                     logf ('chech NIHOPETXN034 1 l_error_message: '|| l_error_message , l_module);
                     l_error_message := g_err_message_tbl ('NIHOPETXN034');
                     logf ('chech NIHOPETXN034 2 l_error_message: '|| l_error_message , l_module);

                     --Check if responsibility name, batch number and error message is setup in the setup from
                     --else check if batch number and error message is setup in the setup form
                     --else use default (DT) batch and error message
                     IF g_txn_flow_flags_tbl.EXISTS (
                              g_resp_name
                           || ':'
                           || p_batch_number
                           || ':NIHOPETXN034')
                     THEN
                        logf ('chech NIHOPETXN034 3' , l_module);
                        --The responsibility name, batch number and error message exist in the setup form
                        --hence using the setup
                        --Check for file reject for the following error
                        IF NVL (
                              g_txn_flow_flags_tbl (
                                    g_resp_name
                                 || ':'
                                 || p_batch_number
                                 || ':NIHOPETXN034').file_reject,
                              'N') = 'Y'
                        THEN

                            logf ('chech NIHOPETXN034 4' , l_module);
                           g_file_reject_flag := TRUE;
                        END IF;



                        --Check the reject error transaction flag for the following error
                        IF NVL (
                              g_txn_flow_flags_tbl (
                                    g_resp_name
                                 || ':'
                                 || p_batch_number
                                 || ':NIHOPETXN034').reject_error_txn,
                              'N') = 'Y'
                        THEN

                            logf ('chech NIHOPETXN034 5' , l_module);
                           g_index :=
                                 TRIM (c_dtls2_cur_rec.document_ref)
                              || ':'
                              || TRIM (c_dtls2_cur_rec.document_number)
                              || ':'
                              || TRIM (c_dtls2_cur_rec.can_fy);

                           --If the index does not exist then include the index
                           IF NOT g_txn_reject_tbl.EXISTS (g_index)
                           THEN

                                logf ('chech NIHOPETXN034 6' , l_module);
                              g_txn_reject_tbl (g_index) := 1;
                           END IF;
                        END IF;

                     ELSIF g_txn_flow_flags_tbl.EXISTS (
                              p_batch_number || ':NIHOPETXN034')
                     THEN
                        logf ('chech NIHOPETXN034 7' , l_module);
                        --The batch number and error message exist in the setup form
                        --hence using the setup
                        --Check for file reject for the following error
                        IF NVL (
                              g_txn_flow_flags_tbl (
                                 p_batch_number || ':NIHOPETXN034').file_reject,
                              'N') = 'Y'
                        THEN
                            logf ('chech NIHOPETXN034 8' , l_module);
                           g_file_reject_flag := TRUE;
                        END IF;



                        --Check the reject error transaction flag for the following error
                        IF NVL (
                              g_txn_flow_flags_tbl (
                                 p_batch_number || ':NIHOPETXN034').reject_error_txn,
                              'N') = 'Y'
                        THEN
                            logf ('chech NIHOPETXN034 9' , l_module);
                           g_index :=
                                 TRIM (c_dtls2_cur_rec.document_ref)
                              || ':'
                              || TRIM (c_dtls2_cur_rec.document_number)
                              || ':'
                              || TRIM (c_dtls2_cur_rec.can_fy);

                           --If the index does not exist then include the index
                           IF NOT g_txn_reject_tbl.EXISTS (g_index)
                           THEN
                                logf ('chech NIHOPETXN034 10' , l_module);
                              g_txn_reject_tbl (g_index) := 1;
                           END IF;
                        END IF;

                     ELSE
                        logf ('chech NIHOPETXN034 11' , l_module);
                        --The batch number and error message does not exist in the setup form
                        --hence using the default DT setup
                        --Check for file reject for the following error
                        IF NVL (
                              g_txn_flow_flags_tbl ('DT:NIHOPETXN034').file_reject,
                              'N') = 'Y'
                        THEN
                            logf ('chech NIHOPETXN034 12' , l_module);
                           g_file_reject_flag := TRUE;
                        END IF;



                        --Check the reject error transaction flag for the following error
                        IF NVL (
                              g_txn_flow_flags_tbl ('DT:NIHOPETXN034').reject_error_txn,
                              'N') = 'Y'
                        THEN
                            logf ('chech NIHOPETXN034 13' , l_module);
                           g_index :=
                                 TRIM (c_dtls2_cur_rec.document_ref)
                              || ':'
                              || TRIM (c_dtls2_cur_rec.document_number)
                              || ':'
                              || TRIM (c_dtls2_cur_rec.can_fy);

                           --If the index does not exist then include the index
                           IF NOT g_txn_reject_tbl.EXISTS (g_index)
                           THEN
                              logf ('chech NIHOPETXN034 14' , l_module);
                              g_txn_reject_tbl (g_index) := 1;
                           END IF;
                        END IF;
                     END IF;
                   end if;--CR NBSCH0002499
                     logf ('chech NIHOPETXN034 15 l_error_message: '|| l_error_message , l_module);
                     --End of change 1
                     record_error (l_error_message,
                                   l_ope_batch_id,
                                   l_ope_detail_id,
                                   l_record_id);
                     lb_success := FALSE;
                  END IF;

                     logf ('chech beetwen NIHOPETXN034 and NIHOPETXN010 l_error_message: '|| l_error_message , l_module);
                   -------------------Validate Transaction Code is not null and Valid------------


                  IF TRIM (c_dtls2_cur_rec.tcode) IS NULL
                  THEN
                     --Start of change 1 by Yash
                     --Transaction Code value is Blank

                        logf ('chech NIHOPETXN010 1' , l_module);
                     l_error_message := g_err_message_tbl ('NIHOPETXN010');

                     --Check if responsibility name, batch number and error message is setup in the setup from
                     --else check if batch number and error message is setup in the setup form
                     --else use default (DT) batch and error message
                     IF g_txn_flow_flags_tbl.EXISTS (
                              g_resp_name
                           || ':'
                           || p_batch_number
                           || ':NIHOPETXN010')
                     THEN
                        --The responsibility name, batch number and error message exist in the setup form
                        --hence using the setup
                        --Check for file reject for the following error
                        IF NVL (
                              g_txn_flow_flags_tbl (
                                    g_resp_name
                                 || ':'
                                 || p_batch_number
                                 || ':NIHOPETXN010').file_reject,
                              'N') = 'Y'
                        THEN
                           g_file_reject_flag := TRUE;
                        END IF;


                        --Check the reject error transaction flag for the following error
                        IF NVL (
                              g_txn_flow_flags_tbl (
                                    g_resp_name
                                 || ':'
                                 || p_batch_number
                                 || ':NIHOPETXN010').reject_error_txn,
                              'N') = 'Y'
                        THEN
                           g_index :=
                                 TRIM (c_dtls2_cur_rec.document_ref)
                              || ':'
                              || TRIM (c_dtls2_cur_rec.document_number)
                              || ':'
                              || TRIM (c_dtls2_cur_rec.can_fy);

                           --If the index does not exist then include the index
                           IF NOT g_txn_reject_tbl.EXISTS (g_index)
                           THEN
                              g_txn_reject_tbl (g_index) := 1;
                           END IF;
                        END IF;

                     ELSIF g_txn_flow_flags_tbl.EXISTS (
                              p_batch_number || ':NIHOPETXN010')
                     THEN
                        --The batch number and error message exist in the setup form
                        --hence using the setup
                        --Check for file reject for the following error
                        IF NVL (
                              g_txn_flow_flags_tbl (
                                 p_batch_number || ':NIHOPETXN010').file_reject,
                              'N') = 'Y'
                        THEN
                           g_file_reject_flag := TRUE;
                        END IF;


                        --Check the reject error transaction flag for the following error
                        IF NVL (
                              g_txn_flow_flags_tbl (
                                 p_batch_number || ':NIHOPETXN010').reject_error_txn,
                              'N') = 'Y'
                        THEN
                           g_index :=
                                 TRIM (c_dtls2_cur_rec.document_ref)
                              || ':'
                              || TRIM (c_dtls2_cur_rec.document_number)
                              || ':'
                              || TRIM (c_dtls2_cur_rec.can_fy);

                           --If the index does not exist then include the index
                           IF NOT g_txn_reject_tbl.EXISTS (g_index)
                           THEN
                              g_txn_reject_tbl (g_index) := 1;
                           END IF;
                        END IF;

                     ELSE
                        --The batch number and error message does not exist in the setup form
                        --hence using the default DT setup
                        --Check for file reject for the following error
                        IF NVL (
                              g_txn_flow_flags_tbl ('DT:NIHOPETXN010').file_reject,
                              'N') = 'Y'
                        THEN
                           g_file_reject_flag := TRUE;
                        END IF;



                        --Check the reject error transaction flag for the following error
                        IF NVL (
                              g_txn_flow_flags_tbl ('DT:NIHOPETXN010').reject_error_txn,
                              'N') = 'Y'
                        THEN
                           g_index :=
                                 TRIM (c_dtls2_cur_rec.document_ref)
                              || ':'
                              || TRIM (c_dtls2_cur_rec.document_number)
                              || ':'
                              || TRIM (c_dtls2_cur_rec.can_fy);

                           --If the index does not exist then include the index
                           IF NOT g_txn_reject_tbl.EXISTS (g_index)
                           THEN
                              g_txn_reject_tbl (g_index) := 1;
                           END IF;
                        END IF;
                     END IF;
                     --End of change 1
                     record_error (l_error_message,
                                   l_ope_batch_id,
                                   l_ope_detail_id,
                                   l_record_id);
                     lb_success := FALSE;

                  ELSIF NOT tcode_exists (p_tcode => c_dtls2_cur_rec.tcode, p_yesno => l_yesno) --'FAH1
                  THEN
                     --Start of change 1 by Yash
                     --Transaction Code is Invalid
                     l_error_message := g_err_message_tbl ('NIHOPETXN011');

                     --Check if responsibility name, batch number and error message is setup in the setup from
                     --else check if batch number and error message is setup in the setup form
                     --else use default (DT) batch and error message
                     IF g_txn_flow_flags_tbl.EXISTS (g_resp_name || ':' || p_batch_number || ':NIHOPETXN011')
                     THEN
                        --The responsibility name, batch number and error message exist in the setup form
                        --hence using the setup
                        --Check for file reject for the following error
                        IF NVL (
                              g_txn_flow_flags_tbl (g_resp_name || ':' || p_batch_number || ':NIHOPETXN011').file_reject,
                              'N') = 'Y'
                        THEN
                           g_file_reject_flag := TRUE;
                        END IF;

                        --Check the hold for ope error handling flag for the following error
                        IF NVL (
                              g_txn_flow_flags_tbl (g_resp_name || ':' || p_batch_number || ':NIHOPETXN011').hold_for_ope_eh,
                              'N') = 'Y'
                        THEN
                           g_index :=
                                 TRIM (c_dtls2_cur_rec.document_ref)
                              || ':'
                              || TRIM (c_dtls2_cur_rec.document_number)
                              || ':'
                              || TRIM (c_dtls2_cur_rec.can_fy);

                           --If the index does not exist then include the index
                           IF NOT g_txn_hold_tbl.EXISTS (g_index)
                           THEN
                              g_txn_hold_tbl (g_index) := 1;
                           END IF;
                        END IF;

                        --Check the reject error transaction flag for the following error
                        IF NVL (
                              g_txn_flow_flags_tbl (g_resp_name || ':' || p_batch_number || ':NIHOPETXN011').reject_error_txn,
                              'N') = 'Y'
                        THEN
                           g_index :=
                                 TRIM (c_dtls2_cur_rec.document_ref)
                              || ':'
                              || TRIM (c_dtls2_cur_rec.document_number)
                              || ':'
                              || TRIM (c_dtls2_cur_rec.can_fy);

                           --If the index does not exist then include the index
                           IF NOT g_txn_reject_tbl.EXISTS (g_index)
                           THEN
                              g_txn_reject_tbl (g_index) := 1;
                           END IF;
                        END IF;
                     ELSIF g_txn_flow_flags_tbl.EXISTS (p_batch_number || ':NIHOPETXN011')
                     THEN
                        --The batch number and error message exist in the setup form
                        --hence using the setup
                        --Check for file reject for the following error
                        IF NVL (g_txn_flow_flags_tbl (p_batch_number || ':NIHOPETXN011').file_reject, 'N') = 'Y'
                        THEN
                           g_file_reject_flag := TRUE;
                        END IF;

                        --Check the hold for ope error handling flag for the following error
                        IF NVL (g_txn_flow_flags_tbl (p_batch_number || ':NIHOPETXN011').hold_for_ope_eh, 'N') = 'Y'
                        THEN
                           g_index :=
                                 TRIM (c_dtls2_cur_rec.document_ref)
                              || ':'
                              || TRIM (c_dtls2_cur_rec.document_number)
                              || ':'
                              || TRIM (c_dtls2_cur_rec.can_fy);

                           --If the index does not exist then include the index
                           IF NOT g_txn_hold_tbl.EXISTS (g_index)
                           THEN
                              g_txn_hold_tbl (g_index) := 1;
                           END IF;
                        END IF;

                        --Check the reject error transaction flag for the following error
                        IF NVL (g_txn_flow_flags_tbl (p_batch_number || ':NIHOPETXN011').reject_error_txn, 'N') = 'Y'
                        THEN
                           g_index :=
                                 TRIM (c_dtls2_cur_rec.document_ref)
                              || ':'
                              || TRIM (c_dtls2_cur_rec.document_number)
                              || ':'
                              || TRIM (c_dtls2_cur_rec.can_fy);

                           --If the index does not exist then include the index
                           IF NOT g_txn_reject_tbl.EXISTS (g_index)
                           THEN
                              g_txn_reject_tbl (g_index) := 1;
                           END IF;
                        END IF;
                     ELSE
                        --The batch number and error message does not exist in the setup form
                        --hence using the default DT setup
                        --Check for file reject for the following error
                        IF NVL (g_txn_flow_flags_tbl ('DT:NIHOPETXN011').file_reject, 'N') = 'Y'
                        THEN
                           g_file_reject_flag := TRUE;
                        END IF;

                        --Check the hold for ope error handling flag for the following error
                        IF NVL (g_txn_flow_flags_tbl ('DT:NIHOPETXN011').hold_for_ope_eh, 'N') = 'Y'
                        THEN
                           g_index :=
                                 TRIM (c_dtls2_cur_rec.document_ref)
                              || ':'
                              || TRIM (c_dtls2_cur_rec.document_number)
                              || ':'
                              || TRIM (c_dtls2_cur_rec.can_fy);

                           --If the index does not exist then include the index
                           IF NOT g_txn_hold_tbl.EXISTS (g_index)
                           THEN
                              g_txn_hold_tbl (g_index) := 1;
                           END IF;
                        END IF;

                        --Check the reject error transaction flag for the following error
                        IF NVL (g_txn_flow_flags_tbl ('DT:NIHOPETXN011').reject_error_txn, 'N') = 'Y'
                        THEN
                           g_index :=
                                 TRIM (c_dtls2_cur_rec.document_ref)
                              || ':'
                              || TRIM (c_dtls2_cur_rec.document_number)
                              || ':'
                              || TRIM (c_dtls2_cur_rec.can_fy);

                           --If the index does not exist then include the index
                           IF NOT g_txn_reject_tbl.EXISTS (g_index)
                           THEN
                              g_txn_reject_tbl (g_index) := 1;
                           END IF;
                        END IF;
                     END IF;


                     --End of change 1
                     record_error (l_error_message,
                                   l_ope_batch_id,
                                   l_ope_detail_id,
                                   l_record_id);
                     lb_success := FALSE;

                  END IF;



              -------------Validate Reverse code is not null and Valid---------


                  IF TRIM (c_dtls2_cur_rec.reverse_code) IS NULL
                  THEN
                     --Start of change 1 by Yash
                     --Reverse Code value is Blank
                     l_error_message := g_err_message_tbl ('NIHOPETXN012');

                     --Check if responsibility name, batch number and error message is setup in the setup from
                     --else check if batch number and error message is setup in the setup form
                     --else use default (DT) batch and error message
                     IF g_txn_flow_flags_tbl.EXISTS (
                              g_resp_name
                           || ':'
                           || p_batch_number
                           || ':NIHOPETXN012')
                     THEN
                        --The responsibility name, batch number and error message exist in the setup form
                        --hence using the setup
                        --Check for file reject for the following error
                        IF NVL (
                              g_txn_flow_flags_tbl (
                                    g_resp_name
                                 || ':'
                                 || p_batch_number
                                 || ':NIHOPETXN012').file_reject,
                              'N') = 'Y'
                        THEN
                           g_file_reject_flag := TRUE;
                        END IF;



                        --Check the reject error transaction flag for the following error
                        IF NVL (
                              g_txn_flow_flags_tbl (
                                    g_resp_name
                                 || ':'
                                 || p_batch_number
                                 || ':NIHOPETXN012').reject_error_txn,
                              'N') = 'Y'
                        THEN
                           g_index :=
                                 TRIM (c_dtls2_cur_rec.document_ref)
                              || ':'
                              || TRIM (c_dtls2_cur_rec.document_number)
                              || ':'
                              || TRIM (c_dtls2_cur_rec.can_fy);

                           --If the index does not exist then include the index
                           IF NOT g_txn_reject_tbl.EXISTS (g_index)
                           THEN
                              g_txn_reject_tbl (g_index) := 1;
                           END IF;
                        END IF;

                     ELSIF g_txn_flow_flags_tbl.EXISTS (
                              p_batch_number || ':NIHOPETXN012')
                     THEN
                        --The batch number and error message exist in the setup form
                        --hence using the setup
                        --Check for file reject for the following error
                        IF NVL (
                              g_txn_flow_flags_tbl (
                                 p_batch_number || ':NIHOPETXN012').file_reject,
                              'N') = 'Y'
                        THEN
                           g_file_reject_flag := TRUE;
                        END IF;


                        --Check the reject error transaction flag for the following error
                        IF NVL (
                              g_txn_flow_flags_tbl (
                                 p_batch_number || ':NIHOPETXN012').reject_error_txn,
                              'N') = 'Y'
                        THEN
                           g_index :=
                                 TRIM (c_dtls2_cur_rec.document_ref)
                              || ':'
                              || TRIM (c_dtls2_cur_rec.document_number)
                              || ':'
                              || TRIM (c_dtls2_cur_rec.can_fy);

                           --If the index does not exist then include the index
                           IF NOT g_txn_reject_tbl.EXISTS (g_index)
                           THEN
                              g_txn_reject_tbl (g_index) := 1;
                           END IF;
                        END IF;

                     ELSE
                        --The batch number and error message does not exist in the setup form
                        --hence using the default DT setup
                        --Check for file reject for the following error
                        IF NVL (
                              g_txn_flow_flags_tbl ('DT:NIHOPETXN012').file_reject,
                              'N') = 'Y'
                        THEN
                           g_file_reject_flag := TRUE;
                        END IF;



                        --Check the reject error transaction flag for the following error
                        IF NVL (
                              g_txn_flow_flags_tbl ('DT:NIHOPETXN012').reject_error_txn,
                              'N') = 'Y'
                        THEN
                           g_index :=
                                 TRIM (c_dtls2_cur_rec.document_ref)
                              || ':'
                              || TRIM (c_dtls2_cur_rec.document_number)
                              || ':'
                              || TRIM (c_dtls2_cur_rec.can_fy);

                           --If the index does not exist then include the index
                           IF NOT g_txn_reject_tbl.EXISTS (g_index)
                           THEN
                              g_txn_reject_tbl (g_index) := 1;
                           END IF;
                        END IF;
                     END IF;

                     --End of change 1
                     record_error (l_error_message,
                                   l_ope_batch_id,
                                   l_ope_detail_id,
                                   l_record_id);
                     lb_success := FALSE;

                  ELSIF TRIM (c_dtls2_cur_rec.reverse_code) NOT IN ('1', '2')
                  THEN
                     --Start of change 1 by Yash
                     --Reverse Code is Invalid
                     l_error_message := g_err_message_tbl ('NIHOPETXN013');

                     --Check if responsibility name, batch number and error message is setup in the setup from
                     --else check if batch number and error message is setup in the setup form
                     --else use default (DT) batch and error message
                     IF g_txn_flow_flags_tbl.EXISTS (
                              g_resp_name
                           || ':'
                           || p_batch_number
                           || ':NIHOPETXN013')
                     THEN
                        --The responsibility name, batch number and error message exist in the setup form
                        --hence using the setup
                        --Check for file reject for the following error
                        IF NVL (
                              g_txn_flow_flags_tbl (
                                    g_resp_name
                                 || ':'
                                 || p_batch_number
                                 || ':NIHOPETXN013').file_reject,
                              'N') = 'Y'
                        THEN
                           g_file_reject_flag := TRUE;
                        END IF;


                        --Check the reject error transaction flag for the following error
                        IF NVL (
                              g_txn_flow_flags_tbl (
                                    g_resp_name
                                 || ':'
                                 || p_batch_number
                                 || ':NIHOPETXN013').reject_error_txn,
                              'N') = 'Y'
                        THEN
                           g_index :=
                                 TRIM (c_dtls2_cur_rec.document_ref)
                              || ':'
                              || TRIM (c_dtls2_cur_rec.document_number)
                              || ':'
                              || TRIM (c_dtls2_cur_rec.can_fy);

                           --If the index does not exist then include the index
                           IF NOT g_txn_reject_tbl.EXISTS (g_index)
                           THEN
                              g_txn_reject_tbl (g_index) := 1;
                           END IF;
                        END IF;

                     ELSIF g_txn_flow_flags_tbl.EXISTS (
                              p_batch_number || ':NIHOPETXN013')
                     THEN
                        --The batch number and error message exist in the setup form
                        --hence using the setup
                        --Check for file reject for the following error
                        IF NVL (
                              g_txn_flow_flags_tbl (
                                 p_batch_number || ':NIHOPETXN013').file_reject,
                              'N') = 'Y'
                        THEN
                           g_file_reject_flag := TRUE;
                        END IF;



                        --Check the reject error transaction flag for the following error
                        IF NVL (
                              g_txn_flow_flags_tbl (
                                 p_batch_number || ':NIHOPETXN013').reject_error_txn,
                              'N') = 'Y'
                        THEN
                           g_index :=
                                 TRIM (c_dtls2_cur_rec.document_ref)
                              || ':'
                              || TRIM (c_dtls2_cur_rec.document_number)
                              || ':'
                              || TRIM (c_dtls2_cur_rec.can_fy);

                           --If the index does not exist then include the index
                           IF NOT g_txn_reject_tbl.EXISTS (g_index)
                           THEN
                              g_txn_reject_tbl (g_index) := 1;
                           END IF;
                        END IF;

                     ELSE
                        --The batch number and error message does not exist in the setup form
                        --hence using the default DT setup
                        --Check for file reject for the following error
                        IF NVL (
                              g_txn_flow_flags_tbl ('DT:NIHOPETXN013').file_reject,
                              'N') = 'Y'
                        THEN
                           g_file_reject_flag := TRUE;
                        END IF;



                        --Check the reject error transaction flag for the following error
                        IF NVL (
                              g_txn_flow_flags_tbl ('DT:NIHOPETXN013').reject_error_txn,
                              'N') = 'Y'
                        THEN
                           g_index :=
                                 TRIM (c_dtls2_cur_rec.document_ref)
                              || ':'
                              || TRIM (c_dtls2_cur_rec.document_number)
                              || ':'
                              || TRIM (c_dtls2_cur_rec.can_fy);

                           --If the index does not exist then include the index
                           IF NOT g_txn_reject_tbl.EXISTS (g_index)
                           THEN
                              g_txn_reject_tbl (g_index) := 1;
                           END IF;
                        END IF;
                     END IF;

                     --End of change 1
                     record_error (l_error_message,
                                   l_ope_batch_id,
                                   l_ope_detail_id,
                                   l_record_id);
                     lb_success := FALSE;
                  END IF;


              --------------------------------------------------------------
                  logf ('Before checking for balance',l_module);

                  IF lb_success and not (c_dtls2_cur_rec.tcode in ('181', '191') and c_dtls2_cur_rec.reverse_code = 2) --NBSCH0003234
                  THEN
                     IF tcode_exists (p_tcode   => c_dtls2_cur_rec.tcode,
                                      p_yesno   => l_yesno)
                     THEN
                        IF     l_yesno = 'Y'
                           AND c_dtls2_cur_rec.tcode IN ('181', '191')
                               --Animesh Added this AND condition to restrict this check for 050 only as this one was
                        THEN
                           /*
                           || Check for obligation existence
                           */
                           l_obg_exist :=
                              chk_obligation_exist (
                                 p_doc_num        => TRIM (
                                                          c_dtls2_cur_rec.document_ref
                                                       || c_dtls2_cur_rec.document_number),
                                 --- p_fiscal_year    => c_dtls2_cur_rec.can_fy,
                                 --  p_fiscal_year    => nvl(c_dtls2_cur_rec.new_fy,c_dtls2_cur_rec.can_fy), -- FCI Change 11/5/19
                                 p_fiscal_year    => c_dtls2_cur_rec.new_fy, -- FCI Change 11/18/19 CR39655
                                 p_batch_number   => c_dtls2_cur_rec.batch_number,
                                 p_can            => c_dtls2_cur_rec.can,				--CR NBSCH0003180
                                 p_object_class   => c_dtls2_cur_rec.object_class);		--CR NBSCH0003180

                           -- START  FCI Change CR39655
                           IF l_obg_exist = 0
                           THEN
                              l_obg_exist :=
                                 chk_obligation_exist (
                                    p_doc_num        => TRIM (
                                                             c_dtls2_cur_rec.document_ref
                                                          || c_dtls2_cur_rec.document_number),
                                    --- p_fiscal_year    => c_dtls2_cur_rec.can_fy,
                                    p_fiscal_year    => c_dtls2_cur_rec.can_fy,
                                    p_batch_number   => c_dtls2_cur_rec.batch_number,
                                    p_can            => c_dtls2_cur_rec.can,			--CR NBSCH0003180
                                    p_object_class   => c_dtls2_cur_rec.object_class);	--CR NBSCH0003180
                           END IF;

                           -- END FCI Change CR39655

                           IF l_obg_exist = 0 and p_batch_number <> 'QR'
                           THEN
                              --Start of change 1 by Yash
                              --Obligation Line does not exists for combination of Document Ref/Number, Fiscal Year, Batch Number and T-code
                              l_error_message :=
                                    g_err_message_tbl ('NIHOPETXN014')
                                 || ':'
                                 || TRIM (
                                          c_dtls2_cur_rec.document_ref
                                       || c_dtls2_cur_rec.document_number)
                                 || '|'
                                 || c_dtls2_cur_rec.can_fy
                                 || '|'
                                 || c_dtls2_cur_rec.batch_number
                                 || '|'
                                 || c_dtls2_cur_rec.tcode;
                              logf (l_error_message,l_module);
                              l_error_message :=
                                 g_err_message_tbl ('NIHOPETXN014');

                              --Check if responsibility name, batch number and error message is setup in the setup from
                              --else check if batch number and error message is setup in the setup form
                              --else use default (DT) batch and error message
                              IF g_txn_flow_flags_tbl.EXISTS (
                                       g_resp_name
                                    || ':'
                                    || p_batch_number
                                    || ':NIHOPETXN014')
                              THEN
                                 --The responsibility name, batch number and error message exist in the setup form
                                 --hence using the setup
                                 --Check for file reject for the following error
                                 IF NVL (
                                       g_txn_flow_flags_tbl (
                                             g_resp_name
                                          || ':'
                                          || p_batch_number
                                          || ':NIHOPETXN014').file_reject,
                                       'N') = 'Y'
                                 THEN
                                    g_file_reject_flag := TRUE;
                                 END IF;



                                 --Check the reject error transaction flag for the following error
                                 IF NVL (
                                       g_txn_flow_flags_tbl (
                                             g_resp_name
                                          || ':'
                                          || p_batch_number
                                          || ':NIHOPETXN014').reject_error_txn,
                                       'N') = 'Y'
                                 THEN
                                    g_index :=
                                          TRIM (c_dtls2_cur_rec.document_ref)
                                       || ':'
                                       || TRIM (
                                             c_dtls2_cur_rec.document_number)
                                       || ':'
                                       || TRIM (c_dtls2_cur_rec.can_fy);

                                    --If the index does not exist then include the index
                                    IF NOT g_txn_reject_tbl.EXISTS (g_index)
                                    THEN
                                       g_txn_reject_tbl (g_index) := 1;
                                    END IF;
                                 END IF;
                              ELSIF g_txn_flow_flags_tbl.EXISTS (
                                       p_batch_number || ':NIHOPETXN014')
                              THEN
                                 --The batch number and error message exist in the setup form
                                 --hence using the setup
                                 --Check for file reject for the following error
                                 IF NVL (
                                       g_txn_flow_flags_tbl (
                                          p_batch_number || ':NIHOPETXN014').file_reject,
                                       'N') = 'Y'
                                 THEN
                                    g_file_reject_flag := TRUE;
                                 END IF;



                                 --Check the reject error transaction flag for the following error
                                 IF NVL (
                                       g_txn_flow_flags_tbl (
                                          p_batch_number || ':NIHOPETXN014').reject_error_txn,
                                       'N') = 'Y'
                                 THEN
                                    g_index :=
                                          TRIM (c_dtls2_cur_rec.document_ref)
                                       || ':'
                                       || TRIM (
                                             c_dtls2_cur_rec.document_number)
                                       || ':'
                                       || TRIM (c_dtls2_cur_rec.can_fy);

                                    --If the index does not exist then include the index
                                    IF NOT g_txn_reject_tbl.EXISTS (g_index)
                                    THEN
                                       g_txn_reject_tbl (g_index) := 1;
                                    END IF;
                                 END IF;

                              ELSE
                                 --The batch number and error message does not exist in the setup form
                                 --hence using the default DT setup
                                 --Check for file reject for the following error
                                 IF NVL (
                                       g_txn_flow_flags_tbl (
                                          'DT:NIHOPETXN014').file_reject,
                                       'N') = 'Y'
                                 THEN
                                    g_file_reject_flag := TRUE;
                                 END IF;



                                 --Check the reject error transaction flag for the following error
                                 IF NVL (
                                       g_txn_flow_flags_tbl (
                                          'DT:NIHOPETXN014').reject_error_txn,
                                       'N') = 'Y'
                                 THEN
                                    g_index :=
                                          TRIM (c_dtls2_cur_rec.document_ref)
                                       || ':'
                                       || TRIM (
                                             c_dtls2_cur_rec.document_number)
                                       || ':'
                                       || TRIM (c_dtls2_cur_rec.can_fy);

                                    --If the index does not exist then include the index
                                    IF NOT g_txn_reject_tbl.EXISTS (g_index)
                                    THEN
                                       g_txn_reject_tbl (g_index) := 1;
                                    END IF;
                                 END IF;
                              END IF;

                              --End of change 1
                              record_error (l_error_message,
                                            l_ope_batch_id,
                                            l_ope_detail_id,
                                            l_record_id);
                              lb_success := FALSE;
                           END IF;

                ----------------------------
                           IF lb_success
                           THEN
                              /*
                               || Check for obligation amount balance existence
                               */
                              l_obg_amnt_bal_exist :=
                                 chk_obligation_amnt_bal_exist (
                                    p_doc_num         => TRIM (
                                                              c_dtls2_cur_rec.document_ref
                                                           || c_dtls2_cur_rec.document_number),
                                    p_fiscal_year     => c_dtls2_cur_rec.can_fy,
                                    p_batch_number    => c_dtls2_cur_rec.batch_number,
                                    p_can             => c_dtls2_cur_rec.can,				--CR NBSCH0003180
                                    p_object_class    => c_dtls2_cur_rec.object_class,		--CR NBSCH0003180
                                    p_ope_detail_id   => c_dtls2_cur_rec.ope_detail_id);

                              logf (
                                    ' Obligation Balance:'
                                 || l_obg_amnt_bal_exist, l_module);

                              IF l_obg_amnt_bal_exist >= 0
                              THEN
                                 --logf(' Obligation Balance exist for Tcode: '||c_dtls2_cur_rec.tcode);
                                 NULL;
                              ELSE
                                 lb_success := FALSE;
                                 --Start of change 1 by Yash
                                 --Obligation Balance is either Negative/Zero or Less than Disbursement Amount
                                 l_error_message :=
                                    g_err_message_tbl ('NIHOPETXN015');

                                 --Check if responsibility name, batch number and error message is setup in the setup from
                                 --else check if batch number and error message is setup in the setup form
                                 --else use default (DT) batch and error message
                                 IF g_txn_flow_flags_tbl.EXISTS (
                                          g_resp_name
                                       || ':'
                                       || p_batch_number
                                       || ':NIHOPETXN015')
                                 THEN
                                    --The responsibility name, batch number and error message exist in the setup form
                                    --hence using the setup
                                    --Check for file reject for the following error
                                    IF NVL (
                                          g_txn_flow_flags_tbl (
                                                g_resp_name
                                             || ':'
                                             || p_batch_number
                                             || ':NIHOPETXN015').file_reject,
                                          'N') = 'Y'
                                    THEN
                                       g_file_reject_flag := TRUE;
                                    END IF;



                                    --Check the reject error transaction flag for the following error
                                    IF NVL (
                                          g_txn_flow_flags_tbl (
                                                g_resp_name
                                             || ':'
                                             || p_batch_number
                                             || ':NIHOPETXN015').reject_error_txn,
                                          'N') = 'Y'
                                    THEN
                                       g_index :=
                                             TRIM (
                                                c_dtls2_cur_rec.document_ref)
                                          || ':'
                                          || TRIM (
                                                c_dtls2_cur_rec.document_number)
                                          || ':'
                                          || TRIM (c_dtls2_cur_rec.can_fy);

                                       --If the index does not exist then include the index
                                       IF NOT g_txn_reject_tbl.EXISTS (
                                                 g_index)
                                       THEN
                                          g_txn_reject_tbl (g_index) := 1;
                                       END IF;
                                    END IF;
                                 ELSIF g_txn_flow_flags_tbl.EXISTS (
                                          p_batch_number || ':NIHOPETXN015')
                                 THEN
                                    --The batch number and error message exist in the setup form
                                    --hence using the setup
                                    --Check for file reject for the following error
                                    IF NVL (
                                          g_txn_flow_flags_tbl (
                                                p_batch_number
                                             || ':NIHOPETXN015').file_reject,
                                          'N') = 'Y'
                                    THEN
                                       g_file_reject_flag := TRUE;
                                    END IF;


                                    --Check the reject error transaction flag for the following error
                                    IF NVL (
                                          g_txn_flow_flags_tbl (
                                                p_batch_number
                                             || ':NIHOPETXN015').reject_error_txn,
                                          'N') = 'Y'
                                    THEN
                                       g_index :=
                                             TRIM (
                                                c_dtls2_cur_rec.document_ref)
                                          || ':'
                                          || TRIM (
                                                c_dtls2_cur_rec.document_number)
                                          || ':'
                                          || TRIM (c_dtls2_cur_rec.can_fy);

                                       --If the index does not exist then include the index
                                       IF NOT g_txn_reject_tbl.EXISTS (
                                                 g_index)
                                       THEN
                                          g_txn_reject_tbl (g_index) := 1;
                                       END IF;
                                    END IF;

                                 ELSE
                                    --The batch number and error message does not exist in the setup form
                                    --hence using the default DT setup
                                    --Check for file reject for the following error
                                    IF NVL (
                                          g_txn_flow_flags_tbl (
                                             'DT:NIHOPETXN015').file_reject,
                                          'N') = 'Y'
                                    THEN
                                       g_file_reject_flag := TRUE;
                                    END IF;


                                    --Check the reject error transaction flag for the following error
                                    IF NVL (
                                          g_txn_flow_flags_tbl (
                                             'DT:NIHOPETXN015').reject_error_txn,
                                          'N') = 'Y'
                                    THEN
                                       g_index :=
                                             TRIM (
                                                c_dtls2_cur_rec.document_ref)
                                          || ':'
                                          || TRIM (
                                                c_dtls2_cur_rec.document_number)
                                          || ':'
                                          || TRIM (c_dtls2_cur_rec.can_fy);

                                       --If the index does not exist then include the index
                                       IF NOT g_txn_reject_tbl.EXISTS (
                                                 g_index)
                                       THEN
                                          g_txn_reject_tbl (g_index) := 1;
                                       END IF;
                                    END IF;
                                 END IF;

                                 --End of change 1
                                 record_error (l_error_message,
                                               l_ope_batch_id,
                                               l_ope_detail_id,
                                               l_record_id);
                              END IF;
                           END IF;
                        END IF;

             ---------------------------------------------------------------
                        --  Add logic to do the regular Obligation balance check.

                        IF lb_success
                        THEN
                           IF Balance_check_required (c_dtls2_cur_rec.tcode)
                           THEN
                              /*
                              || Check for obligation amount balance existence
                              */

                              logf (
                                    'Checking for Receivable balance: '
                                 || c_dtls2_cur_rec.tcode , l_module);
                              l_amnt_bal_exist :=
                                 chk_amnt_bal_exist (
                                    p_doc_num         => TRIM (
                                                              c_dtls2_cur_rec.document_ref
                                                           || c_dtls2_cur_rec.document_number),
                                    p_fiscal_year     => c_dtls2_cur_rec.can_fy,
                                    p_batch_number    => c_dtls2_cur_rec.batch_number,
                                    p_tcode           => c_dtls2_cur_rec.tcode,
                                    p_can             => c_dtls2_cur_rec.can,				--CR NBSCH0003180
                                    p_object_class    => c_dtls2_cur_rec.object_class,		--CR NBSCH0003180
                                    p_ope_detail_id   => c_dtls2_cur_rec.ope_detail_id);

                              IF l_amnt_bal_exist >= 0
                              THEN
                                 logf (
                                       'Amount Balance exist for Tcode -- REC: '
                                    || c_dtls2_cur_rec.tcode , l_module);
                                 NULL;
                              ELSE
                                 --Amount Balance is either Negative/Zero or Less than Disbursement Amount
                                 l_error_message :=
                                    g_err_message_tbl ('NIHOPETXN032');

                                 --Check if responsibility name, batch number and error message is setup in the setup from
                                 --else check if batch number and error message is setup in the setup form
                                 --else use default (DT) batch and error message
                                 IF g_txn_flow_flags_tbl.EXISTS (
                                          g_resp_name
                                       || ':'
                                       || p_batch_number
                                       || ':NIHOPETXN032')
                                 THEN
                                    --The responsibility name, batch number and error message exist in the setup form
                                    --hence using the setup
                                    --Check for file reject for the following error

                                    IF NVL (
                                          g_txn_flow_flags_tbl (
                                                g_resp_name
                                             || ':'
                                             || p_batch_number
                                             || ':NIHOPETXN032').file_reject,
                                          'N') = 'Y'
                                    THEN
                                       g_file_reject_flag := TRUE;
                                    END IF;





                                    --Check the reject error transaction flag for the following error
                                    IF NVL (
                                          g_txn_flow_flags_tbl (
                                                g_resp_name
                                             || ':'
                                             || p_batch_number
                                             || ':NIHOPETXN032').reject_error_txn,
                                          'N') = 'Y'
                                    THEN
                                       g_index :=
                                             TRIM (
                                                c_dtls2_cur_rec.document_ref)
                                          || ':'
                                          || TRIM (
                                                c_dtls2_cur_rec.document_number)
                                          || ':'
                                          || TRIM (c_dtls2_cur_rec.can_fy);

                                       --If the index does not exist then include the index
                                       IF NOT g_txn_reject_tbl.EXISTS (
                                                 g_index)
                                       THEN
                                          g_txn_reject_tbl (g_index) := 1;
                                       END IF;
                                    END IF;

                                    logf ('After getting the message6' , l_module);
                                 ELSIF g_txn_flow_flags_tbl.EXISTS (
                                          p_batch_number || ':NIHOPETXN032')
                                 THEN
                                    --The batch number and error message exist in the setup form
                                    --hence using the setup
                                    --Check for file reject for the following error
                                    IF NVL (
                                          g_txn_flow_flags_tbl (
                                                p_batch_number
                                             || ':NIHOPETXN032').file_reject,
                                          'N') = 'Y'
                                    THEN
                                       g_file_reject_flag := TRUE;
                                    END IF;





                                    --Check the reject error transaction flag for the following error
                                    IF NVL (
                                          g_txn_flow_flags_tbl (
                                                p_batch_number
                                             || ':NIHOPETXN032').reject_error_txn,
                                          'N') = 'Y'
                                    THEN
                                       g_index :=
                                             TRIM (
                                                c_dtls2_cur_rec.document_ref)
                                          || ':'
                                          || TRIM (
                                                c_dtls2_cur_rec.document_number)
                                          || ':'
                                          || TRIM (c_dtls2_cur_rec.can_fy);

                                       --If the index does not exist then include the index
                                       IF NOT g_txn_reject_tbl.EXISTS (
                                                 g_index)
                                       THEN
                                          g_txn_reject_tbl (g_index) := 1;
                                       END IF;
                                    END IF;

                                 ELSE
                                    --The batch number and error message does not exist in the setup form
                                    --hence using the default DT setup
                                    --Check for file reject for the following error
                                    IF NVL (
                                          g_txn_flow_flags_tbl (
                                             'DT:NIHOPETXN032').file_reject,
                                          'N') = 'Y'
                                    THEN
                                       g_file_reject_flag := TRUE;
                                    END IF;



                                    --Check the reject error transaction flag for the following error
                                    IF NVL (
                                          g_txn_flow_flags_tbl (
                                             'DT:NIHOPETXN032').reject_error_txn,
                                          'N') = 'Y'
                                    THEN
                                       g_index :=
                                             TRIM (
                                                c_dtls2_cur_rec.document_ref)
                                          || ':'
                                          || TRIM (
                                                c_dtls2_cur_rec.document_number)
                                          || ':'
                                          || TRIM (c_dtls2_cur_rec.can_fy);

                                       --If the index does not exist then include the index
                                       IF NOT g_txn_reject_tbl.EXISTS (
                                                 g_index)
                                       THEN
                                          g_txn_reject_tbl (g_index) := 1;
                                       END IF;
                                    END IF;
                                 END IF;

                                 --End of change 1
                                 record_error (l_error_message,
                                               l_ope_batch_id,
                                               l_ope_detail_id,
                                               l_record_id);
                              END IF;
                           END IF;
                        END IF;
                     END IF;
                  END IF;

            --------------------------------------------------

                  l_project_id := NULL;
                  l_task_id := NULL;
                  l_exporg_id := NULL;

                  IF lb_success
                  THEN
                     /*
                     || Validate the CAN
                     || and populate the value of attribute2, project_id
                     || task_id and exp org id
                     */
                     -- FCI Code chage CR39674
                     IF validate_task_number (
                           p_can                            => c_dtls2_cur_rec.can,
                           p_can_fyr                        => c_dtls2_cur_rec.can_fy, -- FCI Change
                           p_task_num                       => c_dtls2_cur_rec.task_number, -- FCI Change
                           p_exp_item_dt                    => c_dtls2_cur_rec.expenditure_item_date,
                           p_project_id                     => l_project_id,
                           p_task_id                        => l_task_id,
                           p_carrying_out_organization_id   => l_exporg_id,
                           p_attribute2                     => l_attribute2)
                     THEN

                        logf ('Under IF validate_task_numberr (2)' , l_module);
                        logf ('l_ope_detail_id: '|| l_ope_detail_id , l_module);
                        /*
                                   || Update the intf detail table with
                                   ||  required values
                                   */
                        UPDATE nihgl_ope_acct_dtls_int_tbl
                           SET chart_of_accounts_id =
                                  c_org_rec.chart_of_accounts_id,
                               project_id = l_project_id,
                               task_id = l_task_id,
                               exp_org_id = l_exporg_id,
                               project_exp_type = l_expenditure_type,
                               --  code_combination_id = l_ccid,
                               --  code_combination_flex = l_ccid_flex,
                               project_number =
                                  (SELECT segment1
                                     FROM pa_projects_all
                                    WHERE project_id = l_project_id),
                               /* task_number =  --change1  comment FCI Change
                                   (SELECT task_number
                                      FROM pa_tasks
                                     WHERE task_id = l_task_id), */
                               transaction_code = l_transaction_code,
                               period_name = g_accounting_period,
                               attribute5 =
                                  TO_CHAR (g_accounting_date, 'DD-MON-YYYY'),
                               attribute14 = l_batch_desc,
                               last_updated_by = g_user_id,
                               last_update_date = SYSDATE,
                               request_id = g_request_id
                         WHERE ope_detail_id = l_ope_detail_id;

                        logf ('UPDATE nihgl_ope_acct_dtls_int_tbl DONE' , l_module);
                     END IF;
                  END IF;
                  
                 --CR CR NBSCH0003319 begin
                --NIHOPETXN039    The Suspense Transaction failed validations
                  IF lb_success
                    then
                        IF upper(c_dtls2_cur_rec.document_number) like 'SUSP%' and c_dtls2_cur_rec.batch_number = 'CD' and c_dtls2_cur_rec.tcode = '241'
                        THEN

                           l_rev_amnt_exist :=
                              chk_sum_simple (
                                    c_dtls2_cur_rec.document_ref
                                 || c_dtls2_cur_rec.document_number,
                                 c_dtls2_cur_rec.can_fy,
                                 c_dtls2_cur_rec.batch_number,
                                 c_dtls2_cur_rec.tcode,
                                 c_dtls2_cur_rec.can,				
                                 c_dtls2_cur_rec.object_class,		
                                 c_dtls2_cur_rec.amount,
                                 c_dtls2_cur_rec.ope_detail_id);

                           IF l_rev_amnt_exist <> 0
                           THEN

                              l_error_message :=
                                 g_err_message_tbl ('NIHOPETXN039');

                              logf (l_error_message,l_module);
                              record_error (l_error_message,
                                            l_ope_batch_id,
                                            l_ope_detail_id,
                                            l_record_id);
                              lb_success := FALSE;
                           END IF;
                        END IF;
                  END IF;
                  --CR CR NBSCH0003319 END
                --CR CR NBSCH0002499 begin
                --NIHOPETXN036    The amount on the reversal line is greater than the available amount for the combination of Batch Number, Doc Ref/Number, CAN, Task, OCC, and CAN FY
                  IF lb_success
                    then
                        IF c_dtls2_cur_rec.reverse_code = 2 and p_batch_number <> 'QR'
                        THEN
                           /*
                           || Check for existing transaction to reverse existence
                           || for reverse code 2
                           */
                           l_rev_amnt_exist :=
                              chk_amnt_bal_rev_exist (
                                    c_dtls2_cur_rec.document_ref
                                 || c_dtls2_cur_rec.document_number,
                                 c_dtls2_cur_rec.can_fy,
                                 c_dtls2_cur_rec.batch_number,
                                 c_dtls2_cur_rec.tcode,
                                 c_dtls2_cur_rec.can,				--CR NBSCH0003180
                                 c_dtls2_cur_rec.object_class,		--CR NBSCH0003180
                                 c_dtls2_cur_rec.amount,
                                 c_dtls2_cur_rec.ope_detail_id);

                           IF l_rev_amnt_exist < 0
                           THEN
                              --Amount to reverse is greater than the existing total
                              l_error_message :=
                                 g_err_message_tbl ('NIHOPETXN036');
                              -- Can said to skip it
                              --Check if responsibility name, batch number and error message is setup in the setup from
                              --else check if batch number and error message is setup in the setup form
                              --else use default (DT) batch and error message
                              /*
                              IF g_txn_flow_flags_tbl.EXISTS (
                                       g_resp_name
                                    || ':'
                                    || p_batch_number
                                    || ':NIHOPETXN036')
                              THEN
                                 --The responsibility name, batch number and error message exist in the setup form
                                 --hence using the setup
                                 --Check for file reject for the following error
                                 IF NVL (
                                       g_txn_flow_flags_tbl (
                                             g_resp_name
                                          || ':'
                                          || p_batch_number
                                          || ':NIHOPETXN036').file_reject,
                                       'N') = 'Y'
                                 THEN
                                    g_file_reject_flag := TRUE;
                                 END IF;


                                 --Check the reject error transaction flag for the following error
                                 IF NVL (
                                       g_txn_flow_flags_tbl (
                                             g_resp_name
                                          || ':'
                                          || p_batch_number
                                          || ':NIHOPETXN036').reject_error_txn,
                                       'N') = 'Y'
                                 THEN
                                    g_index :=
                                          TRIM (c_dtls2_cur_rec.document_ref)
                                       || ':'
                                       || TRIM (
                                             c_dtls2_cur_rec.document_number)
                                       || ':'
                                       || TRIM (c_dtls2_cur_rec.can_fy);

                                    --If the index does not exist then include the index
                                    IF NOT g_txn_reject_tbl.EXISTS (g_index)
                                    THEN
                                       g_txn_reject_tbl (g_index) := 1;
                                    END IF;
                                 END IF;

                              ELSIF g_txn_flow_flags_tbl.EXISTS (
                                       p_batch_number || ':NIHOPETXN036')
                              THEN
                                 --The batch number and error message exist in the setup form
                                 --hence using the setup
                                 --Check for file reject for the following error
                                 IF NVL (
                                       g_txn_flow_flags_tbl (
                                          p_batch_number || ':NIHOPETXN036').file_reject,
                                       'N') = 'Y'
                                 THEN
                                    g_file_reject_flag := TRUE;
                                 END IF;



                                 --Check the reject error transaction flag for the following error
                                 IF NVL (
                                       g_txn_flow_flags_tbl (
                                          p_batch_number || ':NIHOPETXN036').reject_error_txn,
                                       'N') = 'Y'
                                 THEN
                                    g_index :=
                                          TRIM (c_dtls2_cur_rec.document_ref)
                                       || ':'
                                       || TRIM (
                                             c_dtls2_cur_rec.document_number)
                                       || ':'
                                       || TRIM (c_dtls2_cur_rec.can_fy);

                                    --If the index does not exist then include the index
                                    IF NOT g_txn_reject_tbl.EXISTS (g_index)
                                    THEN
                                       g_txn_reject_tbl (g_index) := 1;
                                    END IF;
                                 END IF;

                              ELSE
                                 --The batch number and error message does not exist in the setup form
                                 --hence using the default DT setup
                                 --Check for file reject for the following error
                                 IF NVL (
                                       g_txn_flow_flags_tbl (
                                          'DT:NIHOPETXN036').file_reject,
                                       'N') = 'Y'
                                 THEN
                                    g_file_reject_flag := TRUE;
                                 END IF;



                                 --Check the reject error transaction flag for the following error
                                 IF NVL (
                                       g_txn_flow_flags_tbl (
                                          'DT:NIHOPETXN036').reject_error_txn,
                                       'N') = 'Y'
                                 THEN
                                    g_index :=
                                          TRIM (c_dtls2_cur_rec.document_ref)
                                       || ':'
                                       || TRIM (
                                             c_dtls2_cur_rec.document_number)
                                       || ':'
                                       || TRIM (c_dtls2_cur_rec.can_fy);

                                    --If the index does not exist then include the index
                                    IF NOT g_txn_reject_tbl.EXISTS (g_index)
                                    THEN
                                       g_txn_reject_tbl (g_index) := 1;
                                    END IF;
                                 END IF;
                              END IF;*/

                              --End of change 1
                              logf (l_error_message,l_module);
                              record_error (l_error_message,
                                            l_ope_batch_id,
                                            l_ope_detail_id,
                                            l_record_id);
                              lb_success := FALSE;
                           END IF;
                        END IF;
                  END IF;
                  --CR CR NBSCH0002499 END
                  


                  IF lb_success
                  THEN
                     /*
                     || Update the intf table  for tcode 050 as (V)alidate
                     || There is no validate_amnt validation based on doc num/ref
                     */
                     IF c_dtls2_cur_rec.tcode = '050'
                     THEN
                        IF c_dtls2_cur_rec.reverse_code = 2
                        THEN
                           /*
                           || Check for Deobligation existence
                           || for reverse code 2 and tcode 050
                           */
                           l_deoblg_amnt_exist :=
                              chk_deobligation_exist (
                                    c_dtls2_cur_rec.document_ref
                                 || c_dtls2_cur_rec.document_number,
                                 c_dtls2_cur_rec.can_fy,
                                 c_dtls2_cur_rec.batch_number,
                                 c_dtls2_cur_rec.can,			--CR NBSCH0003180
                                 c_dtls2_cur_rec.object_class,	--CR NBSCH0003180
                                 c_dtls2_cur_rec.amount,
                                 c_dtls2_cur_rec.ope_detail_id);

                           IF l_deoblg_amnt_exist < 0
                           THEN
                              --Start of change 1 by Yash
                              --De-Obligation Amount is greater than the Obligation Balance
                              l_error_message :=
                                 g_err_message_tbl ('NIHOPETXN017');

                              --Check if responsibility name, batch number and error message is setup in the setup from
                              --else check if batch number and error message is setup in the setup form
                              --else use default (DT) batch and error message
                              IF g_txn_flow_flags_tbl.EXISTS (
                                       g_resp_name
                                    || ':'
                                    || p_batch_number
                                    || ':NIHOPETXN017')
                              THEN
                                 --The responsibility name, batch number and error message exist in the setup form
                                 --hence using the setup
                                 --Check for file reject for the following error
                                 IF NVL (
                                       g_txn_flow_flags_tbl (
                                             g_resp_name
                                          || ':'
                                          || p_batch_number
                                          || ':NIHOPETXN017').file_reject,
                                       'N') = 'Y'
                                 THEN
                                    g_file_reject_flag := TRUE;
                                 END IF;


                                 --Check the reject error transaction flag for the following error
                                 IF NVL (
                                       g_txn_flow_flags_tbl (
                                             g_resp_name
                                          || ':'
                                          || p_batch_number
                                          || ':NIHOPETXN017').reject_error_txn,
                                       'N') = 'Y'
                                 THEN
                                    g_index :=
                                          TRIM (c_dtls2_cur_rec.document_ref)
                                       || ':'
                                       || TRIM (
                                             c_dtls2_cur_rec.document_number)
                                       || ':'
                                       || TRIM (c_dtls2_cur_rec.can_fy);

                                    --If the index does not exist then include the index
                                    IF NOT g_txn_reject_tbl.EXISTS (g_index)
                                    THEN
                                       g_txn_reject_tbl (g_index) := 1;
                                    END IF;
                                 END IF;

                              ELSIF g_txn_flow_flags_tbl.EXISTS (
                                       p_batch_number || ':NIHOPETXN017')
                              THEN
                                 --The batch number and error message exist in the setup form
                                 --hence using the setup
                                 --Check for file reject for the following error
                                 IF NVL (
                                       g_txn_flow_flags_tbl (
                                          p_batch_number || ':NIHOPETXN017').file_reject,
                                       'N') = 'Y'
                                 THEN
                                    g_file_reject_flag := TRUE;
                                 END IF;


                                 --Check the reject error transaction flag for the following error
                                 IF NVL (
                                       g_txn_flow_flags_tbl (
                                          p_batch_number || ':NIHOPETXN017').reject_error_txn,
                                       'N') = 'Y'
                                 THEN
                                    g_index :=
                                          TRIM (c_dtls2_cur_rec.document_ref)
                                       || ':'
                                       || TRIM (
                                             c_dtls2_cur_rec.document_number)
                                       || ':'
                                       || TRIM (c_dtls2_cur_rec.can_fy);

                                    --If the index does not exist then include the index
                                    IF NOT g_txn_reject_tbl.EXISTS (g_index)
                                    THEN
                                       g_txn_reject_tbl (g_index) := 1;
                                    END IF;
                                 END IF;

                              ELSE
                                 --The batch number and error message does not exist in the setup form
                                 --hence using the default DT setup
                                 --Check for file reject for the following error
                                 IF NVL (
                                       g_txn_flow_flags_tbl (
                                          'DT:NIHOPETXN017').file_reject,
                                       'N') = 'Y'
                                 THEN
                                    g_file_reject_flag := TRUE;
                                 END IF;



                                 --Check the reject error transaction flag for the following error
                                 IF NVL (
                                       g_txn_flow_flags_tbl (
                                          'DT:NIHOPETXN017').reject_error_txn,
                                       'N') = 'Y'
                                 THEN
                                    g_index :=
                                          TRIM (c_dtls2_cur_rec.document_ref)
                                       || ':'
                                       || TRIM (
                                             c_dtls2_cur_rec.document_number)
                                       || ':'
                                       || TRIM (c_dtls2_cur_rec.can_fy);

                                    --If the index does not exist then include the index
                                    IF NOT g_txn_reject_tbl.EXISTS (g_index)
                                    THEN
                                       g_txn_reject_tbl (g_index) := 1;
                                    END IF;
                                 END IF;
                              END IF;

                              --End of change 1
                              logf (l_error_message,l_module);
                              record_error (l_error_message,
                                            l_ope_batch_id,
                                            l_ope_detail_id,
                                            l_record_id);
                              lb_success := FALSE;
                           END IF;
                        END IF;

                --------------------------------------------------
                        IF lb_success
                        THEN
                           BEGIN
                           logf ('V 1 c_dtls2_cur_rec.document_number: '||c_dtls2_cur_rec.document_number
                                 ||' c_dtls2_cur_rec.tcode : '||c_dtls2_cur_rec.tcode
                                 ||' c_dtls2_cur_rec.ope_detail_id: '||c_dtls2_cur_rec.ope_detail_id  , l_module);
                              UPDATE nihgl_ope_acct_dtls_int_tbl
                                 SET record_status = 'V' --Start of change 1 by Yash
                                                        ,
                                     ERROR_CODE = 'VALIDATED'
                               --End of change 1
                               WHERE     batch_number =
                                            c_dtls2_cur_rec.batch_number
                                     AND TRIM (
                                            document_ref || document_number) =
                                            TRIM (
                                                  c_dtls2_cur_rec.document_ref
                                               || c_dtls2_cur_rec.document_number)
                                     AND can_fy = c_dtls2_cur_rec.can_fy
                                     AND tcode = c_dtls2_cur_rec.tcode
                                     AND ope_detail_id =
                                            c_dtls2_cur_rec.ope_detail_id
                                     AND record_status = 'N';
                           EXCEPTION
                              WHEN OTHERS
                              THEN

                                 --Issue while updating Intf table for  tcode 050
                                 --as (V)alidated for ope_detail_id: &OPE_DETAIL_ID
                                 l_error_message :=
                                       REPLACE (
                                          REPLACE (
                                             g_err_message_tbl (
                                                'NIHOPETXN028'),
                                             '&TCODE',
                                             c_dtls2_cur_rec.tcode),
                                          '&OPE_DETAIL_ID',
                                          l_ope_detail_id)
                                    || '-'
                                    || SUBSTR (SQLERRM, 1, 250);
                                 logf (l_error_message,l_module);

                                 --l_error_message := g_err_message_tbl ('NIHOPETXN028');

                                 --Check if responsibility name, batch number and error message is setup in the setup from
                                 --else check if batch number and error message is setup in the setup form
                                 --else use default (DT) batch and error message
                                 IF g_txn_flow_flags_tbl.EXISTS (
                                          g_resp_name
                                       || ':'
                                       || p_batch_number
                                       || ':NIHOPETXN028')
                                 THEN
                                    --The responsibility name, batch number and error message exist in the setup form
                                    --hence using the setup
                                    --Check for file reject for the following error
                                    IF NVL (
                                          g_txn_flow_flags_tbl (
                                                g_resp_name
                                             || ':'
                                             || p_batch_number
                                             || ':NIHOPETXN028').file_reject,
                                          'N') = 'Y'
                                    THEN
                                       g_file_reject_flag := TRUE;
                                    END IF;


                                    --Check the reject error transaction flag for the following error
                                    IF NVL (
                                          g_txn_flow_flags_tbl (
                                                g_resp_name
                                             || ':'
                                             || p_batch_number
                                             || ':NIHOPETXN028').reject_error_txn,
                                          'N') = 'Y'
                                    THEN
                                       g_index :=
                                             TRIM (
                                                c_dtls2_cur_rec.document_ref)
                                          || ':'
                                          || TRIM (
                                                c_dtls2_cur_rec.document_number)
                                          || ':'
                                          || TRIM (c_dtls2_cur_rec.can_fy);

                                       --If the index does not exist then include the index
                                       IF NOT g_txn_reject_tbl.EXISTS (
                                                 g_index)
                                       THEN
                                          g_txn_reject_tbl (g_index) := 1;
                                       END IF;
                                    END IF;
                                 ELSIF g_txn_flow_flags_tbl.EXISTS (
                                          p_batch_number || ':NIHOPETXN028')
                                 THEN
                                    --The batch number and error message exist in the setup form
                                    --hence using the setup
                                    --Check for file reject for the following error
                                    IF NVL (
                                          g_txn_flow_flags_tbl (
                                                p_batch_number
                                             || ':NIHOPETXN028').file_reject,
                                          'N') = 'Y'
                                    THEN
                                       g_file_reject_flag := TRUE;
                                    END IF;



                                    --Check the reject error transaction flag for the following error
                                    IF NVL (
                                          g_txn_flow_flags_tbl (
                                                p_batch_number
                                             || ':NIHOPETXN028').reject_error_txn,
                                          'N') = 'Y'
                                    THEN
                                       g_index :=
                                             TRIM (
                                                c_dtls2_cur_rec.document_ref)
                                          || ':'
                                          || TRIM (
                                                c_dtls2_cur_rec.document_number)
                                          || ':'
                                          || TRIM (c_dtls2_cur_rec.can_fy);

                                       --If the index does not exist then include the index
                                       IF NOT g_txn_reject_tbl.EXISTS (
                                                 g_index)
                                       THEN
                                          g_txn_reject_tbl (g_index) := 1;
                                       END IF;
                                    END IF;

                                 ELSE
                                    --The batch number and error message does not exist in the setup form
                                    --hence using the default DT setup
                                    --Check for file reject for the following error
                                    IF NVL (
                                          g_txn_flow_flags_tbl (
                                             'DT:NIHOPETXN028').file_reject,
                                          'N') = 'Y'
                                    THEN
                                       g_file_reject_flag := TRUE;
                                    END IF;



                                    --Check the reject error transaction flag for the following error
                                    IF NVL (
                                          g_txn_flow_flags_tbl (
                                             'DT:NIHOPETXN028').reject_error_txn,
                                          'N') = 'Y'
                                    THEN
                                       g_index :=
                                             TRIM (
                                                c_dtls2_cur_rec.document_ref)
                                          || ':'
                                          || TRIM (
                                                c_dtls2_cur_rec.document_number)
                                          || ':'
                                          || TRIM (c_dtls2_cur_rec.can_fy);

                                       --If the index does not exist then include the index
                                       IF NOT g_txn_reject_tbl.EXISTS (
                                                 g_index)
                                       THEN
                                          g_txn_reject_tbl (g_index) := 1;
                                       END IF;
                                    END IF;
                                 END IF;

                                 --End of change 1
                                 record_error (l_error_message,
                                               l_ope_batch_id,
                                               l_ope_detail_id,
                                               l_record_id);
                           END;

                           ELSE
                                 logf ('E and R 1 c_dtls2_cur_rec.document_number: '||c_dtls2_cur_rec.document_number
                                 ||' c_dtls2_cur_rec.tcode : '||c_dtls2_cur_rec.tcode
                                 ||' c_dtls2_cur_rec.ope_detail_id: '||c_dtls2_cur_rec.ope_detail_id,l_module);
                        END IF;

                     ELSIF c_dtls2_cur_rec.tcode IN ('132', '133', '139')
                     THEN
                        IF c_dtls2_cur_rec.reverse_code = 2
                        THEN
                           /*
                           || Check for existing transaction to reverse existence
                           || for reverse code 2
                           */
                           l_rev_amnt_exist :=
                              chk_rev_txn_exist (
                                    c_dtls2_cur_rec.document_ref
                                 || c_dtls2_cur_rec.document_number,
                                 c_dtls2_cur_rec.can_fy,
                                 c_dtls2_cur_rec.batch_number,
                                 c_dtls2_cur_rec.tcode,
                                 c_dtls2_cur_rec.can,			--CR NBSCH0003180
                                 c_dtls2_cur_rec.object_class,	--CR NBSCH0003180
                                 c_dtls2_cur_rec.amount,
                                 c_dtls2_cur_rec.ope_detail_id);

                           IF l_rev_amnt_exist < 0
                           THEN
                              --Amount to reverse is greater than the existing total
                              l_error_message :=
                                 g_err_message_tbl ('NIHOPETXN031');

                              --Check if responsibility name, batch number and error message is setup in the setup from
                              --else check if batch number and error message is setup in the setup form
                              --else use default (DT) batch and error message
                              IF g_txn_flow_flags_tbl.EXISTS (
                                       g_resp_name
                                    || ':'
                                    || p_batch_number
                                    || ':NIHOPETXN031')
                              THEN
                                 --The responsibility name, batch number and error message exist in the setup form
                                 --hence using the setup
                                 --Check for file reject for the following error
                                 IF NVL (
                                       g_txn_flow_flags_tbl (
                                             g_resp_name
                                          || ':'
                                          || p_batch_number
                                          || ':NIHOPETXN031').file_reject,
                                       'N') = 'Y'
                                 THEN
                                    g_file_reject_flag := TRUE;
                                 END IF;


                                 --Check the reject error transaction flag for the following error
                                 IF NVL (
                                       g_txn_flow_flags_tbl (
                                             g_resp_name
                                          || ':'
                                          || p_batch_number
                                          || ':NIHOPETXN031').reject_error_txn,
                                       'N') = 'Y'
                                 THEN
                                    g_index :=
                                          TRIM (c_dtls2_cur_rec.document_ref)
                                       || ':'
                                       || TRIM (
                                             c_dtls2_cur_rec.document_number)
                                       || ':'
                                       || TRIM (c_dtls2_cur_rec.can_fy);

                                    --If the index does not exist then include the index
                                    IF NOT g_txn_reject_tbl.EXISTS (g_index)
                                    THEN
                                       g_txn_reject_tbl (g_index) := 1;
                                    END IF;
                                 END IF;

                              ELSIF g_txn_flow_flags_tbl.EXISTS (
                                       p_batch_number || ':NIHOPETXN031')
                              THEN
                                 --The batch number and error message exist in the setup form
                                 --hence using the setup
                                 --Check for file reject for the following error
                                 IF NVL (
                                       g_txn_flow_flags_tbl (
                                          p_batch_number || ':NIHOPETXN031').file_reject,
                                       'N') = 'Y'
                                 THEN
                                    g_file_reject_flag := TRUE;
                                 END IF;



                                 --Check the reject error transaction flag for the following error
                                 IF NVL (
                                       g_txn_flow_flags_tbl (
                                          p_batch_number || ':NIHOPETXN031').reject_error_txn,
                                       'N') = 'Y'
                                 THEN
                                    g_index :=
                                          TRIM (c_dtls2_cur_rec.document_ref)
                                       || ':'
                                       || TRIM (
                                             c_dtls2_cur_rec.document_number)
                                       || ':'
                                       || TRIM (c_dtls2_cur_rec.can_fy);

                                    --If the index does not exist then include the index
                                    IF NOT g_txn_reject_tbl.EXISTS (g_index)
                                    THEN
                                       g_txn_reject_tbl (g_index) := 1;
                                    END IF;
                                 END IF;

                              ELSE
                                 --The batch number and error message does not exist in the setup form
                                 --hence using the default DT setup
                                 --Check for file reject for the following error
                                 IF NVL (
                                       g_txn_flow_flags_tbl (
                                          'DT:NIHOPETXN031').file_reject,
                                       'N') = 'Y'
                                 THEN
                                    g_file_reject_flag := TRUE;
                                 END IF;



                                 --Check the reject error transaction flag for the following error
                                 IF NVL (
                                       g_txn_flow_flags_tbl (
                                          'DT:NIHOPETXN031').reject_error_txn,
                                       'N') = 'Y'
                                 THEN
                                    g_index :=
                                          TRIM (c_dtls2_cur_rec.document_ref)
                                       || ':'
                                       || TRIM (
                                             c_dtls2_cur_rec.document_number)
                                       || ':'
                                       || TRIM (c_dtls2_cur_rec.can_fy);

                                    --If the index does not exist then include the index
                                    IF NOT g_txn_reject_tbl.EXISTS (g_index)
                                    THEN
                                       g_txn_reject_tbl (g_index) := 1;
                                    END IF;
                                 END IF;
                              END IF;

                              --End of change 1
                              logf (l_error_message,l_module);
                              record_error (l_error_message,
                                            l_ope_batch_id,
                                            l_ope_detail_id,
                                            l_record_id);
                              lb_success := FALSE;
                           END IF;
                        END IF;

                        IF lb_success
                        THEN
                                 logf ('V 2 c_dtls2_cur_rec.document_number: '||c_dtls2_cur_rec.document_number
                                 ||' c_dtls2_cur_rec.tcode : '||c_dtls2_cur_rec.tcode
                                 ||' c_dtls2_cur_rec.ope_detail_id: '||c_dtls2_cur_rec.ope_detail_id, l_module);

                           BEGIN
                              UPDATE nihgl_ope_acct_dtls_int_tbl
                                 SET record_status = 'V',
                                     ERROR_CODE = 'VALIDATED'
                               WHERE     batch_number =
                                            c_dtls2_cur_rec.batch_number
                                     AND TRIM (
                                            document_ref || document_number) =
                                            TRIM (
                                                  c_dtls2_cur_rec.document_ref
                                               || c_dtls2_cur_rec.document_number)
                                     AND can_fy = c_dtls2_cur_rec.can_fy
                                     AND tcode = c_dtls2_cur_rec.tcode
                                     AND ope_detail_id =
                                            c_dtls2_cur_rec.ope_detail_id
                                     AND record_status = 'N';
                           EXCEPTION
                              WHEN OTHERS
                              THEN
                                 --Issue while updating Intf table for  tcode 050
                                 --as (V)alidated for ope_detail_id: &OPE_DETAIL_ID
                                 l_error_message :=
                                       REPLACE (
                                          REPLACE (
                                             g_err_message_tbl (
                                                'NIHOPETXN028'),
                                             '&TCODE',
                                             c_dtls2_cur_rec.tcode),
                                          '&OPE_DETAIL_ID',
                                          l_ope_detail_id)
                                    || '-'
                                    || SUBSTR (SQLERRM, 1, 250);
                                 logf (l_error_message,l_module);

                                 --l_error_message := g_err_message_tbl ('NIHOPETXN028');

                                 --Check if responsibility name, batch number and error message is setup in the setup from
                                 --else check if batch number and error message is setup in the setup form
                                 --else use default (DT) batch and error message
                                 IF g_txn_flow_flags_tbl.EXISTS (
                                          g_resp_name
                                       || ':'
                                       || p_batch_number
                                       || ':NIHOPETXN028')
                                 THEN
                                    --The responsibility name, batch number and error message exist in the setup form
                                    --hence using the setup
                                    --Check for file reject for the following error
                                    IF NVL (
                                          g_txn_flow_flags_tbl (
                                                g_resp_name
                                             || ':'
                                             || p_batch_number
                                             || ':NIHOPETXN028').file_reject,
                                          'N') = 'Y'
                                    THEN
                                       g_file_reject_flag := TRUE;
                                    END IF;



                                    --Check the reject error transaction flag for the following error
                                    IF NVL (
                                          g_txn_flow_flags_tbl (
                                                g_resp_name
                                             || ':'
                                             || p_batch_number
                                             || ':NIHOPETXN028').reject_error_txn,
                                          'N') = 'Y'
                                    THEN
                                       g_index :=
                                             TRIM (
                                                c_dtls2_cur_rec.document_ref)
                                          || ':'
                                          || TRIM (
                                                c_dtls2_cur_rec.document_number)
                                          || ':'
                                          || TRIM (c_dtls2_cur_rec.can_fy);

                                       --If the index does not exist then include the index
                                       IF NOT g_txn_reject_tbl.EXISTS (
                                                 g_index)
                                       THEN
                                          g_txn_reject_tbl (g_index) := 1;
                                       END IF;
                                    END IF;

                                 ELSIF g_txn_flow_flags_tbl.EXISTS (
                                          p_batch_number || ':NIHOPETXN028')
                                 THEN
                                    --The batch number and error message exist in the setup form
                                    --hence using the setup
                                    --Check for file reject for the following error
                                    IF NVL (
                                          g_txn_flow_flags_tbl (
                                                p_batch_number
                                             || ':NIHOPETXN028').file_reject,
                                          'N') = 'Y'
                                    THEN
                                       g_file_reject_flag := TRUE;
                                    END IF;



                                    --Check the reject error transaction flag for the following error
                                    IF NVL (
                                          g_txn_flow_flags_tbl (
                                                p_batch_number
                                             || ':NIHOPETXN028').reject_error_txn,
                                          'N') = 'Y'
                                    THEN
                                       g_index :=
                                             TRIM (
                                                c_dtls2_cur_rec.document_ref)
                                          || ':'
                                          || TRIM (
                                                c_dtls2_cur_rec.document_number)
                                          || ':'
                                          || TRIM (c_dtls2_cur_rec.can_fy);

                                       --If the index does not exist then include the index
                                       IF NOT g_txn_reject_tbl.EXISTS (
                                                 g_index)
                                       THEN
                                          g_txn_reject_tbl (g_index) := 1;
                                       END IF;
                                    END IF;

                                 ELSE
                                    --The batch number and error message does not exist in the setup form
                                    --hence using the default DT setup
                                    --Check for file reject for the following error
                                    IF NVL (
                                          g_txn_flow_flags_tbl (
                                             'DT:NIHOPETXN028').file_reject,
                                          'N') = 'Y'
                                    THEN
                                       g_file_reject_flag := TRUE;
                                    END IF;



                                    --Check the reject error transaction flag for the following error
                                    IF NVL (
                                          g_txn_flow_flags_tbl (
                                             'DT:NIHOPETXN028').reject_error_txn,
                                          'N') = 'Y'
                                    THEN
                                       g_index :=
                                             TRIM (
                                                c_dtls2_cur_rec.document_ref)
                                          || ':'
                                          || TRIM (
                                                c_dtls2_cur_rec.document_number)
                                          || ':'
                                          || TRIM (c_dtls2_cur_rec.can_fy);

                                       --If the index does not exist then include the index
                                       IF NOT g_txn_reject_tbl.EXISTS (
                                                 g_index)
                                       THEN
                                          g_txn_reject_tbl (g_index) := 1;
                                       END IF;
                                    END IF;
                                 END IF;

                                 --End of change 1
                                 record_error (l_error_message,
                                               l_ope_batch_id,
                                               l_ope_detail_id,
                                               l_record_id);
                            END;


                        ELSE
                                 logf ('E and R  2 c_dtls2_cur_rec.document_number: '||c_dtls2_cur_rec.document_number
                                 ||' c_dtls2_cur_rec.tcode : '||c_dtls2_cur_rec.tcode
                                 ||' c_dtls2_cur_rec.ope_detail_id: '||c_dtls2_cur_rec.ope_detail_id ,l_module);
                        END IF;

                --------------------------------------------------
                     ELSIF c_dtls2_cur_rec.batch_number = '3Z'
                     THEN
                        /*
                       || Update the intf table  for Batch  3Z as (V)alidate
                       || There is no validate_amnt validation based on doc num/ref
                       */
                        BEGIN
                                logf ('V 3 c_dtls2_cur_rec.document_number: '||c_dtls2_cur_rec.document_number
                                 ||' c_dtls2_cur_rec.tcode : '||c_dtls2_cur_rec.tcode
                                 ||' c_dtls2_cur_rec.ope_detail_id: '||c_dtls2_cur_rec.ope_detail_id , l_module);
                           UPDATE nihgl_ope_acct_dtls_int_tbl
                              SET record_status = 'V',
                                  ERROR_CODE = 'VALIDATED'
                            WHERE     batch_number =
                                         c_dtls2_cur_rec.batch_number
                                  AND TRIM (document_ref || document_number) =
                                         TRIM (
                                               c_dtls2_cur_rec.document_ref
                                            || c_dtls2_cur_rec.document_number)
                                  AND can_fy = c_dtls2_cur_rec.can_fy
                                  AND tcode = c_dtls2_cur_rec.tcode
                                  AND ope_detail_id =
                                         c_dtls2_cur_rec.ope_detail_id
                                  AND record_status = 'N';
                        EXCEPTION
                           WHEN OTHERS
                           THEN
                              --Start of change 1 by Yash
                              --Issue while  updating 3Z Batch as (V)alidated For ope_detail_id: &OPE_DETAIL_ID
                              l_error_message :=
                                 REPLACE (g_err_message_tbl ('NIHOPETXN022'),
                                          '&OPE_DETAIL_ID',
                                          l_ope_detail_id);
                              logf (l_error_message,l_module);

                              /*l_error_message :=
                                 'Issue while updating 3Z Batch as (V)alidated';*/

                              --Check if responsibility name, batch number and error message is setup in the setup from
                              --else check if batch number and error message is setup in the setup form
                              --else use default (DT) batch and error message
                              IF g_txn_flow_flags_tbl.EXISTS (
                                       g_resp_name
                                    || ':'
                                    || p_batch_number
                                    || ':NIHOPETXN022')
                              THEN
                                 --The responsibility name, batch number and error message exist in the setup form
                                 --hence using the setup
                                 --Check for file reject for the following error
                                 IF NVL (
                                       g_txn_flow_flags_tbl (
                                             g_resp_name
                                          || ':'
                                          || p_batch_number
                                          || ':NIHOPETXN022').file_reject,
                                       'N') = 'Y'
                                 THEN
                                    g_file_reject_flag := TRUE;
                                 END IF;



                                 --Check the reject error transaction flag for the following error
                                 IF NVL (
                                       g_txn_flow_flags_tbl (
                                             g_resp_name
                                          || ':'
                                          || p_batch_number
                                          || ':NIHOPETXN022').reject_error_txn,
                                       'N') = 'Y'
                                 THEN
                                    g_index :=
                                          TRIM (c_dtls2_cur_rec.document_ref)
                                       || ':'
                                       || TRIM (
                                             c_dtls2_cur_rec.document_number)
                                       || ':'
                                       || TRIM (c_dtls2_cur_rec.can_fy);

                                    --If the index does not exist then include the index
                                    IF NOT g_txn_reject_tbl.EXISTS (g_index)
                                    THEN
                                       g_txn_reject_tbl (g_index) := 1;
                                    END IF;
                                 END IF;
                              ELSIF g_txn_flow_flags_tbl.EXISTS (
                                       p_batch_number || ':NIHOPETXN022')
                              THEN
                                 --The batch number and error message exist in the setup form
                                 --hence using the setup
                                 --Check for file reject for the following error
                                 IF NVL (
                                       g_txn_flow_flags_tbl (
                                          p_batch_number || ':NIHOPETXN022').file_reject,
                                       'N') = 'Y'
                                 THEN
                                    g_file_reject_flag := TRUE;
                                 END IF;

                                 --Check the hold for ope error handling flag for the following error
                                 IF NVL (
                                       g_txn_flow_flags_tbl (
                                          p_batch_number || ':NIHOPETXN022').hold_for_ope_eh,
                                       'N') = 'Y'
                                 THEN
                                    g_index :=
                                          TRIM (c_dtls2_cur_rec.document_ref)
                                       || ':'
                                       || TRIM (
                                             c_dtls2_cur_rec.document_number)
                                       || ':'
                                       || TRIM (c_dtls2_cur_rec.can_fy);

                                    --If the index does not exist then include the index
                                    IF NOT g_txn_hold_tbl.EXISTS (g_index)
                                    THEN
                                       g_txn_hold_tbl (g_index) := 1;
                                    END IF;
                                 END IF;

                                 --Check the reject error transaction flag for the following error
                                 IF NVL (
                                       g_txn_flow_flags_tbl (
                                          p_batch_number || ':NIHOPETXN022').reject_error_txn,
                                       'N') = 'Y'
                                 THEN
                                    g_index :=
                                          TRIM (c_dtls2_cur_rec.document_ref)
                                       || ':'
                                       || TRIM (
                                             c_dtls2_cur_rec.document_number)
                                       || ':'
                                       || TRIM (c_dtls2_cur_rec.can_fy);

                                    --If the index does not exist then include the index
                                    IF NOT g_txn_reject_tbl.EXISTS (g_index)
                                    THEN
                                       g_txn_reject_tbl (g_index) := 1;
                                    END IF;
                                 END IF;
                              ELSE
                                 --The batch number and error message does not exist in the setup form
                                 --hence using the default DT setup
                                 --Check for file reject for the following error
                                 IF NVL (
                                       g_txn_flow_flags_tbl (
                                          'DT:NIHOPETXN022').file_reject,
                                       'N') = 'Y'
                                 THEN
                                    g_file_reject_flag := TRUE;
                                 END IF;



                                 --Check the reject error transaction flag for the following error
                                 IF NVL (
                                       g_txn_flow_flags_tbl (
                                          'DT:NIHOPETXN022').reject_error_txn,
                                       'N') = 'Y'
                                 THEN
                                    g_index :=
                                          TRIM (c_dtls2_cur_rec.document_ref)
                                       || ':'
                                       || TRIM (
                                             c_dtls2_cur_rec.document_number)
                                       || ':'
                                       || TRIM (c_dtls2_cur_rec.can_fy);

                                    --If the index does not exist then include the index
                                    IF NOT g_txn_reject_tbl.EXISTS (g_index)
                                    THEN
                                       g_txn_reject_tbl (g_index) := 1;
                                    END IF;
                                 END IF;
                              END IF;

                              --End of change 1
                              record_error (l_error_message,
                                            l_ope_batch_id,
                                            l_ope_detail_id,
                                            l_record_id);
                        END;
                     ELSE
                                logf ('E and R 3 c_dtls2_cur_rec.document_number: '||c_dtls2_cur_rec.document_number
                                 ||' c_dtls2_cur_rec.tcode : '||c_dtls2_cur_rec.tcode
                                 ||' c_dtls2_cur_rec.ope_detail_id: '||c_dtls2_cur_rec.ope_detail_id , l_module);
                     END IF;
                  END IF;
               EXCEPTION
                  WHEN OTHERS
                  THEN
                     l_error_message :=
                        l_error_message || '-' || SUBSTR (SQLERRM, 1, 250);
                     logf (l_error_message , l_module);
                     record_error (l_error_message,
                                   l_ope_batch_id,
                                   l_ope_detail_id,
                                   l_record_id);
               END;
            END LOOP;

         ELSE
            IF v_org_flag = 'N'
            THEN
               l_error_message := 'Org Information not found';
               logf (l_error_message, l_module);
            ELSIF g_accounting_date IS NULL
            THEN
               l_error_message := 'Accounting date is NULL';
               logf (l_error_message, l_module);
            ELSIF g_accounting_period IS NULL
            THEN

               l_error_message :=
                  REPLACE (g_err_message_tbl ('NIHOPETXN030'),
                           '&ACCOUNTING_DATE',
                           g_accounting_date);

               --Check if responsibility name, batch number and error message is setup in the setup from
               --else check if batch number and error message is setup in the setup form
               --else use default (DT) batch and error message
               IF g_txn_flow_flags_tbl.EXISTS (
                     g_resp_name || ':' || p_batch_number || ':NIHOPETXN030')
               THEN
                  --The responsibility name, batch number and error message exist in the setup form
                  --hence using the setup
                  --Check for file reject for the following error
                  IF NVL (
                        g_txn_flow_flags_tbl (
                              g_resp_name
                           || ':'
                           || p_batch_number
                           || ':NIHOPETXN030').file_reject,
                        'N') = 'Y'
                  THEN
                     g_file_reject_flag := TRUE;
                  END IF;



                  --Check the reject error transaction flag for the following error
                  --- not sure what the hell the below code tyring to acheive , instea of setting reject flag seetnig hold table update

                  IF NVL (
                        g_txn_flow_flags_tbl (
                              g_resp_name
                           || ':'
                           || p_batch_number
                           || ':NIHOPETXN030').reject_error_txn,
                        'N') = 'Y'
                  THEN
                     --Reject all the records
                     FOR cur_dtls2_rec
                        IN (SELECT document_number, document_ref, can_fy
                              FROM nihgl_ope_acct_dtls_int_tbl
                             WHERE     ope_batch_id =
                                          c_dtls1_cur_rec.ope_batch_id
                                   AND batch_number =
                                          c_dtls1_cur_rec.batch_number)
                     LOOP
                        g_index :=
                              TRIM (cur_dtls2_rec.document_ref)
                           || ':'
                           || TRIM (cur_dtls2_rec.document_number)
                           || ':'
                           || TRIM (cur_dtls2_rec.can_fy);

                        --If the index does not exist then include the index
                        IF NOT g_txn_hold_tbl.EXISTS (g_index)
                        THEN
                           g_txn_hold_tbl (g_index) := 1;
                        END IF;
                     END LOOP;
                  END IF;
                   ---------------------------------------------------------


               ELSIF g_txn_flow_flags_tbl.EXISTS (
                        p_batch_number || ':NIHOPETXN030')
               THEN
                  --The batch number and error message exist in the setup form
                  --hence using the setup
                  --Check for file reject for the following error
                  IF NVL (
                        g_txn_flow_flags_tbl (
                           p_batch_number || ':NIHOPETXN030').file_reject,
                        'N') = 'Y'
                  THEN
                     g_file_reject_flag := TRUE;
                  END IF;


                   --- KS same as above the hold table does nothing , not sure if they really mean to set reject global flag

                  --Check the reject error transaction flag for the following error
                  IF NVL (
                        g_txn_flow_flags_tbl (
                           p_batch_number || ':NIHOPETXN030').reject_error_txn,
                        'N') = 'Y'
                  THEN
                     --Reject all the records
                     FOR cur_dtls2_rec
                        IN (SELECT document_number, document_ref, can_fy
                              FROM nihgl_ope_acct_dtls_int_tbl
                             WHERE     ope_batch_id =
                                          c_dtls1_cur_rec.ope_batch_id
                                   AND batch_number =
                                          c_dtls1_cur_rec.batch_number)
                     LOOP
                        g_index :=
                              TRIM (cur_dtls2_rec.document_ref)
                           || ':'
                           || TRIM (cur_dtls2_rec.document_number)
                           || ':'
                           || TRIM (cur_dtls2_rec.can_fy);

                        --If the index does not exist then include the index
                        IF NOT g_txn_hold_tbl.EXISTS (g_index)
                        THEN
                           g_txn_hold_tbl (g_index) := 1;
                        END IF;
                     END LOOP;
                  END IF;

               ELSE

                  --The batch number and error message does not exist in the setup form
                  --hence using the default DT setup
                  --Check for file reject for the following error
                  IF NVL (
                        g_txn_flow_flags_tbl ('DT:NIHOPETXN030').file_reject,
                        'N') = 'Y'
                  THEN
                     g_file_reject_flag := TRUE;
                  END IF;


                 --- KS same issue as above block of code

                  --Check the reject error transaction flag for the following error
                  IF NVL (
                        g_txn_flow_flags_tbl ('DT:NIHOPETXN030').reject_error_txn,
                        'N') = 'Y'
                  THEN
                     --Reject all the records
                     FOR cur_dtls2_rec
                        IN (SELECT document_number, document_ref, can_fy
                              FROM nihgl_ope_acct_dtls_int_tbl
                             WHERE     ope_batch_id =
                                          c_dtls1_cur_rec.ope_batch_id
                                   AND batch_number =
                                          c_dtls1_cur_rec.batch_number)
                     LOOP
                        g_index :=
                              TRIM (cur_dtls2_rec.document_ref)
                           || ':'
                           || TRIM (cur_dtls2_rec.document_number)
                           || ':'
                           || TRIM (cur_dtls2_rec.can_fy);

                        --If the index does not exist then include the index
                        IF NOT g_txn_hold_tbl.EXISTS (g_index)
                        THEN
                           g_txn_hold_tbl (g_index) := 1;
                        END IF;
                     END LOOP;
                  END IF;

               END IF;

               --Mark all the records for the given file as error
               FOR cur_dtls2_rec
                  IN (SELECT record_id, ope_batch_id, ope_detail_id
                        FROM nihgl_ope_acct_dtls_int_tbl
                       WHERE     ope_batch_id = c_dtls1_cur_rec.ope_batch_id
                             AND batch_number = c_dtls1_cur_rec.batch_number)
               LOOP
                  record_error (l_error_message,
                                cur_dtls2_rec.ope_batch_id,
                                cur_dtls2_rec.ope_detail_id,
                                cur_dtls2_rec.record_id);
               END LOOP;

               --End of Change 1
               logf (l_error_message, l_module);

        ---------------------verified KS-----------------------------------------
            ELSIF g_account IS NULL
            THEN
               l_error_message :=
                     'Initial Account is not Set up in lookup '
                  || 'NIHGL_OPE_CONTROLS to geting CCID';
               logf (l_error_message , l_module);
            ELSIF g_ope_je_source IS NULL
            THEN
               l_error_message := 'Journal Source is not setup ';
               logf (l_error_message, l_module);
            ELSIF g_ope_je_category IS NULL
            THEN
               l_error_message := 'Journal Category is not setup';
               logf (l_error_message, l_module);
            ELSIF g_fund_type_tcode_lookup IS NULL
            THEN
               l_error_message :=
                     'USSGL transaction Code and TCODE mapping not setup'
                  || ' in NIHGL_OPE_TCODE_MAPPING Lookup';
               logf (l_error_message, l_module);
            END IF;

            lb_success := FALSE;

            BEGIN
              logf ('U 1 c_dtls1_cur_rec.ope_batch_id: '||c_dtls1_cur_rec.ope_batch_id , l_module);
               /*
              || Mark all Master records as Unprocesses--
              */
               UPDATE nihgl_ope_acct_btchs_int_tbl
                  SET record_status = 'U',
                      error_message = l_error_message,
                      ERROR_CODE = 'UNPROCESSED'
                WHERE     batch_number = c_dtls1_cur_rec.batch_number
                      AND ope_batch_id = c_dtls1_cur_rec.ope_batch_id
                      AND record_status = 'N';

               /*
               || Mark all detail records as Unprocessed---
               */
               UPDATE nihgl_ope_acct_dtls_int_tbl
                  SET record_status = 'U',
                      error_message = l_error_message,
                      ERROR_CODE = 'UNPROCESSED'
                WHERE     batch_number = c_dtls1_cur_rec.batch_number
                      AND ope_batch_id = c_dtls1_cur_rec.ope_batch_id
                      AND record_status = 'N';
            EXCEPTION
               WHEN OTHERS
               THEN
                  l_error_message :=
                        'Issue while  updating Intf tables (Master/Detail) for '
                     || '(U)nprocess for ope_detail_id :'
                     || l_ope_detail_id
                     || '-'
                     || SUBSTR (SQLERRM, 1, 250);
                  logf (l_error_message, l_module);
            END;
         END IF;
      END LOOP;

      COMMIT;
       logf('Record Status Before update_err_doc_number', l_module);
       FOR rec IN (
           SELECT record_status, COUNT(*) cnt
           FROM nihgl_ope_acct_dtls_int_tbl
           WHERE  
               --AND can_fy          = c_docdtl_rec.can_fy
                 file_name = p_file_name
           GROUP BY record_status
       )
       LOOP
          logf('Record Status: ' || rec.record_status ||
               ' Count: ' || rec.cnt,
               l_module);
       END LOOP;
      /*
      || Update if any of the record error out by above process
      || Mark all as error out for the same doc number block
      */

      update_err_doc_number (p_batch_number, p_file_name);           --TESTING
      COMMIT;
      
       logf('Record Status After update_err_doc_number', l_module);
       FOR rec IN (
           SELECT record_status, COUNT(*) cnt
           FROM nihgl_ope_acct_dtls_int_tbl
           WHERE  --batch_number = p_batch_number
                 --AND TRIM (document_ref || document_number) = TRIM (p_doc_ref || p_doc_number) 
               --AND can_fy          = c_docdtl_rec.can_fy
                 file_name = p_file_name
           GROUP BY record_status
       )
       LOOP
          logf('Record Status: ' || rec.record_status ||
               ' Count: ' || rec.cnt,
               l_module);
       END LOOP;
      /*
     ||Use a cursor to start buckets based validation
     ||revenue existence, amount equality
     */
      FOR c_docmas_rec IN c_docmas_cur
      LOOP
         l_proess_flag := TRUE;

         FOR c_docdtl_rec
            IN c_docdtl_cur (                           --c_docmas_rec.can_fy,
                             c_docmas_rec.batch_number,
                             c_docmas_rec.document_number,
                             c_docmas_rec.file_name)
         LOOP
                logf('Record Status START FOR', l_module);
                   FOR rec IN (
                       SELECT record_status, COUNT(*) cnt
                       FROM nihgl_ope_acct_dtls_int_tbl
                       WHERE  batch_number = c_docdtl_rec.batch_number
                             AND TRIM (document_ref || document_number) = c_docdtl_rec.document_number
                           --AND can_fy          = c_docdtl_rec.can_fy
                             AND file_name = c_docdtl_rec.file_name
                       GROUP BY record_status
                   )
                   LOOP
                      logf('Record Status: ' || rec.record_status ||
                           ' Count: ' || rec.cnt,
                           l_module);
                   END LOOP;
            /*
            || call procedure to check amount equality based on
            || doc_num and fy and Batch Number
            */
            l_error_message := NULL;
            l_rev_line_chk_flag := 'Y';

            -- Animesh As we don't need this check for collection and deposit transactions
            -- Hardcoding to ignore the check for these accounts.
            IF c_docdtl_rec.tcode NOT IN
                  ('133', '139', '232', '224', '132', '220', '241')
            THEN
               IF NOT validate_amnt (
                         p_docnum         => c_docdtl_rec.document_number,
                         --p_fy          =>c_docdtl_rec.can_fy,
                         p_file_name      => c_docdtl_rec.file_name,
                         p_batch_number   => c_docdtl_rec.batch_number)
               THEN
                  --Start of change 1 by Yash
                  --Amounts are not equal for the combination of Document Ref/Number, Fiscal Year and Tcode
                  l_error_message :=
                        g_err_message_tbl ('NIHOPETXN016')
                     || ':'
                     || c_docdtl_rec.document_number
                     || ': '                           --||c_docdtl_rec.can_fy
                     || ': '
                     || c_docdtl_rec.batch_number;
                  logf (l_error_message, l_module);
                  l_error_message := g_err_message_tbl ('NIHOPETXN016');

                  --Check if responsibility name, batch number and error message is setup in the setup from
                  --else check if batch number and error message is setup in the setup form
                  --else use default (DT) batch and error message
                  IF g_txn_flow_flags_tbl.EXISTS (
                           g_resp_name
                        || ':'
                        || p_batch_number
                        || ':NIHOPETXN016')
                  THEN
                     --The responsibility name, batch number and error message exist in the setup form
                     --hence using the setup
                     --Check for file reject for the following error
                     IF NVL (
                           g_txn_flow_flags_tbl (
                                 g_resp_name
                              || ':'
                              || p_batch_number
                              || ':NIHOPETXN016').file_reject,
                           'N') = 'Y'
                     THEN
                        g_file_reject_flag := TRUE;
                     END IF;


                     --Check the reject error transaction flag for the following error
                     IF NVL (
                           g_txn_flow_flags_tbl (
                                 g_resp_name
                              || ':'
                              || p_batch_number
                              || ':NIHOPETXN016').reject_error_txn,
                           'N') = 'Y'
                     THEN
                        g_index :=
                              TRIM (c_docmas_rec.doc_ref)
                           || ':'
                           || TRIM (c_docmas_rec.doc_number)
                           || ':'
                           || TRIM (c_docmas_rec.can_fy);

                        --If the index does not exist then include the index
                        IF NOT g_txn_reject_tbl.EXISTS (g_index)
                        THEN
                           g_txn_reject_tbl (g_index) := 1;
                        END IF;
                     END IF;

                  ELSIF g_txn_flow_flags_tbl.EXISTS ( p_batch_number || ':NIHOPETXN016')
                  THEN
                     --The batch number and error message exist in the setup form
                     --hence using the setup
                     --Check for file reject for the following error
                     IF NVL (  g_txn_flow_flags_tbl ( p_batch_number || ':NIHOPETXN016').file_reject, 'N') = 'Y'
                     THEN
                        g_file_reject_flag := TRUE;
                     END IF;



                     --Check the reject error transaction flag for the following error
                     IF NVL ( g_txn_flow_flags_tbl (  p_batch_number || ':NIHOPETXN016').reject_error_txn, 'N') = 'Y'
                     THEN
                        g_index :=
                              TRIM (c_docmas_rec.doc_ref)
                           || ':'
                           || TRIM (c_docmas_rec.doc_number)
                           || ':'
                           || TRIM (c_docmas_rec.can_fy);

                        --If the index does not exist then include the index
                        IF NOT g_txn_reject_tbl.EXISTS (g_index)
                        THEN
                           g_txn_reject_tbl (g_index) := 1;
                        END IF;
                     END IF;

                  ELSE
                     --The batch number and error message does not exist in the setup form
                     --hence using the default DT setup
                     --Check for file reject for the following error
                     IF NVL ( g_txn_flow_flags_tbl ('DT:NIHOPETXN016').file_reject, 'N') = 'Y'
                     THEN
                        g_file_reject_flag := TRUE;
                     END IF;




                     --Check the reject error transaction flag for the following error
                     IF NVL ( g_txn_flow_flags_tbl ('DT:NIHOPETXN016').reject_error_txn, 'N') = 'Y'
                     THEN
                        g_index :=
                              TRIM (c_docmas_rec.doc_ref)
                           || ':'
                           || TRIM (c_docmas_rec.doc_number)
                           || ':'
                           || TRIM (c_docmas_rec.can_fy);

                        --If the index does not exist then include the index
                        IF NOT g_txn_reject_tbl.EXISTS (g_index)  THEN
                           g_txn_reject_tbl (g_index) := 1;
                        END IF;
                     END IF;
                  END IF;

                  --End of change 1

                  BEGIN
                     UPDATE nihgl_ope_acct_dtls_int_tbl
                        SET error_message =  error_message || DECODE (error_message,  NULL, l_error_message, '; ' || l_error_message),
                            ERROR_CODE    = 'ERROR',
                            last_updated_by = fnd_global.user_id,
                            last_update_date = SYSDATE,
                            last_update_login = fnd_global.login_id,
                            record_status = 'E'
                      WHERE TRIM (document_ref || document_number) = c_docdtl_rec.document_number
                        AND file_name    = c_docdtl_rec.file_name
                      --AND can_fy       = c_docdtl_rec.can_fy
                        AND batch_number = c_docdtl_rec.batch_number;

                     COMMIT;
                  EXCEPTION
                     WHEN OTHERS THEN
                        logf ( 'Error while Updating intf table for Error ' || '(Amounts Are Not Equal)' || SUBSTR (SQLERRM, 1, 250) , l_module);
                  END;
                  l_rev_line_chk_flag := 'N';
               END IF;
            END IF;

      logf('Record Status Before funds_check FIRST', l_module);
       FOR rec IN (
           SELECT record_status, COUNT(*) cnt
           FROM nihgl_ope_acct_dtls_int_tbl
           WHERE  --batch_number = p_batch_number
                 --AND TRIM (document_ref || document_number) = TRIM (p_doc_ref || p_doc_number) 
               --AND can_fy          = c_docdtl_rec.can_fy
                 file_name = p_file_name
           GROUP BY record_status
       )
       LOOP
          logf('Record Status: ' || rec.record_status ||
               ' Count: ' || rec.cnt,
               l_module);
       END LOOP;
        --------------------Fund Check -----------------------------------
            logf('Record Status Before funds_check',
                   l_module);
           FOR rec IN (
               SELECT record_status, COUNT(*) cnt
               FROM nihgl_ope_acct_dtls_int_tbl
               WHERE  batch_number = c_docdtl_rec.batch_number
                     AND TRIM (document_ref || document_number) = c_docdtl_rec.document_number
                   --AND can_fy          = c_docdtl_rec.can_fy
                     AND file_name = c_docdtl_rec.file_name
               GROUP BY record_status
           )
           LOOP
              logf('Record Status: ' || rec.record_status ||
                   ' Count: ' || rec.cnt,
                   l_module);
           END LOOP;
            BEGIN
              logf ('c_docdtl_rec.batch_number :'||c_docdtl_rec.batch_number||
                    ' c_docdtl_rec.document_number : '||c_docdtl_rec.document_number||
                    ' c_docdtl_rec.file_name :'||c_docdtl_rec.file_name, l_module);

              funds_check( c_docdtl_rec.batch_number, c_docdtl_rec.document_number,
                          c_docdtl_rec.file_name,  v_result_out,  v_errnum, v_errtext);

                          logf ('v_result_out :'||v_result_out , l_module);

                   IF v_result_out != 0 THEN
                      logf ('v_result_out :'||v_result_out , l_module);
                      l_rev_line_chk_flag := 'N';
                      logf ('l_rev_line_chk_flag :'||l_rev_line_chk_flag , l_module);
                      BEGIN
                         UPDATE nihgl_ope_acct_dtls_int_tbl
                            SET error_message = v_errtext,
                                ERROR_CODE = 'ERROR',
                                last_updated_by = fnd_global.user_id,
                                last_update_date = SYSDATE,
                                last_update_login = fnd_global.login_id,
                                record_status = 'E'
                          WHERE TRIM (document_ref || document_number) = c_docdtl_rec.document_number
                            AND file_name = c_docdtl_rec.file_name
                          --AND can_fy       = c_docdtl_rec.can_fy
                            AND batch_number = c_docdtl_rec.batch_number;
                         logf ('Update Fund Error :'||sql%rowcount , l_module);
                         COMMIT;
                      EXCEPTION
                         WHEN OTHERS THEN
                            logf ( 'Error while Updating intf table for Error '
                               || '(Amounts Are Not Equal)'
                               || SUBSTR (SQLERRM, 1, 250), l_module);
                      END;
                   END IF;
            EXCEPTION
               WHEN OTHERS  THEN
                  logf ('Fund Check Proc call completed with Error '|| SUBSTR (SQLERRM, 1, 250), l_module);
            END;

            /*
            || Update the successful validated document number, mark as (V)alidate
            */
            logf('Record Status After funds_check',
                   l_module);
           FOR rec IN (
               SELECT record_status, COUNT(*) cnt
               FROM nihgl_ope_acct_dtls_int_tbl
               WHERE  batch_number = c_docdtl_rec.batch_number
                     AND TRIM (document_ref || document_number) = c_docdtl_rec.document_number
                   --AND can_fy          = c_docdtl_rec.can_fy
                     AND file_name = c_docdtl_rec.file_name
               GROUP BY record_status
           )
           LOOP
              logf('Record Status: ' || rec.record_status ||
                   ' Count: ' || rec.cnt,
                   l_module);
           END LOOP;
            logf ('After Fund Error :'||l_rev_line_chk_flag , l_module);
            IF l_rev_line_chk_flag = 'Y' THEN
               logf ('After Fund Error update  record to V:', l_module);
               BEGIN
                  logf ('l_rev_line_chk_flag = Y c_docdtl_rec.batch_number: '||c_docdtl_rec.batch_number||' c_docdtl_rec.document_number : '||c_docdtl_rec.document_number, l_module);
                  UPDATE nihgl_ope_acct_dtls_int_tbl
                     SET record_status = 'V'
                       , ERROR_CODE = 'VALIDATED'  -- change 1 by Yash
                   WHERE  batch_number = c_docdtl_rec.batch_number
                     AND TRIM (document_ref || document_number) = c_docdtl_rec.document_number
                   --AND record_status IN ('N', 'E')	--CR NBSCH0003319
                     AND record_status IN ('N')			--CR NBSCH0003319
                   --AND can_fy          = c_docdtl_rec.can_fy
                     AND file_name = c_docdtl_rec.file_name;
                  logf ('Update completed. Rows updated: ' || SQL%ROWCOUNT, l_module);
               EXCEPTION
                  WHEN OTHERS THEN
                     l_error_message :=
                           'Error while updating V(alidate) Record for '
                        || 'combination of Document Rref/Number|FileName|BatchNum  : '
                        || c_docdtl_rec.document_number
                        || ':'
                        --||c_docdtl_rec.can_fy
                        || ':'
                        || c_docdtl_rec.batch_number;
                     logf (l_error_message, l_module);
               END;
            END IF;
            EXIT;
         END LOOP;
      END LOOP;
      COMMIT;
   EXCEPTION
      WHEN OTHERS THEN
         lc_errbuf :=  'Unhandled Exception in procedure validate_billing_data: ' || SQLERRM;
         logf (lc_errbuf, l_module);
         g_retcode := 2;
         g_errbuf  := 'Unhandled Exception in procedure validate_billing_data: ' || SUBSTR (SQLERRM, 1, 250);
   END validate_billing_data;
---------------------------------------------------------------------------------------------------------------
-- Procedure validate_hdr_data is staging data validation procedure to validate staging header records
-- in NBP interface staging header table nihgl_ope_acct_btchs_stg_tbl.
------------------------------------------------------------------------------------------------


   PROCEDURE validate_hdr_data ( p_file_name in varchar2,
                                 p_batch_id  in number,
                                 errbuf OUT   VARCHAR2,
                                 retcode OUT  VARCHAR2)
   IS    PRAGMA AUTONOMOUS_TRANSACTION;
      CURSOR ope_batch_file
      IS
           SELECT UNIQUE file_name, batch_number
             FROM nihgl_ope_acct_btchs_stg_tbl
            WHERE record_status = 'N'
            and file_name = p_file_name
            and request_id = p_batch_id
            AND hdr_ftr = g_hdr;


      CURSOR ope_duplicate_batch (p_file_name VARCHAR2)
      IS
         SELECT COUNT (*) cnt
           FROM nihgl_ope_acct_btchs_stg_tbl
          WHERE record_status = 'N'
         AND file_name = p_file_name
         and request_id = p_batch_id;

      CURSOR ope_dtl_line_cnt (p_file_name VARCHAR2)
      IS
         SELECT COUNT (*) cnt
           FROM nihgl_ope_acct_dtls_stg_tbl
          WHERE file_name = p_file_name
          AND record_status = 'N';

      CURSOR ope_dtl_tol_amnt_cnt (p_file_name VARCHAR2)
      IS
         SELECT SUM (NVL (amount, 0)) amt
           FROM nihgl_ope_acct_dtls_stg_tbl
          WHERE file_name = p_file_name
          AND record_status = 'N';

      lb_success          BOOLEAN := TRUE;
      lb_error_flag       BOOLEAN := FALSE;
      lc_message          VARCHAR2 (2000);
      ln_batch_head_line_tot NUMBER;
      ln_batch_line_tot   NUMBER;
      ln_dtl_line_tot     NUMBER;
      ln_dtl_amt_tot      NUMBER;
      ln_error_code       VARCHAR2 (40);
      ln_error_message    VARCHAR2 (2000);
      l_no_batch_exist    BOOLEAN := FALSE;
      l_cnt               NUMBER;
      l_line_total        NUMBER;
      l_line_amnt_total   NUMBER;
      l_hdr_batch         nihgl_ope_acct_btchs_stg_tbl.batch_number%TYPE;
      l_tlr_batch         nihgl_ope_acct_btchs_stg_tbl.batch_number%TYPE;
      l_batch_desc        VARCHAR2 (150);
      l_no_batch_cnt      NUMBER;
      l_je_category       VARCHAR2 (2500);
      l_file_counter      NUMBER := 0;
      l_batch             varchar2(25);
      l_module varchar2(100) := 'validate_hdr_data';
      v_module varchar2(100) := 'validate_hdr_data';
       l_html VARCHAR2(32767);


  BEGIN

    errbuf  := 'Success';
        retcode := 0;
        l_no_batch_exist   := TRUE;
        ln_batch_head_line_tot :=0;
      logf ('Starts validate_hdr_data  for : '||g_request_id, l_module);
      /*
      || Validate the files totals in the order the file is read.
      || This is required to check if the file is copied correctly
      || and there does not exists any error in file as per the total
      || lines and total amount within a file.
      */
      FOR ope_batch_file_rec IN ope_batch_file
      LOOP
         ln_batch_head_line_tot := ln_batch_head_line_tot+1;
         logf ('in loop ln_batch_head_line_tot : '||ln_batch_head_line_tot, l_module);
         l_file_counter := l_file_counter + 1;
         g_file_name (l_file_counter) := ope_batch_file_rec.file_name;
         l_batch  := ope_batch_file_rec.batch_number;
         lb_success := TRUE;
         l_no_batch_exist := FALSE;
         ln_dtl_line_tot := 0;
         ln_dtl_amt_tot := 0;
         ln_batch_line_tot := 0;
         ln_batch_line_tot := 0;

     lc_errbuf := NULL;
         ln_error_code :=0;
         --
       /* load_report_data (
            p_request_id     => g_request_id,
            p_file_name      => ope_batch_file_rec.file_name,
            p_batch_number   => ope_batch_file_rec.batch_number,
            p_user_id        => g_user_id,
            p_login_id       => g_login_id); */


         logf ( 'Started File Level Validation for  Batch Number: ' || ope_batch_file_rec.batch_number, l_module);

         FOR ope_duplicate_batch_t
            IN ope_duplicate_batch (ope_batch_file_rec.file_name)
         LOOP
            ln_batch_line_tot := ope_duplicate_batch_t.cnt;

            logf ( 'PSV_1 - ln_batch_line_tot ' || ln_batch_line_tot, v_module);
            IF ln_batch_line_tot > 2
            THEN
               lc_errbuf := NULL;
               ln_error_code := 2;
               ln_error_message := NULL;

               --Duplicate  Batch Header or Footer record found in file
               lc_errbuf :=
                  REPLACE (g_err_message_tbl ('NIHOPEBAT009'),
                           '&FILE_NAME',
                           ope_batch_file_rec.file_name);
               logf (lc_errbuf, l_module);
               /*lc_errbuf :=' Duplicate Batch Header or Footer records found in the file';*/
               --End ofchange 1 by Yash
               lb_success := FALSE;
  --CR NBSCH0002499 REMOVE NIHOPEBAT007 Validation
  /*
            ELSIF ln_batch_line_tot = 2
            THEN
               lc_errbuf := NULL;
               ln_error_code := 2 ;
               ln_error_message := NULL;

               SELECT COUNT (*)
                 INTO l_cnt
                 FROM nihgl_ope_acct_btchs_stg_tbl
                WHERE file_name = ope_batch_file_rec.file_name
                  AND record_status = 'N'
                  AND hdr_ftr = g_hdr;
                 --AND lines_total IS  NULL
                 --AND lines_total_amount IS  NULL


               IF l_cnt = 2
               THEN
                   ln_error_code :=2;
                  --Duplicate Header records found in the file
                  lc_errbuf := g_err_message_tbl ('NIHOPEBAT007');
                  --End of change 1
                  logf (lc_errbuf, l_module);

                  lb_success := FALSE;
               END IF;*/
            ELSIF ln_batch_line_tot = 1
            THEN
               lc_errbuf := NULL;
               ln_error_code :=2;
               ln_error_message := NULL;
               --Start of change 1 by Yash
               --Batch Footer record is missing in the File
               lc_errbuf := g_err_message_tbl ('NIHOPEBAT006');
               logf (lc_errbuf , l_module);
               --lc_errbuf := ' Batch Footer record is missing in the File';
               --End of change 1
               lb_success := FALSE;
            END IF;
         END LOOP;

       --  retcode :=   ln_error_code ;
       --  errbuf  :=   lc_errbuf;

         /*
         || check for Batch Validity
         */
         IF lb_success
         THEN
            l_no_batch_exist := FALSE;

            IF  /* CR NBSCH0002499
                (NOT batch_exists (
                      p_batch_number   => ope_batch_file_rec.batch_number,
                      p_batch_desc     => l_batch_desc,
                      p_je_category    => l_je_category))
                */
                      g_batch_number <> ope_batch_file_rec.batch_number -- CR NBSCH0002499
            THEN

                logf ( 'PSV_2 - NOT batch_exists ', v_module);
               lc_errbuf := NULL;
               ln_error_code := 2;
               ln_error_message := NULL;
               --Start of change 1 by Yash
               --Batch Number: ope_batch_file_rec.batch_number is not a Valid Batch in the file
               lc_errbuf :=
                  REPLACE (g_err_message_tbl ('NIHOPEBAT003'),
                           '&BATCH_NUMBER',
                           ope_batch_file_rec.batch_number);
               --End of change 1
               logf (lc_errbuf, l_module);
               lb_success := FALSE;
               l_no_batch_exist := TRUE;
            ELSE
               logf (' Valid Batch : ' || ope_batch_file_rec.batch_number, l_module);
            END IF;
         END IF;

         IF lb_success
         THEN
            FOR ope_dtl_line_cnt_t
               IN ope_dtl_line_cnt (ope_batch_file_rec.file_name)
            LOOP
               ln_dtl_line_tot := ope_dtl_line_cnt_t.cnt;


            logf ( 'PSV_3 - ln_dtl_line_tot ' || ln_dtl_line_tot, v_module);
            END LOOP;

            IF ln_dtl_line_tot <= 0
            THEN
               lc_errbuf := NULL;
               ln_error_code := 2;
               ln_error_message := NULL;
               --Start of change 1 by Yash
               --Batch Detail line Total is: ln_dtl_line_tot (Should not be Zero or Negatvie), file rejected
               lc_errbuf :=
                  REPLACE (g_err_message_tbl ('NIHOPEBAT010'),
                           '&DETAIL_LINE_TOT',
                           ln_dtl_line_tot);
               --End of change 1
               logf (lc_errbuf, l_module);
               lb_success := FALSE;
            END IF;

            IF lb_success
            THEN
               l_line_total := 0;

               BEGIN
                    SELECT NVL (lines_total, 0)
                      INTO l_line_total
                      FROM nihgl_ope_acct_btchs_stg_tbl
                     WHERE     record_status = 'N'
                           AND file_name = ope_batch_file_rec.file_name
                           AND hdr_ftr = g_ftr
                  --AND lines_total   IS NOT NULL
                  --AND lines_total_amount IS NOT NULL
                  ORDER BY file_name;

            logf ( 'PSV_4 - l_line_total ' || l_line_total, v_module);
                  IF l_line_total <= 0
                  THEN
                     lc_errbuf := NULL;
                     ln_error_code := 2;
                     ln_error_message := NULL;
                     --Start of change 1 by Yash
                     --Batch line Total is: l_line_total (Should not be Zero or Negatvie), file rejected
                     lc_errbuf :=
                        REPLACE (g_err_message_tbl ('NIHOPEBAT011'),
                                 '&BATCH_LINE_TOT',
                                 l_line_total);
                     --End of change 1
                     logf (lc_errbuf, l_module);
                     lb_success := FALSE;
                  END IF;
               EXCEPTION
                  WHEN OTHERS
                  THEN
                     l_line_total := 0;
               END;
            END IF;

            IF lb_success
            THEN

            logf ( 'PSV_5 - l_line_total ' || l_line_total, v_module);
            logf ( 'PSV_5 - ln_dtl_line_tot ' || ln_dtl_line_tot, v_module);
               IF l_line_total <> ln_dtl_line_tot
               THEN
                  lc_errbuf := NULL;
                  ln_error_code := 2;
                  ln_error_message := NULL;
                  --Start of change 1 by Yash
                  --Batch line total: l_line_total is not matching with Detail line total : ln_dtl_line_tot
                  lc_errbuf :=
                     REPLACE (
                        REPLACE (g_err_message_tbl ('NIHOPEBAT001'),
                                 '&BATCH_LINE_TOT',
                                 l_line_total),
                        '&DETAIL_LINE_TOT',
                        ln_dtl_line_tot);
                  --End of change 1
                  logf (lc_errbuf,l_Module);
                  lb_success := FALSE;
               ELSE
                  logf (
                        ' Batch and Detail Line Total Count Matched for File Name : '
                     || ope_batch_file_rec.file_name, l_module);
               END IF;
            END IF;
         END IF;

         IF lb_success
         THEN
            FOR ope_dtl_tol_amnt_cnt_t
               IN ope_dtl_tol_amnt_cnt (ope_batch_file_rec.file_name)
            LOOP
               ln_dtl_amt_tot := ope_dtl_tol_amnt_cnt_t.amt;

            logf ( 'PSV_6 - ln_dtl_amt_tot ' || ln_dtl_amt_tot, v_module);
            END LOOP;

            IF ln_dtl_amt_tot <= 0
            THEN
               lc_errbuf := NULL;
               ln_error_code := 2;
               ln_error_message := NULL;

               --Batch Detail Line Total Amount is: ln_dtl_amt_tot (Should not be Zero or Negatvie), file rejected
               lc_errbuf :=
                  REPLACE (g_err_message_tbl ('NIHOPEBAT012'),
                           '&DETAIL_LINE_TOT',
                           ln_dtl_amt_tot);
               --End of change 1
               logf (lc_errbuf, l_module);
               lb_success := FALSE;
            END IF;

            IF lb_success
            THEN
               l_line_amnt_total := 0;

               BEGIN
                    SELECT NVL (lines_total_amount, 0)
                      INTO l_line_amnt_total
                      FROM nihgl_ope_acct_btchs_stg_tbl
                     WHERE     record_status = 'N'
                           AND file_name = ope_batch_file_rec.file_name
                           AND hdr_ftr = g_ftr;

            logf ( 'PSV_7 - l_line_amnt_total ' || l_line_amnt_total, v_module);

                  IF l_line_amnt_total <= 0
                  THEN
                     lc_errbuf := NULL;
                     ln_error_code := 2;
                     ln_error_message := NULL;

                     --Batch Total Amount is: l_line_amnt_total (Should not be Zero or Negatvie), file rejected
                     lc_errbuf :=
                        REPLACE (g_err_message_tbl ('NIHOPEBAT013'),
                                 '&BATCH_AMT_TOT',
                                 l_line_amnt_total);

                     logf (lc_errbuf, l_module);
                     lb_success := FALSE;
                  END IF;
               EXCEPTION
                  WHEN OTHERS
                  THEN
                     l_line_amnt_total := 0;
               END;
            END IF;

            IF lb_success
            THEN
               IF l_line_amnt_total <> ln_dtl_amt_tot
               THEN
                  lc_errbuf := NULL;
                  ln_error_code := 2;
                  ln_error_message := NULL;

            logf ( 'PSV_8 - l_line_amnt_total <> ln_dtl_amt_tot', v_module);
                  --Batch Amount Total: l_line_amnt_total Not Matching with Detail Amount Total : ln_dtl_amt_tot
                  lc_errbuf :=
                     REPLACE (
                        REPLACE (g_err_message_tbl ('NIHOPEBAT002'),
                                 '&BATCH_AMT_TOT',
                                 l_line_amnt_total),
                        '&DETAIL_AMT_TOT',
                        ln_dtl_amt_tot);
                  --End of change 1
                  logf (lc_errbuf, l_module);
                  lb_success := FALSE;
               ELSE
                  logf (
                        ' Batch Total Amount Match with Detail Line Total Amount: '
                     || ope_batch_file_rec.file_name, l_module);
               END IF;
            END IF;
         END IF;

         IF lb_success
         THEN
            /*
            || check for header batch match with Footer batch
            */
            BEGIN
               SELECT batch_number
                 INTO l_hdr_batch
                 FROM nihgl_ope_acct_btchs_stg_tbl
                WHERE     file_name = ope_batch_file_rec.file_name
                      AND hdr_ftr = g_hdr            --AND LINES_TOTAL IS NULL
                      --AND LINES_TOTAL_amount IS NULL
                      AND record_status = 'N';

            logf ( 'PSV_9 - l_hdr_batch ' || l_hdr_batch, v_module);
            EXCEPTION
               WHEN OTHERS
               THEN
                  l_hdr_batch := NULL;
                  logf (
                        ' Issue while getting Header batch number for file name: '
                     || ope_batch_file_rec.file_name
                     || '-'
                     || SUBSTR (SQLERRM, 1, 250), l_module);
            END;

            BEGIN
               SELECT batch_number
                 INTO l_tlr_batch
                 FROM nihgl_ope_acct_btchs_stg_tbl
                WHERE     file_name = ope_batch_file_rec.file_name
                      AND hdr_ftr = g_ftr        --AND LINES_TOTAL IS NOT NULL
                      --AND LINES_TOTAL_amount IS NOT NULL
                      AND record_status = 'N';

            logf ( 'PSV_10 - l_tlr_batch '  || l_tlr_batch, v_module);
            EXCEPTION
               WHEN OTHERS
               THEN
                  l_tlr_batch := NULL;
                  logf (
                        ' Issue while getting Footer batch number for file name: '
                     || ope_batch_file_rec.file_name
                     || '-'
                     || SUBSTR (SQLERRM, 1, 250), l_module);
            END;

            IF NVL (TRIM (l_hdr_batch), 'X') <> NVL (TRIM (l_tlr_batch), 'Y')
            THEN
               lc_errbuf := NULL;
               ln_error_code := 2;
               ln_error_message := NULL;
               --Start of change 1 by Yash
               --Header Batch Number: l_hdr_batch is not matching with Footer Batch Number: l_tlr_batch
               lc_errbuf :=
                  REPLACE (
                     REPLACE (g_err_message_tbl ('NIHOPEBAT004'),
                              '&BATCH_HDR_NUM',
                              l_hdr_batch),
                     '&BATCH_FTR_NUM',
                     l_tlr_batch);
               --End of change 1
               logf (lc_errbuf, l_module);
               lb_success := FALSE;

            logf ( 'PSV_11 -  <>)', v_module);
            END IF;
         END IF;

         -- FCI Code Starts here for validatin expenditure Item Date

         IF lb_success
         THEN
            FOR c_exp_dt_rec
               IN (SELECT Expenditure_item_date
                     FROM nihgl_ope_acct_dtls_stg_tbl
                    WHERE file_name = ope_batch_file_rec.file_name)
            LOOP

            logf ( 'PSV_12 - c_exp_dt_rec.Expenditure_item_date ' || c_exp_dt_rec.Expenditure_item_date, v_module);
               IF c_exp_dt_rec.Expenditure_item_date = '01001900'
               THEN
                  lc_errbuf := NULL;
                  ln_error_code := 2;
                  ln_error_message := NULL;
                  lc_errbuf := g_err_message_tbl ('NIHOPEBAT015');
                  logf (lc_errbuf, l_module);
                  lb_success := FALSE;
               END IF;
            END LOOP;
         END IF;

         -- FCI Code Ends here for validating expenditure item date

         IF lb_success
         THEN
         logf('Inserting into Interface tables ' , l_module);
            /*
            || Insert into Interface tables (master and detail)
            */

            insert_into_intf_tbl (p_file_name => ope_batch_file_rec.file_name);
         ELSE
            /*
            || Validate file level as batch line total and batch line total amount
            || shoud match with details line.If any validation failed and
            || send email to Feeder System with error information
            */
            lb_error_flag := TRUE;

         logf('Calling send mail with header error message ' || lc_errbuf , l_module);


        -- sending mail

           --  send_hdr_rejection_mail (ope_batch_file_rec.file_name , l_batch,lc_errbuf);

         END IF;

         logf (
               'End of File Level Validation for  Batch Number: '
            || ope_batch_file_rec.batch_number, l_module);
         logf (
            '-----------------------------------------------------------------', l_module);
      END LOOP;



     logf ('after loop ln_batch_head_line_tot : '||ln_batch_head_line_tot, l_module);
     IF ln_batch_head_line_tot = 0
            THEN
               lc_errbuf := NULL;
               ln_error_code :=2;
               ln_error_message := NULL;
               lc_errbuf := g_err_message_tbl ('NIHOPEBAT005');
               logf (lc_errbuf , l_module);
               lb_success := FALSE;
            END IF;
     IF lb_success  then

      /*  Check if no header/footer found but Detail   records found in staging table   */

      FOR no_batch_val_cur_rec
         IN (SELECT UNIQUE file_name FROM nihgl_ope_acct_dtls_stg_tbl)
      LOOP
         l_no_batch_cnt := 0;
         l_no_batch_exist := FALSE;

         SELECT COUNT (*)
           INTO l_no_batch_cnt
           FROM nihgl_ope_acct_btchs_stg_tbl
          WHERE     file_name = no_batch_val_cur_rec.file_name
                AND record_status = 'N';

            logf ( 'PSV_13 - l_no_batch_cnt ' || l_no_batch_cnt, v_module);
         IF l_no_batch_cnt = 0
         THEN
            lc_errbuf := NULL;
            --Start of change 1 by Yash
            --No Valid Batch Header/Footer Records found for file Name : no_batch_val_cur_rec.file_name
            lc_errbuf :=
               REPLACE (g_err_message_tbl ('NIHOPEBAT014'),
                        '&FILE_NAME',
                        no_batch_val_cur_rec.file_name);
            logf (lc_errbuf, l_module);
            lb_error_flag := TRUE;
            ln_error_code := 2;

            --End of change 1
            inst_arch_file_vald_error (
               p_file_name   => no_batch_val_cur_rec.file_name,
               p_error_msg   => lc_errbuf);

            BEGIN
               DELETE nihgl_ope_acct_dtls_stg_tbl
                WHERE file_name = no_batch_val_cur_rec.file_name;
            EXCEPTION
               WHEN OTHERS
               THEN
                  logf (
                        ' Error while deleting staging table :'
                     || SUBSTR (SQLERRM, 1, 250), l_module);
            END;

         -- sending email notification
             send_hdr_rejection_mail (p_file_name,l_batch, lc_errbuf);
            COMMIT;
         END IF;


      END LOOP;

     End if;

           -- return the error

      -- if (l_no_batch_exist   ) then  --CR NBSCH0002499
      --      retcode := 2;             --CR NBSCH0002499
      --      errbuf  := 'NO Valid ath exists';--CR NBSCH0002499
      --     else                       --CR NBSCH0002499
             retcode :=    ln_error_code;
         errbuf  :=    lc_errbuf;
      --      end if;                   --CR NBSCH0002499


   EXCEPTION
      WHEN OTHERS
      THEN
         lc_message :=
               ' Unhandled Exception in procedure validate_hdr_data : '
            || SQLERRM
            || SQLCODE;
         logf (lc_message, l_module);
         g_retcode := 2;
         g_errbuf :=
               'Unhandled Exception in procedure validate_hdr_data: '
            || SUBSTR (SQLERRM, 1, 250);
   END validate_hdr_data;

-------------------------------------------------------------------------------------------------------
-- Procedure insert_reversal_txn
-- In case of any error or audit .. store in the OPE interface table
-- --------------------------------------------------------------------------------------------------
   PROCEDURE insert_reversal_txn (p_batch_number   IN     VARCHAR2,
                                  p_file_name      IN     VARCHAR2,
                                  p_status         IN OUT VARCHAR2,
                                  p_message        IN OUT VARCHAR2)
   IS   PRAGMA AUTONOMOUS_TRANSACTION;
     /*  -----------------------------------------------------------------------------------------------
      |to add dynamic logic
      || where everything for OPE can be configured instead of
      || hardcodings.

       * PLEASE NOTE THAT THE REVERSALS LIKE THE BELOW LOGIC MAY  NOT BE BALANCED.
       * BELOW LOGIC IS TO CHECK IF THE SUSPENSE TCODE EXISTS AND  REVERSE THE CURRENT TRANSACTION
         AMOUNT OTHERWISE IGNORE.

       * Select all the detail records whose attribute6 = C. Selected to reverse
       * the previous schedule number.
       * In this scenario we are not failing this logic. If that schedule exists
       * then reverse that transaction else ignore and move on.
       * This will be enhanced later.

     -------------------------------------------------------------------------------------------------------  */


      CURSOR c_dtls_cur
      IS
         SELECT *
           FROM nihgl_ope_acct_dtls_int_tbl
          WHERE record_status = 'V'
            AND batch_number = p_batch_number
            AND file_name = p_file_name
            AND attribute6 = 'C';

      CURSOR c_schedule_exist (
         p_ope_detail_id      NUMBER,
         p_schedule_number    VARCHAR2,
         p_tcode              VARCHAR2)
      IS
         SELECT *
           FROM nihgl_ope_acct_dtls_all_v
          WHERE record_status IN ('N', 'V', 'P')
            AND schedule_number = p_schedule_number
            AND tcode =
                       (SELECT attribute6
                          FROM fnd_lookup_values_vl
                         WHERE lookup_type = 'NIHGL_OPE_TCODES'
                           AND lookup_code = p_tcode
                           AND (  NVL (enabled_flag, 'Y') = 'Y'
                                  AND ( NVL (start_date_active, SYSDATE) <= SYSDATE
                                         AND (  end_date_active >= SYSDATE
                                              OR end_date_active IS NULL)))
                               AND attribute6 IS NOT NULL)
            AND ope_detail_id <> p_ope_detail_id;

      l_status         VARCHAR2 (100);
      l_message        VARCHAR2 (2000);
      l_exist          VARCHAR2 (10);
      l_can_fy         nihgl_ope_acct_dtls_int_tbl.can_fy%TYPE;
      l_can            nihgl_ope_acct_dtls_int_tbl.can%TYPE;
      l_object_class   nihgl_ope_acct_dtls_int_tbl.object_class%TYPE;
      v_module varchar2(100) := 'insert_reversal_txn';
   BEGIN
      logf ( 'Begin with file_name: ' || p_file_name, v_module);
      FOR c_dtls_rec IN c_dtls_cur
      LOOP
         FOR c_schedule_rec
            IN c_schedule_exist (c_dtls_rec.ope_detail_id,
                                 c_dtls_rec.schedule_number,
                                 c_dtls_rec.tcode)
         LOOP
            logf ( 'OPE_BATCH_ID: ' || c_dtls_rec.OPE_BATCH_ID, v_module);
            INSERT
              INTO apps.nihgl_ope_acct_dtls_int_tbl (OPE_BATCH_ID,
                                                     OPE_DETAIL_ID,
                                                     BATCH_NUMBER,
                                                     ACCOUNTING_DATE,
                                                     TCODE,
                                                     REVERSE_CODE,
                                                     MODIFIER_CODE,
                                                     DOCUMENT_REF,
                                                     DOCUMENT_NUMBER,
                                                     OTH_DOCUMENT_REF,
                                                     OTH_DOCUMENT_NUMBER,
                                                     GEO_CODE,
                                                     CAN_FY,
                                                     CAN,
                                                     OBJECT_CLASS,
                                                     AMOUNT,
                                                     GOV_NONGOV,
                                                     PRIMARY_EIN,
                                                     SECONDARY_EIN,
                                                     SCHEDULE_NUMBER,
                                                     CATEGORY,
                                                     CASE_11_CODE,
                                                     BALANCE_OF_PAYMENT,
                                                     GL_CODE,
                                                     TYPE_OF_SERVICE,
                                                     RESERVED,
                                                     GRANT_NUMBER,
                                                     GRANT_BEGIN_DATE,
                                                     GRANT_END_DATE,
                                                     PARM_DATE,
                                                     USERIII,
                                                     CLERKKID,
                                                     ADBIS,
                                                     SOURCE_CODE,
                                                     PERIOD_NAME,
                                                     TRANSACTION_CODE,
                                                     PROJECT_NUMBER,
                                                     TASK_NUMBER,
                                                     PROJECT_ID,
                                                     TASK_ID,
                                                     PROJECT_EXP_TYPE,
                                                     EXP_ORG_ID,
                                                     ORG_ID,
                                                     SET_OF_BOOKS_ID,
                                                     CODE_COMBINATION_ID,
                                                     CODE_COMBINATION_FLEX,
                                                     CHART_OF_ACCOUNTS_ID,
                                                     FUNDS_CHECK_MESSAGE,
                                                     FUNDS_CHECK_CODE,
                                                     PACKET_ID,
                                                     SEGMENT1,
                                                     SEGMENT2,
                                                     SEGMENT3,
                                                     SEGMENT4,
                                                     SEGMENT5,
                                                     SEGMENT6,
                                                     SEGMENT7,
                                                     SEGMENT8,
                                                     SEGMENT9,
                                                     SEGMENT10,
                                                     SEGMENT11,
                                                     SEGMENT12,
                                                     SEGMENT13,
                                                     SEGMENT14,
                                                     SEGMENT15,
                                                     SEGMENT16,
                                                     SEGMENT17,
                                                     SEGMENT18,
                                                     SEGMENT19,
                                                     SEGMENT20,
                                                     CONTEXT,
                                                     ATTRIBUTE1,
                                                     ATTRIBUTE2,
                                                     ATTRIBUTE3,
                                                     ATTRIBUTE4,
                                                     ATTRIBUTE5,
                                                     ATTRIBUTE6,
                                                     ATTRIBUTE7,
                                                     ATTRIBUTE8,
                                                     ATTRIBUTE9,
                                                     ATTRIBUTE10,
                                                     ATTRIBUTE11,
                                                     ATTRIBUTE12,
                                                     ATTRIBUTE13,
                                                     ATTRIBUTE14,
                                                     ATTRIBUTE15,
                                                     EXPENDITURE_ITEM_DATE, -- change6
                                                     FILE_NAME,
                                                     AGENCY_CODE,
                                                     RECORD_ID,
                                                     RECORD_STATUS,
                                                     ERROR_CODE,
                                                     ERROR_MESSAGE,
                                                     REQUEST_ID,
                                                     CREATED_BY,
                                                     CREATION_DATE,
                                                     LAST_UPDATED_BY,
                                                     LAST_UPDATE_DATE,
                                                     LAST_UPDATE_LOGIN)
            VALUES (c_dtls_rec.OPE_BATCH_ID,
                    nihgl_ope_acct_dtls_int_s.NEXTVAL,
                    c_dtls_rec.BATCH_NUMBER,
                    c_dtls_rec.ACCOUNTING_DATE,
                    c_schedule_rec.TCODE,
                    '2',                            --c_dtls_rec.REVERSE_CODE,
                    c_dtls_rec.MODIFIER_CODE,
                    c_dtls_rec.DOCUMENT_REF,
                    c_dtls_rec.DOCUMENT_NUMBER,
                    c_dtls_rec.OTH_DOCUMENT_REF,
                    c_dtls_rec.OTH_DOCUMENT_NUMBER,
                    c_dtls_rec.GEO_CODE,
                    c_schedule_rec.CAN_FY,
                    c_schedule_rec.CAN,
                    c_schedule_rec.OBJECT_CLASS,
                    c_dtls_rec.AMOUNT,
                    c_dtls_rec.GOV_NONGOV,
                    c_dtls_rec.PRIMARY_EIN,
                    c_dtls_rec.SECONDARY_EIN,
                    c_dtls_rec.SCHEDULE_NUMBER,
                    c_dtls_rec.CATEGORY,
                    c_dtls_rec.CASE_11_CODE,
                    c_dtls_rec.BALANCE_OF_PAYMENT,
                    c_dtls_rec.GL_CODE,
                    c_dtls_rec.TYPE_OF_SERVICE,
                    c_dtls_rec.RESERVED,
                    c_dtls_rec.GRANT_NUMBER,
                    c_dtls_rec.GRANT_BEGIN_DATE,
                    c_dtls_rec.GRANT_END_DATE,
                    c_dtls_rec.PARM_DATE,
                    c_dtls_rec.USERIII,
                    c_dtls_rec.CLERKKID,
                    c_dtls_rec.ADBIS,
                    c_dtls_rec.SOURCE_CODE,
                    c_dtls_rec.PERIOD_NAME,
                    c_schedule_rec.TRANSACTION_CODE,
                    c_schedule_rec.PROJECT_NUMBER,
                    c_schedule_rec.TASK_NUMBER,
                    c_schedule_rec.PROJECT_ID,
                    c_schedule_rec.TASK_ID,
                    c_schedule_rec.PROJECT_EXP_TYPE,
                    c_schedule_rec.EXP_ORG_ID,
                    c_schedule_rec.ORG_ID,
                    c_schedule_rec.SET_OF_BOOKS_ID,
                    c_schedule_rec.CODE_COMBINATION_ID,
                    c_schedule_rec.CODE_COMBINATION_FLEX,
                    c_schedule_rec.CHART_OF_ACCOUNTS_ID,
                    c_dtls_rec.FUNDS_CHECK_MESSAGE,
                    c_dtls_rec.FUNDS_CHECK_CODE,
                    c_dtls_rec.PACKET_ID,
                    c_schedule_rec.SEGMENT1,
                    c_schedule_rec.SEGMENT2,
                    c_schedule_rec.SEGMENT3,
                    c_schedule_rec.SEGMENT4,
                    c_schedule_rec.SEGMENT5,
                    c_schedule_rec.SEGMENT6,
                    c_schedule_rec.SEGMENT7,
                    c_schedule_rec.SEGMENT8,
                    c_schedule_rec.SEGMENT9,
                    c_schedule_rec.SEGMENT10,
                    c_schedule_rec.SEGMENT11,
                    c_schedule_rec.SEGMENT12,
                    c_schedule_rec.SEGMENT13,
                    c_schedule_rec.SEGMENT14,
                    c_schedule_rec.SEGMENT15,
                    c_schedule_rec.SEGMENT16,
                    c_schedule_rec.SEGMENT17,
                    c_schedule_rec.SEGMENT18,
                    c_schedule_rec.SEGMENT19,
                    c_schedule_rec.SEGMENT20,
                    c_dtls_rec.CONTEXT,
                    c_dtls_rec.ATTRIBUTE1,
                    c_dtls_rec.ATTRIBUTE2,
                    c_dtls_rec.ATTRIBUTE3,
                    c_dtls_rec.ATTRIBUTE4,
                    c_dtls_rec.ATTRIBUTE5,
                    c_dtls_rec.ATTRIBUTE6,
                    c_dtls_rec.ATTRIBUTE7,
                    c_dtls_rec.ATTRIBUTE8,
                    c_dtls_rec.ATTRIBUTE9,
                    c_dtls_rec.ATTRIBUTE10,
                    c_dtls_rec.ATTRIBUTE11,
                    c_dtls_rec.ATTRIBUTE12,
                    c_dtls_rec.ATTRIBUTE13,
                    c_dtls_rec.ATTRIBUTE14,
                    c_dtls_rec.ATTRIBUTE15,
                    c_dtls_rec.EXPENDITURE_ITEM_DATE,                --change7
                    c_dtls_rec.FILE_NAME,
                    c_dtls_rec.AGENCY_CODE,
                    c_dtls_rec.RECORD_ID,
                    c_dtls_rec.RECORD_STATUS,
                    c_dtls_rec.ERROR_CODE,
                    c_dtls_rec.ERROR_MESSAGE,
                    c_dtls_rec.REQUEST_ID,
                    fnd_global.user_id,                          --CREATED_BY,
                    SYSDATE,                                  --CREATION_DATE,
                    fnd_global.user_id,                     --LAST_UPDATED_BY,
                    SYSDATE,                               --LAST_UPDATE_DATE,
                    fnd_global.login_id);

            EXIT;
         END LOOP;
      END LOOP;

      p_status := 'SUCCESS';
   EXCEPTION
      WHEN OTHERS THEN
         p_status := 'ERROR';
         p_message := 'Unhandled Exception in ' || SQLERRM;
   END insert_reversal_txn;
-- ---------------------------------------------------------------------------------------------
-- Procedure validate_txn_data
-- -----------------------------------------------------------------------------------------------
   PROCEDURE validate_txn_data (errbuf                  OUT VARCHAR2,
                                retcode                 OUT VARCHAR2,
                                p_sob_id             IN     NUMBER,
                                p_file_name          IN     VARCHAR2,
                                p_batch_number       IN     VARCHAR2,
                                p_called_from_form   IN     VARCHAR2)
   IS  PRAGMA AUTONOMOUS_TRANSACTION;
      CURSOR cur_intf_dtls
      IS
           SELECT UNIQUE file_name, batch_number, ope_batch_id
             FROM nihgl_ope_acct_btchs_int_tbl
            WHERE record_status = 'N'
              AND file_name = p_file_name
              AND batch_number = p_batch_number;
         --ORDER BY batch_number, file_name;

      lc_message           VARCHAR2 (2000);
      v_val_gl_arc_exist   BOOLEAN := FALSE;
      l_cnt                NUMBER;
      req_retcode          NUMBER;
      req_errbuf           VARCHAR2 (2000);
      v_error_code         NUMBER;
      v_error_message      VARCHAR2 (2000);
      lc_retcode           VARCHAR2 (1);
      lc_errbuf            VARCHAR2 (2000);
      l_status             VARCHAR2 (100);
      l_message            VARCHAR2 (2000);
      l_CURSOR VARCHAR2 (100);
      v_request_id         NUMBER;
      v_module varchar2(100) := 'validate_txn_data';

   BEGIN
       logf ( 'Starts validate_txn_data...  for ' || p_file_name, v_module);
       logf ( 'PSV_1 - p_sob_id ' || p_sob_id, v_module);
       logf ( 'PSV_2 - p_file_name ' || p_file_name, v_module);
       logf ( 'PSV_3 - p_batch_number ' || p_batch_number, v_module);
       logf ( 'PSV_4 - p_called_from_form ' || p_called_from_form, v_module);
       begin
       select record_status into l_CURSOR from (SELECT UNIQUE file_name, batch_number, ope_batch_id, record_status
             FROM nihgl_ope_acct_btchs_int_tbl
            WHERE --record_status = 'N'
              --AND
              file_name = p_file_name
              AND batch_number = p_batch_number and rownum=1);
       logf ( 'PSV_5 - l_CURSOR1 ' || l_CURSOR, v_module);
        exception when no_data_found then
       logf ( 'PSV_6 - l_CURSOR2 0', v_module);

       end;


      FOR cur_intf_dtls_rec IN cur_intf_dtls
      LOOP
         BEGIN

            logf ( 'Started Detail Line Level Validation and GL process for Batch Number :' ||
                    cur_intf_dtls_rec.batch_number, v_module);
            v_val_gl_arc_exist := TRUE;

            g_txn_reject_tbl.DELETE;

            /*
            ||Validate if any CAN, OB, FY error based on Document number
            ||IF any validation failed, mark the record as (E)rror
            */

            validate_billing_data (
               p_sob_id             => p_sob_id,
               p_batch_number       => cur_intf_dtls_rec.batch_number,
               p_file_name          => cur_intf_dtls_rec.file_name,
               p_doc_ref            => NULL,
               p_doc_number         => NULL,
               p_can_fy             => NULL,
               p_called_from_form   => NULL
               );


            --Add a logic to insert the reversal transactions.
            -- Make sure to insert this data in OPE Interface table to make sure
            -- any future Audit is taken care of.

            logf (' before insert_reversal :' || cur_intf_dtls_rec.batch_number,v_module);

            insert_reversal_txn (
               p_batch_number   => cur_intf_dtls_rec.batch_number,
               p_file_name      => cur_intf_dtls_rec.file_name,
               p_status         => l_status,
               p_message        => l_message);
            --
            logf (' after insert_reversal :' || l_status || '-' || cur_intf_dtls_rec.batch_number,v_module);
            --
            IF l_status <> 'SUCCESS'
            THEN
               logf ( 'Error during reversing the transactions : ' || l_status || ' ' || l_message, v_module);
            END IF;


            logf ( ' Before create SLA Events:' || cur_intf_dtls_rec.batch_number, v_module);

            v_request_id := g_request_id;

            create_sla_events (
               p_batch_number   => cur_intf_dtls_rec.batch_number,
               p_file_name      => cur_intf_dtls_rec.file_name,
               retcode          => v_error_code,
               errbuf           => v_error_message);

            logf (' After create SLA Events :' || '-' || v_error_message || '-' || cur_intf_dtls_rec.batch_number, v_Module);



            process_sla(p_batch_number   => cur_intf_dtls_rec.batch_number,
                        p_file_name      => cur_intf_dtls_rec.file_name,
                        p_type           => 'R');

            logf (' After Process_gl :' || cur_intf_dtls_rec.batch_number, v_module);

           -- g_request_id := v_request_id;



            logf ('1--> callling update_lcnt  for Processsed status' , v_module);
            l_cnt := 0;
            l_cnt :=
               update_lcnt (p_file_name       => cur_intf_dtls_rec.file_name,
                            p_record_status   => 'P',
                            p_doc_ref         => NULL,
                            p_doc_number      => NULL,
                            p_can_fy          => NULL
                            );

            logf ('1--> Process status count : ' ||l_cnt , v_module);

            IF l_cnt = 0  THEN
               logf (' No Record eligible For GL Import For Batch : ' || cur_intf_dtls_rec.batch_number, v_Module);
            END IF;


         EXCEPTION
            WHEN OTHERS
            THEN
               logf (
                     ' Issue in Cursor cur_intf_dtls for batch number | File Name : '
                  || cur_intf_dtls_rec.batch_number
                  || '|'
                  || cur_intf_dtls_rec.file_name
                  || '|'
                  || SUBSTR (SQLERRM, 1, 250) , V_mODULE);
         END;
         --

         logf ( 'End Detail Line Level Validation and GL process for Batch Number :' || cur_intf_dtls_rec.batch_number , V_mODULE);

      END LOOP;

     COMMIT;


   EXCEPTION
      WHEN OTHERS THEN
         lc_message := ' Unhandled Exception in procedure validate_txn_data : ' || SQLERRM || SQLCODE;
         logf (lc_message , V_mODULE);
         g_retcode := 2;
         g_errbuf := 'Unhandled Exception in procedure validate_txn_data: ' || SUBSTR (SQLERRM, 1, 250);
   END validate_txn_data;

-- =====================================================================================
-- PROCEDURE reject_document -- called from validate_txn_data.
-- This procedure is not registered as any concurrent program in NBS EBS.
-- ===================================================================================
   PROCEDURE reject_document (errbuf              OUT VARCHAR2,
                              retcode             OUT VARCHAR2,
                              p_file_name      IN     VARCHAR2,
                              p_batch_number   IN     VARCHAR2,
                              p_doc_ref        IN     VARCHAR2,
                              p_doc_number     IN     VARCHAR2,
                              p_can_fy         IN     NUMBER)

   IS PRAGMA AUTONOMOUS_TRANSACTION;
      l_cnt   NUMBER := 0;
      L_MODULE VARCHAR2(100) := 'REJECT_DOCUMENT';

   BEGIN

       logf('begin ' , l_Module);
      /*
      ||  Update the detail transaction record to REJECT (R) in the details interface table
      */
      -- CR 31524 - Remove "User rejected the document" message
      UPDATE nihgl_ope_acct_dtls_int_tbl
         SET
             ERROR_CODE = 'REJECT',
             last_updated_by = fnd_global.user_id,
             last_update_date = SYSDATE,
             last_update_login = fnd_global.login_id,
             record_status = 'R'
       WHERE file_name = p_file_name
         AND batch_number = p_batch_number
         AND NVL (document_ref, 'NULL') = NVL (p_doc_ref, 'NULL')
         AND NVL (document_number, 'NULL') = NVL (p_doc_number, 'NULL')
      -- AND NVL (can_fy, -2013) = NVL (p_can_fy, -2013) -- NBSCH0001185 commented out to reject entire docment reference on 26-Jul-23
         AND record_status <> 'R';
      --
      IF SQL%ROWCOUNT > 0
      THEN
         COMMIT;
      END IF;
      --
      --Send the record status back
      retcode := 0;
      errbuf := 'Successfully processed data';
   EXCEPTION
      WHEN OTHERS THEN
         errbuf :=
               ' Unhandled Exception in procedure reject_document : '
            || SQLERRM
            || SQLCODE;
         retcode := 2;
   END reject_document;

/*-------------------------------------------------------------------------------------------------
-- Main Procedure to validate the Billing data file coming from
-- feeder Systems to NBS and process it to final destination GL.
--
--
-----------------------------------------------------------------------------------------------------*/
   PROCEDURE  process_file (errbuf        out       VARCHAR2 ,
                            retcode        out      number ,
                   p_upload_id          in Number,
                   p_sob_id                 NUMBER,
                   p_batch_number           VARCHAR2,
                   p_file_name              VARCHAR2,
                   p_called_from_form       VARCHAR2
                   )
   IS

      lc_message   VARCHAR2 (2000);
      lc_retcode   VARCHAR2 (2);
      l_module varchar2(100) := 'process_file';

   BEGIN
      /*
      || Billing File from BPB(Feder System) has below structure.
      || 1. First record type 1 will have the batch header,Table NIHGL_OPE_ACCT_BTCHS_STG_TBL
      || 2. Second record type 2 will be the detail information of the OPE Billing.
      ||    All these are stored in NIHGL_OPE_ACCT_DTLS_STG_TBL table.
      || 3. Third record type 4 will be the batch detail containing the line
      ||    total and the total amount in the batch. Table NIHGL_OPE_ACCT_BTCHS_STG_TBL
      */

      /*
      || Initialize the Global variables.
      */
      g_batch_number := p_batch_number; --CR NBSCH0002499
      g_request_id := p_upload_id;
      g_user_id    := fnd_global.user_id;
      g_login_id   := fnd_global.login_id;
      g_resp_name := fnd_global.resp_name;

/*

      apps_initialize
  (
    user_id IN NUMBER,
    resp_id IN NUMBER,
    resp_appl_id IN NUMBER,
    security_group_id IN NUMBER DEFAULT 0,
    server_id IN NUMBER DEFAULT -1
  );

 */

      logf ('Starts the Process for Upload ID  : '||g_request_id   ||  ' Processing File  : ' || p_file_name  || ' Batch  :'  || p_batch_number , l_Module);


    logf('calling  validate_hdr_data', l_module);
         validate_hdr_data (p_file_name , g_request_id, errbuf => lc_message, retcode => lc_retcode);

    logf( 'error code ' || retcode  || '  error mesg ' || errbuf , l_module);
         IF lc_retcode = 2
         THEN
            errbuf := lc_message;
            retcode := lc_retcode;
            RETURN;
         END IF;




      -------------------------------------------------------------------

       --- Process interface records


    logf('calling  validate_txn_data', l_module);

      validate_txn_data (errbuf               => lc_message,
                         retcode              => lc_retcode,
                         p_sob_id             => p_sob_id,
                         p_file_name          => p_file_name,
                         p_batch_number       => p_batch_number,
                         p_called_from_form   => p_called_from_form);

     --if lc_retcode = 0 then


    --logf(l_module  ,'calling   send_status_mail');
           --  send_status_mail( p_file_name  ,p_batch_number, lc_message);

    logf('calling  nbp_archive', l_module);
          nbp_archive (
                           p_file_name       => p_file_name,
               errbuf               => errbuf,
                           retcode              => retcode);


    logf( 'error code ' || retcode  || '  error mesg ' || errbuf , l_module);





   EXCEPTION
      WHEN OTHERS THEN
         lc_message := ' Unhandled Exception in procedure process_file: ' || SQLERRM || SQLCODE;
         logf (lc_message , l_module);
         retcode := 2;
   END ;


 ---------------------------------------------------------------------------------------------------------------

  PROCEDURE  main( errbuf                out VARCHAR2 ,
                   retcode               out number ,
                   totalrec              out number,
                   processed             out number,
                   errored               out number,
                   rejected              out number,
                   p_upload_id           in  Number,
                   p_sob_id              in  NUMBER,
                   p_batch_number        in  VARCHAR2,
                   p_file_name           in  VARCHAR2,
                   p_called_from_form    in  VARCHAR2,
                   p_batch_desc          in  VARCHAR2, -- CR NBSCH0002499
                   p_JOURNAL_CATEGORY    in  VARCHAR2  -- CR NBSCH0002499
                   )

 is
  l_errbuf varchar2(500);
  l_retcode number;
  l_module varchar2(100) := 'main';
 begin
    logf ('Start process. Calling process_file for ' || p_file_name , l_Module);
    --logf ('FND_LOG.LEVEL_STATEMENT ' || FND_LOG.LEVEL_STATEMENT , l_Module);
    --logf ('FND_LOG.G_CURRENT_RUNTIME_LEVEL ' || FND_LOG.G_CURRENT_RUNTIME_LEVEL , l_Module);
    logf ('p_batch_desc ' || p_batch_desc , l_Module);
    logf ('p_JOURNAL_CATEGORY ' || p_JOURNAL_CATEGORY , l_Module);
    g_batch_desc := p_batch_desc;               --CR NBSCH0002499
    g_JOURNAL_CATEGORY := p_JOURNAL_CATEGORY;   --CR NBSCH0002499

    fnd_global.apps_initialize                  --CR NBSCH0002499
    (                                           --CR NBSCH0002499
      user_id => 23130, --SYSOPER               --CR NBSCH0002499
      resp_id => 50447, --NIHGL Scheduler       --CR NBSCH0002499
      resp_appl_id => 20006                     --CR NBSCH0002499
    );                                          --CR NBSCH0002499

         process_file (errbuf ,
                       retcode ,
                   p_upload_id       ,
                   p_sob_id           ,
                   p_batch_number       ,
                   p_file_name          ,
                   p_called_from_form
                   );

    select count(*)  ,
             nvl(sum(decode(record_status , 'E', 1, 0)),0) errored,
             nvl(sum(decode(record_status , 'P', 1, 0)),0) processed,
             nvl(sum(decode(record_status , 'R', 1, 0)),0) rejected
        into totalrec, errored, processed, rejected
        from nihgl_ope_acct_dtls_arc_tbl
        where file_name = p_file_name
        and   request_id  = p_upload_id;



     logf (' process_file completed  ....' || errbuf , l_module);
EXCEPTION
      WHEN OTHERS THEN
        errbuf := ' Unhandled Exception in procedure main : ' || SQLERRM || SQLCODE;
         logf (errbuf, l_module);
         retcode := 2;
   END ;



  -- =======================================
-- procedure ope_archive
   --Start of change 1 by Yash
   --Added the below overloaded procedure for OPE billing interface
   --error handling form
   /*
      || Archive the Interface data, error or processs
      || but leave the (U)processed record for next run
      */
-- =======================================
   PROCEDURE ope_archive (p_file_name        VARCHAR2 --Added the below parameter for the
                                                     --error handling from req
                          ,
                          p_record_status    VARCHAR2 DEFAULT NULL,
                          p_doc_ref          VARCHAR2 DEFAULT NULL,
                          p_doc_number       VARCHAR2 DEFAULT NULL,
                          p_can_fy           NUMBER DEFAULT NULL --End of the parameters
                                                                )
   IS
      lc_errbuf             VARCHAR2 (2000);
      lb_error_exist        BOOLEAN := TRUE;
      l_posting_status      VARCHAR2 (1);

      --Start of change 1 by Yash
      lb_chk_hdr_arc_flag   BOOLEAN := FALSE;
      ln_cnt                NUMBER := 0;

      --End of change 1

        l_module varchar2(100)  :=  'ope_archive';

      CURSOR get_acct_btch
      IS
         SELECT UNIQUE ope_batch_id, file_name
           FROM nihgl_ope_acct_btchs_int_tbl
          WHERE file_name = p_file_name AND record_status <> 'U';

   BEGIN
      FOR get_acct_btch_rec IN get_acct_btch
      LOOP
         IF     p_record_status IS NULL
            AND p_doc_ref IS NULL
            AND p_doc_number IS NULL
            AND p_can_fy IS NULL
         THEN
            --Start of original code
            BEGIN
               INSERT INTO nihgl_ope_acct_btchs_arc_tbl
                  SELECT *
                    FROM nihgl_ope_acct_btchs_int_tbl
                   WHERE     ope_batch_id = get_acct_btch_rec.ope_batch_id
                         AND file_name = get_acct_btch_rec.file_name
                         AND record_status <> 'U';

               INSERT INTO nihgl_ope_acct_dtls_arc_tbl
                  SELECT *
                    FROM nihgl_ope_acct_dtls_int_tbl
                   WHERE     ope_batch_id = get_acct_btch_rec.ope_batch_id
                         AND file_name = get_acct_btch_rec.file_name
                         AND record_status <> 'U';
            EXCEPTION
               WHEN OTHERS
               THEN
                  lb_error_exist := FALSE;
                  lc_errbuf :=
                        'Error in inserting record into Archive Tables :'
                     || SQLERRM;
                  logf (lc_errbuf, l_module);
            END;

            IF lb_error_exist
            THEN
               BEGIN
                  DELETE FROM nihgl_ope_acct_btchs_int_tbl
                        WHERE     ope_batch_id =
                                     get_acct_btch_rec.ope_batch_id
                              AND file_name = get_acct_btch_rec.file_name
                              AND record_status <> 'U';

                  DELETE FROM nihgl_ope_acct_dtls_int_tbl
                        WHERE     ope_batch_id =
                                     get_acct_btch_rec.ope_batch_id
                              AND file_name = get_acct_btch_rec.file_name
                              AND record_status <> 'U';
               EXCEPTION
                  WHEN OTHERS
                  THEN
                     lb_error_exist := TRUE;
                     lc_errbuf :=
                           'Error While Purging record from  Interface Tables :'
                        || SQLERRM;
                     logf (lc_errbuf, l_module);
               END;
            END IF;
         --End of original code
         ELSIF p_record_status IS NOT NULL
         THEN
            lb_chk_hdr_arc_flag := TRUE;

            BEGIN
               INSERT INTO nihgl_ope_acct_dtls_arc_tbl
                  SELECT *
                    FROM nihgl_ope_acct_dtls_int_tbl
                   WHERE     ope_batch_id = get_acct_btch_rec.ope_batch_id
                         AND file_name = get_acct_btch_rec.file_name
                         AND record_status = p_record_status;
            EXCEPTION
               WHEN OTHERS
               THEN
                  lb_error_exist := FALSE;
                  lc_errbuf :=
                        'Error in inserting detail record(s) based on record status into Archive Tables :'
                     || SQLERRM;
                  logf (lc_errbuf, l_module);
            END;

            IF lb_error_exist
            THEN
               BEGIN
                  DELETE FROM nihgl_ope_acct_dtls_int_tbl
                        WHERE     ope_batch_id =
                                     get_acct_btch_rec.ope_batch_id
                              AND file_name = get_acct_btch_rec.file_name
                              AND record_status = p_record_status;
               EXCEPTION
                  WHEN OTHERS
                  THEN
                     lb_error_exist := TRUE;
                     lc_errbuf :=
                           'Error While Purging detail record(s) based on record status from  Interface Tables :'
                        || SQLERRM;
                     logf (lc_errbuf, l_module);
               END;
            END IF;
         ELSIF     p_doc_ref IS NOT NULL
               AND p_doc_number IS NOT NULL
               AND p_can_fy IS NOT NULL
         THEN
            lb_chk_hdr_arc_flag := TRUE;

            BEGIN
               INSERT INTO nihgl_ope_acct_dtls_arc_tbl
                  SELECT *
                    FROM nihgl_ope_acct_dtls_int_tbl
                   WHERE     ope_batch_id = get_acct_btch_rec.ope_batch_id
                         AND file_name = get_acct_btch_rec.file_name
                         AND document_ref = p_doc_ref
                         AND document_number = p_doc_number
                         AND can_fy = p_can_fy;
            EXCEPTION
               WHEN OTHERS
               THEN
                  lb_error_exist := FALSE;
                  lc_errbuf :=
                        'Error in inserting detail record(s) based on doc ref/number and can fy into Archive Tables :'
                     || SQLERRM;
                  logf (lc_errbuf, l_module);
            END;

            IF lb_error_exist
            THEN
               BEGIN
                  DELETE FROM nihgl_ope_acct_dtls_int_tbl
                        WHERE     ope_batch_id =
                                     get_acct_btch_rec.ope_batch_id
                              AND file_name = get_acct_btch_rec.file_name
                              AND document_ref = p_doc_ref
                              AND document_number = p_doc_number
                              AND can_fy = p_can_fy;
               EXCEPTION
                  WHEN OTHERS
                  THEN
                     lb_error_exist := TRUE;
                     lc_errbuf :=
                           'Error While Purging detail record(s) based on doc ref/number and can fy from  Interface Tables :'
                        || SQLERRM;
                     logf (lc_errbuf, l_module);
               END;
            END IF;
         END IF;

         IF lb_chk_hdr_arc_flag
         THEN
            SELECT COUNT (*)
              INTO ln_cnt
              FROM nihgl_ope_acct_dtls_int_tbl
             WHERE     ope_batch_id = get_acct_btch_rec.ope_batch_id
                   AND file_name = get_acct_btch_rec.file_name
                   AND record_status <> 'U';

            IF ln_cnt = 0
            THEN
               --No more detail records found
               --Hence archive the header record
               BEGIN
                  INSERT INTO nihgl_ope_acct_btchs_arc_tbl
                     SELECT *
                       FROM nihgl_ope_acct_btchs_int_tbl
                      WHERE     ope_batch_id = get_acct_btch_rec.ope_batch_id
                            AND file_name = get_acct_btch_rec.file_name
                            AND record_status <> 'U';
               EXCEPTION
                  WHEN OTHERS
                  THEN
                     lb_error_exist := FALSE;
                     lc_errbuf :=
                           'Error in inserting header record into Archive Tables :'
                        || SQLERRM;
                     logf (lc_errbuf, l_module);
               END;

               IF lb_error_exist
               THEN
                  BEGIN
                     DELETE FROM nihgl_ope_acct_btchs_int_tbl
                           WHERE     ope_batch_id =
                                        get_acct_btch_rec.ope_batch_id
                                 AND file_name = get_acct_btch_rec.file_name
                                 AND record_status <> 'U';
                  EXCEPTION
                     WHEN OTHERS
                     THEN
                        lb_error_exist := TRUE;
                        lc_errbuf :=
                              'Error While Purging header record from Interface Tables :'
                           || SQLERRM;
                        logf (lc_errbuf, l_module);
                  END;
               END IF;
            END IF;
         END IF;
      END LOOP;

      COMMIT;
   EXCEPTION
      WHEN OTHERS
      THEN
         lb_error_exist := TRUE;
         lc_errbuf :=
            'Error in inserting record into Archive Tables :' || SQLERRM;
         logf (lc_errbuf,l_module);
         g_retcode := 2;
         g_errbuf :=
               'Error in inserting record into Archive Tables: '
            || SUBSTR (SQLERRM, 1, 250);
   END ope_archive;

-- =======================================
-- procedure submit_ope_report
-- =======================================
   PROCEDURE submit_ope_report (p_requestId IN NUMBER)
   IS
      l_request_id   NUMBER;
      l_layout       BOOLEAN;
      l_phase        VARCHAR2 (50);
      l_status       VARCHAR2 (50);
      l_dev_phase    VARCHAR2 (50);
      l_dev_status   VARCHAR2 (50);
      l_message      VARCHAR2 (100);
      l_wait         BOOLEAN;
      l_instance     VARCHAR2 (240);

       l_module varchar2(100)  :=  'submit_ope_report';

   BEGIN
      logf ('************************************************************************************', l_module);
      logf ('********************   START OF OPE ERROR REPORT SUBMISSION   *********************', l_module);
      logf ('************************************************************************************', l_module);
      logf ('Adding Layout', l_module);
      logf ('g_request_id 1: '||g_request_id, l_module);

      l_layout :=
         apps.fnd_request.add_layout (
            template_appl_name   => 'NIHGL',
            template_code        => 'NIHGL_OPE_ERROR_RPT_RTF',
            template_language    => 'en',
            template_territory   => 'US',
            output_format        => 'RTF');

      SELECT REGEXP_REPLACE (UPPER (instance_name), '[0-9]', '')
        INTO l_instance
        FROM V$instance;

      IF l_layout
      THEN
         --
         --Submitting Concurrent Request
         --
         logf ('Submitting OPE Error Report', l_module);

         l_request_id :=
            fnd_request.submit_request (
               application   => 'NIHGL',
               program       => 'NIHGL_OPE_ERROR_RPT',
               description   => 'NIHGL OPE Billing Error Report',
               start_time    => SYSDATE,
               sub_request   => FALSE,
               argument1     => g_request_id,
               argument2     => 'Y',
               argument3     => l_instance);
         --
         COMMIT;
         --
         IF l_request_id = 0
         THEN
            logf ( 'Concurrent request failed to submit', l_module);
         ELSE
            logf ( 'Successfully Submitted the Concurrent Request :- ' || l_request_id, l_module);
         END IF;
      END IF;
   EXCEPTION
      WHEN OTHERS THEN
         logf ('SUBMIT_OPE_REPORT ERROR :- ' || SQLERRM, l_module);
   END submit_ope_report;


--/*=======================================
-- NFE_BILLING_MAIN Procedure to validate the Billing data file coming from
-- feeder Systems to NBS and process it to final destination GL.
--
-- This procedure has been registered as a concurrent program "NIHGL NFE Billing Interface" in NBS EBS,
-- =======================================*/
   PROCEDURE NFE_BILLING_MAIN (errbuf               OUT VARCHAR2,
                   retcode              OUT NUMBER,
                   p_sob_id                 NUMBER,
				   --Start of change 1 by Yash
                   p_batch_number           VARCHAR2,
                   p_file_name              VARCHAR2,
                   p_called_from_form       VARCHAR2
				   --End of change 1
                   )
   IS
      lc_message   VARCHAR2 (2000);
      lc_retcode   VARCHAR2 (2);
       l_module varchar2(100)  :=  'NFE_BILLING_MAIN';
   BEGIN
      /*
      || Billing File from OPE (Feder System) has below structure.
      || 1. First record type 1 will have the batch header,Table NIHGL_OPE_ACCT_BTCHS_STG_TBL
      || 2. Second record type 2 will be the detail information of the OPE Billing.
      ||    All these are stored in NIHGL_OPE_ACCT_DTLS_STG_TBL table.
      || 3. Third record type 4 will be the batch detail containing the line
      ||    total and the total amount in the batch. Table NIHGL_OPE_ACCT_BTCHS_STG_TBL
      */
      /*
      || Initialize the Global variables.
      */
      g_request_id := fnd_global.conc_request_id;
      g_user_id    := fnd_global.user_id;
      g_login_id   := fnd_global.login_id;
      logf ('g_request_id 10: '||g_request_id, l_module);
      IF p_called_from_form = 'N'
      THEN

         validate_hdr_data (
							p_file_name => p_file_name,
							p_batch_id => g_request_id,
							errbuf => lc_message,
							retcode => lc_retcode
						   );

         IF g_retcode = '2'
         THEN
            retcode := g_retcode;
            errbuf := g_errbuf;
            RETURN;
         END IF;
      END IF;

      /*
      || If P_CALLED_FROM_FORM parameter is 'Y'
      || then update header record to 'N'
      */
      IF p_called_from_form = 'Y'
      THEN
         g_file_name (1) := p_file_name;
         --Update the batch header record status to 'N'
         --for regular processing

         record_btch_hdr_status (p_record_status   => 'N',
                                 p_file_name       => p_file_name,
                                 p_batch_number    => p_batch_number,
                                 p_error_message   => NULL,
                                 p_error_code      => NULL,
                                 p_ope_batch_id    => NULL);

         IF g_retcode = '2'
         THEN
            retcode := g_retcode;
            errbuf := g_errbuf;
            RETURN;
         END IF;

         /*
         ||Update the error message for the records to NULL
         */
         update_err_msg (p_error_message         => NULL,
                         p_funds_check_message   => NULL,
                         p_funds_check_code      => NULL,
                         p_doc_ref               => NULL,
                         p_doc_number            => NULL,
                         p_can_fy                => NULL,
                         p_file_name             => p_file_name,
                         p_batch_number          => p_batch_number,
                         p_record_status         => 'N',
                         p_error_code            => NULL);

         IF g_retcode = '2'
         THEN
            retcode := g_retcode;
            errbuf := g_errbuf;
            RETURN;
         END IF;
      END IF;


      /*
      || Now Select the different batch including the (U)nprocess one
      || from Detail intf table to be process further
      */
      validate_txn_data (errbuf               => lc_message,
                         retcode              => lc_retcode,
                         p_sob_id             => p_sob_id,
                         p_file_name          => p_file_name,
                         p_batch_number       => p_batch_number,
                         p_called_from_form   => p_called_from_form);

      IF g_retcode = '2'
      THEN
         --
         --CR#37860
         -- Krishna Aravapalli
         -- Generate theXML Publisher  error report with all messages included in Interface and Archive
         --
         -- write_report;
         submit_ope_report (p_requestId => g_request_id);
         retcode := g_retcode;
         errbuf := g_errbuf;
         RETURN;
      END IF;

      --
      -- CR#37860
      -- Krishna Aravapalli
      -- Generate theXML Publisher  error report with all messages included in Interface and Archive
      --
      -- write_report;

      submit_ope_report (p_requestId => g_request_id);

      --
      -- CR#36693
      -- Krishna Aravapalli
      -- Generate the error report with all messages included in Interface and Archive
      --/*
      --      build_email_attachment1 (p_batch_number    => p_batch_number,
      --                               p_file_name       => p_file_name,
      --                               p_call_type       => 'USERS',
      --                               p_record_status   => 'E');
      --*/
      /*
    || Update the master request id to all childs priority request id
    || to mark the parent child relationship
    */
      FOR conc_req_cur IN (SELECT request_id
                             FROM fnd_concurrent_requests
                            WHERE parent_request_id = g_request_id)
      LOOP
         BEGIN
            UPDATE fnd_concurrent_requests
               SET priority_request_id = g_request_id
             WHERE request_id = conc_req_cur.request_id;

            COMMIT;
         EXCEPTION
            WHEN OTHERS THEN
               logf (
                     ' Issue While Populateing the Parent Req ID to '
                  || ' theirs Childs  for Given Master Req ID :'
                  || g_request_id
                  || ' AND child req id :'
                  || conc_req_cur.request_id
                  || ' And Error is (if any) : '
                  || SQLERRM, l_module);
         END;
      END LOOP;



	  --NBSCH0002993 - To Archive all record to Archive table from Interface table
	 ope_archive (p_file_name       => p_batch_number,
			   p_record_status   => NULL,
			   p_doc_ref         => NULL,
			   p_doc_number      => NULL,
			   p_can_fy          => NULL );


   EXCEPTION
      WHEN OTHERS THEN
         lc_message := ' Unhandled Exception in procedure NFE_BILLING_MAIN : ' || SQLERRM || SQLCODE;
         logf (lc_message, l_module);
         retcode := 2;
   END NFE_BILLING_MAIN;


---------------------------------------------------------------------------------------------------
-- Set up for global variables.


BEGIN
   -- ============= Derive g_fund_value_set_name
   BEGIN
      SELECT UNIQUE flex_value_set_name
        INTO g_fund_value_set_name
        FROM apps.fnd_flex_value_sets ffvs
       WHERE ffvs.flex_value_set_name = 'GL_HHS_FUND';
   EXCEPTION
      WHEN OTHERS THEN
         g_fund_value_set_name := NULL;
   END;

---------------------------------------------------------------------------------------------------
   -- ============= Derive g_fund_type_tcode_lookup
   BEGIN
      SELECT UNIQUE lookup_type
        INTO g_fund_type_tcode_lookup
        FROM fnd_lookup_values
       WHERE lookup_type = 'NIHGL_OPE_TCODE_MAPPING';
   EXCEPTION
      WHEN OTHERS THEN
         g_fund_type_tcode_lookup := NULL;
   END;
---------------------------------------------------------------------------------------------------
   -- ============= Derive g_instance_name
   BEGIN
      SELECT d.NAME
        INTO g_instance_name
        FROM v$database d;
   EXCEPTION
      WHEN OTHERS THEN
         logf ( 'Global Variable INSTANCE NAME not found : ' || SUBSTR (SQLERRM, 1, 250),g_Module);
   END;
---------------------------------------------------------------------------------------------------
   -- ============= Derive g_ope_je_source
   BEGIN
      SELECT UNIQUE user_je_source_name
        INTO g_ope_je_source
        FROM gl_je_sources
       WHERE UPPER (user_je_source_name) = 'NIH OPE';
   EXCEPTION
      WHEN OTHERS THEN
         g_ope_je_source := NULL;
   END;
   -- ============= Derive g_cfy
   BEGIN
      -- get the current fy
      SELECT gp.period_year
        INTO g_cfy
        FROM gl_periods gp, gl_period_statuses gps, fnd_application fa
       WHERE gp.period_set_name = 'NIH_CALENDAR'
         AND gp.period_name = gps.period_name
         AND fa.application_id = gps.application_id
         AND gps.closing_status IN ('O', 'F')
         AND gps.set_of_books_id = 1
         AND gp.adjustment_period_flag = 'N'
         AND fa.application_short_name = 'SQLGL'
         AND TRUNC (SYSDATE) BETWEEN gp.start_date AND gp.end_date;
   EXCEPTION
      WHEN OTHERS THEN
         g_cfy := NULL;
   END;
---------------------------------------------------------------------------------------------------
   -- ============= Derive g_ope_je_category
   BEGIN
      SELECT user_je_category_name
        INTO g_ope_je_category
        FROM gl_je_categories
       WHERE UPPER (user_je_category_name) = 'NIH CS FFS';
   EXCEPTION
      WHEN OTHERS THEN
         g_ope_je_category := NULL;
   END;
---------------------------------------------------------------------------------------------------
   BEGIN
     SELECT directory_path
       INTO g_path
       FROM dba_directories
     WHERE directory_name = 'NIH_GL_OUT';
   EXCEPTION
      WHEN OTHERS THEN
         g_path := NULL;
   END;
---------------------------------------------------------------------------------------------------
   -- ============= Derive g_account
   BEGIN
      SELECT meaning ACCOUNT
        INTO g_account
        FROM fnd_lookup_values
       WHERE lookup_type = 'NIHGL_OPE_CONTROLS'
         AND lookup_code = 'INITIAL_SGL'
         AND ( NVL (enabled_flag, 'Y') = 'Y'
         AND ( NVL (start_date_active, SYSDATE) <= SYSDATE
               AND (  end_date_active >= SYSDATE
                      OR end_date_active IS NULL)));
   EXCEPTION
      WHEN OTHERS THEN
         g_account := NULL;
   END;



   -- ============= ------------------------------------------------------------------

    --Build the logic to load all the error messages
   --into index by table before processing.
   BEGIN
      FOR i IN (
                SELECT message_name, MESSAGE_TEXT
                  FROM NIHGL_NFI_VALIDATION_MSGS /*fnd_new_messages
                 WHERE message_name LIKE 'NIHOPE%'*/)-- CR NBSCH0002499
     -- AND TYPE = 'ERROR') -- Animesh why we need condition type as ERROR
      LOOP
         --Add new error record
         g_err_message_tbl (i.message_name) := i.MESSAGE_TEXT;
      END LOOP;
   EXCEPTION
      WHEN OTHERS THEN
         g_err_message_tbl.DELETE;
         RAISE;
   END;
   -- ----------------------------------------------------------------------------------------------------
   --Build the logic to load all the processing flows
   --into index by table before processing.
   BEGIN
      FOR i IN (SELECT NVL2 (
                       responsibility_name,
                       responsibility_name || ':'  || batch_number || ':' || message_name,
                       batch_number || ':' || message_name) index_value,
                       batch_number,
                       message_name,
                       file_reject,
                       hold_for_ope_eh,
                       reject_error_txn
               FROM apps.nihopeeh_error_messages_setups
              WHERE NVL (active_flag, 'N') = 'Y')
      LOOP
         --Add new error record
         g_txn_flow_flags_tbl (i.index_value).batch_number := i.batch_number;
         g_txn_flow_flags_tbl (i.index_value).message_name := i.message_name;
         g_txn_flow_flags_tbl (i.index_value).file_reject := i.file_reject;
         g_txn_flow_flags_tbl (i.index_value).hold_for_ope_eh :=  i.hold_for_ope_eh;
         g_txn_flow_flags_tbl (i.index_value).reject_error_txn := i.reject_error_txn;
      END LOOP;
   EXCEPTION
      WHEN OTHERS THEN
         g_err_message_tbl.DELETE;
         RAISE;
   END;
   ---------------------------------------------------------

   g_txn_hold_tbl.DELETE;
   g_txn_reject_tbl.DELETE;

   -------------------------------------------
EXCEPTION
   WHEN OTHERS THEN
      logf ( 'Error in Pkg Global Var Declartion : ' || SUBSTR (SQLERRM, 1, 250) , g_Module);
END nihgl_NFI_interface_pkg;
/

