CREATE OR REPLACE FORCE VIEW APPS.NIHGL_GRANTS_RECON_V_OutOfCycle PR create
(
   NBRSS_SEQ_NBR,
   GRANT_NUMBER,
   FISCAL_YEAR,
   CAN,
   ICD,
   OBJECT_CLASS,
   AMOUNT,
   TRANSACTION_CODE,
   INCREASE_OR_DECREASE,
   NBS_GL_POST_DATE,
   DOCNO,
   MODFCODE,
   EIN,
   NBS_RCVD_DATE,
   INPUT_DATE,
   DOC_TYPE,
   DOC_STATUS,
   APPL_ID,
   SENT_TO_PMS_DATE,
   SOURCE,
   ERRORCODE,
   ERRORMESSAGE
)
AS
   SELECT  --/*+ RULE */
          /*+ index(hist) index(gjh) */
               /*   Change History:
                    4/18/2017: CR 36468 PRB0013805-changes to eRA obligation WebService to include actual Grant source name (eRA, OFM or NBS-YE-CLOSE)
                  - Removed filter   hist.transaction_source IN ('eRA', 'OFM')
                  - sending hist.transaction_source as is; no more decoding to 'eRA Grants'
                */
          hist.interface_record_id nbrss_seq_nbr,
          hist.grant_number grant_number,
          TO_CHAR (hist.fy) fiscal_year,
          hist.can,
          (SELECT ffv.description
             FROM fnd_flex_values_vl ffv, pa_projects_all ppa
            WHERE     TRIM (
                         REGEXP_SUBSTR (REPLACE (ppa.attribute2, '.', '. '),
                                        '[^.]+',
                                        1,
                                        4)) = ffv.flex_value
                  AND ffv.flex_value_set_id = 1006041
                  AND ppa.name = hist.can)
             icd,
          hist.object_class object_class,
          hist.award_amount amount,
          hist.transaction_code transaction_code,
          TO_CHAR (hist.increase_or_decrease) increase_or_decrease,
          TRUNC (gjh.posted_date) nbs_gl_post_date,
          hist.document_number docno,
          hist.revision_indicator modfcode,
          hist.ein ein,
          TRUNC (hist.accounting_date) nbs_rcvd_date,
          TRUNC (hist.creation_date) input_date,
          DECODE (hist.transaction_code,
                  '040', 'Commitment',
                  '050', 'Obligation',
                  '059', 'Obligation')
             doc_type,
          DECODE (gjh.posted_date, NULL, 'Unposted', 'Posted') doc_status,
          hist.attribute1 appl_id,
          hist.oblig_send_to_pms_date sent_to_pms_date,
          -- CR 36468
          --          'eRA Grants' source,
          hist.transaction_source source,
          '' errorcode,
          '' errormessage
     FROM nihgrt_transactions_hist hist, gl_je_headers gjh
    WHERE 1 = 1 AND hist.je_header_id = gjh.je_header_id(+)        -- CR 36468
                                                            --          AND hist.transaction_source IN ('eRA', 'OFM')
          AND hist.record_status = 'P' AND hist.award_amount <> 0
   UNION ALL
   -- #2 Zero Dollar Transactions --
   SELECT hist.interface_record_id nbrss_seq_nbr,
          hist.grant_number grant_number,
          TO_CHAR (hist.fy) fiscal_year,
          hist.can,
          (SELECT ffv.description
             FROM fnd_flex_values_vl ffv, pa_projects_all ppa
            WHERE     TRIM (
                         REGEXP_SUBSTR (REPLACE (ppa.attribute2, '.', '. '),
                                        '[^.]+',
                                        1,
                                        4)) = ffv.flex_value
                  AND ffv.flex_value_set_id = 1006041
                  AND ppa.name = hist.can)
             icd,
          hist.object_class object_class,
          hist.award_amount amount,
          hist.transaction_code transaction_code,
          TO_CHAR (hist.increase_or_decrease) increase_or_decrease,
          NULL nbs_gl_post_date,
          hist.document_number docno,
          hist.revision_indicator modfcode,
          hist.ein ein,
          TRUNC (hist.accounting_date) nbs_rcvd_date,
          TRUNC (hist.creation_date) input_date,
          DECODE (hist.transaction_code,
                  '040', 'Commitment',
                  '050', 'Obligation',
                  '059', 'Obligation')
             doc_type,
          'Unposted' doc_status,
          hist.attribute1 appl_id,
          hist.oblig_send_to_pms_date sent_to_pms_date,
          -- CR 36468
          --          'eRA Grants' source,
          hist.transaction_source source,
          '' errorcode,
          '' errormessage
     FROM nihgrt_transactions_hist hist
    WHERE hist.record_status = 'P'                                 -- CR 36468
                                   --          AND hist.transaction_source IN ('eRA', 'OFM')
          AND hist.award_amount = 0
   UNION ALL
   -- #3 Query to get data for document_type = Error --
   SELECT hist.transaction_id nbrss_seq_nbr,
          hist.grant_number grant_number,
          TO_CHAR (hist.fy) fiscal_year,
          hist.can,
          (SELECT DISTINCT glcc.segment4
             FROM gl_code_combinations glcc
            WHERE glcc.code_combination_id = hist.code_combination_id)
             icd,
          hist.object_class object_class,
          hist.award_amount amount,
          hist.transaction_code transaction_code,
          TO_CHAR (hist.increase_or_decrease) increase_or_decrease,
          NULL nbs_gl_post_date,
          hist.document_number docno,
          hist.revision_indicator modfcode,
          hist.ein ein,
          TRUNC (hist.accounting_date) nbs_rcvd_date,
          TRUNC (hist.creation_date) input_date,
          DECODE (hist.transaction_code,
                  '040', 'Commitment',
                  '050', 'Obligation',
                  '059', 'Obligation')
             doc_type,
          'Error' doc_status,
          hist.attribute1 appl_id,
          NULL sent_to_pms_date,
          -- CR 36468
          --          'eRA Grants' source,
          hist.transaction_source source,
          hist.ERROR_CODE errorcode,
          hist.error_message errormessage
     FROM nihgrt_transactions_int hist
    WHERE hist.record_status = 'E'
   -- CR 36468
   -- AND hist.transaction_source = 'eRA'
   UNION ALL
   -- #4 Get VALTRAN data from Staging --
   SELECT                                                   --/*+ index(vs) */
         vs.valtran_staging_id nbrss_seq_nbr,
          vs.grant_no grant_number,
          CASE vs.fy
             WHEN TO_CHAR (ADD_MONTHS (vs.parm_date, 3), 'Y')
             THEN
                TO_CHAR (ADD_MONTHS (vs.parm_date, 3), 'YYYY')
             WHEN TO_CHAR (ADD_MONTHS (vs.parm_date, 3 + 12 * 1), 'Y')
             THEN
                TO_CHAR (ADD_MONTHS (vs.parm_date, 3 + 12 * 1), 'YYYY')
             WHEN TO_CHAR (ADD_MONTHS (vs.parm_date, 3 + 12 * 2), 'Y')
             THEN
                TO_CHAR (ADD_MONTHS (vs.parm_date, 3 + 12 * 2), 'YYYY')
             WHEN TO_CHAR (ADD_MONTHS (vs.parm_date, 3 + 12 * 3), 'Y')
             THEN
                TO_CHAR (ADD_MONTHS (vs.parm_date, 3 + 12 * 3), 'YYYY')
             WHEN TO_CHAR (ADD_MONTHS (vs.parm_date, 3 + 12 * 4), 'Y')
             THEN
                TO_CHAR (ADD_MONTHS (vs.parm_date, 3 + 12 * 4), 'YYYY')
             WHEN TO_CHAR (ADD_MONTHS (vs.parm_date, 3 + 12 * 5), 'Y')
             THEN
                TO_CHAR (ADD_MONTHS (vs.parm_date, 3 - 12 * 5), 'YYYY')
             WHEN TO_CHAR (ADD_MONTHS (vs.parm_date, 3 + 12 * 6), 'Y')
             THEN
                TO_CHAR (ADD_MONTHS (vs.parm_date, 3 - 12 * 4), 'YYYY')
             WHEN TO_CHAR (ADD_MONTHS (vs.parm_date, 3 + 12 * 7), 'Y')
             THEN
                TO_CHAR (ADD_MONTHS (vs.parm_date, 3 - 12 * 3), 'YYYY')
             WHEN TO_CHAR (ADD_MONTHS (vs.parm_date, 3 + 12 * 8), 'Y')
             THEN
                TO_CHAR (ADD_MONTHS (vs.parm_date, 3 - 12 * 2), 'YYYY')
             WHEN TO_CHAR (ADD_MONTHS (vs.parm_date, 3 + 12 * 9), 'Y')
             THEN
                TO_CHAR (ADD_MONTHS (vs.parm_date, 3 - 12 * 1), 'YYYY')
          END
             fiscal_year,
          vs.can,
          vs.acronym_icd icd,
          vs.nbs_object_class object_class,
          vs.amount amount,
          vs.tran_code transaction_code,
          vs.rev_code increase_or_decrease,
          TRUNC (vs.date_written_to_gl) nbs_gl_post_date,
          vs.orig_doc docno,
          vs.mod_code modfcode,
          vs.vendor_ssn ein,
          vs.proc_date nbs_rcvd_date,
          vs.proc_date input_date,
          'Obligation' doc_type,
          DECODE (vs.date_written_to_gl, NULL, 'Unposted', 'Posted')
             doc_status,
          NULL appl_id,
          NULL sent_to_pms_date,
          'Valtran' source,
          '' errorcode,
          '' errormessage
     FROM nihgl_valtran_staging vs
    WHERE vs.batch = '85'
   UNION ALL
   -- #5 Get VALTRAN data from Archive --
   SELECT                                                   --/*+ index(va) */
         va.valtran_staging_id nbrss_seq_nbr,
          va.grant_no grant_number,
          CASE va.fy
             WHEN TO_CHAR (ADD_MONTHS (va.parm_date, 3), 'Y')
             THEN
                TO_CHAR (ADD_MONTHS (va.parm_date, 3), 'YYYY')
             WHEN TO_CHAR (ADD_MONTHS (va.parm_date, 3 + 12 * 1), 'Y')
             THEN
                TO_CHAR (ADD_MONTHS (va.parm_date, 3 + 12 * 1), 'YYYY')
             WHEN TO_CHAR (ADD_MONTHS (va.parm_date, 3 + 12 * 2), 'Y')
             THEN
                TO_CHAR (ADD_MONTHS (va.parm_date, 3 + 12 * 2), 'YYYY')
             WHEN TO_CHAR (ADD_MONTHS (va.parm_date, 3 + 12 * 3), 'Y')
             THEN
                TO_CHAR (ADD_MONTHS (va.parm_date, 3 + 12 * 3), 'YYYY')
             WHEN TO_CHAR (ADD_MONTHS (va.parm_date, 3 + 12 * 4), 'Y')
             THEN
                TO_CHAR (ADD_MONTHS (va.parm_date, 3 + 12 * 4), 'YYYY')
             WHEN TO_CHAR (ADD_MONTHS (va.parm_date, 3 + 12 * 5), 'Y')
             THEN
                TO_CHAR (ADD_MONTHS (va.parm_date, 3 - 12 * 5), 'YYYY')
             WHEN TO_CHAR (ADD_MONTHS (va.parm_date, 3 + 12 * 6), 'Y')
             THEN
                TO_CHAR (ADD_MONTHS (va.parm_date, 3 - 12 * 4), 'YYYY')
             WHEN TO_CHAR (ADD_MONTHS (va.parm_date, 3 + 12 * 7), 'Y')
             THEN
                TO_CHAR (ADD_MONTHS (va.parm_date, 3 - 12 * 3), 'YYYY')
             WHEN TO_CHAR (ADD_MONTHS (va.parm_date, 3 + 12 * 8), 'Y')
             THEN
                TO_CHAR (ADD_MONTHS (va.parm_date, 3 - 12 * 2), 'YYYY')
             WHEN TO_CHAR (ADD_MONTHS (va.parm_date, 3 + 12 * 9), 'Y')
             THEN
                TO_CHAR (ADD_MONTHS (va.parm_date, 3 - 12 * 1), 'YYYY')
          END
             fiscal_year,
          va.can,
          va.acronym_icd icd,
          va.nbs_object_class object_class,
          va.amount amount,
          va.tran_code transaction_code,
          va.rev_code increase_or_decrease,
          TRUNC (va.date_written_to_gl) nbs_gl_post_date,
          va.orig_doc docno,
          va.mod_code modfcode,
          va.vendor_ssn ein,
          va.proc_date nbs_rcvd_date,
          va.proc_date input_date,
          'Obligation' doc_type,
          DECODE (va.date_written_to_gl, NULL, 'Unposted', 'Posted')
             doc_status,
          NULL appl_id,
          NULL sent_to_pms_date,
          'Valtran' source,
          '' errorcode,
          '' errormessage
     FROM nihgl_valtran_archive va
    WHERE va.batch = '85';