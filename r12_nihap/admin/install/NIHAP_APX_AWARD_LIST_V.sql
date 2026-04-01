CREATE OR REPLACE VIEW APPS.NIHAP_APX_AWARD_LIST_V as
SELECT ds.sitecode
             prism_doc_site,
         pd.docnum
             prism_docnum,
         pd.ordernum
             prism_ordnum,
         ph.versionnum
             prism_version_num,
         st.descr
             prism_doc_status,
         op.segment1
             oracle_po_num,
         NVL (uc.description, cu.formattedname)
             prism_co_name,
         uc.email_address
             prism_co_email,
         NVL (ub.description, bu.formattedname)
             prism_buyer_name,
         ub.email_address
             prism_buyer_email,
         NULL
             unsettled_trx_exists,
         NVL (uo.description, ou.formattedname)
             prism_doc_owner_name,
         uo.email_address
             prism_doc_owner_email,
         ph.awardtype
             prism_awdtype_code,
            (CASE WHEN ph.awardtype IN (222, 225) THEN 'External ' END)
         || pt.descr
             prism_awdtype,
         pd.siteid
             prism_doc_siteid,
         ph.dockey
             prism_dockey,
         ph.verkey
             prism_verkey,
         op.po_header_id
             oracle_po_header_id,
         uc.employee_id
             oracle_co_emp_id,
         ub.employee_id
             oracle_buyer_emp_id,
         uo.employee_id
             oracle_owner_emp_id,
         'N' processed_flag,
         ah.reasonformodification,
         pv.vendorcode To_vendor_number,
         dv.addresscode To_vendor_site_code,
         vo.vendorcode From_vendor_number,
         dov.addresscode From_vendor_site_code   
    FROM apps.po_headers_all     op,
         header@readonlyprism    po,
         vendor@readonlyprism    vo,
         docvendor@readonlyprism dov,
         header@readonlyprism    ph,
         document@readonlyprism  pd,
         vendor@readonlyprism    pv,
         docvendor@readonlyprism dv,
         site@readonlyprism      ds,
         awdheader@readonlyprism ah,
         code@readonlyprism      rc,
         statustype@readonlyprism st,
         TYPE@readonlyprism      pt,
         apps.fnd_user           uc,
         usertable@READONLYPRISM cu,
         apps.fnd_user           ub,
         usertable@READONLYPRISM bu,
         apps.fnd_user           uo,
         usertable@READONLYPRISM ou
   WHERE     1 = 1
         AND (CASE
                  WHEN ph.awardtype = '225' AND ph.ctrdockey IS NOT NULL
                  THEN
                      (CASE
                           WHEN     SUBSTR (ph.docnum, 1, 4) = 'HHSN'
                                AND SUBSTR (ph.ordernum, 1, 4) = 'HHSN'
                           THEN
                                  SUBSTR (LTRIM (RTRIM (ph.docnum)), 5, 7)
                               || SUBSTR (LTRIM (RTRIM (ph.docnum)),
                                          13,
                                          LENGTH (ph.docnum))
                               || SUBSTR (LTRIM (RTRIM (ph.ordernum)),
                                          5,
                                          LENGTH (ph.ordernum))
                           WHEN     SUBSTR (ph.docnum, 1, 2) = '75'
                                AND SUBSTR (ph.ordernum, 1, 2) = '75'
                           THEN
                                  SUBSTR (LTRIM (RTRIM (ph.docnum)), 4, 6)
                               || SUBSTR (LTRIM (RTRIM (ph.docnum)), 12)
                               || SUBSTR (LTRIM (RTRIM (ph.ordernum)), 4)
                           WHEN     SUBSTR (ph.docnum, 1, 4) = 'HHSN'
                                AND SUBSTR (ph.ordernum, 1, 2) = '75'
                           THEN
                                  SUBSTR (LTRIM (RTRIM (ph.docnum)), 5, 3)
                               || SUBSTR (LTRIM (RTRIM (ph.docnum)), 10, 2)
                               || SUBSTR (LTRIM (RTRIM (ph.docnum)),
                                          14,
                                          LENGTH (ph.docnum))
                               || SUBSTR (LTRIM (RTRIM (ph.ordernum)), 4)
                           ELSE
                               'Invalid Award/Order Number format and cannot translate to Oracle Segment1/Release Number values.'
                       END)
                  WHEN ph.awardtype IN ('222', '225') AND ph.ctrdockey IS NULL
                  THEN
                      (CASE
                           WHEN SUBSTR (ph.ordernum, 1, 4) = 'HHSN'
                           THEN
                               SUBSTR (LTRIM (RTRIM (ph.ordernum)),
                                       5,
                                       LENGTH (ph.ordernum))
                           ELSE
                               LTRIM (RTRIM (ph.ordernum))
                       END)
                  WHEN    ph.awardtype IN ('222', '224')
                       OR ph.awardtype NOT IN ('221',
                                               '223',
                                               '226',
                                               '227',
                                               '245',
                                               '248')
                  THEN
                      (CASE
                           WHEN SUBSTR (ph.docnum, 1, 4) = 'HHSN'
                           THEN
                               SUBSTR (LTRIM (RTRIM (ph.docnum)),
                                       5,
                                       LENGTH (ph.docnum))
                           ELSE
                               LTRIM (RTRIM (ph.docnum))
                       END)
                  ELSE
                      LTRIM (RTRIM (ph.docnum))
              END) =
             op.segment1(+)
        --- AND ah.reasonformodification = <Prism_rfm_code_selected_on_vendor_page>
         AND ah.reasonformodification in ('J', 'V')
         AND ph.dockey = ah.dockey(+)
         AND ph.verkey = ah.verkey(+)
         AND ah.reasonformodification = rc.code(+)
         AND rc.TYPE(+) = 980
         AND st.status = ph.status
         AND pt.TYPE = ph.awardtype
         AND cu.action || bu.action LIKE '%A%'  --
         --AND cu.action || bu.action IS NOT NULL
         AND cu.userid = uc.user_name(+)
         --AND cu.action(+) = 'A'
         AND ph.ctrofficeruserkey = cu.userkey(+)
         AND bu.userid = ub.user_name(+)
         --AND bu.action(+) = 'A'
         AND ph.buyeruserkey = bu.userkey(+)
         --AND NVL (pd.currentbuyeruserkey, ph.buyeruserkey) = bu.userkey(+)
         AND ou.userid = uo.user_name(+)
         --AND ou.action(+) = 'A'
         AND ph.owneruserkey = ou.userkey(+)
      ---   AND dv.addresscode = <To_Vendor_Site_Code_UEI_selected_on_vendor_page>
         AND dv.addrtype = DECODE (ph.awardtype, 224, 123, 114)
         AND ph.dockey = dv.dockey
         AND ph.verkey = dv.verkey
      --   AND pv.vendorcode = <To_Vendor_Number_selected_on_vendor_page>
         AND ph.vendorkey = pv.vendorkey
         AND NVL (ph.ctrdockey, -1) < 1
         AND ph.doctype = '500'
         AND ph.status = '1'
         AND pd.siteid = ds.siteid(+)
         AND ph.verkey = pd.max_verkey
         AND ph.dockey = pd.dockey
      ---   AND dov.addresscode = <From_Vendor_Site_Code_UEI_selected_on_vendor_page>
         AND dov.addrtype = DECODE (po.awardtype, 224, 123, 114)
         AND po.dockey = dov.dockey
         AND po.verkey = dov.verkey
       --  AND vo.vendorcode = From_Vendor_Number_selected_on_vendor_page
         AND po.vendorkey = vo.vendorkey
         AND NVL (po.ctrdockey, -1) < 1
         AND po.doctype = '500'
         AND po.dockey = ph.dockey
         AND (po.docnum, NVL (po.ordernum, 'NULL'), po.verkey) =
             (  SELECT pr.docnum, NVL (pr.ordernum, 'NULL'), MAX (pr.verkey)
                  FROM header@readonlyprism pr
                 WHERE     po.docnum = pr.docnum
                       AND NVL (po.ordernum, 'NULL') = NVL (pr.ordernum, 'NULL')
                       AND po.dockey = pr.dockey
                       AND pr.status = '5'
              GROUP BY pr.docnum, NVL (pr.ordernum, 'NULL'))
/

