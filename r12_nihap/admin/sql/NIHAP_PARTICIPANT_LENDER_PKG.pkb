CREATE OR REPLACE PACKAGE BODY APPS.nihap_participant_lender_pkg
AS
   --****************************************************************************************************************
   --* Purpose: This package consists of procedures and functions that will be used to load LRP participant         *
   --*          and Lender information into NBS .                                                                   *
   --*          If the vendor exists and is active in NBS PO vendors table will be updated and a new                *
   --*          LRP site will be created . If the vendor exists and is inactive then the record will be written     *
   --*          to the output file. If the participant does not exist as a vendor then a vendor record and a LRP    *
   --*          site will be created                                                                                *
   --*                                                                                                              *
   --*                                                                                                              *
   --*Person      Date    Comments                                                                                  *
   -- ---------   ------  ------------------------------------------                                                *
   -- Sangeeta    4/2/07   Created package                                                                          *
   -- Narender    4/21/11  Ticket# 0000015081 for May release, Added following procderes create_parti_success_file  *
   --                      create_parti_error_file, update_parti_hist_tbl.                                          *
   -- Vinod K     8/15/12  CRMOP 0001364633 LRP Participant interface - modifications needed                        *
   -- Vinod K     9/18/12  CRMOP 0001731556 Modify LRP_Participant_Interface
   -- Sandeep B   8/21/14  Changed NIHPO_VENDORS view,PO_VENDORS to AP_SUPPLIERS,PO_VENDOR_SITES_ALL to AP_SUPPLIER_SITES_ALL
   --                      Changed vendor_type_lookup_code from `EMPLOYEE¿ to `NIH_EMPLOYEE¿                        *
   --                      Used ap_vendor_pub_pkg.create_vendor instead of ap_po_vendors_apis_pkg.insert_new_vendor  to craete vendor
   -- Animesh S   4/24/15  RT Defect -- The Withholding Tax at the Participant site level should be set to No on the vendor site level.
   --                      Changes on version 6305
   -- Animesh S   4/30/15  Federal reportable flag should be set to Y while creating new Supplier Header for PARTICIPANT.
   --                      Also added a condition not to update the header records for participant sites if below already exists.
   --                      vendor_name_alt
   --                      federal_reportable_flag = 'Y'
   --                      always_take_disc_flag = 'N'
   --                      allow_awt_flag = 'Y'
   -- Krishna A  05/06/2019 Address Line 2 even though it is null API is not taking values so using fnd_api.g_miss_char
   -- Redmedybiz 11/28/2021 NIH - Change Request 42642 - PRB0022391 LRP COMMONS ID Implementation
   -- Redmedybiz 121/10/2021 NIH - Change Request 42642 - PRB0022391 LRP COMMONS ID Implementation fixiing SSN - Commons id 
   -- Gouthami G 02/03/2022  Changes for CR No: 42412 - 19c Upgrade changes -  Modified UTL file path to directory name
   -- Remedybiz 09/06/2022  NIH - Change Request 43395 - PRB0023772 COMMONS ID missing in response file sent back to LRP for some participants 
   -- Ravi R    09/25/2025  CR#NBSCH0003121- PRB0028672_Update LRP Participant load process to suffix Vendor Number to Vendor Name on HZ_PARTIES
   --                       Added new procedure Update_vendor  
   --****************************************************************************************************************
   -- ******************Procedure:nihap_lrp_load_main *********************************************************************
   -- *  This procedure will call the main  nihap_load_lrp_participant and will send all exceptions during the participant*
   -- *  upload process to the output file.                                                                               *
   -- *                                                                                                                   *
   -- *********************************************************************************************************************
   --
   /* Declaring global variables for the package body */
   g_debug   NUMBER := 1;      -- Set this variable to 0 to turn off debugging

   --
   PROCEDURE nihap_lrp_load_main (errbuf OUT VARCHAR2, retcode OUT NUMBER)
   IS
      CURSOR error_rec
      IS
         SELECT tin_number,
                vendor_name,
                vendor_type,
                error_message,
                address_line_1,
                address_line_2,
                city,
                state,
                zip,
                country
           FROM nihap_lrp_participant_load
          WHERE load_status = 'E' AND error_message IS NOT NULL;

      v_error_ct   NUMBER;
   BEGIN
      --country information is validated against fnd_territories_vl when API is called;
      UPDATE nihap_lrp_participant_load
         SET country = 'United States'
       WHERE country = 'United States of America';

      COMMIT;
      fnd_file.put_line (
         fnd_file.output,
         '===================================================1');
      nihap_load_lrp_participant;
      fnd_file.put_line (
         fnd_file.output,
         '===================================================2');
      v_error_ct := 0;

      FOR e_rec IN error_rec
      LOOP
         v_error_ct := v_error_ct + 1;
         fnd_file.put_line (
            fnd_file.output,
            '===================================================');
         fnd_file.put_line (fnd_file.output, 'Error Record:');
         fnd_file.put_line (fnd_file.output,
                            'Vendor Name  : ' || e_rec.vendor_name);
         fnd_file.put_line (fnd_file.output,
                            'TIN Number   : ' || e_rec.tin_number);
         fnd_file.put_line (fnd_file.output,
                            'Vendor Type  : ' || e_rec.vendor_type);
         fnd_file.put_line (fnd_file.output,
                            'Address Line1: ' || e_rec.address_line_1);
         fnd_file.put_line (fnd_file.output,
                            'Address Line2: ' || e_rec.address_line_2);
         fnd_file.put_line (fnd_file.output,
                            'City              : ' || e_rec.city);
         fnd_file.put_line (fnd_file.output,
                            'State        : ' || e_rec.state);
         fnd_file.put_line (fnd_file.output, 'Zip        : ' || e_rec.zip);
         fnd_file.put_line (fnd_file.output,
                            'Country    : ' || e_rec.country);
         fnd_file.put_line (fnd_file.output,
                            'Error Message: ' || e_rec.error_message);
         fnd_file.put_line (
            fnd_file.output,
            '====================================================');
      END LOOP;

      IF v_error_ct > 0
      THEN
         retcode := 1;
         errbuf := 'View output file for Error Records';
      ELSE
         fnd_file.put_line (fnd_file.output, ' ');
         fnd_file.put_line (
            fnd_file.output,
            '===================NO EXCEPTIONS FOUND ==========================');
      END IF;

      fnd_file.put_line (fnd_file.output, 'Records in error :' || v_error_ct);
   EXCEPTION
      WHEN OTHERS
      THEN
         retcode := -1;
         errbuf := SQLERRM;
         DBMS_OUTPUT.put_line ('nihap_lrp_load_main error:' || SQLERRM);
         fnd_file.put_line (fnd_file.LOG,
                            ' nihap_lrp_load_main error: ' || SQLERRM);
         fnd_file.put_line (fnd_file.LOG, ' and  : ' || SQLCODE);
   END nihap_lrp_load_main;

   PROCEDURE update_address (p_address1         IN VARCHAR2,
                             p_location_id      IN NUMBER,
                             p_vendor_id        IN NUMBER,
                             p_vendor_site_id   IN NUMBER)
   IS
      v_vendor_site_rec_type   AP_VENDOR_PUB_PKG.r_vendor_site_rec_type;
      v_location_rec           HZ_LOCATION_V2PUB.LOCATION_REC_TYPE;
      v_return_status          VARCHAR2 (3);
      v_msg_ct                 NUMBER;
      v_msg_data               VARCHAR2 (240);
      v_msg                    VARCHAR2 (2000);
      v_msg_index_out          NUMBER;
      v_ovn                    NUMBER;
   BEGIN
      Fnd_File.PUT_LINE (Fnd_File.LOG, ' Update_Address - Entered');
      v_vendor_site_rec_type := NULL;
      v_vendor_site_rec_type.address_line1 := p_address1;
      v_vendor_site_rec_type.vendor_id := p_vendor_id;
      v_vendor_site_rec_type.vendor_site_id := p_vendor_site_id;
      fnd_msg_pub.Initialize;
      ap_vendor_pub_pkg.Update_Vendor_Site_public (
         p_api_version        => 1.0,
         p_init_msg_list      => FND_API.G_TRUE,
         p_commit             => FND_API.G_TRUE,
         p_validation_level   => FND_API.G_VALID_LEVEL_FULL,
         x_return_status      => v_return_status,
         x_msg_count          => v_msg_ct,
         x_msg_data           => v_msg_data,
         p_vendor_site_rec    => v_vendor_site_rec_type,
         p_vendor_site_id     => p_vendor_site_id);

      IF v_return_status <> 'S'
      THEN
         IF v_msg_ct > 0
         THEN
            v_msg := NULL;

            FOR v_index IN 1 .. v_msg_ct
            LOOP
               fnd_msg_pub.get (p_msg_index       => v_index,
                                p_encoded         => 'F',
                                p_data            => v_msg_data,
                                p_msg_index_out   => v_msg_index_out);
               v_msg := v_msg || '|' || SUBSTR (v_msg_data, 1, 100);
            END LOOP;
         END IF;

         Fnd_File.PUT_LINE (
            Fnd_File.LOG,
               '#108 Update_Address - Error occured while updating Vendor Site address Information :'
            || v_msg);
      ELSE
         Fnd_File.PUT_LINE (
            Fnd_File.LOG,
            '#109 Update_Address -Updated Vendor Site address Information successfully.');
      END IF;
   --      END IF;
   END update_address;
   
   --NBSCH0003121 Changes
   -- Update vendor calls API to update vendor/party name 
   PROCEDURE Update_vendor (p_vendor_id IN NUMBER,
                            p_vendor_name IN VARCHAR2,
							p_vendor_num  IN VARCHAR2)
   IS
   v_vendor_rec_type        AP_VENDOR_PUB_PKG.r_vendor_rec_type;
   v_return_status          VARCHAR2 (100);
   v_msg_ct                 NUMBER;
   v_msg_data               VARCHAR2 (2400);
   v_msg                    VARCHAR2 (3000);
   v_msg_index_out          NUMBER;
   v_ovn                    NUMBER;
   
   BEGIN
     Fnd_File.PUT_LINE (Fnd_File.LOG, ' Update_Vendor API calling..');
	 
	 v_vendor_rec_type := NULL;
     v_vendor_rec_type.vendor_name := UPPER (p_vendor_name || ':' || p_vendor_num);            
     v_vendor_rec_type.vendor_id   := p_vendor_id;
   --   
     fnd_msg_pub.Initialize;
     ap_vendor_pub_pkg.Update_Vendor_Public (
                     p_api_version        => 1.0,
                     p_init_msg_list      => FND_API.G_TRUE,
                     p_commit             => FND_API.G_TRUE,
                     p_validation_level   => FND_API.G_VALID_LEVEL_FULL,
                     x_return_status      => v_return_status,
                     x_msg_count          => v_msg_ct,
                     x_msg_data           => v_msg_data,
                     p_vendor_rec         => v_vendor_rec_type,
                     p_vendor_id          => p_vendor_id);
					 
		IF v_return_status <> 'S'
      THEN
         IF v_msg_ct > 0
         THEN
            v_msg := NULL;

            FOR v_index IN 1 .. v_msg_ct
            LOOP
               fnd_msg_pub.get (p_msg_index       => v_index,
                                p_encoded         => 'F',
                                p_data            => v_msg_data,
                                p_msg_index_out   => v_msg_index_out);
               v_msg := v_msg || '|' || SUBSTR (v_msg_data, 1, 100);
            END LOOP;
         END IF;

         Fnd_File.PUT_LINE (
            Fnd_File.LOG,
               'Update_Vendor - Error occured while updating Vendor name Information :'
            || v_msg);
      ELSE
         Fnd_File.PUT_LINE (
            Fnd_File.LOG,
            'Update_Vendor -Updated Vendor name Information successfully.');
      END IF;			 
   END Update_vendor; --End changes

   PROCEDURE nihap_check_if_vendor_exists (errbuf    OUT VARCHAR2,
                                           retcode   OUT NUMBER)
   IS
      -- ******************Procedure:nihap_check_if_vendor_exists*********************************************************************
      -- *  This procedure will Check against po vendors if vendor exists in NBS . Inactive vendors will be wriiten to output file   *
      -- *  upload process to the output file. For active /existing vendors the vendor_exists Flag will be set to 'Y' ; For vendors  *
      --*   that do not exist in the NBS the vendor exists flag will be set to 'N' . If two active vendor records exist for vendor   *                                                                       *
      -- *  the vendor exists flag will be set to 'E'                                                                                                                *
      -- *********************************************************************************************************************
      -- Check against po vendors if vendor exists in NBS

      CURSOR staging_cur_cid
      IS
           --Use this for testing
           --          SELECT vendor_name, tin_number, ROWNUM
           --            FROM (SELECT   vendor_name, tin_number
           --                  FROM nihap_lrp_participant_load
           --                  -- WHERE     ((vendor_exists is null AND vendor_exists != 'E')
           --              WHERE tin_number in ('356825334','151689901','158549476') -- )
           --                  ORDER BY vendor_name)
           --           WHERE ROWNUM < 11;
           SELECT vendor_name, tin_number, commons_id
             FROM nihap_lrp_participant_load
            WHERE vendor_exists IS NULL OR vendor_exists = 'E' -- - original code
         ORDER BY vendor_name;

      CURSOR staging_cur
      IS
           --Use this for testing
           --          SELECT vendor_name, tin_number, ROWNUM
           --            FROM (SELECT   vendor_name, tin_number
           --                  FROM nihap_lrp_participant_load
           --                  -- WHERE     ((vendor_exists is null AND vendor_exists != 'E')
           --              WHERE tin_number in ('356825334','151689901','158549476') -- )
           --                  ORDER BY vendor_name)
           --           WHERE ROWNUM < 11;
           SELECT vendor_name, tin_number, commons_id
             FROM nihap_lrp_participant_load
            WHERE     (vendor_exists IS NULL OR vendor_exists = 'E')
                  AND tin_number IS NOT NULL
                  AND load_status <> 'I'
         -- - original code
         ORDER BY vendor_name;

      -- inactive vendors only
      CURSOR check_inactive_vendor (
         p_tin VARCHAR2)
      IS
         SELECT vendor_id,
                vendor_name,
                num_1099,
                start_date_active,
                end_date_active,
                v.vendor_type_lookup_code
           FROM nihpo_vendors v
          WHERE     v.num_1099 = p_tin
                AND NOT EXISTS
                           (SELECT NULL
                              FROM nihpo_vendors v_act
                             WHERE     v_act.num_1099 = p_tin
                                   AND v_act.num_1099 = v.num_1099
                                   AND NVL (v_act.end_date_active,
                                            SYSDATE + 1) > SYSDATE);

      CURSOR check_vendor (
         p_tin VARCHAR2)
      IS
         SELECT vendor_id,
                vendor_name,
                num_1099,
                start_date_active,
                end_date_active
           FROM nihpo_vendors
          WHERE     num_1099 = p_tin
                AND NVL (end_date_active, SYSDATE + 1) > SYSDATE;

      x_ven_name             nihpo_vendors.vendor_name%TYPE := NULL;
      x_ssn                  nihpo_vendors.num_1099%TYPE := NULL;
      x_vendor_id            nihpo_vendors.vendor_id%TYPE := NULL;
      x_st_dt_active         nihpo_vendors.start_date_active%TYPE := NULL;
      x_end_dt_active        nihpo_vendors.end_date_active%TYPE := NULL;
      x_vendor_lookup_code   nihpo_vendors.vendor_type_lookup_code%TYPE
                                := NULL;
      x_err_msg              VARCHAR2 (1500) := NULL;
      x_lkp_ssn              nihpo_vendors.num_1099%TYPE := NULL;
      x_ssn_valid            VARCHAR2 (1);
      v_vendor_id            NUMBER;
      v_vendor_site_id       NUMBER;
      v_ssn                  VARCHAR2 (50);
      v_commons_id           VARCHAR2 (50);
   BEGIN
      --DBMS_APPLICATION_INFO.set_client_info ('103');  --sandeep commented off
      MO_GLOBAL.SET_POLICY_CONTEXT ('S', 103);                ---sandeep added

      --CR 42642  begin
      FOR prec_cid IN staging_cur_cid
      LOOP
         x_ssn_valid := NULL;
         x_lkp_ssn := NULL;

         BEGIN
            fnd_file.put_line (
               fnd_file.LOG,
               '******************************************************');
            fnd_file.put_line (
               fnd_file.LOG,
                  ' Deriving SSN for the vendor Commons ID : '
               || prec_cid.commons_id);
            x_lkp_ssn := NULL;

            /*   SELECT ssn
                 INTO x_lkp_ssn
                 FROM apps.nihap_lrp_sprs_lkp
                WHERE commons_id = prec_cid.commons_id AND ROWNUM = 1
             ORDER BY sprs_last_update_date DESC; */

            SELECT ssn
              INTO x_lkp_ssn
              FROM (  SELECT *
                        FROM apps.nihap_lrp_sprs_lkp
                       WHERE commons_id =prec_cid.commons_id --AND ROWNUM = 1
                    ORDER BY sprs_last_update_date DESC)
             WHERE ROWNUM = 1;

            UPDATE nihap_lrp_participant_load
               SET tin_number = x_lkp_ssn
             WHERE commons_id = prec_cid.commons_id;

            fnd_file.put_line (
               fnd_file.LOG,
                  ' Derived SSN for the vendor COMMONS ID/SSN : '
               || prec_cid.commons_id
               || TRANSLATE (SUBSTR (x_lkp_ssn, 1, LENGTH (x_lkp_ssn) - 4),
                             x_lkp_ssn,
                             'XXXXXXXXXX')
               || SUBSTR (x_lkp_ssn, LENGTH (x_lkp_ssn) - 3));
            x_ssn_valid := 'S';
         EXCEPTION
            WHEN NO_DATA_FOUND
            THEN
               DBMS_OUTPUT.put_line (
                     'Could not derive SSN for given Commons ID'
                  || prec_cid.commons_id);
               fnd_file.put_line (
                  fnd_file.LOG,
                     ' Could not derive SSN for given Commons ID : '
                  || prec_cid.commons_id);

               UPDATE nihap_lrp_participant_load
                  SET last_update_date = SYSDATE,
                      error_message =
                            'Error in Fetching SSN for the vendor with Commons ID : '
                         || prec_cid.commons_id,
                      load_status = 'I'
                WHERE commons_id = prec_cid.commons_id;

               x_ssn_valid := 'I';
            WHEN OTHERS
            THEN
               DBMS_OUTPUT.put_line (
                     'Could not derive SSN for given Commons ID'
                  || prec_cid.commons_id);
               fnd_file.put_line (
                  fnd_file.LOG,
                  '******************************************************');
               fnd_file.put_line (
                  fnd_file.LOG,
                     ' Could not derive SSN for given Commons ID'
                  || prec_cid.commons_id);

               UPDATE nihap_lrp_participant_load
                  SET last_update_date = SYSDATE,
                      error_message =
                            'Error in Fetching SSN for the vendor with Commons ID :  '
                         || prec_cid.commons_id,
                      load_status = 'I'
                WHERE commons_id = prec_cid.commons_id;
         END;

         IF x_ssn_valid <> 'I'
         THEN
            -- Check for ssn for given commons id
            BEGIN
               SELECT ven.vendor_id,
                      site.vendor_site_id,
                      ven.num_1099,
                      ven.attribute9
                 INTO v_vendor_id,
                      v_vendor_site_id,
                      v_ssn,
                      v_commons_id
                 FROM apps.ap_suppliers ven,
                      apps.ap_supplier_sites_all site,
                      nihap_lrp_participant_load lrp_stg
                WHERE     site.vendor_id = ven.vendor_id
                      AND site.pay_site_flag = 'Y'
                      AND site.vendor_site_code = 'LRP_PARTICIPANT'
                      AND NVL (ven.end_date_active, SYSDATE + 1) > SYSDATE
                      AND ven.attribute9 = prec_cid.commons_id
                      --  AND lrp_stg.commons_id = stg.commons_id -- Missing Join
                      AND ROWNUM = 1;
            EXCEPTION
               WHEN NO_DATA_FOUND
               THEN
                  v_vendor_id := NULL;
                  v_vendor_site_id := NULL;
                  v_ssn := NULL;
                  v_commons_id := NULL;
            END;

            fnd_file.put_line (
               fnd_file.LOG,
                  ' ************ Processing Commons ID : '
               || prec_cid.commons_id
               || ' ************');

            fnd_file.put_line (
               fnd_file.LOG,
               'commons ID  ' || v_commons_id || ' : ' || prec_cid.commons_id);

            fnd_file.put_line (fnd_file.LOG,
                               'SSN         ' || v_ssn || ' : ' || x_lkp_ssn);

            IF x_lkp_ssn <> NVL (v_ssn, x_lkp_ssn)
            THEN
               UPDATE nihap_lrp_participant_load
                  SET last_update_date = SYSDATE,
                      error_message =
                            'This Commons ID '
                         || prec_cid.commons_id
                         || ' exists in NBS Vendor table with different SSN Vendor id/Vendor site id/SSN : '
                         || v_vendor_id
                         || '/'
                         || v_vendor_site_id
                         || '/'
                         || TRANSLATE (SUBSTR (v_ssn, 1, LENGTH (v_ssn) - 4),
                                       v_ssn,
                                       'XXXXXXXXXX')
                         || SUBSTR (v_ssn, LENGTH (v_ssn) - 3),
                      load_status = 'I'
                WHERE commons_id = prec_cid.commons_id;
            END IF;

            v_vendor_id := NULL;
            v_vendor_site_id := NULL;
            v_ssn := NULL;
            v_commons_id := NULL;

            -- Check for commons id  for given SSN
            BEGIN
               SELECT ven.vendor_id,
                      site.vendor_site_id,
                      ven.num_1099,
                      ven.attribute9
                 INTO v_vendor_id,
                      v_vendor_site_id,
                      v_ssn,
                      v_commons_id
                 FROM apps.po_vendors ven,
                      apps.ap_supplier_sites_all site,
                      nihap_lrp_participant_load lrp_stg
                WHERE     site.vendor_id = ven.vendor_id
                      AND site.pay_site_flag = 'Y'
                      AND site.vendor_site_code = 'LRP_PARTICIPANT'
                      AND NVL (ven.end_date_active, SYSDATE + 1) > SYSDATE
                      AND ven.num_1099 = x_lkp_ssn
                      --  AND lrp_stg.commons_id = stg.commons_id -- Missing Join
                      AND ROWNUM = 1;
            EXCEPTION
               WHEN NO_DATA_FOUND
               THEN
                  v_vendor_id := NULL;
                  v_vendor_site_id := NULL;
                  v_ssn := NULL;
                  v_commons_id := NULL;
            END;

            fnd_file.put_line (
               fnd_file.LOG,
               'commons ID  ' || v_commons_id || ' : ' || prec_cid.commons_id);

            fnd_file.put_line (fnd_file.LOG,
                               'SSN         ' || v_ssn || ' : ' || x_lkp_ssn);

            IF prec_cid.commons_id <> NVL (v_commons_id, prec_cid.commons_id)
            THEN
               UPDATE nihap_lrp_participant_load
                  SET last_update_date = SYSDATE,
                      error_message =
                            'This SSN for Commons ID : '
                         || prec_cid.commons_id
                         || ' exists in NBS Vendor table with different Commons ID Vendor id/Vendor site/Commons ID/SSN : '
                         || v_vendor_id
                         || '/'
                         || v_vendor_site_id
                         || '/'
                         || v_commons_id
                         || '/'
                         || TRANSLATE (SUBSTR (v_ssn, 1, LENGTH (v_ssn) - 4),
                                       v_ssn,
                                       'XXXXXXXXXX')
                         || SUBSTR (v_ssn, LENGTH (v_ssn) - 3),
                      load_status = 'I'
                WHERE commons_id = prec_cid.commons_id;
            END IF;
         END IF;
      END LOOP;

      --CR 42642 END
      FOR prec IN staging_cur
      LOOP
         -- fnd_file.put_line (fnd_file.LOG,'Begin Loop : '||prec.tin_number);
         x_vendor_id := NULL;
         x_ven_name := NULL;
         x_ssn := NULL;
         x_st_dt_active := NULL;
         x_end_dt_active := NULL;

         --dbms_output.put_line ('vendor name ...'|| prec.participant_name ||':'|| prec.tin_number);
         OPEN check_inactive_vendor (prec.tin_number);

         FETCH check_inactive_vendor
            INTO x_vendor_id,
                 x_ven_name,
                 x_ssn,
                 x_st_dt_active,
                 x_end_dt_active,
                 x_vendor_lookup_code;

         IF check_inactive_vendor%FOUND
         THEN
            DBMS_OUTPUT.put_line (
               'Vendor record exists in PO vendors but is inactive');
            fnd_file.put_line (
               fnd_file.output,
               '===================================================');
            fnd_file.put_line (
               fnd_file.output,
               'Vendor record exists in PO vendors but is inactive ');
            fnd_file.put_line (fnd_file.output,
                               'Vendor ID         : ' || x_vendor_id);
            fnd_file.put_line (fnd_file.output,
                               'Vendor Name       : ' || x_ven_name);
            fnd_file.put_line (fnd_file.output,
                               'SSN                 : ' || x_ssn);
            fnd_file.put_line (fnd_file.output,
                               'Start Date Active : ' || x_st_dt_active);
            fnd_file.put_line (fnd_file.output,
                               'End Date Active   : ' || x_end_dt_active);
            fnd_file.put_line (
               fnd_file.output,
               'Vendor Lookup Code: ' || x_vendor_lookup_code);
            fnd_file.put_line (
               fnd_file.output,
               '====================================================');

            UPDATE nihap_lrp_participant_load
               SET vendor_exists = 'E',
                   last_update_date = SYSDATE,
                   error_message =
                      'Vendor Record exists in PO vendors but is inactive'
             WHERE tin_number = prec.tin_number;
         ELSE
            BEGIN
               SELECT pv.vendor_id,
                      pv.vendor_name,
                      pv.num_1099,
                      pv.start_date_active,
                      pv.end_date_active
                 INTO x_vendor_id,
                      x_ven_name,
                      x_ssn,
                      x_st_dt_active,
                      x_end_dt_active
                 FROM nihpo_vendors pv
                WHERE     pv.num_1099 = prec.tin_number
                      AND NVL (pv.end_date_active, SYSDATE + 1) > SYSDATE;

               DBMS_OUTPUT.put_line ('Vendor record exists in PO vendors');
               fnd_file.put_line (
                  fnd_file.LOG,
                  '******************************************************');
               fnd_file.put_line (fnd_file.LOG,
                                  ' Vendor record exists in PO vendors');
               fnd_file.put_line (fnd_file.LOG,
                                  ' Vendor Name : ' || x_ven_name);
               fnd_file.put_line (fnd_file.LOG, ' Vendor SSN  : ' || x_ssn);

               UPDATE nihap_lrp_participant_load
                  SET vendor_exists = 'Y',
                      last_update_date = SYSDATE,
                      error_message = NULL
                WHERE tin_number = prec.tin_number;
                
               -- NIH - Change Request 43395 
                 UPDATE ap_suppliers v
                    SET attribute9 = prec.commons_id,
                        LAST_UPDATE_DATE = sysdate,
                        LAST_UPDATED_BY = fnd_global.user_id
                  WHERE num_1099 = prec.tin_number
                    AND vendor_id = x_vendor_id;    

               fnd_file.put_line (
                  fnd_file.LOG,
                  ' After update of staging table set vendor_exists to Y - Yes ');
               fnd_file.put_line (
                  fnd_file.LOG,
                  '******************************************************');
            EXCEPTION
               WHEN NO_DATA_FOUND
               THEN
                  DBMS_OUTPUT.put_line (
                     'Vendor record does not exist in PO vendors');
                  fnd_file.put_line (
                     fnd_file.LOG,
                     '******************************************************');
                  fnd_file.put_line (
                     fnd_file.LOG,
                     ' Vendor record does not exist in PO vendors 1');
                  fnd_file.put_line (fnd_file.LOG,
                                     ' Vendor Name : ' || prec.vendor_name);
                  fnd_file.put_line (fnd_file.LOG,
                                     ' Vendor SSN  : ' || prec.tin_number);

                  UPDATE nihap_lrp_participant_load
                     SET vendor_exists = 'N',
                         last_update_date = SYSDATE,
                         error_message = NULL
                   WHERE tin_number = prec.tin_number;
                   


                  fnd_file.put_line (
                     fnd_file.LOG,
                     ' After update of staging table set vendor_exists to N ');
                  fnd_file.put_line (
                     fnd_file.LOG,
                     '******************************************************');
               WHEN OTHERS
               THEN
                  BEGIN
                     SELECT pv.vendor_id,
                            pv.vendor_name,
                            pv.num_1099,
                            pv.start_date_active,
                            pv.end_date_active
                       INTO x_vendor_id,
                            x_ven_name,
                            x_ssn,
                            x_st_dt_active,
                            x_end_dt_active
                       FROM nihpo_vendors pv, ap_supplier_sites_all pvs --sandeep changes
                      WHERE     pv.num_1099 = prec.tin_number
                            AND NVL (pv.end_date_active, SYSDATE + 1) >
                                   SYSDATE
                            AND pv.vendor_id = pvs.vendor_id -- Added by VK CRMOP 0001364633, Check if LRP site exists --
                            AND pvs.vendor_site_code = 'LRP_PARTICIPANT' -- Added by VK CRMOP 0001364633, Check if LRP site exists --
                            AND NVL (pvs.inactive_date, SYSDATE + 1) >
                                   SYSDATE; -- Added by VK CRMOP 0001364633, Check if LRP site exists --

                     fnd_file.put_line (
                        fnd_file.LOG,
                           'LRP PARTICIPANT Site exists for Vendor SSN  : '
                        || x_ssn);

                     UPDATE nihap_lrp_participant_load
                        SET vendor_exists = 'Y',
                            last_update_date = SYSDATE,
                            error_message = NULL
                      WHERE tin_number = prec.tin_number;
                      

                      
                     fnd_file.put_line (
                        fnd_file.LOG,
                        ' After update of staging table set vendor_exists to Y ');
                  EXCEPTION
                     WHEN NO_DATA_FOUND
                     THEN
                        /*Retcode ='1'ends program with a with warning when run from the concurrent manager*/
                        retcode := '1';
                        errbuf :=
                           'Please Look at Log File for Exception Report';
                        x_err_msg := SQLERRM;
                        -- Vendor may have 2 active records check NBS
                        fnd_file.put_line (
                           fnd_file.LOG,
                           '******************************************************');
                        fnd_file.put_line (
                           fnd_file.LOG,
                           ' Issue with Vendor record: Duplicate Vendors in NBS' --|| x_err_msg
                                                                                );
                        fnd_file.put_line (
                           fnd_file.LOG,
                           ' Vendor Name : ' || prec.vendor_name);
                        fnd_file.put_line (
                           fnd_file.LOG,
                           ' Vendor SSN  : ' || prec.tin_number);

                        UPDATE nihap_lrp_participant_load
                           SET vendor_exists = 'E',
                               last_update_date = SYSDATE,
                               error_message =
                                  'Issue with Vendor record: Duplicate Vendors in NBS'
                         --                            SUBSTR ('Issue with Vendor record: ' || x_err_msg,
                         --                                    1,
                         --                                    2000
                         --                                   )
                         WHERE tin_number = prec.tin_number;

                        fnd_file.put_line (
                           fnd_file.LOG,
                           ' After update of staging table set vendor_exists to E ');
                        fnd_file.put_line (
                           fnd_file.LOG,
                           '******************************************************');
                        fnd_file.put_line (
                           fnd_file.output,
                           '===================================================');
                        fnd_file.put_line (
                           fnd_file.output,
                              'Issue with Vendor record -- Check NBS: '
                           || x_err_msg);
                        fnd_file.put_line (
                           fnd_file.output,
                           'Vendor ID         : ' || x_vendor_id);
                        fnd_file.put_line (
                           fnd_file.output,
                           'Vendor Name       : ' || x_ven_name);
                        fnd_file.put_line (fnd_file.output,
                                           'SSN                 : ' || x_ssn);
                        fnd_file.put_line (
                           fnd_file.output,
                           '====================================================');
                     WHEN OTHERS
                     THEN
                        /*Retcode ='1'ends program with a with warning when run from the concurrent manager*/
                        retcode := '1';
                        errbuf :=
                           'Please Look at Log File for Exception Report';
                        x_err_msg := SQLERRM;
                        -- Vendor may have 2 active records check NBS
                        fnd_file.put_line (
                           fnd_file.LOG,
                           '******************************************************');
                        fnd_file.put_line (
                           fnd_file.LOG,
                           ' Issue with Vendor record: Duplicate Vendor and LRP Participant Sites found' --|| x_err_msg
                                                                                                        );
                        fnd_file.put_line (
                           fnd_file.LOG,
                           ' Vendor Name : ' || prec.vendor_name);
                        fnd_file.put_line (
                           fnd_file.LOG,
                           ' Vendor SSN  : ' || prec.tin_number);

                        UPDATE nihap_lrp_participant_load
                           SET vendor_exists = 'E',
                               last_update_date = SYSDATE,
                               error_message =
                                  'Issue with Vendor record: Duplicate Vendor and LRP Participant Sites found'
                         --                            SUBSTR ('Issue with Vendor record: ' || x_err_msg,
                         --                                    1,
                         --                                    2000
                         --                                   )
                         WHERE tin_number = prec.tin_number;

                        fnd_file.put_line (
                           fnd_file.LOG,
                           ' After update of staging table set vendor_exists to E ');
                        fnd_file.put_line (
                           fnd_file.LOG,
                           '******************************************************');
                        fnd_file.put_line (
                           fnd_file.output,
                           '===================================================');
                        fnd_file.put_line (
                           fnd_file.output,
                              'Issue with Vendor record -- Check NBS: '
                           || x_err_msg);
                        fnd_file.put_line (
                           fnd_file.output,
                           'Vendor ID         : ' || x_vendor_id);
                        fnd_file.put_line (
                           fnd_file.output,
                           'Vendor Name       : ' || x_ven_name);
                        fnd_file.put_line (fnd_file.output,
                                           'SSN                 : ' || x_ssn);
                        fnd_file.put_line (
                           fnd_file.output,
                           '====================================================');
                  END;
            --
            --
            END;
         END IF;                                      -- Check Inactive Vendor

         CLOSE check_inactive_vendor;

         COMMIT;
      END LOOP;
   EXCEPTION
      WHEN OTHERS
      THEN
         retcode := -1;
         errbuf := SQLERRM;
         fnd_file.put_line (
            fnd_file.LOG,
               'Error while updating staging table- nihap_check_if_vendor_exists procedure  : '
            || SQLERRM);
         fnd_file.put_line (fnd_file.LOG, ' and the error code : ');
         fnd_file.put_line (fnd_file.LOG, SQLCODE);
   END;

   /* This procedure will create LRP SITE for participant   */
   PROCEDURE nihap_insert_vendor_site (
      l_vendor_id        IN VARCHAR2 DEFAULT NULL,
      p_tin_num          IN VARCHAR2 DEFAULT NULL,
      p_site_name        IN VARCHAR2 DEFAULT NULL,
      --will be defaulted to LRP participant for participants
      p_address_line_1   IN VARCHAR2 DEFAULT NULL,
      p_address_line_2   IN VARCHAR2 DEFAULT NULL,
      p_city             IN VARCHAR2 DEFAULT NULL,
      p_state            IN VARCHAR2 DEFAULT NULL,
      p_zip              IN VARCHAR2 DEFAULT NULL,
      p_country          IN VARCHAR2 DEFAULT NULL,
      p_end_date         IN DATE DEFAULT NULL)
   IS
      /******************************************************************************
      1.2      25-Apr-11    Narender Valaboju     Ticket# 0000015081 for May release, modified nihap_insert_vendor_site procedure to
                                                   update Vendors site added p_ to the procedure parameters.
      ******************************************************************************/
      --  x_main_add_site_id   NUMBER;                                      --out
      --  x_status             VARCHAR2 (30);                             ----out
      --  x_msg                VARCHAR2 (430);                             ---out
      v_orgid             NUMBER := fnd_profile.VALUE ('org_id');
      v_exception_site    VARCHAR2 (1000);
      v_site_count        NUMBER;
      v_country_code      fnd_territories_vl.territory_code%TYPE;
      v_country           fnd_territories_vl.territory_code%TYPE := NULL;
      l_return_status     VARCHAR2 (1);
      l_msg               VARCHAR2 (4000);
      l_vendor_site_rec   ap_vendor_pub_pkg.r_vendor_site_rec_type;
      l_party_site_id     NUMBER;                                    --sandeep
      l_msg_count         NUMBER;                                    --sandeep
      l_msg_data          VARCHAR2 (4000);                           --sandeep
      l_vendor_site_id    NUMBER;                                     --sandep
      --       l_msg_count          NUMBER;
      l_api_version       NUMBER := 1.0;
      l_msg_index_out     NUMBER;                                    --sandeep
      --sandeep
      l_location_id       NUMBER;
      l_ovn               NUMBER;
      vAddline1           per_addresses.address_line1%TYPE;
      vAddline2           per_addresses.address_line2%TYPE;
      vAddline3           per_addresses.address_line3%TYPE;
      vcity               per_addresses.town_or_city%TYPE;
      vstate              per_addresses.region_2%TYPE;
      vzip                per_addresses.postal_code%TYPE;
      vcountry            per_addresses.country%TYPE;
      v_location_rec      HZ_LOCATION_V2PUB.LOCATION_REC_TYPE;
      v_return_status     VARCHAR2 (3);
      v_msg_ct            NUMBER;
      v_msg_data          VARCHAR2 (240);
      v_msg               VARCHAR2 (2000);
      v_msg_index_out     NUMBER;
   --sandeep
   BEGIN
      DBMS_OUTPUT.put_line ('Vendor id : ' || l_vendor_id);
      DBMS_OUTPUT.put_line ('num_1099 : ' || p_tin_num);
      fnd_file.put_line (fnd_file.LOG, 'In Vendor sites Insert procedure');
      fnd_file.put_line (fnd_file.LOG, 'Vendor ID :' || l_vendor_id);
      fnd_file.put_line (fnd_file.LOG, 'Tin Number: ' || p_tin_num);
      l_return_status := NULL;
      l_msg := NULL;
      -- DBMS_APPLICATION_INFO.set_client_info ('103');--sandeep
      MO_GLOBAL.SET_POLICY_CONTEXT ('S', 103);                       --sandeep

      --- check if LRP site exists
      SELECT COUNT (*)
        INTO v_site_count
        FROM ap_supplier_sites_all
       WHERE     vendor_id = l_vendor_id
             AND vendor_site_code = 'LRP_PARTICIPANT'
             AND NVL (inactive_date, SYSDATE + 1) > SYSDATE; -- Added by VK 08292012 --

      IF v_site_count = 1
      THEN
         fnd_file.put_line (
            fnd_file.LOG,
            'LRP Site exists for Participant -Vendor ID  : ' || l_vendor_id);
         fnd_file.put_line (
            fnd_file.LOG,
            'Updating PO_vendor_sites_all with Address for participant');

         -- Modified by VK CRMOP 0001364633, validate country using fnd_territories. Can see updates from front end (FORM) --
         BEGIN
            SELECT territory_code
              INTO v_country
              FROM fnd_territories_vl
             WHERE territory_short_name = INITCAP (RTRIM (LTRIM (p_country)));
         EXCEPTION
            WHEN NO_DATA_FOUND
            THEN
               UPDATE nihap_lrp_participant_load
                  SET vendor_site_inserted = 'N',
                      load_status = 'E',
                      error_message =
                            'Country doesnt exists in fnd_territories for Vendor ID: '
                         || l_vendor_id,
                      last_update_date = SYSDATE
                WHERE tin_number = p_tin_num;

               fnd_file.put_line (
                  fnd_file.LOG,
                     'Country doesnt exists in fnd_territories for Vendor ID: '
                  || l_vendor_id);
            WHEN OTHERS
            THEN
               v_country := UPPER (p_country);
               fnd_file.put_line (
                  fnd_file.LOG,
                     'LRP Country doesnt exists in fnd_territories for Vendor ID: '
                  || l_vendor_id);
         END;

         IF v_country IS NOT NULL
         THEN
            l_vendor_site_rec := NULL;                               --sandeep
            l_party_site_id := NULL;                                 --sandeep
            l_location_id := NULL;                                   --sandeep
            l_msg_count := NULL;                                     --sandeep
            l_msg_data := NULL;                                      --sandeep
            l_return_status := NULL;                                 --sandeep
            l_vendor_site_id := NULL;                                --sandeep

            BEGIN
               SELECT vendor_site_id
                 INTO l_vendor_site_id
                 FROM ap_supplier_sites_all
                WHERE     vendor_id = l_vendor_id
                      AND vendor_site_code = 'LRP_PARTICIPANT'
                      AND NVL (inactive_date, SYSDATE + 1) > SYSDATE;
            EXCEPTION
               WHEN NO_DATA_FOUND
               THEN
                  UPDATE nihap_lrp_participant_load
                     SET vendor_site_inserted = 'N',
                         load_status = 'E',
                         error_message =
                               'No SITE ID Found  for Vendor ID: '
                            || l_vendor_id,
                         last_update_date = SYSDATE
                   WHERE tin_number = p_tin_num;

                  fnd_file.put_line (
                     fnd_file.LOG,
                     'SITE ID Issue for Vendor ID: ' || l_vendor_id);
               WHEN OTHERS
               THEN
                  fnd_file.put_line (
                     fnd_file.LOG,
                        'LRP SITE ID doesnt exists  for Vendor ID: '
                     || l_vendor_id);
            END;


            fnd_file.put_line (
               fnd_file.LOG,
                  'Print Address: '
               || RTRIM (UPPER (p_address_line_1))
               || '  line2  '
               || RTRIM (UPPER (p_address_line_2)));



            vAddline1 := RTRIM (UPPER (p_address_line_1));
            vAddline2 :=
               NVL (RTRIM (UPPER (p_address_line_2)), FND_API.G_MISS_CHAR); --Krishna Aravapalli 05/06/2019 Please pass on null value if the address line2 is null
            vcity := RTRIM (UPPER (p_city));
            vstate := UPPER (p_state);
            vzip := p_zip;
            vcountry := RTRIM (UPPER (v_country_code));

            BEGIN
               SELECT loc.location_id, loc.object_version_number
                 INTO l_location_id, l_ovn
                 FROM ap_supplier_sites_all ass, hz_locations loc
                WHERE     ass.location_id = loc.location_id
                      AND ass.vendor_site_id = l_vendor_site_id
                      AND ass.vendor_site_code = 'LRP_PARTICIPANT'
                      AND ROWNUM = 1;
            EXCEPTION
               WHEN OTHERS
               THEN
                  l_location_id := NULL;
                  l_ovn := NULL;
            END;

            IF l_location_id IS NOT NULL
            THEN
               v_location_rec.location_id := l_location_id;
               v_location_rec.address1 := vAddline1;
               v_location_rec.address2 := vAddline2;
               v_location_rec.address3 := vAddline3;
               v_location_rec.city := vcity;
               v_location_rec.state := vstate;
               v_location_rec.province := NULL;
               v_location_rec.postal_code := vzip;
               v_location_rec.county := NULL;
               v_location_rec.country := vcountry;
               fnd_msg_pub.Initialize;
               v_return_status := NULL;
               v_msg_ct := 0;
               v_msg_data := NULL;
               v_msg := NULL;
               v_msg_index_out := NULL;
               HZ_LOCATION_V2PUB.update_location (
                  p_init_msg_list           => 'F',
                  p_location_rec            => v_location_rec,
                  p_object_version_number   => l_ovn,
                  x_return_status           => v_return_status,
                  x_msg_count               => v_msg_ct,
                  x_msg_data                => v_msg_data);

               IF v_return_status <> 'S'
               THEN
                  IF v_msg_ct > 0
                  THEN
                     v_msg := NULL;

                     FOR v_index IN 1 .. v_msg_ct
                     LOOP
                        fnd_msg_pub.get (p_msg_index       => v_index,
                                         p_encoded         => 'F',
                                         p_data            => v_msg_data,
                                         p_msg_index_out   => v_msg_index_out);
                        v_msg := v_msg || '|' || SUBSTR (v_msg_data, 1, 100);
                     END LOOP;
                  END IF;

                  Fnd_File.PUT_LINE (
                     Fnd_File.LOG,
                        'Error occurred while Updating Location Address for Location_id: '
                     || l_location_id
                     || ' is: '
                     || v_msg);
                  DBMS_OUTPUT.PUT_LINE (
                        'Error occurred while Updating Location Address for Location_id: '
                     || l_location_id
                     || ' is: '
                     || v_msg);
               ELSE
                  Fnd_File.PUT_LINE (
                     Fnd_File.LOG,
                        'Location Address Updated successfully.  Location_id = '
                     || l_location_id);
                  DBMS_OUTPUT.PUT_LINE (
                        'Location Address Updated successfully.  Location_id = '
                     || l_location_id);
               END IF;
            END IF;


            Fnd_File.PUT_LINE (
               Fnd_File.LOG,
                  'ADDR -- '
               || p_address_line_1
               || ' '
               || p_address_line_2
               || ' '
               || p_city
               || ' '
               || p_state
               || ' '
               || p_zip
               || ' '
               || p_country);

            l_vendor_site_rec.vendor_site_code := p_site_name;
            --  l_vendor_site_rec.location_id :=l_location_id;
            l_vendor_site_rec.vendor_id := l_vendor_id;
            l_vendor_site_rec.address_line1 := UPPER (p_address_line_1);
            l_vendor_site_rec.address_line2 :=
               NVL (UPPER (p_address_line_2), FND_API.G_MISS_CHAR);
            l_vendor_site_rec.address_lines_alt :=
               UPPER (
                     p_address_line_1
                  || ' '
                  || p_address_line_2
                  || ' '
                  || p_city
                  || ' '
                  || p_state
                  || ' '
                  || p_zip
                  || ' '
                  || p_country);
            l_vendor_site_rec.city := UPPER (p_city);
            l_vendor_site_rec.state := UPPER (p_state);
            l_vendor_site_rec.zip := UPPER (p_zip);
            l_vendor_site_rec.country := UPPER (v_country_code);
            l_vendor_site_rec.inactive_date := p_end_date;
            l_vendor_site_rec.last_update_date := SYSDATE;
            l_vendor_site_rec.last_updated_by := fnd_global.user_id;
            l_vendor_site_rec.vendor_id := l_vendor_id;
            l_vendor_site_rec.vendor_site_id := l_vendor_site_id;
            l_vendor_site_rec.ext_payee_rec.default_pmt_method := 'EFT'; --sandeep
            --     l_vendor_site_rec.exclusive_payment_flag := 'Y';
            l_vendor_site_rec.ALLOW_AWT_FLAG := 'N'; -- Animesh R12TU RT Allow AWT Flag should be set to N
            ap_vendor_pub_pkg.Update_Vendor_Site_public (
               p_api_version        => l_api_version,
               p_init_msg_list      => FND_API.G_FALSE,
               p_commit             => FND_API.G_FALSE,
               p_validation_level   => FND_API.G_VALID_LEVEL_FULL,
               x_return_status      => l_return_status,
               x_msg_count          => l_msg_count,
               x_msg_data           => l_msg_data,
               p_vendor_site_rec    => l_vendor_site_rec,
               p_vendor_site_id     => l_vendor_site_id,
               p_calling_prog       => NULL);

            IF l_return_status <> 'S'
            THEN
               IF l_msg_count > 0
               THEN
                  l_msg := NULL;

                  FOR l_index IN 1 .. l_msg_count
                  LOOP
                     fnd_msg_pub.get (p_msg_index       => l_index,
                                      p_encoded         => 'F',
                                      p_data            => l_msg_data,
                                      p_msg_index_out   => l_msg_index_out);
                     l_msg := l_msg || '|' || SUBSTR (l_msg_data, 1, 100);
                  END LOOP;
               END IF;

               Fnd_File.PUT_LINE (
                  Fnd_File.LOG,
                  'Error occurred while Updating Vendor Site' || l_msg);
            ELSE
               Fnd_File.PUT_LINE (Fnd_File.LOG,
                                  'Vendor Site Updated successfully.');
            END IF;

            fnd_file.put_line (
               fnd_file.LOG,
               'After Update of PO_vendor_sites_all with Address for participant');
            fnd_file.put_line (
               fnd_file.output,
               '=========================================================================');
            fnd_file.put_line (
               fnd_file.output,
                  'LRP site successfully updated for participant -- tin number '
               || p_tin_num);
            fnd_file.put_line (fnd_file.output,
                               'Vendor ID              : ' || l_vendor_id);
            fnd_file.put_line (
               fnd_file.output,
               '=========================================================================');

            UPDATE nihap_lrp_participant_load
               SET vendor_site_inserted = 'Y',
                   error_message = NULL,
                   last_update_date = SYSDATE,
                   load_status = 'P'
             WHERE tin_number = p_tin_num;

            fnd_file.put_line (
               fnd_file.LOG,
               'After update of Staging table; Record Processed setting load status = P');
         END IF;
      ELSE                                                     -- v_site_count
         fnd_file.put_line (
            fnd_file.LOG,
            'LRP Site does not exist for Vendor ID: ' || l_vendor_id);
         fnd_file.put_line (fnd_file.LOG,
                            'Before API to create LRP site for participant');

         -- insert _vendor_site
         -- Venkat: API is already updating the who columns below. So no need.
         BEGIN
            SELECT territory_code
              INTO v_country_code
              FROM fnd_territories_vl
             WHERE territory_short_name = INITCAP (RTRIM (LTRIM (p_country)));
         EXCEPTION
            WHEN OTHERS
            THEN
               v_country_code := NULL;
               fnd_file.put_line (
                  fnd_file.LOG,
                     'LRP Country Code doesnt exists for Vendor ID: '
                  || l_vendor_id);
         END;

         --- A
         BEGIN
            l_vendor_site_rec := NULL;                               --sandeep
            l_party_site_id := NULL;                                 --sandeep
            l_location_id := NULL;                                   --sandeep
            l_msg_count := NULL;                                     --sandeep
            l_msg_data := NULL;                                      --sandeep
            l_vendor_site_id := NULL;                                --sandeep
            l_return_status := NULL;                                 --sandeep
            l_vendor_site_rec.vendor_site_code := p_site_name;
            l_vendor_site_rec.address_line1 :=
               UPPER (p_address_line_1) || p_site_name;
            l_vendor_site_rec.address_line2 := UPPER (p_address_line_2);
            l_vendor_site_rec.city := UPPER (p_city);
            l_vendor_site_rec.state := UPPER (p_state);
            l_vendor_site_rec.zip := UPPER (p_zip);
            l_vendor_site_rec.country := UPPER (v_country_code);
            l_vendor_site_rec.purchasing_site_flag := 'Y';
            l_vendor_site_rec.pay_site_flag := 'Y';
            l_vendor_site_rec.rfq_only_site_flag := 'N';
            l_vendor_site_rec.org_id := v_orgid;
            l_vendor_site_rec.vendor_id := l_vendor_id;
            l_vendor_site_rec.ext_payee_rec.default_pmt_method := 'EFT'; --sandeep
            l_vendor_site_rec.ALLOW_AWT_FLAG := 'N'; -- Animesh R12TU RT Allow AWT Flag should be set to N
            ap_vendor_pub_pkg.Create_Vendor_Site (
               p_api_version        => l_api_version,
               p_init_msg_list      => FND_API.G_FALSE,
               p_commit             => FND_API.G_FALSE,
               p_validation_level   => FND_API.G_VALID_LEVEL_FULL,
               x_return_status      => l_return_status,
               x_msg_count          => l_msg_count,
               x_msg_data           => l_msg_data,
               p_vendor_site_rec    => l_vendor_site_rec,
               x_vendor_site_id     => l_vendor_site_id,
               x_party_site_id      => l_party_site_id,
               x_location_id        => l_location_id);

            IF l_return_status <> 'S'
            THEN
               IF l_msg_count > 0
               THEN
                  l_msg := NULL;

                  FOR l_index IN 1 .. l_msg_count
                  LOOP
                     fnd_msg_pub.get (p_msg_index       => l_index,
                                      p_encoded         => 'F',
                                      p_data            => l_msg_data,
                                      p_msg_index_out   => l_msg_index_out);
                     l_msg := l_msg || '|' || SUBSTR (l_msg_data, 1, 1000);
                  END LOOP;
               END IF;

               Fnd_File.PUT_LINE (
                  Fnd_File.LOG,
                  'Error occurred while creating Vendor site ' || l_msg);
            ELSE
               Fnd_File.PUT_LINE (Fnd_File.LOG,
                                  'Vendor sitecreated successfully.');

               IF l_location_id IS NOT NULL
               THEN
                  update_address (UPPER (p_address_line_1), --sandeeep Mar 2015
                                  l_location_id,
                                  l_vendor_id,
                                  l_vendor_site_id);
               END IF;
            END IF;
         /* ap_po_vendors_apis_pkg.insert_new_vendor_site
        (p_vendor_site_code          => 'LRP_PARTICIPANT',
         p_vendor_id                 => l_vendor_id,
         p_org_id                    => v_orgid,
         p_address_line1             => UPPER
                                           (p_address_line_1
                                           ),
         p_address_line2             => UPPER
                                           (p_address_line_2
                                           ),
         p_address_line3             => NULL,
         p_address_line4             => NULL,
         p_city                      => UPPER (p_city),
         p_state                     => UPPER
                                             (p_state),
         p_zip                       => UPPER (p_zip),
         p_province                  => NULL,
         p_county                    => NULL,
         p_country                   => UPPER
                                           (v_country_code
                                           ),
         p_area_code                 => NULL,
         p_phone                     => NULL,
         p_fax_area_code             => NULL,
         p_fax                       => NULL,
         p_email_address             => NULL,
         p_purchasing_site_flag      => 'Y',
         p_pay_site_flag             => 'Y',
         p_rfq_only_site_flag        => 'N',
         x_vendor_site_id            => x_main_add_site_id,
         x_status                    => x_status,
         x_exception_msg             => x_msg
        );*/
         EXCEPTION
            WHEN OTHERS
            THEN
               v_exception_site := SUBSTR (SQLERRM, 1, 500);
         END;

         DBMS_OUTPUT.put_line ('no execpetion');
         DBMS_OUTPUT.put_line (
               'VENDOR SITE msg:'
            || l_msg
            || ' VENDOR SITE status BEFORE:'
            || l_return_status);
         DBMS_OUTPUT.put_line (
            'VENDOR SITE id: ' || TO_CHAR (l_vendor_site_id));

         IF l_return_status = 'S'
         THEN
            fnd_file.put_line (
               fnd_file.LOG,
                  'LRP site created for participant -- Vendor_id : '
               || l_vendor_id);
            fnd_file.put_line (fnd_file.LOG,
                               'Vendor Site ID :  ' || l_vendor_site_id);
            fnd_file.put_line (
               fnd_file.output,
               '=========================================================================');
            fnd_file.put_line (
               fnd_file.output,
                  'LRP site successfully created for participant -- tin number '
               || p_tin_num);
            fnd_file.put_line (fnd_file.output,
                               'Vendor ID              : ' || l_vendor_id);
            fnd_file.put_line (
               fnd_file.output,
               'Vendor Site ID         : ' || l_vendor_site_id);
            fnd_file.put_line (
               fnd_file.output,
               '=========================================================================');

            BEGIN
               UPDATE ap_supplier_sites_all
                  SET pay_group_lookup_code = 'LRP',
                      ---sandeep  payment_method_lookup_code = 'EFT',
                      attribute3 = 'MIS',
                      address_lines_alt =
                         UPPER (
                               p_address_line_1
                            || ' '
                            || p_address_line_2
                            || ' '
                            || p_city
                            || ' '
                            || p_state
                            || ' '
                            || p_zip
                            || ' '
                            || p_country),
                      inactive_date = p_end_date,
                      exclusive_payment_flag = 'Y'
                --added 0n 5/4/07  on Munir's request
                WHERE     vendor_id = l_vendor_id
                      AND vendor_site_id = l_vendor_site_id;

               fnd_file.put_line (
                  fnd_file.LOG,
                     'After update of paygroup for Vendor Site ID : '
                  || l_vendor_site_id);
            EXCEPTION
               WHEN OTHERS
               THEN
                  v_exception_site := SQLERRM;

                  UPDATE nihap_lrp_participant_load
                     SET error_message = v_exception_site,
                         vendor_site_inserted = 'N',
                         load_status = 'E',
                         last_update_date = SYSDATE
                   WHERE tin_number = p_tin_num;
            END;

            UPDATE nihap_lrp_participant_load
               SET vendor_site_inserted =
                      'Y' || '  ' || 'vendor site id:  ' || l_vendor_site_id,
                   error_message = NULL,
                   last_update_date = SYSDATE,
                   load_status = 'P'
             WHERE tin_number = p_tin_num;

            DBMS_OUTPUT.put_line (
                  'Post update of staging table setting load = P in '
               || 'nihap_insert_vendor_site');
            COMMIT;
            fnd_file.put_line (
               fnd_file.LOG,
               'Post update of staging table setting load status = P ');
            fnd_file.put_line (
               fnd_file.LOG,
               'Set vendor site inserted to Y for tin number :' || p_tin_num);
            fnd_file.put_line (
               fnd_file.LOG,
               '*******************************************************************************');
         ELSIF NVL (l_return_status, 'E') <> 'S'
         THEN
            fnd_file.put_line (
               fnd_file.LOG,
                  'LRP site not created for participant -- Vendor_id :'
               || l_vendor_id);

            UPDATE nihap_lrp_participant_load
               SET vendor_site_inserted = 'N',
                   load_status = 'E',
                   error_message = v_exception_site || l_msg,
                   last_update_date = SYSDATE
             WHERE tin_number = p_tin_num;

            COMMIT;
            fnd_file.put_line (
               fnd_file.LOG,
                  'Post update of staging table setting vendor_site_inserted to N for vendor ID: '
               || l_vendor_id);
            fnd_file.put_line (fnd_file.LOG,
                               'Exception : ' || v_exception_site || l_msg);
            fnd_file.put_line (
               fnd_file.LOG,
               '***************************************************************************');
         END IF;

         DBMS_OUTPUT.put_line (
               'VENDOR SITE msg:'
            || l_msg
            || ' VENDOR SITE status AFTER UPDATE:'
            || l_return_status);
         l_return_status := NULL;
      END IF;
   EXCEPTION
      WHEN OTHERS
      THEN
         fnd_file.put_line (fnd_file.LOG, 'Exception');
         fnd_file.put_line (
            fnd_file.LOG,
               'site_id: '
            || TO_CHAR (l_vendor_site_id)
            || ' msg:'
            || l_msg
            || ' status:'
            || l_return_status);
         fnd_file.put_line (fnd_file.LOG, SUBSTR (SQLERRM, 1, 500));
   END nihap_insert_vendor_site;

   -- ******************Procedure:nihap_load_lrp_participant **************************************************************
   -- *  This procedure will load participant information in po_vendor and po_vendor_sites_all                            *
   -- *  Once vendor and vendor site is created load status in staging will be set to 'P'                                 *                                                                    *
   -- *                                                                                                                   *
   -- *********************************************************************************************************************
   PROCEDURE nihap_load_lrp_participant
   IS
      --cursor get_participant_info is
      CURSOR stage_cur
      IS
           SELECT vendor_name,
                  vendor_type,
                  tin_number,
                  --  site_name,
                  address_line_1,
                  address_line_2,
                  city,
                  state,
                  zip,
                  country,
                  load_status,
                  vendor_exists,
                  error_message,
                  creation_date,
                  last_update_date,
                  vendor_inserted,
                  vendor_site_inserted,
                  inactive_dt,
                  commons_id,
                  ROWID
             FROM nihap_lrp_participant_load
            WHERE     vendor_exists <> 'E'
                  AND address_line_1 IS NOT NULL                   -- Narender
                  AND tin_number IS NOT NULL                       -- Narender
                  AND country IS NOT NULL                          -- Narender
                  AND (   (    country = 'United States'
                           AND state IS NOT NULL
                           AND city IS NOT NULL
                           AND zip IS NOT NULL)
                       OR (country <> 'United States')) -- Modified by VK CRMOP 0001364633
         ORDER BY vendor_name;

      -- Added by Narender Error out if tin_number is null or address_line_1 is null
      CURSOR check_add_tin
      IS
         SELECT a.ROWID, a.*
           FROM nihap_lrp_participant_load a
          WHERE     TRUNC (a.creation_date) = TRUNC (SYSDATE)
                AND load_status <> 'I';

      v_new_error_message   VARCHAR2 (2000);
      v_user_id             NUMBER;
      v_login_id            NUMBER;
      v_vendor_id           nihpo_vendors.vendor_id%TYPE;
      v_vendor_name         nihpo_vendors.vendor_name%TYPE;
      v_start_dt            nihpo_vendors.start_date_active%TYPE;
      v_end_dt              nihpo_vendors.end_date_active%TYPE;
      v_ssn                 nihpo_vendors.num_1099%TYPE;
      v_exception_message   VARCHAR2 (1000);
      v_end_date_active     VARCHAR2 (100) := NULL;
      v_site_name           VARCHAR2 (15) := 'LRP_PARTICIPANT';
      l_vendor_rec          AP_VENDOR_PUB_PKG.R_VENDOR_REC_TYPE;    --sanndeep
      l_return_status       VARCHAR2 (1);                           --sanndeep
      l_msg_count           NUMBER;                                  --sandeep
      l_msg_data            VARCHAR2 (4000);                         --sandeep
      l_party_id            NUMBER;                                 -- sandeep
      l_vendor_id           NUMBER;                                  --sandeep
      l_msg                 VARCHAR2 (4000);                         --sandeep
      l_msg_index_out       VARCHAR2 (4000);                         --sandeep
      l_api_version         NUMBER := 1.0;                           --sandeep
	       
	  l_vendor_num          VARCHAR2(30);      -- Added for NBSCH0003121
   BEGIN
      --sandeep DBMS_APPLICATION_INFO.set_client_info ('103');
      MO_GLOBAL.SET_POLICY_CONTEXT ('S', FND_PROFILE.VALUE ('ORG_ID')); --sandeep
      fnd_file.put_line (
         fnd_file.output,
         '===================================================3');

      FOR ven_rec IN stage_cur
      LOOP
         fnd_file.put_line (
            fnd_file.output,
            '===================================================4');

         ----insert vendor ---
         IF ven_rec.vendor_exists = 'N' AND ven_rec.vendor_inserted IS NULL
         THEN
            -- 'vendor does not exist
            fnd_file.put_line (
               fnd_file.LOG,
               '********************************************************************************');
            fnd_file.put_line (
               fnd_file.LOG,
                  'Vendor does not exist in the NBS before insert a new vendor  '
               || ven_rec.tin_number);
            fnd_file.put_line (fnd_file.LOG,
                               'Participant Name : ' || ven_rec.vendor_name);
            fnd_file.put_line (fnd_file.LOG,
                               'Tin number       : ' || ven_rec.tin_number);
            DBMS_OUTPUT.put_line (
                  'vendor does not exist before insert a new vendor : '
               || ven_rec.vendor_name
               || ':'
               || ven_rec.tin_number);

            BEGIN
               /*ap_po_vendors_apis_pkg.insert_new_vendor
                              (p_vendor_name                     => UPPER
                                                                       (ven_rec.vendor_name
                                                                       ),
                               -- || ':'
                                -- || ven_rec.tin_number ),
                               p_vendor_type_lookup_code         => 'LOAN REPAYMENT',
                               --'Update to LOAN REPAYMENT' once defined by Munir
                               p_taxpayer_id                     => ven_rec.tin_number,
                               p_tax_registration_id             => NULL,
                               p_women_owned_flag                => NULL,
                               p_small_business_flag             => NULL,
                               p_minority_group_lookup_code      => NULL,
                               p_supplier_number                 => NULL,
                               x_vendor_id                       => l_vendor_id,
                               -- out
                               x_status                          => l_status,
                               --out
                               x_exception_msg                   => l_msg
                              -- out
                              );
               DBMS_OUTPUT.put_line ('SANGEETA no exception');
               DBMS_OUTPUT.put_line (   ' SANGEETA msg:'
                                     || l_msg
                                     || ' status:'
                                     || l_status
                                    );
               DBMS_OUTPUT.put_line (   ' SANGEETA vendor id:'
                                     || TO_CHAR (l_vendor_id)
                                    );*/
               --sandeep Code
               l_vendor_rec := NULL;                                 --sandeep
               l_return_status := NULL;                              --sandeep
               l_msg_count := NULL;                                  --sandeep
               l_msg_data := NULL;                                   --sandeep
               l_party_id := NULL;                                  -- sandeep
               l_vendor_id := NULL;                                  --sandeep
               l_msg := NULL;                                        --sandeep
               l_msg_index_out := NULL;                              --sandeep
               l_vendor_rec.vendor_name := UPPER (ven_rec.vendor_name);
               l_vendor_rec.vendor_type_lookup_code := 'LOAN REPAYMENT'; --UPPER(ven_rec.vendor_type);
               --  l_vendor_rec.minority_group_lookup_code := 'N';
               l_vendor_rec.women_owned_flag := 'N';
               l_vendor_rec.small_business_flag := 'N';
               l_vendor_rec.federal_reportable_flag := 'Y'; -- R12TU Animesh This should be set to Y as in existing production. Verified with Sangeeta
               l_vendor_rec.vendor_name_alt := UPPER (ven_rec.vendor_name);
               l_vendor_rec.auto_calculate_interest_flag := 'N';
               l_vendor_rec.attribute3 := 'MIS';
               l_vendor_rec.always_take_disc_flag := 'N';
               l_vendor_rec.jgzz_fiscal_code := ven_rec.tin_number;
               l_vendor_rec.ext_payee_rec.default_pmt_method := 'EFT'; --sandeep March 2015
               l_vendor_rec.allow_awt_flag := 'Y';        --sandeep March 2015
               l_vendor_rec.attribute9 := ven_rec.commons_id; --Raghu Sep 2021
               fnd_msg_pub.Initialize;
               ap_vendor_pub_pkg.create_vendor (
                  p_api_version        => l_api_version,
                  p_init_msg_list      => FND_API.G_FALSE,
                  p_commit             => FND_API.G_FALSE,
                  p_validation_level   => FND_API.G_VALID_LEVEL_FULL,
                  x_return_status      => l_return_status,
                  x_msg_count          => l_msg_count,
                  x_msg_data           => l_msg_data,
                  p_vendor_rec         => l_vendor_rec,
                  x_vendor_id          => l_vendor_id,
                  x_party_id           => l_party_id);

               IF l_return_status <> 'S'
               THEN
                  IF l_msg_count > 0
                  THEN
                     l_msg := NULL;

                     FOR l_index IN 1 .. l_msg_count
                     LOOP
                        fnd_msg_pub.get (p_msg_index       => l_index,
                                         p_encoded         => 'F',
                                         p_data            => l_msg_data,
                                         p_msg_index_out   => l_msg_index_out);
                        l_msg := l_msg || '|' || SUBSTR (l_msg_data, 1, 100);
                     END LOOP;
                  END IF;

                  Fnd_File.PUT_LINE (
                     Fnd_File.LOG,
                     'Error occurred while creating Vendor ' || l_msg);
               ELSE
                  Fnd_File.PUT_LINE (Fnd_File.LOG,
                                     'Vendor created successfully.');
               END IF;
            EXCEPTION
               WHEN OTHERS
               THEN
                  DBMS_OUTPUT.put_line ('Exception');
                  DBMS_OUTPUT.put_line (
                        'vendor_id: '
                     || TO_CHAR (l_vendor_id)
                     || ' msg:'
                     || l_msg
                     || ' status:'
                     || l_return_status);
                  DBMS_OUTPUT.put_line (SUBSTR (SQLERRM, 1, 500));
                  v_exception_message := (SUBSTR (SQLERRM, 1, 500));
            END;

            IF l_return_status = 'S'
            THEN
               fnd_file.put_line (
                  fnd_file.LOG,
                     'Vendor Successfully inserted for tin number'
                  || ven_rec.tin_number);
               fnd_file.put_line (fnd_file.LOG,
                                  'Vendor ID :  ' || l_vendor_id);
               ---insert _vendor_site
               DBMS_OUTPUT.put_line (
                  ' IN STATUS IF THEN UPDATE:' || l_return_status);

               UPDATE ap_suppliers v
                  SET vendor_name = UPPER (vendor_name || ':' || segment1),
                      num_1099 =
                         DECODE (v.vendor_type_lookup_code,
                                 --sandeep 'EMPLOYEE', NULL,
                                 'NIH_EMPLOYEE', NULL,
                                 ven_rec.tin_number),
                      allow_awt_flag = 'Y' -- R12TU this was added based on defect raised
                WHERE vendor_id = l_vendor_id;
				
				  fnd_file.put_line (
                  fnd_file.LOG,
                  'After PO_Vendors update for vendor ID ' || l_vendor_id);
				
				--NBSCH0003121 changes
				--Calling Vendor Update API
				IF l_vendor_id IS NOT NULL THEN
				
				  SELECT segment1 INTO l_vendor_num
				  FROM  ap_suppliers
				  WHERE vendor_id = l_vendor_id;
				 
					Update_vendor (l_vendor_id,
								   UPPER (ven_rec.vendor_name),
								   l_vendor_num);
				END IF; --+ 

               UPDATE nihap_lrp_participant_load
                  SET vendor_inserted = 'Y' || '   vendor id:' || l_vendor_id
                WHERE tin_number = ven_rec.tin_number;

               fnd_file.put_line (
                  fnd_file.LOG,
                     'After staging table update for tin number'
                  || ven_rec.tin_number);
               ---vendor succesfully inserted  so calling vendor site
               fnd_file.put_line (
                  fnd_file.LOG,
                  'Calling Vendor sites insert API' || ven_rec.tin_number);
               fnd_file.put_line (fnd_file.LOG,
                                  '#1. COUNTRY' || ven_rec.country);
               nihap_insert_vendor_site (
                  l_vendor_id        => l_vendor_id,
                  p_tin_num          => ven_rec.tin_number,
                  p_site_name        => v_site_name,
                  p_address_line_1   => ven_rec.address_line_1,
                  p_address_line_2   => ven_rec.address_line_2,
                  p_city             => ven_rec.city,
                  p_state            => ven_rec.state,
                  p_zip              => ven_rec.zip,
                  p_country          => ven_rec.country,
                  p_end_date         => ven_rec.inactive_dt);
            --l_status := null;
            ELSIF NVL (l_return_status, 'F') <> 'S'
            THEN                                                   -- l_status
               fnd_file.put_line (
                  fnd_file.LOG,
                     'Error while inserting Vendor for tin number '
                  || ven_rec.tin_number);
               fnd_file.put_line (
                  fnd_file.LOG,
                     'Error while inserting Vendor exception message'
                  || v_exception_message);
               fnd_file.put_line (
                  fnd_file.LOG,
                  '********************************************************************************');
               DBMS_OUTPUT.put_line ('Error messge' || l_msg);

               UPDATE nihap_lrp_participant_load
                  SET vendor_inserted = NULL,
                      load_status = 'E',
                      error_message = v_exception_message || l_msg,
                      last_update_date = SYSDATE
                WHERE tin_number = ven_rec.tin_number;
            END IF;
         ELSIF    (    ven_rec.vendor_exists = 'Y'
                   AND ven_rec.vendor_inserted IS NULL)
               OR (ven_rec.vendor_site_inserted = 'N') -- Vendor Exists (Added 04/24)
         THEN
            fnd_file.put_line (
               fnd_file.LOG,
               '*******************************************************************************');
            fnd_file.put_line (fnd_file.LOG,
                               'Vendor Record already exists in NBS');
            fnd_file.put_line (fnd_file.LOG,
                               'Participant Name : ' || ven_rec.vendor_name);
            fnd_file.put_line (fnd_file.LOG,
                               'Tin number       : ' || ven_rec.tin_number);

            BEGIN
               v_end_date_active := NULL;

               -- PO_VENDORS may have multiple vendor records.
               SELECT pv.vendor_id,
                      pv.vendor_name,
                      pv.start_date_active,
                      pv.end_date_active
                 INTO v_vendor_id,
                      v_vendor_name,
                      v_start_dt,
                      v_end_dt
                 FROM nihpo_vendors pv
                WHERE     pv.num_1099 = ven_rec.tin_number
                      AND NVL (pv.end_date_active, SYSDATE + 1) > SYSDATE;
            EXCEPTION
               WHEN NO_DATA_FOUND
               THEN
                  fnd_file.put_line (
                     fnd_file.LOG,
                     '******************************************************');
                  fnd_file.put_line (
                     fnd_file.LOG,
                     ' Vendor record does not exist in PO vendors');
                  fnd_file.put_line (
                     fnd_file.LOG,
                     ' Vendor Name : ' || ven_rec.vendor_name);
                  fnd_file.put_line (fnd_file.LOG,
                                     ' Vendor SSN  : ' || ven_rec.tin_number);

                  UPDATE nihap_lrp_participant_load
                     SET load_status = 'E',
                         last_update_date = SYSDATE,
                         error_message =
                            'Vendor record does not exist in PO vendors'
                   WHERE tin_number = ven_rec.tin_number;

                  fnd_file.put_line (
                     fnd_file.LOG,
                     ' After update of staging table set vendor_exists to N ');
                  fnd_file.put_line (
                     fnd_file.LOG,
                     '******************************************************');
               WHEN OTHERS
               THEN
                  BEGIN
                     SELECT pv.vendor_id,
                            pv.vendor_name,
                            pv.num_1099,
                            pv.start_date_active,
                            pv.end_date_active
                       INTO v_vendor_id,
                            v_vendor_name,
                            v_ssn,
                            v_start_dt,
                            v_end_dt
                       FROM nihpo_vendors pv, ap_supplier_sites_all pvs
                      WHERE     pv.num_1099 = ven_rec.tin_number
                            AND NVL (pv.end_date_active, SYSDATE + 1) >
                                   SYSDATE
                            AND pv.vendor_id = pvs.vendor_id -- Added by VK CRMOP 0001731556, Check if LRP site exists --
                            AND pvs.vendor_site_code = 'LRP_PARTICIPANT' -- Added by VK CRMOP 0001731556, Check if LRP site exists --
                            AND NVL (pvs.inactive_date, SYSDATE + 1) >
                                   SYSDATE;

                     fnd_file.put_line (
                        fnd_file.LOG,
                           'LRP PARTICIPANT Site exists for Vendor SSN  : '
                        || v_ssn);
                  EXCEPTION
                     WHEN NO_DATA_FOUND
                     THEN
                        fnd_file.put_line (
                           fnd_file.LOG,
                              'LRP PARTICIPANT Site does not exists for Vendor TIN  : '
                           || ven_rec.tin_number);
                        v_end_dt := SYSDATE + 1;
                     WHEN OTHERS
                     THEN
                        fnd_file.put_line (
                           fnd_file.LOG,
                           'Issue in getting End Date Active');
                        v_end_date_active := 'N';

                        -- Setting to N will not process rec until issue is resolved.
                        UPDATE nihap_lrp_participant_load
                           SET load_status = 'E',
                               error_message =
                                  'Issue in getting End Date Active',
                               last_update_date = SYSDATE
                         WHERE tin_number = ven_rec.tin_number;
                  END;
            -- PO_VENDORS may have multiple vendor records.
            END;

            IF v_end_date_active = 'N'
            THEN
               fnd_file.put_line (fnd_file.LOG,
                                  'Issue in getting End Date Active');
            ELSIF NVL (v_end_dt, SYSDATE + 1) > SYSDATE
            THEN                          -- insert new vendor-site for vendor
               fnd_file.put_line (fnd_file.LOG, 'Vendor is Active');
               fnd_file.put_line (
                  fnd_file.LOG,
                  'Calling Vendor Sites API to create new LRP site');
               fnd_file.put_line (fnd_file.LOG,
                                  'Partcipant Name:' || ven_rec.vendor_name);
               fnd_file.put_line (fnd_file.LOG,
                                  'Vendor ID      :' || v_vendor_id);

               ---Update po_vendors table for existing vendors add update statement
               UPDATE ap_suppliers v
                  SET vendor_name_alt =
                         UPPER (
                            SUBSTR (v_vendor_name,
                                    1,
                                    INSTR (v_vendor_name, ':') - 1)), --UPPER (ven_rec.vendor_name),
                      -- auto_calculate_interest_flag = 'N', --intrest invoices -- 5/3/07 Munir said not to update for existing vendors for now.
                      federal_reportable_flag = 'Y',         -- --1099 vendors
                      always_take_disc_flag = 'N',    --'always take discount'
                      allow_awt_flag = 'Y', -- R12TU this was added based on the defect raised.
                      last_update_date = SYSDATE,
                      last_updated_by = fnd_global.user_id
                WHERE     EXISTS
                             (SELECT 1
                                FROM ap_supplier_sites_all vs
                               WHERE     v.vendor_id = vs.vendor_id
                                     AND NVL (vs.inactive_date, SYSDATE + 1) >
                                            SYSDATE
                                     AND vs.vendor_site_code =
                                            'LRP_PARTICIPANT') -- Added by VK CRMOP 0001364633
                      AND (   (    num_1099 = ven_rec.tin_number
                               AND v.vendor_type_lookup_code <>
                                      'NIH_EMPLOYEE'
                               AND NVL (v.end_date_active, SYSDATE + 1) >
                                      SYSDATE)
                           OR (    v.vendor_type_lookup_code = 'NIH_EMPLOYEE'
                               AND NVL (v.end_date_active, SYSDATE + 1) >
                                      SYSDATE
                               AND v.vendor_id IN
                                      (SELECT v1.vendor_id
                                         FROM ap_suppliers v1,
                                              per_all_people_f ppf1
                                        WHERE     REPLACE (
                                                     ppf1.national_identifier,
                                                     '-') =
                                                     ven_rec.tin_number
                                              AND ppf1.effective_end_date =
                                                     (SELECT MAX (
                                                                ppf2.effective_end_date)
                                                        FROM per_all_people_f ppf2
                                                       WHERE ppf2.person_id =
                                                                ppf1.person_id)
                                              AND v1.attribute15 =
                                                     ppf1.person_id
                                              AND v1.vendor_type_lookup_code =
                                                     'NIH_EMPLOYEE')))
                      --R12TU Animesh Added this condition not to do the above updates if
                      -- all the values we are updating is same as exists
                      AND (   NVL (vendor_name_alt, 'XX') <>
                                 UPPER (
                                    SUBSTR (v_vendor_name,
                                            1,
                                            INSTR (v_vendor_name, ':') - 1))
                           OR NVL (federal_reportable_flag, 'X') <> 'Y'
                           OR NVL (always_take_disc_flag, 'X') <> 'N'
                           OR NVL (allow_awt_flag, 'X') <> 'Y');

               -- call   nihap_insert_vendor_site to create /update site
               fnd_file.put_line (fnd_file.LOG,
                                  '#2. COUNTRY: ' || ven_rec.country);
               nihap_insert_vendor_site (
                  l_vendor_id        => v_vendor_id,
                  p_tin_num          => ven_rec.tin_number,
                  p_site_name        => v_site_name,                   --NULL,
                  p_address_line_1   => ven_rec.address_line_1,
                  p_address_line_2   => ven_rec.address_line_2,
                  p_city             => ven_rec.city,
                  p_state            => ven_rec.state,
                  p_zip              => ven_rec.zip,
                  p_country          => ven_rec.country,
                  p_end_date         => ven_rec.inactive_dt                 --
                                                           );
            ELSE                                                     -- End Dt
               fnd_file.put_line (fnd_file.LOG,
                                  'Vendor exists and is inactive');
               fnd_file.put_line (fnd_file.LOG, 'Vendor_id' || v_vendor_id);
               fnd_file.put_line (fnd_file.LOG,
                                  'Vendor_name:' || v_vendor_name);
               fnd_file.put_line (fnd_file.LOG,
                                  'Start_date_active:' || v_start_dt);
               fnd_file.put_line (fnd_file.LOG,
                                  'End_date_active:' || v_end_dt);
               fnd_file.put_line (fnd_file.LOG,
                                  'Tin Number' || ven_rec.tin_number);
               DBMS_OUTPUT.put_line (
                  'Vendor exists in Oracle and is inactive');
               DBMS_OUTPUT.put_line ('Vendor_id' || v_vendor_id);
               DBMS_OUTPUT.put_line ('Vendor_name:' || v_vendor_name);
               DBMS_OUTPUT.put_line ('Start_date_active:' || v_start_dt);
               DBMS_OUTPUT.put_line ('End_date_active:' || v_end_dt);
               DBMS_OUTPUT.put_line ('Tin Number' || ven_rec.tin_number);
               fnd_file.put_line (
                  fnd_file.LOG,
                  '********************************************************************************');
            --
            END IF;
         END IF;
      END LOOP;

      --
      -- Added by Narender Error out if tin_number is null or address_line_1 is null
      FOR parti_rec IN check_add_tin
      LOOP
         -- count1 := count1+1;
         --exit when count1 = 101;
         BEGIN
            IF parti_rec.tin_number IS NULL
            THEN
               UPDATE nihap_lrp_participant_load
                  SET load_status = 'E',
                      error_message =
                         'There is no Tin number for this Participant record',
                      last_update_date = SYSDATE
                WHERE ROWID = parti_rec.ROWID;
            END IF;

            IF parti_rec.address_line_1 IS NULL
            THEN
               UPDATE nihap_lrp_participant_load
                  SET load_status = 'E',
                      error_message =
                         'Address line1 is null for this Participant record',
                      last_update_date = SYSDATE
                WHERE ROWID = parti_rec.ROWID;
            END IF;

            IF     parti_rec.state IS NULL
               AND parti_rec.country = 'United States' -- Added by VK CRMOP 0001364633
            THEN
               UPDATE nihap_lrp_participant_load
                  SET load_status = 'E',
                      error_message =
                         'State is null for this Participant record',
                      last_update_date = SYSDATE
                WHERE ROWID = parti_rec.ROWID;
            END IF;

            IF parti_rec.city IS NULL AND parti_rec.country = 'United States' -- Added by VK CRMOP 0001364633
            THEN
               UPDATE nihap_lrp_participant_load
                  SET load_status = 'E',
                      error_message =
                         'City is null for this Participant record',
                      last_update_date = SYSDATE
                WHERE ROWID = parti_rec.ROWID;
            END IF;

            IF parti_rec.zip IS NULL AND parti_rec.country = 'United States' -- Added by VK CRMOP 0001364633
            THEN
               UPDATE nihap_lrp_participant_load
                  SET load_status = 'E',
                      error_message =
                         'Zip is null for this Participant record',
                      last_update_date = SYSDATE
                WHERE ROWID = parti_rec.ROWID;
            END IF;

            IF parti_rec.country IS NULL
            THEN
               UPDATE nihap_lrp_participant_load
                  SET load_status = 'E',
                      error_message =
                         'Country is null for this Participant record',
                      last_update_date = SYSDATE
                WHERE ROWID = parti_rec.ROWID;
            END IF;
         END;
      END LOOP;
   --
   EXCEPTION
      WHEN OTHERS
      THEN
         DBMS_OUTPUT.put_line ('Exception');
         DBMS_OUTPUT.put_line (
               'vendor_id: '
            || TO_CHAR (l_vendor_id)
            || ' msg:'
            || l_msg
            || ' status:'
            || l_return_status);
         DBMS_OUTPUT.put_line (SUBSTR (SQLERRM, 1, 500));
   END nihap_load_lrp_participant;

   PROCEDURE nihap_lrp_lender_load_main (errbuf    OUT VARCHAR2,
                                         retcode   OUT NUMBER)
   IS
      CURSOR error_rec
      IS
         SELECT tin_number,
                vendor_name,
                site_name,
                error_message,
                address_line_1,
                address_line_2,
                city,
                state,
                zip,
                country
           FROM nihap_lrp_lender_load
          WHERE load_status = 'E';

   BEGIN
      UPDATE nihap_lrp_lender_load
         SET country = 'United States'
       WHERE country = 'United States of America';

      COMMIT;
      --nihap_check_if_lender_ven_exists;
      nihap_load_lrp_lender;

      FOR e_rec IN error_rec
      LOOP
         fnd_file.put_line (
            fnd_file.output,
            '===================================================');
         fnd_file.put_line (fnd_file.output, 'Error Record::');
         fnd_file.put_line (fnd_file.output,
                            'Vendor Name   : ' || e_rec.vendor_name);
         fnd_file.put_line (fnd_file.output,
                            'TIN Number    : ' || e_rec.tin_number);
         fnd_file.put_line (fnd_file.output,
                            'Site Name        : ' || e_rec.site_name);
         fnd_file.put_line (fnd_file.output,
                            'Address Line1 : ' || e_rec.address_line_1);
         fnd_file.put_line (fnd_file.output,
                            'Address Line2 : ' || e_rec.address_line_2);
         fnd_file.put_line (fnd_file.output,
                            'City              : ' || e_rec.city);
         fnd_file.put_line (fnd_file.output,
                            'State            : ' || e_rec.state);
         fnd_file.put_line (fnd_file.output,
                            'Zip            : ' || e_rec.zip);
         fnd_file.put_line (fnd_file.output,
                            'Country        : ' || e_rec.country);
         fnd_file.put_line (fnd_file.output,
                            'Error Message    : ' || e_rec.error_message);
         fnd_file.put_line (
            fnd_file.output,
            '====================================================');
      END LOOP;
   EXCEPTION
      WHEN OTHERS
      THEN
         retcode := -1;
         errbuf := SQLERRM;
         DBMS_OUTPUT.put_line ('nihap_lrp_load_main error:' || SQLERRM);
         fnd_file.put_line (fnd_file.LOG,
                            ' nihap_lrp_load_main error: ' || SQLERRM);
         fnd_file.put_line (fnd_file.LOG, ' and  : ' || SQLCODE);
   END nihap_lrp_lender_load_main;

   PROCEDURE nihap_chk_if_lender_ven_exists (errbuf    OUT VARCHAR2,
                                             retcode   OUT NUMBER)
   IS
      -- Check against po vendors if vendor exists in NBS
      CURSOR staging_cur
      IS
           SELECT DISTINCT vendor_name, tin_number
             FROM nihap_lrp_lender_load
            WHERE vendor_exists IS NULL OR vendor_exists = 'E' -- original code
         -- vendor_exists is null OR vendor_exists ='E' -- original code
         ORDER BY vendor_name;

      /*  SELECT     participant_name, tin_number
        FROM      nihap_lrp_lender_load
        WHERE     vendor_exists is  null
    AND rownum < 6
    ORDER BY 1; */
      --Use this for testing
      /*  SELECT vendor_name, tin_number, ROWNUM
         FROM (SELECT   vendor_name, tin_number
                   FROM nihap_lrp_lender_load
               -- WHERE     vendor_exists is  null
               ORDER BY vendor_name)
        WHERE ROWNUM < 6;   */
      -- inactive vendors only
      CURSOR check_inactive_vendor (
         p_tin VARCHAR2)
      IS
         SELECT vendor_id,
                vendor_name,
                num_1099,
                start_date_active,
                end_date_active,
                v.vendor_type_lookup_code
           FROM nihpo_vendors v
          WHERE     v.num_1099 = p_tin
                AND NOT EXISTS
                           (SELECT NULL
                              FROM nihpo_vendors v_act
                             WHERE     v_act.num_1099 = p_tin
                                   AND v_act.num_1099 = v.num_1099
                                   AND NVL (v_act.end_date_active,
                                            SYSDATE + 1) > SYSDATE);

      CURSOR check_vendor (
         p_tin VARCHAR2)
      IS
         SELECT vendor_id,
                vendor_name,
                num_1099,
                start_date_active,
                end_date_active
           FROM nihpo_vendors
          WHERE     num_1099 = p_tin
                AND NVL (end_date_active, SYSDATE + 1) > SYSDATE;

      x_ven_name             nihpo_vendors.vendor_name%TYPE := NULL;
      x_ssn                  nihpo_vendors.num_1099%TYPE := NULL;
      x_vendor_id            nihpo_vendors.vendor_id%TYPE := NULL;
      x_st_dt_active         nihpo_vendors.start_date_active%TYPE := NULL;
      x_end_dt_active        nihpo_vendors.end_date_active%TYPE := NULL;
      x_vendor_lookup_code   nihpo_vendors.vendor_type_lookup_code%TYPE
                                := NULL;
   BEGIN
      --DBMS_APPLICATION_INFO.set_client_info ('103');--sandeep commented
      MO_GLOBAL.SET_POLICY_CONTEXT ('S', 103);                 --sandeep added

      FOR prec IN staging_cur
      LOOP
         fnd_file.put_line (fnd_file.LOG, 'Begin Loop' || prec.tin_number);
         x_vendor_id := NULL;
         x_ven_name := NULL;
         x_ssn := NULL;
         x_st_dt_active := NULL;
         x_end_dt_active := NULL;

         --dbms_output.put_line ('vendor name ...'|| prec.participant_name ||':'|| prec.tin_number);
         OPEN check_inactive_vendor (prec.tin_number);

         FETCH check_inactive_vendor
            INTO x_vendor_id,
                 x_ven_name,
                 x_ssn,
                 x_st_dt_active,
                 x_end_dt_active,
                 x_vendor_lookup_code;

         IF check_inactive_vendor%FOUND
         THEN
            DBMS_OUTPUT.put_line (
               'Vendor record exists in PO vendors but is inactive');
            fnd_file.put_line (
               fnd_file.output,
               '===================================================');
            fnd_file.put_line (
               fnd_file.output,
               'Vendor record exists in PO vendors but is inactive ');
            fnd_file.put_line (fnd_file.output,
                               'Vendor ID         : ' || x_vendor_id);
            fnd_file.put_line (fnd_file.output,
                               'Vendor Name       : ' || x_ven_name);
            fnd_file.put_line (fnd_file.output,
                               'SSN                 : ' || x_ssn);
            fnd_file.put_line (fnd_file.output,
                               'Start Date Active : ' || x_st_dt_active);
            fnd_file.put_line (fnd_file.output,
                               'End Date Active   : ' || x_end_dt_active);
            fnd_file.put_line (
               fnd_file.output,
               'Vendor Lookup Code: ' || x_vendor_lookup_code);
            fnd_file.put_line (
               fnd_file.output,
               '====================================================');

            UPDATE nihap_lrp_lender_load
               SET vendor_exists = 'E',
                   last_update_date = SYSDATE,
                   error_message =
                      'Vendor Record exists in PO vendors but is inactive'
             WHERE tin_number = prec.tin_number;
         ELSE
            BEGIN
               SELECT vendor_id,
                      vendor_name,
                      num_1099,
                      start_date_active,
                      end_date_active
                 INTO x_vendor_id,
                      x_ven_name,
                      x_ssn,
                      x_st_dt_active,
                      x_end_dt_active
                 FROM nihpo_vendors
                WHERE     num_1099 = prec.tin_number
                      AND NVL (end_date_active, SYSDATE + 1) > SYSDATE;

               DBMS_OUTPUT.put_line ('Vendor record exists in PO vendors');
               fnd_file.put_line (
                  fnd_file.LOG,
                  '******************************************************');
               fnd_file.put_line (fnd_file.LOG,
                                  ' Vendor record exists in PO vendors');
               fnd_file.put_line (fnd_file.LOG,
                                  ' Vendor Name : ' || x_ven_name);
               fnd_file.put_line (fnd_file.LOG, ' Vendor SSN  : ' || x_ssn);

               UPDATE nihap_lrp_lender_load
                  SET vendor_exists = 'Y',                   --|| x_vendor_id,
                      last_update_date = SYSDATE,
                      error_message = NULL
                WHERE tin_number = prec.tin_number;

               fnd_file.put_line (
                  fnd_file.LOG,
                  ' After update of staging table set vendor_exists to Y  ');
               fnd_file.put_line (
                  fnd_file.LOG,
                  '******************************************************');
            EXCEPTION
               WHEN NO_DATA_FOUND
               THEN
                  DBMS_OUTPUT.put_line (
                     'Vendor record does not exist in PO vendors');
                  fnd_file.put_line (
                     fnd_file.LOG,
                     '******************************************************');
                  fnd_file.put_line (
                     fnd_file.LOG,
                     ' Vendor record does not exist in PO vendors');
                  fnd_file.put_line (fnd_file.LOG,
                                     ' Vendor Name : ' || prec.vendor_name);
                  fnd_file.put_line (fnd_file.LOG,
                                     ' Vendor SSN  : ' || prec.tin_number);

                  UPDATE nihap_lrp_lender_load
                     SET vendor_exists = 'N',
                         last_update_date = SYSDATE,
                         error_message = NULL
                   WHERE tin_number = prec.tin_number;

                  fnd_file.put_line (
                     fnd_file.LOG,
                     ' After update of staging table set vendor_exists to N ');
                  fnd_file.put_line (
                     fnd_file.LOG,
                     '******************************************************');
            END;
         END IF;                                      -- Check Inactive Vendor

         CLOSE check_inactive_vendor;

         COMMIT;
      END LOOP;
   EXCEPTION
      WHEN OTHERS
      THEN
         fnd_file.put_line (
            fnd_file.LOG,
               'Error while updating staging table nihap_check_if_vendor_exists procedure  : '
            || SQLERRM);
         fnd_file.put_line (fnd_file.LOG, ' and the error code : ');
         fnd_file.put_line (fnd_file.LOG, SQLCODE);
   END;

   PROCEDURE nihap_insert_lender_ven_site (
      l_vendor_id      IN VARCHAR2 DEFAULT NULL,
      v_tin_num        IN VARCHAR2 DEFAULT NULL,
      v_site_name      IN VARCHAR2 DEFAULT NULL,
      address_line_1   IN VARCHAR2 DEFAULT NULL,
      address_line_2   IN VARCHAR2 DEFAULT NULL,
      city             IN VARCHAR2 DEFAULT NULL,
      state            IN VARCHAR2 DEFAULT NULL,
      zip              IN VARCHAR2 DEFAULT NULL,
      country          IN VARCHAR2 DEFAULT NULL,
      end_date         IN DATE DEFAULT NULL)
   IS
      --   x_main_add_site_id   NUMBER;                                      --out
      -- x_status             VARCHAR2 (30);                             ----out
      --  x_msg                VARCHAR2 (430);                             ---out
      v_orgid             NUMBER := fnd_profile.VALUE ('org_id');
      v_exception_site    VARCHAR2 (1000);
      v_site_count        NUMBER;
      v_country_code      fnd_territories_vl.territory_code%TYPE;
      l_vendor_site_rec   ap_vendor_pub_pkg.r_vendor_site_rec_type;
      l_party_site_id     NUMBER;
      l_location_id       NUMBER;
      l_msg_count         NUMBER;
      l_msg_data          VARCHAR2 (2000);
      l_api_version       NUMBER := 1.0;
      l_return_status     VARCHAR2 (1);
      l_msg               VARCHAR2 (4000);
      l_vendor_site_id    NUMBER;
      l_msg_index_out     VARCHAR2 (4000);
   BEGIN
      DBMS_OUTPUT.put_line ('vendor id : ' || l_vendor_id);
      DBMS_OUTPUT.put_line ('num_1099 : ' || v_tin_num);
      fnd_file.put_line (fnd_file.LOG, 'In Vendor sites Insert procedure');
      fnd_file.put_line (fnd_file.LOG, 'Vendor ID :' || l_vendor_id);
      fnd_file.put_line (fnd_file.LOG, 'Tin Number: ' || v_tin_num);
      l_return_status := NULL;
      l_msg := NULL;
      --sandeep DBMS_APPLICATION_INFO.set_client_info ('103');
      MO_GLOBAL.SET_POLICY_CONTEXT ('S', 103);                       --sandeep

      --- check if LRP site exists
      SELECT COUNT (*)
        INTO v_site_count
        FROM ap_supplier_sites_all                                   --sandeep
       WHERE vendor_id = l_vendor_id AND vendor_site_code = v_site_name;

      -- 'LRP_PARTICIPANT'; --REVIEW
      IF v_site_count = 1
      THEN
         fnd_file.put_line (fnd_file.LOG,
                            'LRP Site exsists for Lender: ' || l_vendor_id);
         fnd_file.put_line (
            fnd_file.LOG,
            'Updating PO_vendor_sites_all with Address for Lender');
         l_vendor_site_rec := NULL;                                  --sandeep
         l_party_site_id := NULL;                                    --sandeep
         l_location_id := NULL;                                      --sandeep
         l_msg_count := NULL;                                        --sandeep
         l_msg_data := NULL;                                         --sandeep
         l_vendor_site_rec.vendor_site_code := v_site_name;
         l_vendor_site_rec.vendor_id := l_vendor_id;
         l_vendor_site_rec.address_line1 := UPPER (address_line_1);
         l_vendor_site_rec.address_line2 := UPPER (address_line_2);
         l_vendor_site_rec.address_lines_alt :=
            UPPER (
                  address_line_1
               || ' '
               || address_line_2
               || ' '
               || city
               || ' '
               || state
               || ' '
               || zip
               || ' '
               || country);
         l_vendor_site_rec.city := UPPER (city);
         l_vendor_site_rec.state := UPPER (state);
         l_vendor_site_rec.zip := UPPER (zip);
         l_vendor_site_rec.country := UPPER (v_country_code);
         l_vendor_site_rec.inactive_date := end_date;
         l_vendor_site_rec.last_update_date := SYSDATE;
         l_vendor_site_rec.last_updated_by := fnd_global.user_id;
         --  l_vendor_site_rec.exclusive_payment_flag := 'Y';
         ap_vendor_pub_pkg.Update_Vendor_Site (
            p_api_version        => l_api_version,
            p_init_msg_list      => FND_API.G_FALSE,
            p_commit             => FND_API.G_FALSE,
            p_validation_level   => FND_API.G_VALID_LEVEL_FULL,
            x_return_status      => l_return_status,
            x_msg_count          => l_msg_count,
            x_msg_data           => l_msg_data,
            p_vendor_site_rec    => l_vendor_site_rec,
            p_vendor_site_id     => l_vendor_site_id,
            p_calling_prog       => 'NOT ISETUP');

         IF l_return_status <> 'S'
         THEN
            IF l_msg_count > 0
            THEN
               l_msg := NULL;

               FOR l_index IN 1 .. l_msg_count
               LOOP
                  fnd_msg_pub.get (p_msg_index       => l_index,
                                   p_encoded         => 'F',
                                   p_data            => l_msg_data,
                                   p_msg_index_out   => l_msg_index_out);
                  l_msg := l_msg || '|' || SUBSTR (l_msg_data, 1, 100);
               END LOOP;
            END IF;

            Fnd_File.PUT_LINE (
               Fnd_File.LOG,
               'Error occurred while Updating Vendor Site' || l_msg);
         ELSE
            Fnd_File.PUT_LINE (Fnd_File.LOG,
                               'Vendor Site Updated successfully.');
         END IF;

         fnd_file.put_line (
            fnd_file.LOG,
            'After Update of PO_vendor_sites_all with Address for Lender');

         UPDATE nihap_lrp_lender_load
            SET vendor_site_inserted = 'Y',
                error_message = NULL,
                last_update_date = SYSDATE,
                load_status = 'P'
          WHERE tin_number = v_tin_num AND site_name = v_site_name;

         fnd_file.put_line (fnd_file.LOG,
                            'Updating Staging table; Record Processed');
      ELSE                                                     -- v_site_count
         fnd_file.put_line (
            fnd_file.LOG,
            'LRP Site does not exsist for Lender: ' || l_vendor_id);
         fnd_file.put_line (fnd_file.LOG,
                            'Calling API to create LRP site for Lender');

         BEGIN
            SELECT territory_code
              INTO v_country_code
              FROM fnd_territories_vl
             WHERE territory_short_name = INITCAP (RTRIM (LTRIM (country)));
         EXCEPTION
            WHEN OTHERS
            THEN
               v_country_code := NULL;
               fnd_file.put_line (
                  fnd_file.LOG,
                     'LRP Country Code doesnt exists for Vendor ID: '
                  || l_vendor_id);
         END;

         BEGIN
            l_vendor_site_rec := NULL;                               --sandeep
            l_party_site_id := NULL;                                 --sandeep
            l_location_id := NULL;                                   --sandeep
            l_msg_count := NULL;                                     --sandeep
            l_msg_data := NULL;                                      --sandeep
            l_vendor_site_rec.vendor_site_code := v_site_name;
            l_vendor_site_rec.address_line1 := UPPER (address_line_1);
            l_vendor_site_rec.address_line2 := UPPER (address_line_2);
            l_vendor_site_rec.city := UPPER (city);
            l_vendor_site_rec.state := UPPER (state);
            l_vendor_site_rec.zip := UPPER (zip);
            l_vendor_site_rec.country := UPPER (v_country_code);
            l_vendor_site_rec.purchasing_site_flag := 'Y';
            l_vendor_site_rec.pay_site_flag := 'Y';
            l_vendor_site_rec.org_id := v_orgid;
            l_vendor_site_rec.vendor_id := l_vendor_id;
            ap_vendor_pub_pkg.Create_Vendor_Site (
               p_api_version        => l_api_version,
               p_init_msg_list      => FND_API.G_FALSE,
               p_commit             => FND_API.G_FALSE,
               p_validation_level   => FND_API.G_VALID_LEVEL_FULL,
               x_return_status      => l_return_status,
               x_msg_count          => l_msg_count,
               x_msg_data           => l_msg_data,
               p_vendor_site_rec    => l_vendor_site_rec,
               x_vendor_site_id     => l_vendor_site_id,
               x_party_site_id      => l_party_site_id,
               x_location_id        => l_location_id);

            IF l_return_status <> 'S'
            THEN
               IF l_msg_count > 0
               THEN
                  l_msg := NULL;

                  FOR l_index IN 1 .. l_msg_count
                  LOOP
                     fnd_msg_pub.get (p_msg_index       => l_index,
                                      p_encoded         => 'F',
                                      p_data            => l_msg_data,
                                      p_msg_index_out   => l_msg_index_out);
                     l_msg := l_msg || '|' || SUBSTR (l_msg_data, 1, 100);
                  END LOOP;
               END IF;

               Fnd_File.PUT_LINE (
                  Fnd_File.LOG,
                  'Error occurred while creating Vendor site ' || l_msg);
            ELSE
               Fnd_File.PUT_LINE (Fnd_File.LOG,
                                  'Vendor sitecreated successfully.');
            END IF;
         /*ap_po_vendors_apis_pkg.insert_new_vendor_site
                                (p_vendor_site_code          => v_site_name,
                                 p_vendor_id                 => l_vendor_id,
                                 p_org_id                    => v_orgid,
                                 p_address_line1             => UPPER
                                                                   (address_line_1
                                                                   ),
                                 p_address_line2             => UPPER
                                                                   (address_line_2
                                                                   ),
                                 p_address_line3             => NULL,
                                 p_address_line4             => NULL,
                                 p_city                      => UPPER (city),
                                 p_state                     => UPPER
                                                                     (state),
                                 p_zip                       => UPPER (zip),
                                 p_province                  => NULL,
                                 p_county                    => NULL,
                                 p_country                   => UPPER
                                                                   (v_country_code
                                                                   ),
                                 -- country,
                                 p_area_code                 => NULL,
                                 p_phone                     => NULL,
                                 p_fax_area_code             => NULL,
                                 p_fax                       => NULL,
                                 p_email_address             => NULL,
                                 p_purchasing_site_flag      => 'Y',
                                 p_pay_site_flag             => 'Y',
                                 p_rfq_only_site_flag        => 'N',
                                 x_vendor_site_id            => x_main_add_site_id,
                                 x_status                    => x_status,
                                 x_exception_msg             => x_msg
                                );*/
         EXCEPTION
            WHEN OTHERS
            THEN
               v_exception_site := SUBSTR (SQLERRM, 1, 500);
         END;

         DBMS_OUTPUT.put_line ('no execpetion');
         DBMS_OUTPUT.put_line (
               'VENDOR SITE msg:'
            || l_msg
            || ' VENDOR SITE status BEFORE:'
            || l_return_status);
         DBMS_OUTPUT.put_line (
            'VENDOR SITE id: ' || TO_CHAR (l_vendor_site_id));

         IF NVL (l_return_status, 'F') = 'S'
         THEN
            fnd_file.put_line (
               fnd_file.LOG,
               'LRP site created for Lender -- Vendor_id : ' || l_vendor_id);
            fnd_file.put_line (fnd_file.LOG,
                               'Vendor Site ID :  ' || l_vendor_site_id);
            fnd_file.put_line (
               fnd_file.output,
               '=========================================================================');
            fnd_file.put_line (
               fnd_file.output,
                  'LRP site successfully created for Lender -- tin number '
               || v_tin_num);
            fnd_file.put_line (fnd_file.output,
                               'Vendor ID              : ' || l_vendor_id);
            fnd_file.put_line (
               fnd_file.output,
               'Vendor Site ID         : ' || l_vendor_site_id);
            fnd_file.put_line (fnd_file.output,
                               'Vendor Site Name :  ' || v_site_name);
            fnd_file.put_line (
               fnd_file.output,
               '=========================================================================');

            UPDATE nihap_lrp_lender_load
               SET vendor_site_inserted =
                      'Y' || '  ' || 'vendor site id:  ' || l_vendor_site_id,
                   error_message = NULL,
                   last_update_date = SYSDATE,
                   load_status = 'P'
             WHERE tin_number = v_tin_num AND site_name = v_site_name;

            DBMS_OUTPUT.put_line (
                  'Post update of staging table setting load = P in '
               || 'VENDOR SITE PROCEDURE');
            fnd_file.put_line (
               fnd_file.LOG,
                  'Post update of staging table setting load = P in '
               || 'nihap_insert_vendor_site: '
               || l_vendor_site_id);
            COMMIT;
            fnd_file.put_line (
               fnd_file.LOG,
               'Post update of staging table setting load status = P ');
            fnd_file.put_line (
               fnd_file.LOG,
               'Set vendor site inserted to Y for tin number :' || v_tin_num);
            fnd_file.put_line (
               fnd_file.LOG,
               '*******************************************************************************');

            BEGIN
               UPDATE ap_supplier_sites_all
                  SET pay_group_lookup_code = 'LRP',
                      payment_method_lookup_code = 'EFT',
                      attribute3 = 'MIS',
                      address_lines_alt =
                         address_line_1 || ' ' || address_line_2,
                      inactive_date = end_date,
                      exclusive_payment_flag = 'Y'
                -- Added 5/4/07 on Munir's request
                WHERE     vendor_id = l_vendor_id
                      AND vendor_site_id = l_vendor_site_id;

               fnd_file.put_line (
                  fnd_file.LOG,
                     ' After update of paygroup for Vendor Site ID  '
                  || l_vendor_site_id);
            EXCEPTION
               WHEN OTHERS
               THEN
                  v_exception_site := SQLERRM;

                  UPDATE nihap_lrp_lender_load
                     SET error_message = v_exception_site,
                         vendor_site_inserted = 'N',
                         load_status = 'E'
                   WHERE tin_number = v_tin_num AND site_name = site_name;
            END;
         ELSIF NVL (l_return_status, 'E') <> 'S'
         THEN
            fnd_file.put_line (
               fnd_file.LOG,
               'LRP site not created for Lender -- Vendor_id ' || l_vendor_id);
            fnd_file.put_line (
               fnd_file.LOG,
               'LRP site not created for Lender -- Site Name ' || v_site_name);

            UPDATE nihap_lrp_lender_load
               SET vendor_site_inserted = 'N',
                   load_status = 'E',
                   error_message = v_exception_site || l_msg
             WHERE tin_number = v_tin_num AND site_name = v_site_name;

            COMMIT;
            fnd_file.put_line (
               fnd_file.LOG,
                  'Post update of staging table setting vendor_site_inserted to N for vendor ID: '
               || l_vendor_id);
            fnd_file.put_line (fnd_file.LOG,
                               'Exception : ' || v_exception_site || l_msg);
            fnd_file.put_line (
               fnd_file.LOG,
               '***************************************************************************');
         END IF;

         DBMS_OUTPUT.put_line (
               'VENDOR SITE msg:'
            || l_msg
            || ' VENDOR SITE status AFTER UPDATE:'
            || l_return_status);
         l_return_status := NULL;
      END IF;
   EXCEPTION
      WHEN OTHERS
      THEN
         DBMS_OUTPUT.put_line ('exception');
         DBMS_OUTPUT.put_line (
               'site_id: '
            || TO_CHAR (l_vendor_site_id)
            || ' msg:'
            || l_msg
            || ' status:'
            || l_return_status);
         DBMS_OUTPUT.put_line (SUBSTR (SQLERRM, 1, 500));
   --
   END;

   PROCEDURE nihap_load_lrp_lender
   IS
      --cursor get_participant_info is
      CURSOR stage_cur
      IS
           SELECT DISTINCT vendor_name,
                           -- participant_type,
                           tin_number,
                           -- site_name,
                           -- address_line_1,
                           -- address_line_2,
                           -- city, state, zip, country,
                           load_status,
                           vendor_exists,
                           error_message,
                           creation_date,
                           -- last_update_date,
                           vendor_inserted,
                           vendor_site_inserted,
                           inactive_dt
             FROM nihap_lrp_lender_load
            WHERE load_status IN ('A', 'E') AND vendor_exists <> 'E'
         ORDER BY vendor_name;

      CURSOR vendor_site_cur (c_tin VARCHAR2)
      IS
           SELECT *
             FROM nihap_lrp_lender_load
            WHERE tin_number = c_tin
         ORDER BY tin_number, site_name;

      -- CURSOR
      v_new_error_message      VARCHAR2 (2000);
      v_user_id                NUMBER;
      v_login_id               NUMBER;
      v_vendor_id              nihpo_vendors.vendor_id%TYPE;
      v_vendor_name            nihpo_vendors.vendor_name%TYPE;
      v_start_dt               nihpo_vendors.start_date_active%TYPE;
      v_end_dt                 nihpo_vendors.end_date_active%TYPE;
      v_exception_message      VARCHAR2 (1000);
      v_vendor_site_name       VARCHAR2 (100);
      v_vendor_site_counter    VARCHAR2 (10);
      v_vendor_site_position   VARCHAR2 (10);
      v_end_date_active        VARCHAR2 (100) := NULL;
      l_api_name      CONSTANT VARCHAR2 (30) := 'Create_LRP_Vendor'; --sandeep
      l_api_version   CONSTANT NUMBER := 1.0;                        --sandeep
      l_return_status          VARCHAR2 (1);                         --sandeep
      l_msg_count              NUMBER;                               --sandeep
      l_msg_data               VARCHAR2 (1000);                      --sandeep
      l_vendor_rec             ap_vendor_pub_pkg.r_vendor_rec_type := NULL; --sandeep
      l_party_id               ap_suppliers.party_id%TYPE;          -- sandeep
      l_vendor_id              ap_suppliers.vendor_id%TYPE;          --sandeep
      l_msg                    VARCHAR2 (4000);                      --sandeep
      l_msg_index_out          NUMBER;                               --sandeep
   BEGIN
      --DBMS_APPLICATION_INFO.set_client_info ('103');--sandeep commented off
      MO_GLOBAL.SET_POLICY_CONTEXT ('S', 103);                 --sandeep added

      FOR ven_rec IN stage_cur
      LOOP
         ----insert vendor ---
         IF ven_rec.vendor_exists = 'N' AND ven_rec.vendor_inserted IS NULL
         THEN
            -- 'vendor does not exist
            fnd_file.put_line (
               fnd_file.LOG,
               '********************************************************************************');
            fnd_file.put_line (
               fnd_file.LOG,
                  'Vendor does not exist before insert a new vendor  '
               || ven_rec.tin_number);
            fnd_file.put_line (fnd_file.LOG,
                               'Lender Name : ' || ven_rec.vendor_name);
            fnd_file.put_line (fnd_file.LOG,
                               'Tin number : ' || ven_rec.tin_number);
            DBMS_OUTPUT.put_line (
                  'vendor does not exist before insert a new vendor : '
               || ven_rec.vendor_name
               || ':'
               || ven_rec.tin_number);

            BEGIN
               l_vendor_rec := NULL;
               l_return_status := NULL;                              --sandeep
               l_msg_count := NULL;                                  --sandeep
               l_msg_data := NULL;                                   --sandeep
               l_party_id := NULL;                                  -- sandeep
               l_vendor_id := NULL;                                  --sandeep
               l_msg := NULL;                                        --sandeep
               l_msg_index_out := NULL;                              --sandeep
               l_vendor_rec.vendor_name := UPPER (ven_rec.vendor_name);
               l_vendor_rec.minority_group_lookup_code := 'N';
               l_vendor_rec.women_owned_flag := 'N';
               l_vendor_rec.small_business_flag := 'N';
               l_vendor_rec.federal_reportable_flag := 'N';
               l_vendor_rec.vendor_type_lookup_code := 'LOAN REPAYMENT';
               l_vendor_rec.vendor_name_alt := UPPER (ven_rec.vendor_name);
               l_vendor_rec.auto_calculate_interest_flag := 'N';
               l_vendor_rec.attribute3 := 'MIS';
               l_vendor_rec.always_take_disc_flag := 'N';
               l_vendor_rec.jgzz_fiscal_code := ven_rec.tin_number;
               fnd_msg_pub.Initialize;
               ap_vendor_pub_pkg.create_vendor (
                  p_api_version        => l_api_version,
                  p_init_msg_list      => FND_API.G_FALSE,
                  p_commit             => FND_API.G_FALSE,
                  p_validation_level   => FND_API.G_VALID_LEVEL_FULL,
                  x_return_status      => l_return_status,
                  x_msg_count          => l_msg_count,
                  x_msg_data           => l_msg_data,
                  p_vendor_rec         => l_vendor_rec,
                  x_vendor_id          => l_vendor_id,
                  x_party_id           => l_party_id);

               IF l_return_status <> 'S'
               THEN
                  IF l_msg_count > 0
                  THEN
                     l_msg := NULL;

                     FOR l_index IN 1 .. l_msg_count
                     LOOP
                        fnd_msg_pub.get (p_msg_index       => l_index,
                                         p_encoded         => 'F',
                                         p_data            => l_msg_data,
                                         p_msg_index_out   => l_msg_index_out);
                        l_msg := l_msg || '|' || SUBSTR (l_msg_data, 1, 100);
                     END LOOP;
                  END IF;

                  Fnd_File.PUT_LINE (
                     Fnd_File.LOG,
                     'Error occurred while creating Vendor ' || l_msg);
               ELSE
                  Fnd_File.PUT_LINE (Fnd_File.LOG,
                                     'Vendor created successfully.');
               END IF;
            /*sandeep commented off
             ap_po_vendors_apis_pkg.insert_new_vendor
                            (p_vendor_name                     => UPPER
                                                                     (ven_rec.vendor_name
                                                                      --  || ':'
                                                                     --   || ven_rec.tin_number
                                                                     ),
                             p_vendor_type_lookup_code         => 'LOAN REPAYMENT',
                             p_taxpayer_id                     => ven_rec.tin_number,
                             p_tax_registration_id             => NULL,
                             p_women_owned_flag                => NULL,
                             p_small_business_flag             => NULL,
                             p_minority_group_lookup_code      => NULL,
                             p_supplier_number                 => NULL,
                             x_vendor_id                       => l_vendor_id,
                             -- out
                             x_status                          => l_status,
                             --out
                             x_exception_msg                   => l_msg
                            -- out
                            );
             DBMS_OUTPUT.put_line ('SANGEETA no excpetion');
             DBMS_OUTPUT.put_line (   ' SANGEETA msg:'
                                   || l_msg
                                   || ' status:'
                                   || l_status
                                  );
             DBMS_OUTPUT.put_line (   ' SANGEETA vendor id:'
                                   || TO_CHAR (l_vendor_id)
                                  );*/
            EXCEPTION
               WHEN OTHERS
               THEN
                  DBMS_OUTPUT.put_line ('Exception');
                  DBMS_OUTPUT.put_line (
                        'vendor_id: '
                     || TO_CHAR (l_vendor_id)
                     || ' msg:'
                     || l_msg
                     || ' status:'
                     || l_return_status);
                  DBMS_OUTPUT.put_line (SUBSTR (SQLERRM, 1, 500));
                  v_exception_message := (SUBSTR (SQLERRM, 1, 500));
            END;

            IF l_return_status = 'S'                                 -- Status
            THEN
               fnd_file.put_line (
                  fnd_file.LOG,
                  'Vendor Successfully inserted ' || ven_rec.tin_number);
               fnd_file.put_line (fnd_file.LOG,
                                  ' Vendor ID :  ' || l_vendor_id);
               ---insert _vendor_site
               DBMS_OUTPUT.put_line (
                  ' IN STATUS IF THEN UPDATE:' || l_return_status);

               UPDATE ap_suppliers v
                  SET vendor_name = UPPER (vendor_name || ':' || segment1),
                      num_1099 =
                         DECODE (v.vendor_type_lookup_code,
                                 'NIH_EMPLOYEE', NULL,
                                 ven_rec.tin_number)
                WHERE vendor_id = l_vendor_id;

               fnd_file.put_line (fnd_file.LOG,
                                  ' After Vendor update ' || l_vendor_id);

               UPDATE nihap_lrp_lender_load
                  SET vendor_inserted = 'Y' || 'vendor id:' || l_vendor_id
                WHERE     tin_number = ven_rec.tin_number
                      AND vendor_name = ven_rec.vendor_name;

               fnd_file.put_line (
                  fnd_file.LOG,
                  ' After staging table update ' || ven_rec.tin_number);
               ---vendor succesfully inserted  so calling vendor site
               fnd_file.put_line (
                  fnd_file.LOG,
                  ' Calling Vendor sites insert API' || ven_rec.tin_number);

               FOR vendor_site_rec IN vendor_site_cur (ven_rec.tin_number)
               LOOP
                  /*              nihap_insert_lender_ven_site (
                                   l_vendor_id         => l_vendor_id,
                                   tin_num             => ven_rec.tin_number,
                                   site_name           => ven_rec.site_name,
                                   address_line_1      => ven_rec.address_line_1,
                                   address_line_2      => ven_rec.address_line_2,
                                   city                => ven_rec.city,
                                   state               => ven_rec.state,
                                   zip                 => ven_rec.zip,
                                   country             => ven_rec.country   */
                  nihap_insert_lender_ven_site (
                     l_vendor_id      => l_vendor_id,
                     v_tin_num        => vendor_site_rec.tin_number,
                     v_site_name      => vendor_site_rec.site_name,
                     address_line_1   => vendor_site_rec.address_line_1,
                     address_line_2   => vendor_site_rec.address_line_2,
                     city             => vendor_site_rec.city,
                     state            => vendor_site_rec.state,
                     zip              => vendor_site_rec.zip,
                     country          => vendor_site_rec.country,
                     end_date         => ven_rec.inactive_dt);
               END LOOP;
            --l_status := null;
            -- ELSIF nvl(l_status,'F') <> 'S' THEN -- l_status
            ELSE
               fnd_file.put_line (
                  fnd_file.LOG,
                  'Error while inserting Vendor ' || ven_rec.tin_number);
               fnd_file.put_line (
                  fnd_file.LOG,
                  '********************************************************************************');

               UPDATE nihap_lrp_lender_load
                  SET vendor_inserted = NULL,
                      load_status = 'E',
                      error_message = v_exception_message || l_msg,
                      last_update_date = SYSDATE
                WHERE tin_number = ven_rec.tin_number;
            END IF;                                                  -- Status
         ELSIF        ven_rec.vendor_exists = 'Y'
                  AND ven_rec.vendor_inserted IS NULL
               OR (ven_rec.vendor_site_inserted = 'N') -- Vendor Exists (Added 04/24)
         THEN
            fnd_file.put_line (
               fnd_file.LOG,
               '********************************************************************************');
            fnd_file.put_line (fnd_file.LOG, 'Record already exists in NBS');
            fnd_file.put_line (fnd_file.LOG,
                               'Participant Name : ' || ven_rec.vendor_name);
            fnd_file.put_line (fnd_file.LOG,
                               'Tin number : ' || ven_rec.tin_number);

            SELECT vendor_id,
                   vendor_name,
                   start_date_active,
                   end_date_active
              INTO v_vendor_id,
                   v_vendor_name,
                   v_start_dt,
                   v_end_dt
              FROM nihpo_vendors
             WHERE     num_1099 = ven_rec.tin_number
                   AND NVL (end_date_active, SYSDATE + 1) > SYSDATE;

            IF NVL (v_end_dt, SYSDATE + 1) > SYSDATE
            THEN                           --insert new vendoR-site for vendor
               fnd_file.put_line (
                  fnd_file.LOG,
                  '********************************************************************************');
               fnd_file.put_line (fnd_file.LOG, 'Vendor is Active');
               fnd_file.put_line (
                  fnd_file.LOG,
                     'Calling Vendor Sites Pkg to create new LRP site : '
                  || ven_rec.vendor_name);
               fnd_file.put_line (fnd_file.LOG, 'Vendor ID ' || v_vendor_id);

               FOR vendor_site_rec IN vendor_site_cur (ven_rec.tin_number)
               LOOP
                  --                nihap_insert_vendor_site (
                  --                   l_vendor_id         => v_vendor_id,
                  --                   tin_num             => ven_rec.tin_number,
                  --                   site_name           => ven_rec.site_name,
                  --                   address_line_1      => ven_rec.address_line_1,
                  --                   address_line_2      => ven_rec.address_line_2,
                  --                   city                => ven_rec.city,
                  --                   state               => ven_rec.state,
                  --                   zip                 => ven_rec.zip,
                  --                   country             => ven_rec.country
                  nihap_insert_lender_ven_site (
                     l_vendor_id      => v_vendor_id,
                     v_tin_num        => vendor_site_rec.tin_number,
                     v_site_name      => vendor_site_rec.site_name,
                     address_line_1   => vendor_site_rec.address_line_1,
                     address_line_2   => vendor_site_rec.address_line_2,
                     city             => vendor_site_rec.city,
                     state            => vendor_site_rec.state,
                     zip              => vendor_site_rec.zip,
                     country          => vendor_site_rec.country,
                     end_date         => ven_rec.inactive_dt);
               END LOOP;
            ELSE                                                     -- End Dt
               fnd_file.put_line (fnd_file.LOG, 'Vendor is inactive');
               fnd_file.put_line (fnd_file.LOG, ' Vendor_id' || v_vendor_id);
               fnd_file.put_line (fnd_file.LOG,
                                  'Vendor_name:' || v_vendor_name);
               fnd_file.put_line (fnd_file.LOG,
                                  ' Start_date_active:' || v_start_dt);
               fnd_file.put_line (fnd_file.LOG,
                                  'end_date_active:' || v_end_dt);
               DBMS_OUTPUT.put_line (
                  'Vendor exists in Oracle and is inactive');
               DBMS_OUTPUT.put_line (' Vendor_id' || v_vendor_id);
               DBMS_OUTPUT.put_line (' Vendor_name:' || v_vendor_name);
               DBMS_OUTPUT.put_line (' Start_date_active:' || v_start_dt);
               DBMS_OUTPUT.put_line ('end_date_active:' || v_end_dt);
               DBMS_OUTPUT.put_line ('Tin Number' || ven_rec.tin_number);
               fnd_file.put_line (
                  fnd_file.LOG,
                  '********************************************************************************');
               DBMS_OUTPUT.put_line (
                  'Vendor exists in Oracle and is inactive');
               DBMS_OUTPUT.put_line (' Vendor_id' || v_vendor_id);
               DBMS_OUTPUT.put_line (' Vendor_name:' || v_vendor_name);
               DBMS_OUTPUT.put_line (' Start_date_active:' || v_start_dt);
               DBMS_OUTPUT.put_line ('end_date_active:' || v_end_dt);
               DBMS_OUTPUT.put_line ('Tin Number' || ven_rec.tin_number);
            END IF;
         --dbms_output.put_line('vendor EXIST Y=: ' || ven_rec.vendor_exists );
         --     prec.vendor_exits ='y' --get vendor id and vendor_site
         END IF;
      END LOOP;
   EXCEPTION
      WHEN OTHERS
      THEN
         DBMS_OUTPUT.put_line ('exception');
         DBMS_OUTPUT.put_line (
               'vendor_id: '
            || TO_CHAR (l_vendor_id)
            || ' msg:'
            || l_msg
            || ' status:'
            || l_return_status);
         DBMS_OUTPUT.put_line (SUBSTR (SQLERRM, 1, 500));
   END nihap_load_lrp_lender;

   --
   PROCEDURE my_debug (p_debug_text IN VARCHAR2)
   IS
   BEGIN
      IF g_debug = 1
      THEN
         fnd_file.put_line (fnd_file.LOG, p_debug_text);
      END IF;
   END my_debug;

   --
   --
   PROCEDURE create_parti_success_file (errbuf    OUT VARCHAR2,
                                        retcode   OUT NUMBER)
   IS
      --
      /**************************************************************************************
      * Purpose      : This procedure will create an output file (NIHAP_PARTI_SUCCESS.dat)  *
      *                on out directory for successfully processed participant records      *
      *                                                                                     *
      * Change History                                                                      *
      *                                                                                     *
      * Ver      Date            Author                  Description                        *
      * ------   -----------     -----------------       ---------------------------        *
      * 1.0      29-Mar-11     Narender Valaboju         Created                            *
      *                                                                                     *
      ***************************************************************************************/
      --
      l_tab              VARCHAR2 (5) := '    ';
      l_count_lines      NUMBER;
      l_effective_date   DATE;
      l_error_message    VARCHAR2 (240);
      l_errm             VARCHAR2 (240);
      l_request_id       NUMBER := NULL;
      l_file_type        UTL_FILE.file_type;
      -- l_file_path VARCHAR2(1000) := '/oraappl/od-nbs/iapprdev/iapprdevintf/ap/1.0.0/out ';
      l_file_path        VARCHAR2 (240) := NULL;
      --l_file_name     varchar2(240) := 'NIHAP_PARTI_SUCCESS_'||to_char(sysdate,'MMDDYYYY')||'.dat';
      l_file_name        VARCHAR2 (240) := 'NIHAP_PARTI_SUCCESS.dat';
      l_delimiter        VARCHAR2 (10) := ', ';
      l_header_record    VARCHAR2 (37);
      l_line_record      VARCHAR2 (2000) := NULL;
      count1             NUMBER := 0;
      count2             NUMBER := 0;
      count3             NUMBER := 0;

      --
      -- Get processed reccords
      CURSOR get_sucessess_records
      IS
         SELECT pv.attribute9, pv.vendor_id, pvs.vendor_site_id
           FROM apps.po_vendors pv, ap_supplier_sites_all pvs
          WHERE     pv.num_1099 IN
                       (SELECT st.tin_number
                          FROM nihap_lrp_participant_load st
                         WHERE     st.tin_number = pv.num_1099
                               AND st.vendor_exists != 'E')
                AND pvs.vendor_id = pv.vendor_id
                AND pvs.vendor_site_code = 'LRP_PARTICIPANT'
                AND TRUNC (pvs.last_update_date) = TRUNC (SYSDATE)
                AND NVL (pv.end_date_active, SYSDATE + 1) > SYSDATE
                AND NVL (pvs.inactive_date, SYSDATE + 1) > SYSDATE;

   --
   BEGIN
      -- Create the header record
      my_debug ('Forming the Participant Header record');
      fnd_file.put_line (fnd_file.output, '  ');
      fnd_file.put_line (
         fnd_file.output,
         '------------ Successfully Processed Reccords ----------------');
      fnd_file.put_line (fnd_file.output, '  ');
      l_request_id := fnd_global.conc_request_id;
      fnd_file.put_line (
         fnd_file.LOG,
         'Process ID for concurrent job is: ' || l_request_id);
      fnd_file.put_line (
         fnd_file.output,
            RPAD ('Commons ID', 15)
         || CHR (9)
         || RPAD ('Vendor Id', 15)
         || CHR (9)
         || RPAD ('Vendor Site Id', 15));
      --
      -- Write the header record to the output file
      my_debug ('Starting to write to file for Participant');
      --l_file_path := nih_intf_utl.get_intf_out_dir ('ap'); commented by Gouthami for 19c upgrade
      l_file_path := 'NIH_AP_OUT'; -- Added by Gouthami for 19c upgrade
      fnd_file.put_line (fnd_file.LOG, 'Data file : ' || l_file_name);
      fnd_file.put_line (fnd_file.LOG, 'Writing file to :  ' || l_file_path);
      l_file_type := UTL_FILE.fopen (l_file_path, l_file_name, 'w');

      --
      FOR c_lines_rec IN get_sucessess_records
      LOOP
         -- Create the Line Record
         count1 := count1 + 1;
         fnd_file.put_line (
            fnd_file.output,
               RPAD (NVL (c_lines_rec.attribute9, ''), 15)
            || CHR (9)
            || RPAD (NVL (TO_CHAR (c_lines_rec.vendor_id), '        '), 15)
            || CHR (9)
            || RPAD (NVL (TO_CHAR (c_lines_rec.vendor_site_id), ''), 15));
         l_line_record :=
               LTRIM (RTRIM (c_lines_rec.attribute9))
            || '|'
            || LTRIM (RTRIM (TO_CHAR (c_lines_rec.vendor_id)))
            || '|'
            || LTRIM (RTRIM (TO_CHAR (c_lines_rec.vendor_site_id)));
         --
         -- Write the line record to the output file
         UTL_FILE.put_line (l_file_type, l_line_record);
      --
      END LOOP;                                                 -- c_lines_rec

      --
      -- Close the file
      fnd_file.put_line (fnd_file.LOG,
                         'Finished writing ' || count1 || ' records to file');
      --utl_file.fflush(l_file_type);
      UTL_FILE.fclose (l_file_type);
      --
      fnd_file.put_line (fnd_file.output, '  ');
      fnd_file.put_line (fnd_file.output,
                         'Total No Of Participant Processed: ' || count1);
      fnd_file.put_line (
         fnd_file.LOG,
         'Closing data file and ending process --- ' || l_file_name);
   --
   EXCEPTION
      WHEN NO_DATA_FOUND
      THEN
         fnd_file.put_line (fnd_file.LOG,
                            'No data found, PARTICIPANT DATA.  Error!!');
         retcode := 2;
         errbuf := SQLERRM;
      WHEN UTL_FILE.invalid_path
      THEN
         fnd_file.put_line (fnd_file.LOG, 'UTL_FILE Invalid Path Error');
         retcode := 2;
         errbuf := SQLERRM;
      WHEN UTL_FILE.invalid_mode
      THEN
         fnd_file.put_line (fnd_file.LOG, 'UTL_FILE Invalid Mode Error');
         retcode := 2;
         errbuf := SQLERRM;
      WHEN UTL_FILE.invalid_operation
      THEN
         fnd_file.put_line (fnd_file.LOG, 'UTL_FILE Invalid Operation Error');
         retcode := 2;
         errbuf := SQLERRM;
      WHEN UTL_FILE.invalid_filehandle
      THEN
         fnd_file.put_line (fnd_file.LOG,
                            'UTL_FILE Invalid Filehandle Error');
         retcode := 2;
         errbuf := SQLERRM;
      WHEN UTL_FILE.write_error
      THEN
         fnd_file.put_line (fnd_file.LOG, 'UTL_FILE Write Error');
         retcode := 2;
         errbuf := SQLERRM;
      WHEN UTL_FILE.internal_error
      THEN
         fnd_file.put_line (fnd_file.LOG, 'UTL_FILE Internal Error');
         retcode := 2;
         errbuf := SQLERRM;
      WHEN OTHERS
      THEN
         fnd_file.put_line (fnd_file.LOG, 'other sql error' || SQLERRM);
         l_errm := SQLERRM;
         errbuf := SQLERRM;
         retcode := 2;
   END create_parti_success_file;

   --
   --
   PROCEDURE create_parti_error_file (errbuf    OUT VARCHAR2,
                                      retcode   OUT NUMBER)
   IS
      --
      /**************************************************************************************
      * Purpose      : This procedure will create an output file (NIHAP_PARTI_ERROR.dat)    *
      *                on out directory for Participant Error out records                   *
      *                                                                                     *
      * Change History                                                                      *
      *                                                                                     *
      * Ver      Date            Author                  Description                        *
      * ------   -----------     -----------------       ---------------------------        *
      * 1.0      29-Mar-11     Narender Valaboju         Created                            *
      *                                                                                     *
      ***************************************************************************************/
      --
      l_tab              VARCHAR2 (5) := '    ';
      l_count_lines      NUMBER;
      l_effective_date   DATE;
      l_error_message    VARCHAR2 (1000);
      l_request_id       NUMBER;
      op                 VARCHAR2 (3) := 'w';
      l_process_id       NUMBER;
      l_errm             VARCHAR2 (240);
      l_desc             VARCHAR2 (240);
      l_err_count        NUMBER := 0;
      l_file_type        UTL_FILE.file_type;
      l_file_path        VARCHAR2 (1000) := NULL;
      l_delimiter        VARCHAR2 (10) := ', ';
      l_header_record    VARCHAR2 (37);
      l_line_record      VARCHAR2 (1000) := NULL;
      --l_file_name   varchar2(1000) := 'NIHAP_PARTI_ERROR_'||to_char(sysdate,'MMDDYYYY')||'.dat';
      l_file_name        VARCHAR2 (1000) := 'NIHAP_PARTI_ERROR.dat';
      count1             NUMBER := 0;                -- number of recs written
      count2             NUMBER := 0;
      count3             NUMBER := 0;

      --
      -- Get processed reccords
      CURSOR get_error_records
      IS
         SELECT a.*
           FROM nihap_lrp_participant_load a
          WHERE    (a.load_status IN ('E', 'I'))   ----CR 42642 added I clause
                OR     (a.vendor_exists = 'E')
                   AND TRUNC (a.last_update_date) = TRUNC (SYSDATE);

   --
   BEGIN
      -- Create the header record
      my_debug ('Forming the Participant Header record');
      fnd_file.put_line (fnd_file.output, '  ');
      fnd_file.put_line (
         fnd_file.output,
         '  ---------- Participant error out records  -----------------');
      fnd_file.put_line (fnd_file.output, '  ');
      l_err_count := 0;
      l_request_id := fnd_global.conc_request_id;
      fnd_file.put_line (
         fnd_file.LOG,
         'Process ID for concurrent job is: ' || l_request_id);
      --l_file_path := nih_intf_utl.get_intf_out_dir ('ap'); commented by Gouthami for 19c upgrade
      l_file_path := 'NIH_AP_OUT'; -- Added by Gouthami for 19c upgrade
      l_file_type := UTL_FILE.fopen (l_file_path, l_file_name, op);
      fnd_file.put_line (fnd_file.LOG, 'Data file  :' || l_file_name);
      fnd_file.put_line (fnd_file.LOG, 'Writing file to :  ' || l_file_path);
      fnd_file.put_line (fnd_file.LOG, 'opened for write');
      -- Write the header record to the output file
      my_debug ('Starting to write to file for Participant');
      fnd_file.put_line (
         fnd_file.output,
         'Vendor Name' || ', ' || 'Commons ID' || ', ' || 'Error Message');
      fnd_file.put_line (fnd_file.output, '  ');

      FOR c_lines_rec IN get_error_records
      LOOP
         fnd_file.put_line (
            fnd_file.output,
               c_lines_rec.vendor_name
            || ', '
            || c_lines_rec.commons_id
            || ', '
            || c_lines_rec.error_message);
         UTL_FILE.put_line (
            l_file_type,
               c_lines_rec.vendor_name
            || '|'
            || c_lines_rec.commons_id
            || '|'
            || c_lines_rec.error_message);
         count1 := count1 + 1;
      END LOOP;

      fnd_file.put_line (fnd_file.LOG,
                         'Finished writing ' || count1 || ' records to file');
      UTL_FILE.fclose (l_file_type);
      fnd_file.put_line (
         fnd_file.LOG,
         'Closing data file and ending process --- ' || l_file_name);
   --
   EXCEPTION
      WHEN NO_DATA_FOUND
      THEN
         fnd_file.put_line (fnd_file.LOG,
                            'No data found, PARTICIPANT DATA.  Error!!');
         retcode := 2;
         errbuf := SQLERRM;
      WHEN UTL_FILE.invalid_path
      THEN
         fnd_file.put_line (fnd_file.LOG, 'UTL_FILE Invalid Path Error');
         retcode := 2;
         errbuf := SQLERRM;
      WHEN UTL_FILE.invalid_mode
      THEN
         fnd_file.put_line (fnd_file.LOG, 'UTL_FILE Invalid Mode Error');
         retcode := 2;
         errbuf := SQLERRM;
      WHEN UTL_FILE.invalid_operation
      THEN
         fnd_file.put_line (fnd_file.LOG, 'UTL_FILE Invalid Operation Error');
         retcode := 2;
         errbuf := SQLERRM;
      WHEN UTL_FILE.invalid_filehandle
      THEN
         fnd_file.put_line (fnd_file.LOG,
                            'UTL_FILE Invalid Filehandle Error');
         retcode := 2;
         errbuf := SQLERRM;
      WHEN UTL_FILE.write_error
      THEN
         fnd_file.put_line (fnd_file.LOG, 'UTL_FILE Write Error');
         retcode := 2;
         errbuf := SQLERRM;
      WHEN UTL_FILE.internal_error
      THEN
         fnd_file.put_line (fnd_file.LOG, 'UTL_FILE Internal Error');
         retcode := 2;
         errbuf := SQLERRM;
      WHEN OTHERS
      THEN
         fnd_file.put_line (fnd_file.LOG, 'other sql error' || SQLERRM);
         l_errm := SQLERRM;
         errbuf := SQLERRM;
         retcode := 2;
   END create_parti_error_file;

   --
   --
   PROCEDURE update_parti_hist_tbl (errbuf OUT VARCHAR2, retcode OUT NUMBER)
   IS
      --
      /****************************************************************************************
      * Purpose : This procedure will update the History table(NIHAP_LRP_PARTICIPANT_LOAD_HST)*
      *           for successfully processed Participant records                              *
      *                                                                                       *
      * Change History                                                                        *
      *                                                                                       *
      * Ver      Date            Author                  Description                          *
      * ------   -----------     -----------------       ---------------------------          *
      * 1.0      29-Mar-11     Narender Valaboju         Created                              *
      *                                                                                       *
      ***************************************************************************************/
      --
      l_error_message   VARCHAR2 (1000);
      l_request_id      NUMBER;
      l_process_id      NUMBER;
      l_desc            VARCHAR2 (110);
      l_count           NUMBER := 0;
   --
   --
   BEGIN
      my_debug ('Inserting into History Table');

      -- Check the counts for Processed and Error data and Arvhive --
      -- LRP sends will re-submit error records --
      SELECT COUNT (*)
        INTO l_count
        FROM nihap_lrp_participant_load
       WHERE ( (load_status IN ('P', 'E', 'I')) OR (vendor_exists = 'E')); ----CR 42642 Added I clause

      -- Archive data --
      IF l_count > 0
      THEN
         INSERT INTO nihap_lrp_participant_load_hst
            SELECT a.*
              FROM nihap_lrp_participant_load a
             WHERE (   (load_status IN ('P', 'E', 'I')) ----CR 42642 Added I clause
                    OR (vendor_exists = 'E'));

         fnd_file.put_line (
            fnd_file.LOG,
            'LRP Participant Records inserted in archive table: ' || l_count);

         DELETE FROM nihap_lrp_participant_load
               WHERE (   (load_status IN ('P', 'E', 'I')) ----CR 42642 Added I clause
                      OR (vendor_exists = 'E'));                     -- change
      END IF;

      COMMIT;
   EXCEPTION
      WHEN OTHERS
      THEN
         fnd_file.put_line (
            fnd_file.LOG,
            'Error in Updating History Table: ' || SUBSTR (SQLERRM, 1, 150));
         retcode := 2;
   END update_parti_hist_tbl;
--
END nihap_participant_lender_pkg;