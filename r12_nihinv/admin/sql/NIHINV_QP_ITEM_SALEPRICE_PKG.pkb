CREATE OR REPLACE PACKAGE BODY NIHINV_QP_ITEM_SALEPRICE_PKG
AS
--/*===================================================================================================================
-- FILE NAME: NIHINV_QP_ITEM_SALEPRICE_PKG.pkb
-- PURPOSE  : This Package consists of program units used to populate the Item sale price with Item details into a 
--            custom table nihinv_qp_item_saleprice It will be by nVision team for reporting purpose.
--            The package procedure insert_Price_list defined as a conurrent program  'NIHINV Populate QP Sale Price' 
--            in ONBS EBS.
--
-- DEPLOYMENT NOTES: Execute this SQL script from APPS schema in NBS EBS database in SQL window or FlexDeploy release.
--
-- CHANGE HISTORY:
-- Date         Author                Change Description
-- -----------  --------------------  -------------------------------------------------------------------------------
-- 07/22/2025   Ravi R                Initial Creation - CR# NBSCH0003054-PRB0028468
--                                            
--  ================================================================================================================= */
--------------------------------------- 
-- Package global variables --
--------------------------------------- 
g_request_id   NUMBER := FND_GLOBAL.CONC_REQUEST_ID;   
g_user_id      NUMBER := FND_PROFILE.VALUE ('USER_ID');
g_bulkcnt      CONSTANT PLS_INTEGER := 500;
--
---------------------------------------
-- Program Units --
---------------------------------------
PROCEDURE insert_Price_list (
          p_errbuf             OUT VARCHAR2,
          p_retcode            OUT VARCHAR2
--
) IS
  CURSOR price_list_cur IS 
  SELECT msi.inventory_item_id,
         msi.organization_id,
         msi.segment1 item_number,
         msi.description item_description,
         msi.inventory_item_status_code,
         msi.unit_of_issue,
         msi.primary_uom_code,
         nihqp_nihsc_pricing_pkg.get_itemsale_price (inventory_item_id, 477, TRUNC (SYSDATE)) qp_sale_price,
         nihqp_nihsc_pricing_pkg.get_qp_sale_price (inventory_item_id,
         477,
         TRUNC (SYSDATE),
         primary_uom_code) qp_final_sale_price,
         GREATEST (qpp.last_update_date, msi.last_update_date) item_qp_last_update_date,  
         msi.last_updated_by,
         msi.creation_date,
         msi.created_by   
    FROM mtl_system_items_b msi,
         ( SELECT qpa.product_attr_value, 
                  MAX (GREATEST (qpa.last_update_date, qpl.last_update_date)) last_update_date
             FROM qp_list_lines qpl, 
                  qp_pricing_attributes qpa
            WHERE qpa.list_line_id = qpl.list_line_id 
              AND qpa.product_attribute = 'PRICING_ATTRIBUTE1'
         GROUP BY qpa.product_attr_value) qpp
   WHERE msi.organization_id = 477 
     AND qpp.product_attr_value(+) = TO_CHAR (msi.inventory_item_id)
     AND msi.inventory_item_status_code NOT IN ('OBSOLETE','Inactive','DISCONT''D') 
  ;
  --
  TYPE price_list_cur_type IS TABLE OF price_list_cur%ROWTYPE;
  price_list_rec  price_list_cur_type;
  --      
  l_position     VARCHAR2(100) := NULL;
  l_recnt        PLS_INTEGER := 0;
  l_delnt        PLS_INTEGER := 0;
  --
BEGIN
   p_errbuf   := NULL;
   p_retcode  := 0;
   l_position := 'Before deleting nihinv_qp_item_saleprice records ';
   --
   DELETE FROM nihinv_qp_item_saleprice;
   --
   l_delnt    := SQL%ROWCOUNT;
   l_position := 'Total records deleted from nihinv_qp_item_saleprice: ' ||l_delnt;
   fnd_file.put_line (fnd_file.LOG, l_position);
   l_position :=  'Start Insert to nihinv_qp_item_saleprice ';            
   OPEN price_list_cur; 
   LOOP
        FETCH price_list_cur
        BULK COLLECT INTO price_list_rec
        LIMIT g_bulkcnt;
        l_position := 'Bulk collect ';
        IF price_list_rec.COUNT > 0
        THEN
           l_position := ' Price list record count: '||price_list_rec.COUNT;
           fnd_file.put_line (fnd_file.LOG, l_position);
           FOR i IN 1 .. price_list_rec.COUNT
           LOOP 
              l_position := 'Bulk collect2 ';
              -- 
              INSERT INTO nihinv_qp_item_saleprice
              (inventory_item_id         
              ,organization_id           
              ,item_number               
              ,item_description          
              ,inventory_item_status_code
              ,unit_of_issue             
              ,primary_uom_code
              ,qp_sale_price             
              ,qp_final_sale_price 
              ,item_qp_last_update_date
              ,last_update_date
              ,last_updated_by           
              ,creation_date             
              ,created_by                
              ,request_id 
              ,request_date   
              ) VALUES (
              price_list_rec(i).inventory_item_id 
              ,price_list_rec(i).organization_id           
              ,price_list_rec(i).item_number               
              ,price_list_rec(i).item_description          
              ,price_list_rec(i).inventory_item_status_code
              ,price_list_rec(i).unit_of_issue             
              ,price_list_rec(i).primary_uom_code
              ,price_list_rec(i).qp_sale_price             
              ,price_list_rec(i).qp_final_sale_price
              ,price_list_rec(i).item_qp_last_update_date
              ,SYSDATE   
              ,g_user_id           
              ,SYSDATE             
              ,g_user_id  
              ,g_request_id
              ,SYSDATE        
              ) ;
              l_recnt := l_recnt + SQL%ROWCOUNT;           
              --
           END LOOP;        
        END IF;
        EXIT WHEN price_list_cur%NOTFOUND;
    END LOOP;   
    CLOSE price_list_cur;
    fnd_file.put_line (fnd_file.LOG, 'Total number of records created :'||l_recnt);
    IF l_recnt > 0 THEN
       COMMIT;
       l_position := 'Commit completed.';
    ELSE   
       ROLLBACK;  
       p_retcode  := 1;
       l_position := 'Warning! No records created in nihinv_qp_item_saleprice.';      
       p_errbuf   := l_position;       
    END IF;  
    fnd_file.put_line (fnd_file.LOG, l_position);
EXCEPTION 
    WHEN OTHERS THEN  
       fnd_file.put_line (fnd_file.LOG, 'Error in insertion procedure insert_Price_list. Error'||SUBSTR(SQLERRM,1,200));  
       ROLLBACK;
       p_errbuf := TRIM(l_position|| ' Error in insertion procedure insert_Price_list -'||SUBSTR(sqlerrm,1,200) );
       p_retcode := 2;
END insert_Price_list;
--
--
END NIHINV_QP_ITEM_SALEPRICE_PKG;
/