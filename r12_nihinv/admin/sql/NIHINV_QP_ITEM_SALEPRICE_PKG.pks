CREATE OR REPLACE PACKAGE NIHINV_QP_ITEM_SALEPRICE_PKG
AUTHID CURRENT_USER IS
/******************************************************************************
-- FILE NAME:  NIHINV_QP_ITEM_SALEPRICE_PKG.pkb
-- PURPOSE: This Package will be used to insert the Item sale price with Item details into table nihinv_qp_item_saleprice
--          It will be used by nVision team for reporting purpose.This package will be called from conurrent program NIHINV Populate QP Sale Price
--      
-- REVISIONS:
-- Ver        Date        Author            Change Description
-- ---------  ----------  ---------------  ------------------------------------
-- 1.0        07/22/2025  RemedyBiz         Initial Creation - CR# NBSCH0003054-PRB0028468
--                                            
******************************************************************************/
PROCEDURE insert_Price_list( 
    p_errbuf              OUT VARCHAR2
   ,p_retcode             OUT VARCHAR2
   );
END NIHINV_QP_ITEM_SALEPRICE_PKG;
